//! LLM-assisted entity refinement for `mempalace init`.
//!
//! Takes a [`DetectedDict`] (output of `project_scanner::discover_entities`) and
//! a corpus string (prose text from the project directory), sends batches of
//! candidate entity names to an [`LlmProvider`], and returns a refined
//! [`DetectedDict`] with false positives dropped and misclassified entities
//! reclassified.
//!
//! Entry point: [`refine_entities`].
//! Corpus collector: [`collect_corpus_text`].

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;

use serde_json::Value;

use crate::palace::entities::DetectedEntity;
use crate::palace::project_scanner::DetectedDict;

use super::client::LlmProvider;

// Entities per LLM call — balances prompt length against round-trip count.
const BATCH_SIZE: usize = 25;

// Corpus byte cap — bounds memory for context snippet lookup.
const MAX_CORPUS_BYTES: usize = 500_000;

// Context snippets shown per entity in the user prompt.
const MAX_CONTEXT_SNIPPETS: usize = 3;

// Directory depth limit for the corpus walk.
const CORPUS_WALK_DEPTH_LIMIT: usize = 10;

const _: () = assert!(BATCH_SIZE > 0);
const _: () = assert!(MAX_CORPUS_BYTES > 0);
const _: () = assert!(CORPUS_WALK_DEPTH_LIMIT > 0);
const _: () = assert!(MAX_CONTEXT_SNIPPETS > 0);

// Labels the LLM may return; anything else is discarded.
const VALID_LABELS: &[&str] = &["person", "project", "drop"];

const SYSTEM_PROMPT: &str = "\
You are a named entity classifier for software projects.

Given candidates extracted from project files and git history, classify each entity.

Respond ONLY with a JSON object in this exact format:
{
  \"decisions\": {
    \"EntityName\": {\"label\": \"person|project|drop\", \"reason\": \"brief reason\"}
  }
}

Labels:
- person: a real human name (developer, author, contributor, researcher)
- project: a software project, library, tool, product, technology, or brand
- drop: false positive, common word, generic term, URL fragment, or number
";

// ===================== PUBLIC TYPES =====================

/// Output of [`refine_entities`] — refined entities with processing statistics.
pub struct RefineResult {
    /// The refined entity dictionary.
    pub merged: DetectedDict,
    /// Entities moved to a different category by the LLM.
    pub reclassified: usize,
    /// Entities removed as false positives by the LLM.
    pub dropped: usize,
    /// Batches that produced an LLM or parse error.
    pub errors: usize,
    /// Batches successfully sent and parsed.
    pub batches_completed: usize,
    /// Total batch count (`batches_completed + errors`).
    pub batches_total: usize,
    /// Always `false` — Ctrl-C handling is deferred to a future version.
    // Reserved for future Ctrl-C support; currently always `false` and not read by callers.
    #[allow(dead_code)]
    pub cancelled: bool,
}

// ===================== PUBLIC API =====================

/// Refine a [`DetectedDict`] using an LLM to drop false positives and reclassify entities.
///
/// Sends candidates in batches of [`BATCH_SIZE`] to `provider.classify`, parses
/// the JSON decisions, and applies them to `detected`. Returns a [`RefineResult`]
/// with the refined dict and processing statistics.
pub fn refine_entities(
    detected: DetectedDict,
    corpus: &str,
    provider: &dyn LlmProvider,
) -> RefineResult {
    let candidates = refine_entities_collect_candidates(&detected);
    let batches_total = candidates.len().div_ceil(BATCH_SIZE);
    assert!(batches_total <= candidates.len() + 1);

    let corpus_lines: Vec<&str> = corpus.lines().collect();
    let mut all_decisions: HashMap<String, (String, String)> = HashMap::new();
    let mut batches_completed: usize = 0;
    let mut errors: usize = 0;

    for batch in candidates.chunks(BATCH_SIZE) {
        match refine_entities_batch(batch, &corpus_lines, provider) {
            Some(decisions) => {
                all_decisions.extend(decisions);
                batches_completed += 1;
            }
            None => errors += 1,
        }
    }

    assert!(batches_completed + errors == batches_total);

    let (merged, reclassified, dropped) = apply_classifications(detected, &all_decisions);
    RefineResult {
        merged,
        reclassified,
        dropped,
        errors,
        batches_completed,
        batches_total,
        cancelled: false,
    }
}

/// Walk `dir` for prose files (`.md`, `.txt`, `.rst`) and return concatenated text.
///
/// Caps output at [`MAX_CORPUS_BYTES`] and limits recursion via
/// [`CORPUS_WALK_DEPTH_LIMIT`]. Returns an empty string when `dir` is unreadable.
pub fn collect_corpus_text(root_dir: &Path) -> String {
    let prose_extensions: &[&str] = &["md", "txt", "rst"];
    let mut stack: Vec<(std::path::PathBuf, usize)> = vec![(root_dir.to_path_buf(), 0)];
    let mut text = String::new();

    while let Some((current_dir, depth)) = stack.pop() {
        assert!(depth <= CORPUS_WALK_DEPTH_LIMIT);
        if depth >= CORPUS_WALK_DEPTH_LIMIT || text.len() >= MAX_CORPUS_BYTES {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&current_dir) else {
            continue;
        };
        collect_corpus_text_process_entries(
            entries,
            prose_extensions,
            &mut stack,
            &mut text,
            depth,
        );
    }

    debug_assert!(
        text.len() <= MAX_CORPUS_BYTES,
        "corpus must not exceed byte limit"
    );
    text
}

// ===================== PRIVATE HELPERS =====================

/// Collect `(name, entity_type)` pairs from all three categories of `detected`.
///
/// Called by [`refine_entities`] to build the batching input.
fn refine_entities_collect_candidates(detected: &DetectedDict) -> Vec<(&str, &str)> {
    let total = detected.people.len() + detected.projects.len() + detected.uncertain.len();
    assert!(total < 1_000_000, "entity count must be sane");
    let mut candidates: Vec<(&str, &str)> = Vec::with_capacity(total);

    for entity in &detected.people {
        candidates.push((&entity.name, &entity.entity_type));
    }
    for entity in &detected.projects {
        candidates.push((&entity.name, &entity.entity_type));
    }
    for entity in &detected.uncertain {
        candidates.push((&entity.name, &entity.entity_type));
    }

    assert!(candidates.len() == total);
    candidates
}

/// Send one batch to the LLM and parse its decisions.
///
/// Returns `Some(decisions)` on success (map may be empty if the LLM drops all),
/// or `None` when the LLM call or JSON parsing fails.
/// Called by [`refine_entities`] for each chunk.
fn refine_entities_batch(
    batch: &[(&str, &str)],
    corpus_lines: &[&str],
    provider: &dyn LlmProvider,
) -> Option<HashMap<String, (String, String)>> {
    assert!(!batch.is_empty());
    assert!(batch.len() <= BATCH_SIZE);

    let user_prompt = build_user_prompt(batch, corpus_lines);
    let response = provider.classify(SYSTEM_PROMPT, &user_prompt, true).ok()?;

    let expected_names: Vec<&str> = batch.iter().map(|(name, _)| *name).collect();
    assert!(!expected_names.is_empty());
    parse_response(&response.text, &expected_names)
}

/// Build the user prompt for one batch.
///
/// Lists each candidate with its detected type and corpus context snippets.
/// Called by [`refine_entities_batch`].
fn build_user_prompt(batch: &[(&str, &str)], corpus_lines: &[&str]) -> String {
    assert!(!batch.is_empty());
    assert!(batch.len() <= BATCH_SIZE);

    let mut prompt = String::from("Classify the following entities:\n\n");

    for (index, (name, entity_type)) in batch.iter().enumerate() {
        let contexts = collect_contexts(corpus_lines, name);
        // `writeln!` to an owned String never fails (infallible Write impl).
        let _ = writeln!(
            prompt,
            "{}. \"{}\" (detected: {})",
            index + 1,
            name,
            entity_type
        );
        for context in &contexts {
            let _ = writeln!(prompt, "   Context: {context}");
        }
        prompt.push('\n');
    }

    prompt.push_str("Respond with JSON decisions for ALL of the above entities.");

    assert!(!prompt.is_empty());
    assert!(
        prompt.contains("decisions"),
        "prompt must reference the required JSON key"
    );
    prompt
}

/// Return up to [`MAX_CONTEXT_SNIPPETS`] trimmed lines from `corpus_lines` containing `name`.
///
/// Matching is case-insensitive. Called by [`build_user_prompt`].
fn collect_contexts(corpus_lines: &[&str], name: &str) -> Vec<String> {
    assert!(!name.is_empty());

    let name_lower = name.to_lowercase();
    let mut contexts: Vec<String> = Vec::new();

    for line in corpus_lines {
        if contexts.len() >= MAX_CONTEXT_SNIPPETS {
            break;
        }
        if line.to_lowercase().contains(&name_lower) {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                contexts.push(trimmed.to_string());
            }
        }
    }

    assert!(contexts.len() <= MAX_CONTEXT_SNIPPETS);
    contexts
}

/// Extract a JSON block from `text` — handles markdown fences and raw brace pairs.
///
/// Tries ```` ```json ```` fences first, then bare ```` ``` ```` fences, then
/// brace-depth matching. Returns `None` when no JSON structure is found.
/// Called by [`parse_response`].
fn extract_json_block(text: &str) -> Option<&str> {
    assert!(!text.is_empty());

    // 1. Prefer an explicit ```json ... ``` fence.
    if let Some(fence_start) = text.find("```json") {
        let content_start = fence_start + 7;
        if let Some(fence_len) = text[content_start..].find("```") {
            return Some(text[content_start..content_start + fence_len].trim());
        }
    }

    // 2. Fall back to a generic ``` ... ``` fence whose content starts with '{'.
    if let Some(fence_start) = text.find("```") {
        let content_start = fence_start + 3;
        if let Some(fence_len) = text[content_start..].find("```") {
            let candidate = text[content_start..content_start + fence_len].trim();
            if candidate.starts_with('{') {
                return Some(candidate);
            }
        }
    }

    // 3. Match the outermost brace pair by depth-counting.
    extract_json_block_by_braces(text)
}

/// Scan `text` for the outermost `{...}` block using brace-depth counting.
///
/// No recursion — uses a `usize` depth counter. Called by [`extract_json_block`]
/// as a last resort.
fn extract_json_block_by_braces(text: &str) -> Option<&str> {
    assert!(!text.is_empty());

    let brace_start = text.find('{')?;
    let mut depth: usize = 0;

    for (idx, ch) in text[brace_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    break; // malformed: unmatched closing brace
                }
                depth -= 1;
                if depth == 0 {
                    let end = brace_start + idx + 1;
                    assert!(end <= text.len());
                    return Some(text[brace_start..end].trim());
                }
            }
            _ => {}
        }
    }

    None
}

/// Parse an LLM response into a name → `(label, reason)` map.
///
/// Extracts a JSON block, validates each label against [`VALID_LABELS`], and
/// filters to entries in `expected_names` (case-insensitive). Returns `None` when
/// no valid JSON block is found. Called by [`refine_entities_batch`].
fn parse_response(
    text: &str,
    expected_names: &[&str],
) -> Option<HashMap<String, (String, String)>> {
    assert!(!text.is_empty());
    assert!(!expected_names.is_empty());

    let json_text = extract_json_block(text)?;
    let data: Value = serde_json::from_str(json_text).ok()?;
    let decisions_obj = data.get("decisions").and_then(Value::as_object)?;

    let mut decisions: HashMap<String, (String, String)> = HashMap::new();
    for (name, value) in decisions_obj {
        let is_expected = expected_names.iter().any(|n| n.eq_ignore_ascii_case(name));
        if !is_expected {
            continue;
        }
        let label = value
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !VALID_LABELS.contains(&label) {
            continue;
        }
        let reason = value
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or_default();
        decisions.insert(name.clone(), (label.to_string(), reason.to_string()));
    }

    assert!(decisions.len() <= expected_names.len());
    Some(decisions)
}

/// Apply LLM `decisions` to `detected`, routing each entity to its new category.
///
/// Returns `(refined_dict, reclassified_count, dropped_count)`. Entities without
/// a decision are retained in their original category.
/// Called by [`refine_entities`].
fn apply_classifications(
    detected: DetectedDict,
    decisions: &HashMap<String, (String, String)>,
) -> (DetectedDict, usize, usize) {
    let total_input = detected.people.len() + detected.projects.len() + detected.uncertain.len();
    assert!(total_input < 1_000_000, "entity count must be sane");
    assert!(decisions.len() < 1_000_000, "decisions count must be sane");

    let mut people: Vec<DetectedEntity> = Vec::new();
    let mut projects: Vec<DetectedEntity> = Vec::new();
    let mut uncertain: Vec<DetectedEntity> = Vec::new();
    let mut reclassified: usize = 0;
    let mut dropped: usize = 0;

    let all_entities = detected
        .people
        .into_iter()
        .chain(detected.projects)
        .chain(detected.uncertain);

    for entity in all_entities {
        apply_classifications_route_entity(
            entity,
            decisions,
            &mut people,
            &mut projects,
            &mut uncertain,
            &mut reclassified,
            &mut dropped,
        );
    }

    let total_output = people.len() + projects.len() + uncertain.len();
    // Pair assertion: every input entity was either kept or dropped.
    debug_assert!(
        total_output + dropped == total_input,
        "entity count must balance: {total_output} kept + {dropped} dropped != {total_input} input"
    );

    (
        DetectedDict {
            people,
            projects,
            uncertain,
        },
        reclassified,
        dropped,
    )
}

/// Route a single entity to the correct output bucket based on the LLM decision.
///
/// Entities without a matching decision retain their original category.
/// Called by [`apply_classifications`] for every entity in the input dict.
fn apply_classifications_route_entity(
    entity: DetectedEntity,
    decisions: &HashMap<String, (String, String)>,
    people: &mut Vec<DetectedEntity>,
    projects: &mut Vec<DetectedEntity>,
    uncertain: &mut Vec<DetectedEntity>,
    reclassified: &mut usize,
    dropped: &mut usize,
) {
    let original_type = entity.entity_type.as_str();
    let label = decisions
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(&entity.name))
        .map_or(original_type, |(_, (label, _))| label.as_str());

    if label == "drop" {
        *dropped += 1;
        return;
    }
    if label != original_type {
        *reclassified += 1;
    }
    match label {
        "person" => people.push(entity),
        "project" => projects.push(entity),
        _ => uncertain.push(entity),
    }
}

/// Process one directory's `ReadDir` entries for corpus collection.
///
/// Queues subdirectories onto `stack` and appends prose file content to `text`.
/// Called by [`collect_corpus_text`] inside the walk loop.
fn collect_corpus_text_process_entries(
    entries: std::fs::ReadDir,
    prose_extensions: &[&str],
    stack: &mut Vec<(std::path::PathBuf, usize)>,
    text: &mut String,
    depth: usize,
) {
    assert!(depth < CORPUS_WALK_DEPTH_LIMIT);
    assert!(!prose_extensions.is_empty());

    for entry in entries.flatten() {
        if text.len() >= MAX_CORPUS_BYTES {
            break;
        }
        // Use DirEntry::file_type() which does NOT follow symlinks, preventing
        // corpus collection from escaping the project tree via symlinked paths.
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            stack.push((path, depth + 1));
        } else if file_type.is_file() {
            collect_corpus_text_read_file(&path, prose_extensions, text);
        }
        // Symlinks are silently skipped.
    }
}

/// Read a prose file and append its content to `text` if it fits within the byte cap.
///
/// Skips files without a recognized prose extension and files that would exceed
/// the remaining quota (to avoid mid-character UTF-8 truncation).
/// Called by [`collect_corpus_text_process_entries`].
fn collect_corpus_text_read_file(path: &Path, prose_extensions: &[&str], text: &mut String) {
    assert!(text.len() <= MAX_CORPUS_BYTES);
    assert!(!prose_extensions.is_empty());

    let has_prose_ext = path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|file_ext| prose_extensions.contains(&file_ext));
    if !has_prose_ext {
        return;
    }
    let Ok(content) = std::fs::read_to_string(path) else {
        return;
    };
    let remaining = MAX_CORPUS_BYTES - text.len();
    if content.len() <= remaining {
        text.push_str(&content);
    }
    // Files that exceed the remaining quota are skipped to preserve UTF-8 safety.
}

// ===================== TESTS =====================

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::palace::entities::DetectedEntity;
    use crate::palace::project_scanner::DetectedDict;

    fn make_entity(name: &str, entity_type: &str) -> DetectedEntity {
        DetectedEntity {
            name: name.to_string(),
            entity_type: entity_type.to_string(),
            confidence: 0.9,
            frequency: 5,
            signals: vec![],
        }
    }

    fn make_dict(people: Vec<&str>, projects: Vec<&str>, uncertain: Vec<&str>) -> DetectedDict {
        DetectedDict {
            people: people
                .into_iter()
                .map(|n| make_entity(n, "person"))
                .collect(),
            projects: projects
                .into_iter()
                .map(|n| make_entity(n, "project"))
                .collect(),
            uncertain: uncertain
                .into_iter()
                .map(|n| make_entity(n, "uncertain"))
                .collect(),
        }
    }

    // -- extract_json_block --

    #[test]
    fn extract_json_block_finds_json_fence() {
        // A ```json ... ``` fence must be extracted and trimmed.
        let text = "Here is the result:\n```json\n{\"decisions\":{}}\n```\nDone.";
        let block = extract_json_block(text).expect("must find json fence");
        assert!(block.starts_with('{'));
        assert!(block.contains("decisions"));
    }

    #[test]
    fn extract_json_block_finds_bare_fence_with_json() {
        // A ``` ... ``` fence whose content starts with '{' must be extracted.
        let text = "Result:\n```\n{\"decisions\":{}}\n```";
        let block = extract_json_block(text).expect("must find bare fence");
        assert!(block.starts_with('{'));
        assert!(!block.is_empty());
    }

    #[test]
    fn extract_json_block_finds_raw_brace_pair() {
        // Raw JSON with no fence must be found by brace-depth matching.
        let text = "Sure! {\"decisions\":{\"Alice\":{\"label\":\"person\",\"reason\":\"dev\"}}}";
        let block = extract_json_block(text).expect("must find raw braces");
        assert!(block.starts_with('{'));
        assert!(block.ends_with('}'));
    }

    #[test]
    fn extract_json_block_returns_none_for_text_without_braces() {
        // Text with no braces at all must return None.
        let text = "No JSON here at all, just prose text.";
        let result = extract_json_block(text);
        assert!(result.is_none(), "must return None when no braces present");
    }

    // -- parse_response --

    #[test]
    fn parse_response_parses_valid_json() {
        // A well-formed response must parse all expected names with valid labels.
        let text = r#"{"decisions":{"Alice":{"label":"person","reason":"dev"},"MyLib":{"label":"project","reason":"cargo"}}}"#;
        let expected = &["Alice", "MyLib"];
        let decisions = parse_response(text, expected).expect("must parse");
        assert_eq!(decisions.len(), 2);
        assert_eq!(decisions["Alice"].0, "person");
        assert_eq!(decisions["MyLib"].0, "project");
    }

    #[test]
    fn parse_response_ignores_invalid_labels() {
        // Entries with unknown labels must be silently dropped.
        let text = r#"{"decisions":{"Alice":{"label":"unknown","reason":"?"},"Bob":{"label":"person","reason":"dev"}}}"#;
        let expected = &["Alice", "Bob"];
        let decisions = parse_response(text, expected).expect("must parse");
        assert_eq!(decisions.len(), 1, "unknown label must be filtered");
        assert!(decisions.contains_key("Bob"));
    }

    #[test]
    fn parse_response_filters_unexpected_names() {
        // Names not in expected_names must be excluded even with valid labels.
        let text = r#"{"decisions":{"Alice":{"label":"person","reason":"dev"},"Intruder":{"label":"drop","reason":"inject"}}}"#;
        let expected = &["Alice"];
        let decisions = parse_response(text, expected).expect("must parse");
        assert_eq!(decisions.len(), 1, "unlisted name must be excluded");
        assert!(decisions.contains_key("Alice"));
    }

    #[test]
    fn parse_response_returns_none_for_malformed_text() {
        // Plain prose with no JSON structure must return None.
        let text = "Sorry, I cannot classify these entities right now.";
        let result = parse_response(text, &["Alice"]);
        assert!(result.is_none(), "must return None for non-JSON text");
    }

    // -- apply_classifications --

    #[test]
    fn apply_classifications_keeps_confirmed_person() {
        // A person decision must keep the entity in the people list.
        let detected = make_dict(vec!["Alice"], vec![], vec![]);
        let mut decisions = HashMap::new();
        decisions.insert(
            "Alice".to_string(),
            ("person".to_string(), "real dev".to_string()),
        );
        let (result, reclassified, dropped) = apply_classifications(detected, &decisions);
        assert_eq!(result.people.len(), 1);
        assert_eq!(reclassified, 0);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn apply_classifications_drops_entity() {
        // A drop decision must remove the entity from all output lists.
        let detected = make_dict(vec!["Bot42"], vec![], vec![]);
        let mut decisions = HashMap::new();
        decisions.insert("Bot42".to_string(), ("drop".to_string(), "bot".to_string()));
        let (result, reclassified, dropped) = apply_classifications(detected, &decisions);
        assert_eq!(result.people.len(), 0);
        assert_eq!(result.projects.len(), 0);
        assert_eq!(reclassified, 0);
        assert_eq!(dropped, 1);
    }

    #[test]
    fn apply_classifications_reclassifies_person_to_project() {
        // A project decision on a person entity must move it and increment reclassified.
        let detected = make_dict(vec![], vec![], vec!["Tokio"]);
        let mut decisions = HashMap::new();
        decisions.insert(
            "Tokio".to_string(),
            ("project".to_string(), "async runtime".to_string()),
        );
        let (result, reclassified, dropped) = apply_classifications(detected, &decisions);
        assert_eq!(result.projects.len(), 1, "Tokio must move to projects");
        assert_eq!(result.uncertain.len(), 0);
        assert_eq!(reclassified, 1);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn apply_classifications_balances_total_count() {
        // Dropped + kept must equal total input for any combination of decisions.
        let detected = make_dict(vec!["Alice", "Bob"], vec!["MyLib"], vec!["Unknown"]);
        let mut decisions = HashMap::new();
        decisions.insert(
            "Bob".to_string(),
            ("drop".to_string(), "common name".to_string()),
        );
        decisions.insert(
            "MyLib".to_string(),
            ("project".to_string(), "confirmed".to_string()),
        );
        let (result, _, dropped) = apply_classifications(detected, &decisions);
        let kept = result.people.len() + result.projects.len() + result.uncertain.len();
        assert_eq!(kept + dropped, 4, "total must balance");
    }

    // -- collect_contexts --

    #[test]
    fn collect_contexts_finds_matching_lines() {
        // Lines containing the name must be returned (case-insensitive).
        let corpus_lines: Vec<&str> = vec!["Alice wrote this", "bob did that", "Alice again"];
        let contexts = collect_contexts(&corpus_lines, "alice");
        assert!(!contexts.is_empty(), "must find matching lines");
        assert!(contexts.iter().all(|c| c.to_lowercase().contains("alice")));
    }

    #[test]
    fn collect_contexts_respects_max_snippets_limit() {
        // At most MAX_CONTEXT_SNIPPETS lines must be returned.
        let line = "Alice was here";
        let corpus_lines: Vec<&str> = std::iter::repeat_n(line, 20).collect();
        let contexts = collect_contexts(&corpus_lines, "Alice");
        assert_eq!(contexts.len(), MAX_CONTEXT_SNIPPETS, "must cap at limit");
        assert!(contexts.len() <= MAX_CONTEXT_SNIPPETS);
    }

    // -- extract_json_block_by_braces --

    #[test]
    fn extract_json_block_by_braces_handles_nested_objects() {
        // Nested braces must not confuse the depth counter — the outermost block is returned.
        let text = r#"prefix {"outer": {"inner": "value"}} suffix"#;
        let block = extract_json_block_by_braces(text).expect("must find outermost block");
        assert!(block.starts_with('{'));
        assert!(block.ends_with('}'));
        assert!(block.contains("outer") && block.contains("inner"));
    }

    #[test]
    fn extract_json_block_by_braces_returns_none_without_brace() {
        // Text with no opening brace must return None.
        let text = "No braces here at all.";
        let result = extract_json_block_by_braces(text);
        assert!(
            result.is_none(),
            "must return None when no opening brace found"
        );
    }

    // -- refine_entities_collect_candidates --

    #[test]
    fn collect_candidates_includes_all_categories() {
        // Candidates must include all three input categories.
        let detected = make_dict(vec!["Alice"], vec!["MyLib"], vec!["Unknown"]);
        let candidates = refine_entities_collect_candidates(&detected);
        assert_eq!(candidates.len(), 3, "must collect all three categories");
        assert!(candidates.iter().any(|(name, _)| *name == "Alice"));
        assert!(candidates.iter().any(|(name, _)| *name == "MyLib"));
        assert!(candidates.iter().any(|(name, _)| *name == "Unknown"));
    }

    #[test]
    fn collect_candidates_empty_dict_returns_empty() {
        // An empty DetectedDict must produce no candidates.
        let detected = make_dict(vec![], vec![], vec![]);
        let candidates = refine_entities_collect_candidates(&detected);
        assert!(
            candidates.is_empty(),
            "empty dict must produce no candidates"
        );
    }

    // -- build_user_prompt --

    #[test]
    fn build_user_prompt_contains_entity_names_and_types() {
        // The prompt must reference every entity name and type in the batch.
        let batch: Vec<(&str, &str)> = vec![("Alice", "person"), ("MyLib", "project")];
        let corpus_lines: Vec<&str> = vec!["Alice wrote MyLib"];
        let prompt = build_user_prompt(&batch, &corpus_lines);
        assert!(prompt.contains("Alice"), "prompt must include entity name");
        assert!(
            prompt.contains("MyLib"),
            "prompt must include second entity"
        );
        assert!(prompt.contains("person"), "prompt must include entity type");
        assert!(
            prompt.contains("decisions"),
            "prompt must reference the required JSON key"
        );
        assert!(!prompt.is_empty());
    }

    // -- Mock providers for refine_entities tests --

    struct MockProvider {
        response: String,
    }

    impl LlmProvider for MockProvider {
        fn classify(
            &self,
            _system: &str,
            _user: &str,
            _json_mode: bool,
        ) -> crate::error::Result<crate::llm::client::LlmResponse> {
            Ok(crate::llm::client::LlmResponse {
                text: self.response.clone(),
                model: "mock".to_string(),
                provider: "mock".to_string(),
            })
        }
        fn check_available(&self) -> (bool, String) {
            (true, "mock ok".to_string())
        }
        fn name(&self) -> &'static str {
            "mock"
        }
    }

    struct FailProvider;

    impl LlmProvider for FailProvider {
        fn classify(
            &self,
            _system: &str,
            _user: &str,
            _json_mode: bool,
        ) -> crate::error::Result<crate::llm::client::LlmResponse> {
            Err(crate::error::Error::Llm(
                "intentional test failure".to_string(),
            ))
        }
        fn check_available(&self) -> (bool, String) {
            (false, "intentionally failing".to_string())
        }
        fn name(&self) -> &'static str {
            "fail"
        }
    }

    // -- refine_entities --

    #[test]
    fn refine_entities_empty_detected_produces_zero_batches() {
        // An empty DetectedDict must produce a RefineResult with no batches.
        let provider = MockProvider {
            response: "{}".to_string(),
        };
        let detected = make_dict(vec![], vec![], vec![]);
        let result = refine_entities(detected, "", &provider);
        assert_eq!(result.batches_total, 0);
        assert_eq!(result.batches_completed, 0);
        assert_eq!(result.errors, 0);
        assert!(!result.cancelled);
    }

    #[test]
    fn refine_entities_with_mock_provider_classifies_entities() {
        // A valid LLM response must be applied to the detected dict.
        let response = r#"{"decisions":{"Alice":{"label":"person","reason":"dev"},"MyLib":{"label":"project","reason":"cargo"}}}"#;
        let provider = MockProvider {
            response: response.to_string(),
        };
        let detected = make_dict(vec!["Alice"], vec!["MyLib"], vec![]);
        let result = refine_entities(detected, "Alice wrote MyLib", &provider);
        assert_eq!(result.batches_completed, 1);
        assert_eq!(result.errors, 0);
        assert_eq!(result.batches_total, 1);
        // No reclassifications because Alice was already "person" and MyLib was "project".
        assert_eq!(result.reclassified, 0);
    }

    #[test]
    fn refine_entities_with_failing_provider_counts_errors() {
        // A provider that always fails must produce an error for each batch.
        let detected = make_dict(vec!["Alice"], vec![], vec![]);
        let result = refine_entities(detected, "", &FailProvider);
        assert_eq!(result.errors, 1);
        assert_eq!(result.batches_completed, 0);
        assert_eq!(result.batches_total, 1);
    }

    #[test]
    fn refine_entities_drop_decision_removes_entity() {
        // A "drop" decision must remove the entity from the output dict.
        let response = r#"{"decisions":{"Bot42":{"label":"drop","reason":"automation bot"}}}"#;
        let provider = MockProvider {
            response: response.to_string(),
        };
        let detected = make_dict(vec!["Bot42"], vec![], vec![]);
        let result = refine_entities(detected, "", &provider);
        assert_eq!(result.dropped, 1);
        assert!(result.merged.people.is_empty());
        assert!(result.merged.projects.is_empty());
    }

    // -- collect_corpus_text --

    #[test]
    fn collect_corpus_text_reads_prose_files() {
        // .md files in the directory must be included in the corpus.
        let temp = tempfile::tempdir().expect("must create temp dir");
        std::fs::write(temp.path().join("README.md"), "Alice wrote this project.")
            .expect("must write prose file");
        let corpus = collect_corpus_text(temp.path());
        assert!(
            corpus.contains("Alice wrote this project."),
            "must include prose file content"
        );
        assert!(!corpus.is_empty());
    }

    #[test]
    fn collect_corpus_text_skips_non_prose_files() {
        // .rs source files must not be included — only .md, .txt, .rst are prose.
        let temp = tempfile::tempdir().expect("must create temp dir");
        std::fs::write(temp.path().join("main.rs"), "fn main() {}")
            .expect("must write source file");
        let corpus = collect_corpus_text(temp.path());
        assert!(corpus.is_empty(), "non-prose files must not be included");
    }

    #[test]
    fn collect_corpus_text_empty_dir_returns_empty_string() {
        // An empty directory must produce an empty corpus.
        let temp = tempfile::tempdir().expect("must create temp dir");
        let corpus = collect_corpus_text(temp.path());
        assert!(corpus.is_empty(), "empty dir must produce empty corpus");
    }

    #[test]
    fn collect_corpus_text_includes_nested_prose() {
        // Prose files in subdirectories must be included via the directory walk.
        let temp = tempfile::tempdir().expect("must create temp dir");
        let subdirectory = temp.path().join("docs");
        std::fs::create_dir(&subdirectory).expect("must create subdirectory");
        std::fs::write(subdirectory.join("guide.txt"), "User guide content")
            .expect("must write nested prose file");
        let corpus = collect_corpus_text(temp.path());
        assert!(
            corpus.contains("User guide content"),
            "must include nested prose file content"
        );
    }

    #[test]
    fn collect_corpus_text_includes_txt_and_rst_files() {
        // .txt and .rst extensions must both be recognized as prose.
        let temp = tempfile::tempdir().expect("must create temp dir");
        std::fs::write(temp.path().join("notes.txt"), "txt content").expect("must write txt file");
        std::fs::write(temp.path().join("docs.rst"), "rst content").expect("must write rst file");
        let corpus = collect_corpus_text(temp.path());
        assert!(corpus.contains("txt content"), "must include .txt files");
        assert!(corpus.contains("rst content"), "must include .rst files");
    }
}
