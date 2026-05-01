//! Corpus-origin detection for `mempalace init` Pass 0.
//!
//! Determines whether a corpus is an AI-dialogue record and, if so, what
//! platform and persona names the user has assigned to the agent. Two-tier:
//!
//! - Tier 1: [`detect_origin_heuristic`] — cheap regex grep, no API calls.
//! - Tier 2: [`detect_origin_llm`] — LLM-assisted confirmation and persona
//!   extraction; one call, uses the model's pre-trained platform knowledge.
//!
//! Default stance when evidence is thin: `likely_ai_dialogue = true` with low
//! confidence. False-negatives on AI-dialogue detection break downstream
//! classification; false-positives are recoverable in later passes.
//!
//! Public API:
//! - [`CorpusOriginResult`] — structured detection output
//! - [`detect_origin_heuristic`] — Tier 1 cheap detection
//! - [`detect_origin_llm`] — Tier 2 LLM-assisted detection

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use serde::Serialize;
use serde_json::Value;

use super::super::llm::client::LlmProvider;

// Minimum combined text length to declare "meaningful absence" of AI signals.
// Below this floor we can't confidently say the corpus is narrative rather
// than just short.
const MEANINGFUL_TEXT_FLOOR: usize = 150;

// LLM prompt caps — keeps cost bounded while providing enough context.
const MAX_SAMPLES_FOR_LLM: usize = 20;
const MAX_EXCERPT_CHARS: usize = 800;

const _: () = assert!(MEANINGFUL_TEXT_FLOOR > 0);
const _: () = assert!(MAX_SAMPLES_FOR_LLM > 0);
const _: () = assert!(MAX_EXCERPT_CHARS > 0);

// ── Well-known AI brand terms ─────────────────────────────────────────────
//
// UNAMBIGUOUS: no common-English collision → always counted toward signal.
// AMBIGUOUS: collision with common words/names → only counted when at least
//   one unambiguous AI signal also appears (co-occurrence rule). Prevents
//   false-positives on French novels (Claude), astrology corpora (Gemini),
//   poetry corpora (Haiku/Sonnet), llama-ranch journals, etc.
//
// All matching is case-insensitive — users type lowercase constantly.

const AI_UNAMBIGUOUS_TERMS: &[&str] = &[
    "Anthropic",
    "Claude Code",
    "Claude 3",
    "Claude 4",
    "claude mcp",
    "CLAUDE.md",
    ".claude/",
    "ChatGPT",
    "GPT-4",
    "GPT-3",
    "GPT-5",
    "OpenAI",
    "gpt-4o",
    "gpt-4-turbo",
    "o1-preview",
    "o3",
    "gemini-pro",
    "gemini-1.5",
    "Google AI",
    "Mixtral",
    "Cohere",
    "MCP",
    "LLM",
    "RAG",
    "fine-tune",
    "context window",
    "embedding",
];

const AI_AMBIGUOUS_TERMS: &[&str] = &[
    "Claude",  // French masculine name
    "Opus",    // musical work, comic strip, magazine
    "Sonnet",  // 14-line poem form
    "Haiku",   // 17-syllable poem form
    "Gemini",  // zodiac sign
    "Bard",    // poet / Shakespeare
    "Llama",   // South American animal
    "Mistral", // Mediterranean wind
];

// Turn-marker patterns commonly seen in AI-dialogue transcripts.
const TURN_MARKERS: &[&str] = &[
    r"\buser\s*:\s*",
    r"\bassistant\s*:\s*",
    r"\bhuman\s*:\s*",
    r"\bai\s*:\s*",
    r"\b>>>\s*User\b",
    r"\b>>>\s*Assistant\b",
];

// Compiled turn-marker regexes; built once at first use.
// `.ok()` discards any pattern that fails to compile (defensive; never expected).
static TURN_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    TURN_MARKERS
        .iter()
        .filter_map(|p| Regex::new(&format!("(?i){p}")).ok())
        .collect()
});

// LLM system prompt — verbatim port from corpus_origin.py.
const SYSTEM_PROMPT: &str = "You are analyzing a corpus of text to determine whether \
it is a record of conversations with an AI agent (e.g. Claude, ChatGPT, Gemini, \
custom LLM apps), or some other kind of text (personal narrative, story, research \
notes, journal, code, etc.).\n\nUse your pre-existing knowledge of well-known AI \
platforms. You don't need the corpus to explain what Claude or ChatGPT is — you \
already know. Your job is to detect evidence of their presence and identify what \
persona-names the user has assigned to the agent(s) they converse with.\n\n\
CRITICAL distinction:\n  - agent_persona_names are names the USER has assigned to \
the AI AGENT(S) they converse with. Example: \"Echo\", \"Sparrow\", \"Henry\" might \
be names the user calls a Claude instance they're building a relationship with.\n  \
- Do NOT include the USER's own name in agent_persona_names. The user is the human \
author of the corpus, not a persona of the agent. Even if the user's name appears \
frequently in the text (writing about themselves), that is NOT an agent persona.\n  \
- If you can identify the user's name from context, put it in user_name (separate \
field). If unclear, leave user_name null.\n\nRespond with JSON only (no prose \
before or after):\n{\n  \"is_ai_dialogue_corpus\": <true|false>,\n  \"confidence\": \
<0.0 to 1.0>,\n  \"primary_platform\": <\"Claude (Anthropic)\" | \"ChatGPT \
(OpenAI)\" | \"Gemini (Google)\" | other platform name | null>,\n  \"user_name\": \
<user's name if clearly identifiable from context, else null>,\n  \
\"agent_persona_names\": [<names the user has assigned to the AI AGENT(S), NOT \
the user's own name>],\n  \"evidence\": [<short bullet strings explaining the \
decision>]\n}\n\nDefault stance: if evidence is thin or mixed, return \
is_ai_dialogue_corpus=true with low confidence. False-negatives on AI-dialogue \
detection break downstream classification; false-positives are recoverable later.";

// ===================== PUBLIC TYPES =====================

/// Structured output from corpus-origin detection.
///
/// Both detection tiers return this shape. The merge step in `init` combines
/// heuristic and LLM results: `likely_ai_dialogue`/`confidence` from the
/// heuristic, `primary_platform`/`user_name`/`agent_persona_names` from the
/// LLM (when available), and `evidence` concatenated from both.
#[derive(Debug, Clone, Serialize)]
pub struct CorpusOriginResult {
    /// Whether this corpus is most likely an AI-dialogue record.
    pub likely_ai_dialogue: bool,
    /// Confidence in the `likely_ai_dialogue` verdict (0.0–1.0).
    pub confidence: f64,
    /// Best-guess AI platform (e.g. `"Claude (Anthropic)"`) or `None`.
    pub primary_platform: Option<String>,
    /// Corpus author's name if identifiable from context; else `None`.
    pub user_name: Option<String>,
    /// Names the user has assigned to the AI agent(s) in the corpus.
    pub agent_persona_names: Vec<String>,
    /// Human-readable reasons for the classification.
    pub evidence: Vec<String>,
}

impl CorpusOriginResult {
    /// Serialize to a `serde_json::Value` for audit-trail persistence.
    pub fn to_json_value(&self) -> Value {
        // to_value cannot fail for this type (all fields are JSON-serializable).
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

// ===================== PRIVATE HELPERS =====================

/// Build a word-boundary-aware regex string for a brand term.
///
/// Attaches `\b` only on edges where the term itself starts or ends with a
/// word character (alphanumeric or `_`). Without this:
/// - `"Claude"` would falsely match inside `"Claudette"`.
/// - `".claude/"` would fail to match at string start (`\b` before `.`
///   requires a preceding word char).
fn brand_pattern(term: &str) -> String {
    assert!(!term.is_empty(), "brand_pattern: term must be non-empty");
    let escaped = regex::escape(term);
    let first = term.chars().next().unwrap_or(' ');
    let last = term.chars().last().unwrap_or(' ');
    let prefix = if first.is_alphanumeric() || first == '_' {
        r"\b"
    } else {
        ""
    };
    let suffix = if last.is_alphanumeric() || last == '_' {
        r"\b"
    } else {
        ""
    };
    assert!(!escaped.is_empty(), "escaped pattern must be non-empty");
    format!("{prefix}{escaped}{suffix}")
}

/// Count occurrences of each term (case-insensitive) in `combined`.
///
/// Called by [`detect_origin_heuristic`] separately for unambiguous and
/// ambiguous term lists. Returns a map of `term → hit_count` for terms that
/// matched at least once.
fn detect_origin_heuristic_count_terms(combined: &str, terms: &[&str]) -> HashMap<String, usize> {
    let mut hits: HashMap<String, usize> = HashMap::new();
    for &term in terms {
        if term.is_empty() {
            continue;
        }
        let pattern = brand_pattern(term);
        // Pattern is built from a known-good term — failure means a bug in brand_pattern.
        let Ok(re) = Regex::new(&format!("(?i){pattern}")) else {
            continue;
        };
        let count = re.find_iter(combined).count();
        if count > 0 {
            hits.insert(term.to_string(), count);
        }
    }
    debug_assert!(hits.len() <= terms.len(), "hits cannot exceed term list");
    hits
}

/// Count turn-marker pattern occurrences in `combined`.
///
/// Called by [`detect_origin_heuristic`]. Returns `(total_hits, types_found)`
/// where `types_found` is the number of distinct marker patterns that matched.
fn detect_origin_heuristic_count_turns(combined: &str) -> (usize, usize) {
    let mut total_hits: usize = 0;
    let mut types_found: usize = 0;
    for re in TURN_REGEXES.iter() {
        let count = re.find_iter(combined).count();
        if count > 0 {
            total_hits += count;
            types_found += 1;
        }
    }
    assert!(
        types_found <= TURN_MARKERS.len(),
        "types_found cannot exceed pattern count"
    );
    (total_hits, types_found)
}

/// Build the human-readable evidence list for the heuristic result.
///
/// Called by [`detect_origin_heuristic`]. Mirrors the Python evidence-building
/// logic: top-5 brands shown when AI context is present; suppressed-ambiguous
/// note shown when ambiguous-only hits exist without unambiguous co-signal.
fn detect_origin_heuristic_build_evidence(
    unambiguous_hits: &HashMap<String, usize>,
    ambiguous_hits: &HashMap<String, usize>,
    has_ai_context: bool,
    turn_hits: usize,
    turn_types_count: usize,
) -> Vec<String> {
    let mut evidence: Vec<String> = Vec::new();
    let mut shown_hits: HashMap<String, usize> = unambiguous_hits.clone();
    if has_ai_context {
        shown_hits.extend(
            ambiguous_hits
                .iter()
                .map(|(term, count)| (term.clone(), *count)),
        );
    }
    if !shown_hits.is_empty() {
        let mut top: Vec<_> = shown_hits.iter().collect();
        top.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
        top.truncate(5);
        let terms: Vec<String> = top
            .iter()
            .map(|(term, count)| format!("'{term}' ({count}x)"))
            .collect();
        evidence.push(format!("AI brand terms: {}", terms.join(", ")));
    } else if !ambiguous_hits.is_empty() && !has_ai_context {
        // Ambiguous hits present but suppressed — be transparent about it.
        let mut suppressed: Vec<_> = ambiguous_hits.iter().collect();
        suppressed.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
        suppressed.truncate(3);
        let terms: Vec<String> = suppressed
            .iter()
            .map(|(term, count)| format!("'{term}' ({count}x)"))
            .collect();
        evidence.push(format!(
            "Ambiguous terms present but suppressed (no co-occurring AI signal): {}",
            terms.join(", ")
        ));
    }
    if turn_hits > 0 {
        evidence.push(format!(
            "Turn markers detected: {turn_hits} occurrences across {turn_types_count} pattern types"
        ));
    }
    evidence
}

/// Pull the first JSON object out of a possibly-messy LLM response.
///
/// Attempts straight parse first; falls back to a balanced-brace scanner that
/// finds the first `{...}` block without relying on regex (avoids catastrophic
/// backtracking on malformed input). Returns `None` when no valid object found.
fn extract_json(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(val) = serde_json::from_str(trimmed) {
        return Some(val);
    }
    let bytes = trimmed.as_bytes();
    let start = trimmed.find('{')?;
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut i = start;
    // Walk bytes; depth tracking handles nested objects.
    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            if escape {
                escape = false;
            } else if ch == b'\\' {
                escape = true;
            } else if ch == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match ch {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return serde_json::from_str(&trimmed[start..=i]).ok();
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Build the user prompt for the LLM classify call.
///
/// Caps at [`MAX_SAMPLES_FOR_LLM`] samples and [`MAX_EXCERPT_CHARS`] per
/// sample to keep cost bounded. Called by [`detect_origin_llm`].
fn detect_origin_llm_build_prompt(samples: &[String]) -> String {
    assert!(
        !samples.is_empty(),
        "detect_origin_llm_build_prompt: samples must be non-empty"
    );
    let excerpts: Vec<String> = samples
        .iter()
        .take(MAX_SAMPLES_FOR_LLM)
        .enumerate()
        .map(|(index, sample)| {
            let excerpt: String = sample.chars().take(MAX_EXCERPT_CHARS).collect();
            format!("[sample {}]\n{}", index + 1, excerpt)
        })
        .collect();
    assert!(
        !excerpts.is_empty(),
        "excerpts must be non-empty after sampling"
    );
    format!(
        "CORPUS EXCERPTS:\n\n{}\n\nAnalyze and respond with JSON.",
        excerpts.join("\n\n---\n\n")
    )
}

/// Parse a raw LLM response string into a [`CorpusOriginResult`].
///
/// Returns a conservative default-stance result on malformed JSON or missing
/// fields. Never panics. Called by [`detect_origin_llm`].
fn detect_origin_llm_parse_response(raw: &str) -> CorpusOriginResult {
    let Some(parsed) = extract_json(raw) else {
        return CorpusOriginResult {
            likely_ai_dialogue: true,
            confidence: 0.3,
            primary_platform: None,
            user_name: None,
            agent_persona_names: vec![],
            evidence: vec![
                "LLM response was not valid JSON (fallback to default stance)".to_string(),
            ],
        };
    };
    let user_name: Option<String> = parsed
        .get("user_name")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string);
    let mut personas: Vec<String> = parsed
        .get("agent_persona_names")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    // Filter out the user's own name from the persona list — the LLM sometimes
    // leaks it despite the prompt instruction not to.
    if let Some(ref name) = user_name {
        let name_lower = name.to_lowercase();
        personas.retain(|persona| persona.to_lowercase() != name_lower);
    }
    let evidence: Vec<String> = parsed
        .get("evidence")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    CorpusOriginResult {
        likely_ai_dialogue: parsed
            .get("is_ai_dialogue_corpus")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        confidence: parsed
            .get("confidence")
            .and_then(Value::as_f64)
            .unwrap_or(0.5),
        primary_platform: parsed
            .get("primary_platform")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string),
        user_name,
        agent_persona_names: personas,
        evidence,
    }
}

// ===================== PUBLIC API =====================

/// Tier 1 heuristic detection — no API calls, no latency.
///
/// Scores AI-dialogue likelihood by counting occurrences of well-known AI
/// brand terms and turn-marker patterns (user:, assistant:, etc.). Returns
/// a [`CorpusOriginResult`] with confidence derived from signal density.
///
/// Ambiguous terms (Claude, Gemini, Haiku, …) are only counted when an
/// unambiguous co-signal is also present (co-occurrence rule) to avoid
/// false-positives on French novels, astrology corpora, poetry, etc.
// Density calculations require f64; usize corpus lengths or hit counts would need
// to be astronomical (> 2^52 bytes) before the precision loss affects the result.
#[allow(clippy::cast_precision_loss)]
pub fn detect_origin_heuristic(samples: &[String]) -> CorpusOriginResult {
    let combined = samples.join("\n\n");
    // total_chars uses max(1, len) to avoid division by zero in density calc.
    let total_chars_f64 = combined.len().max(1) as f64;

    let unambiguous_hits = detect_origin_heuristic_count_terms(&combined, AI_UNAMBIGUOUS_TERMS);
    let ambiguous_hits = detect_origin_heuristic_count_terms(&combined, AI_AMBIGUOUS_TERMS);
    let (turn_hits, turn_types_count) = detect_origin_heuristic_count_turns(&combined);

    let total_unambiguous: usize = unambiguous_hits.values().sum();
    let total_ambiguous: usize = ambiguous_hits.values().sum();

    let has_ai_context = total_unambiguous > 0 || turn_hits > 0;
    let counted_brand_hits = total_unambiguous + if has_ai_context { total_ambiguous } else { 0 };

    // Density per 1 000 chars; threshold tuned on a small example set.
    let brand_density = counted_brand_hits as f64 / (total_chars_f64 / 1_000.0);
    let turn_density = turn_hits as f64 / (total_chars_f64 / 1_000.0);

    let evidence = detect_origin_heuristic_build_evidence(
        &unambiguous_hits,
        &ambiguous_hits,
        has_ai_context,
        turn_hits,
        turn_types_count,
    );

    if brand_density >= 0.5 || turn_density >= 2.0 {
        let confidence = (0.6_f64 + 0.1 * (brand_density + turn_density)).min(0.95);
        return CorpusOriginResult {
            likely_ai_dialogue: true,
            confidence,
            primary_platform: None,
            user_name: None,
            agent_persona_names: vec![],
            evidence,
        };
    }
    if counted_brand_hits == 0 && turn_hits == 0 && combined.len() >= MEANINGFUL_TEXT_FLOOR {
        // Meaningful absence — enough text and zero signal → confident narrative.
        let mut narrative_evidence = evidence;
        narrative_evidence.push(format!(
            "no unambiguous AI signal across {} chars of text — pure narrative",
            combined.len()
        ));
        return CorpusOriginResult {
            likely_ai_dialogue: false,
            confidence: 0.9,
            primary_platform: None,
            user_name: None,
            agent_persona_names: vec![],
            evidence: narrative_evidence,
        };
    }
    // Ambiguous or too-short-to-tell: default stance is AI-dialogue, low confidence.
    let reason = if counted_brand_hits > 0 || turn_hits > 0 {
        "weak signal"
    } else {
        "insufficient text"
    };
    let mut default_evidence = evidence;
    default_evidence.push(format!(
        "{reason} — applying default-stance (ai_dialogue=True, low confidence). \
        Tier 2 LLM check recommended to confirm or override."
    ));
    CorpusOriginResult {
        likely_ai_dialogue: true,
        confidence: 0.4,
        primary_platform: None,
        user_name: None,
        agent_persona_names: vec![],
        evidence: default_evidence,
    }
}

/// Tier 2 LLM-assisted detection — confirms platform and extracts persona names.
///
/// Passes up to [`MAX_SAMPLES_FOR_LLM`] excerpts to `provider.classify` with a
/// JSON-mode prompt. Falls back to conservative default-stance on any error or
/// malformed response — never panics.
pub fn detect_origin_llm(samples: &[String], provider: &dyn LlmProvider) -> CorpusOriginResult {
    assert!(
        !samples.is_empty(),
        "detect_origin_llm: samples must be non-empty"
    );
    let user_prompt = detect_origin_llm_build_prompt(samples);
    let raw = match provider.classify(SYSTEM_PROMPT, &user_prompt, true) {
        Ok(response) => response.text,
        Err(error) => {
            return CorpusOriginResult {
                likely_ai_dialogue: true,
                confidence: 0.3,
                primary_platform: None,
                user_name: None,
                agent_persona_names: vec![],
                evidence: vec![format!(
                    "LLM provider error (fallback to default stance): {error}"
                )],
            };
        }
    };
    detect_origin_llm_parse_response(&raw)
}

// ===================== TESTS =====================

#[cfg(test)]
// Test code — .expect() is acceptable for test setup with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::error::{Error, Result};

    fn text_samples(texts: &[&str]) -> Vec<String> {
        texts.iter().map(ToString::to_string).collect()
    }

    // ── brand_pattern ──

    #[test]
    fn brand_pattern_adds_word_boundaries_for_word_chars() {
        // "Claude" starts and ends with word chars — must get \b on both sides.
        let pat = brand_pattern("Claude");
        assert!(
            pat.starts_with(r"\b"),
            "word-start term must have prefix \\b"
        );
        assert!(pat.ends_with(r"\b"), "word-end term must have suffix \\b");
    }

    #[test]
    fn brand_pattern_no_prefix_boundary_for_dot_start() {
        // ".claude/" starts with '.' (non-word) — prefix \b would break matching.
        let pat = brand_pattern(".claude/");
        assert!(
            !pat.starts_with(r"\b"),
            "non-word-start term must not have prefix \\b"
        );
        assert!(
            !pat.ends_with(r"\b"),
            "non-word-end term must not have suffix \\b"
        );
    }

    // ── detect_origin_heuristic ──

    #[test]
    fn heuristic_pure_narrative_returns_false_high_confidence() {
        // A plain narrative paragraph with no AI terms and >= 150 chars.
        let text = "The old lighthouse keeper spent his evenings reading novels about \
            the sea and the ships that had passed through the harbour over many decades. \
            He kept meticulous records in leather-bound journals.";
        assert!(text.len() >= MEANINGFUL_TEXT_FLOOR);
        let result = detect_origin_heuristic(&text_samples(&[text]));
        assert!(
            !result.likely_ai_dialogue,
            "pure narrative must return false"
        );
        assert!(
            result.confidence >= 0.8,
            "narrative confidence must be high (got {})",
            result.confidence
        );
        assert!(
            result.evidence.iter().any(|e| e.contains("pure narrative")),
            "evidence must mention pure narrative"
        );
    }

    #[test]
    fn heuristic_heavy_ai_brand_returns_true_high_confidence() {
        // Repeated unambiguous brand hits push brand_density well above 0.5.
        let text = "I asked ChatGPT and OpenAI's GPT-4 model. The response was \
            generated using the LLM API with context window and RAG pipeline. \
            Claude Code also helped via the Anthropic API.";
        let result = detect_origin_heuristic(&text_samples(&[text]));
        assert!(
            result.likely_ai_dialogue,
            "heavy AI brands must return true"
        );
        assert!(
            result.confidence >= 0.6,
            "high-brand confidence must be >= 0.6 (got {})",
            result.confidence
        );
    }

    #[test]
    fn heuristic_turn_markers_trigger_ai_dialogue() {
        // user: / assistant: turn markers push turn_density above 2.0.
        let text = "user: Hello there\nassistant: Hi! How can I help you today?\n\
            user: Tell me about yourself\nassistant: I am an AI language model.\n\
            user: Thank you\nassistant: You're welcome!";
        let result = detect_origin_heuristic(&text_samples(&[text]));
        assert!(
            result.likely_ai_dialogue,
            "turn markers must produce ai_dialogue=true"
        );
        assert!(
            result.evidence.iter().any(|e| e.contains("Turn markers")),
            "evidence must mention turn markers"
        );
    }

    #[test]
    fn heuristic_ambiguous_only_without_co_signal_returns_narrative() {
        // "Claude" alone with no unambiguous AI co-signal — should be suppressed.
        // Corpus is long enough (>= 150 chars) to be confident in narrative verdict.
        let text = "Claude Monet was a French impressionist painter who pioneered the \
            art movement in the late nineteenth century, painting water lilies and \
            haystacks in the open air of Normandy and the Giverny gardens.";
        assert!(text.len() >= MEANINGFUL_TEXT_FLOOR);
        let result = detect_origin_heuristic(&text_samples(&[text]));
        assert!(
            !result.likely_ai_dialogue,
            "ambiguous-only without co-signal must return false (French name 'Claude')"
        );
        assert!(
            result.evidence.iter().any(|e| e.contains("suppressed")),
            "evidence must mention suppressed ambiguous terms"
        );
    }

    #[test]
    fn heuristic_insufficient_text_returns_default_stance() {
        // Very short text — cannot confirm narrative, so default stance fires.
        let text = "Hello world";
        assert!(text.len() < MEANINGFUL_TEXT_FLOOR);
        let result = detect_origin_heuristic(&text_samples(&[text]));
        assert!(
            result.likely_ai_dialogue,
            "insufficient text must use default stance (ai_dialogue=true)"
        );
        assert!(
            (result.confidence - 0.4).abs() < f64::EPSILON,
            "default-stance confidence must be 0.4"
        );
        assert!(
            result
                .evidence
                .iter()
                .any(|e| e.contains("insufficient text")),
            "evidence must mention insufficient text"
        );
    }

    #[test]
    fn heuristic_claudette_does_not_match_claude() {
        // Word boundary on "Claude" must not fire inside "Claudette".
        let text = "Claudette Colbert was a French-American actress. Claudette appeared \
            in many Hollywood films during the golden age of cinema. She was celebrated \
            for her comedic timing and dramatic range throughout her long career.";
        assert!(text.len() >= MEANINGFUL_TEXT_FLOOR);
        let result = detect_origin_heuristic(&text_samples(&[text]));
        // "Claudette" must not trip the "Claude" pattern — pure narrative result.
        assert!(
            !result.likely_ai_dialogue,
            "Claudette must not match Claude word-boundary pattern"
        );
    }

    #[test]
    fn heuristic_dot_claude_dir_matches() {
        // ".claude/" is an unambiguous Anthropic signal — must be counted.
        let text = "The project stores settings in .claude/settings.json and \
            uses .claude/ for tool configuration. This directory is specific to \
            Claude Code and is created during initialization of the workspace.";
        let result = detect_origin_heuristic(&text_samples(&[text]));
        assert!(
            result.likely_ai_dialogue,
            ".claude/ must trigger ai_dialogue=true"
        );
    }

    #[test]
    fn heuristic_empty_samples_returns_default_stance() {
        // Empty sample list → combined is "" → len=0 < MEANINGFUL_TEXT_FLOOR.
        let result = detect_origin_heuristic(&text_samples(&[]));
        assert!(
            result.likely_ai_dialogue,
            "empty samples must use default stance"
        );
        assert!(
            (result.confidence - 0.4).abs() < f64::EPSILON,
            "empty-samples confidence must be 0.4"
        );
    }

    // ── extract_json ──

    #[test]
    fn extract_json_parses_clean_object() {
        let json = r#"{"key": "value", "num": 42}"#;
        let val = extract_json(json).expect("clean JSON must parse");
        assert_eq!(val["key"].as_str(), Some("value"));
        assert_eq!(val["num"].as_i64(), Some(42));
    }

    #[test]
    fn extract_json_balanced_brace_scan_strips_prose_prefix() {
        let text = "Here is my analysis:\n{\"answer\": true, \"confidence\": 0.9}";
        let val = extract_json(text).expect("JSON with prose prefix must be extracted");
        assert_eq!(val["answer"].as_bool(), Some(true));
    }

    #[test]
    fn extract_json_returns_none_for_empty_string() {
        assert!(extract_json("").is_none(), "empty string must return None");
        assert!(
            extract_json("   ").is_none(),
            "whitespace-only must return None"
        );
    }

    #[test]
    fn extract_json_returns_none_for_no_object() {
        assert!(
            extract_json("no braces here at all").is_none(),
            "text without braces must return None"
        );
    }

    // ── detect_origin_llm ──

    struct MockProvider {
        response: String,
    }

    impl LlmProvider for MockProvider {
        fn classify(
            &self,
            _system: &str,
            _user: &str,
            _json_mode: bool,
        ) -> Result<crate::llm::client::LlmResponse> {
            Ok(crate::llm::client::LlmResponse {
                text: self.response.clone(),
            })
        }

        fn check_available(&self) -> (bool, String) {
            (true, "mock".to_string())
        }

        fn name(&self) -> &'static str {
            "mock"
        }

        #[allow(clippy::unnecessary_literal_bound)] // return type fixed by trait signature
        fn endpoint(&self) -> &str {
            "http://localhost:0"
        }

        fn api_key_source(&self) -> Option<crate::llm::client::ApiKeySource> {
            None
        }
    }

    struct ErrorProvider;

    impl LlmProvider for ErrorProvider {
        fn classify(
            &self,
            _system: &str,
            _user: &str,
            _json_mode: bool,
        ) -> Result<crate::llm::client::LlmResponse> {
            Err(Error::Llm("mock error".to_string()))
        }

        fn check_available(&self) -> (bool, String) {
            (false, "always fails".to_string())
        }

        fn name(&self) -> &'static str {
            "error-mock"
        }

        #[allow(clippy::unnecessary_literal_bound)] // return type fixed by trait signature
        fn endpoint(&self) -> &str {
            "http://localhost:0"
        }

        fn api_key_source(&self) -> Option<crate::llm::client::ApiKeySource> {
            None
        }
    }

    #[test]
    fn llm_valid_json_response_propagates_fields() {
        let json = r#"{
            "is_ai_dialogue_corpus": true,
            "confidence": 0.95,
            "primary_platform": "Claude (Anthropic)",
            "user_name": "Alice",
            "agent_persona_names": ["Echo", "Sparrow"],
            "evidence": ["strong Claude signals"]
        }"#;
        let provider = MockProvider {
            response: json.to_string(),
        };
        let samples = text_samples(&["some corpus text"]);
        let result = detect_origin_llm(&samples, &provider);
        assert!(result.likely_ai_dialogue);
        assert!((result.confidence - 0.95).abs() < 1e-6);
        assert_eq!(
            result.primary_platform.as_deref(),
            Some("Claude (Anthropic)")
        );
        assert_eq!(result.user_name.as_deref(), Some("Alice"));
        assert_eq!(result.agent_persona_names, vec!["Echo", "Sparrow"]);
        assert!(result.evidence.iter().any(|e| e.contains("Claude")));
    }

    #[test]
    fn llm_malformed_json_returns_conservative_fallback() {
        let provider = MockProvider {
            response: "This is not JSON at all. Just prose.".to_string(),
        };
        let samples = text_samples(&["some corpus text"]);
        let result = detect_origin_llm(&samples, &provider);
        assert!(
            result.likely_ai_dialogue,
            "malformed JSON must use default stance (ai_dialogue=true)"
        );
        assert!(
            (result.confidence - 0.3).abs() < f64::EPSILON,
            "malformed JSON fallback confidence must be 0.3"
        );
        assert!(
            result.evidence.iter().any(|e| e.contains("not valid JSON")),
            "evidence must mention invalid JSON"
        );
    }

    #[test]
    fn llm_provider_error_returns_conservative_fallback() {
        let samples = text_samples(&["some corpus text"]);
        let result = detect_origin_llm(&samples, &ErrorProvider);
        assert!(
            result.likely_ai_dialogue,
            "provider error must use default stance"
        );
        assert!(
            (result.confidence - 0.3).abs() < f64::EPSILON,
            "provider error fallback confidence must be 0.3"
        );
        assert!(
            result
                .evidence
                .iter()
                .any(|e| e.contains("LLM provider error")),
            "evidence must mention provider error"
        );
    }

    #[test]
    fn llm_filters_user_name_from_persona_list() {
        // The LLM sometimes leaks the user's name into agent_persona_names.
        let json = r#"{
            "is_ai_dialogue_corpus": true,
            "confidence": 0.8,
            "primary_platform": null,
            "user_name": "alice",
            "agent_persona_names": ["Alice", "Echo"],
            "evidence": []
        }"#;
        let provider = MockProvider {
            response: json.to_string(),
        };
        let samples = text_samples(&["some corpus text"]);
        let result = detect_origin_llm(&samples, &provider);
        assert!(
            !result.agent_persona_names.contains(&"Alice".to_string()),
            "user name must be removed from persona list (case-insensitive)"
        );
        assert!(
            result.agent_persona_names.contains(&"Echo".to_string()),
            "other personas must be preserved"
        );
    }

    #[test]
    fn llm_json_with_prose_prefix_is_extracted() {
        // LLM sometimes wraps JSON in a markdown code block or prose.
        let json = "Sure! Here is my analysis:\n{\"is_ai_dialogue_corpus\": false, \
            \"confidence\": 0.85, \"primary_platform\": null, \"user_name\": null, \
            \"agent_persona_names\": [], \"evidence\": [\"narrative text\"]}";
        let provider = MockProvider {
            response: json.to_string(),
        };
        let samples = text_samples(&["some corpus text"]);
        let result = detect_origin_llm(&samples, &provider);
        assert!(!result.likely_ai_dialogue);
        assert!((result.confidence - 0.85).abs() < 1e-6);
    }

    #[test]
    fn to_json_value_round_trips_fields() {
        // Serialization must preserve all fields without losing data.
        let origin = CorpusOriginResult {
            likely_ai_dialogue: true,
            confidence: 0.7,
            primary_platform: Some("ChatGPT (OpenAI)".to_string()),
            user_name: Some("Bob".to_string()),
            agent_persona_names: vec!["Helper".to_string()],
            evidence: vec!["strong signal".to_string()],
        };
        let val = origin.to_json_value();
        assert_eq!(val["likely_ai_dialogue"].as_bool(), Some(true));
        assert!((val["confidence"].as_f64().unwrap_or(0.0) - 0.7).abs() < 1e-6);
        assert_eq!(val["primary_platform"].as_str(), Some("ChatGPT (OpenAI)"));
        assert_eq!(val["user_name"].as_str(), Some("Bob"));
        assert_eq!(val["agent_persona_names"][0].as_str(), Some("Helper"));
    }
}
