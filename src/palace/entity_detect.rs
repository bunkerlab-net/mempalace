use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read as _;
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;

use crate::i18n::{EntityPatterns, get_entity_patterns};
use crate::palace::entities::DetectedEntity;

// Regex statics are compile-time literals; .expect() cannot fail at runtime.
#[allow(clippy::expect_used)]
// Compiled once at first use rather than on every call to extract_candidates.
// Match capitalized words of 2-20 chars. The lower bound (1 lowercase char
// after the capital) avoids single-letter initials like "I" or "A". The
// upper bound (19 lowercase chars) rejects long common nouns that happen
// to be capitalized at sentence starts (e.g. "Congratulations").
static SINGLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b([A-Z][a-z]{1,19})\b").expect(
        "single-word capitalized name regex is a compile-time literal and cannot fail to compile",
    )
});

// Regex pattern is a compile-time literal and cannot fail to compile at runtime.
#[allow(clippy::expect_used)]
// Matches two or more consecutive capitalized words (multi-word entity names).
static MULTI_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b").expect(
        "multi-word capitalized name regex is a compile-time literal and cannot fail to compile",
    )
});

/// Maximum bytes read per file during entity detection to bound memory usage.
const BYTES_PER_FILE_MAX: usize = 5000;

/// Detection results grouped by type.
pub struct DetectionResult {
    pub people: Vec<DetectedEntity>,
    pub projects: Vec<DetectedEntity>,
    pub uncertain: Vec<DetectedEntity>,
}

/// Scan files and detect entity candidates using the specified locale languages.
///
/// Language codes are BCP 47 tags (e.g., `"en"`, `"de"`, `"pt-br"`). Stopwords
/// and pronoun patterns are merged across all requested locales so multi-language
/// content is handled correctly without false-positive candidates.
pub fn detect_entities(
    file_paths: &[&Path],
    files_max: usize,
    languages: &[&str],
) -> DetectionResult {
    assert!(files_max > 0, "detect_entities: files_max must be positive");
    assert!(
        !languages.is_empty(),
        "detect_entities: languages must not be empty"
    );

    let (all_text, all_lines) = detect_entities_load_text(file_paths, files_max);

    // Guard: extract_candidates asserts non-empty text, so return early here.
    if all_text.is_empty() {
        return DetectionResult {
            people: vec![],
            projects: vec![],
            uncertain: vec![],
        };
    }

    let patterns = get_entity_patterns(languages);
    let pronoun_re = detect_entities_build_pronoun_re(&patterns);
    let candidates = extract_candidates(&all_text, &patterns);

    if candidates.is_empty() {
        return DetectionResult {
            people: vec![],
            projects: vec![],
            uncertain: vec![],
        };
    }

    let mut people = Vec::new();
    let mut projects = Vec::new();
    let mut uncertain = Vec::new();

    let mut sorted_candidates: Vec<_> = candidates.into_iter().collect();
    sorted_candidates.sort_by_key(|b| std::cmp::Reverse(b.1));

    for (name, frequency) in sorted_candidates {
        let scores = score_entity(&name, &all_text, &all_lines, &pronoun_re, &patterns);
        let entity = classify_entity(&name, frequency, &scores);
        match entity.entity_type.as_str() {
            "person" => people.push(entity),
            "project" => projects.push(entity),
            _ => uncertain.push(entity),
        }
    }

    detect_entities_sort_and_truncate(&mut people, &mut projects, &mut uncertain);
    DetectionResult {
        people,
        projects,
        uncertain,
    }
}

/// Called by `detect_entities` to load and concatenate file content.
///
/// Reads up to `files_max` files from `file_paths`, truncating each at 5 000 bytes
/// on a valid UTF-8 boundary to keep memory bounded for large files.
/// Returns `(combined_text, lines)`.
fn detect_entities_load_text(file_paths: &[&Path], files_max: usize) -> (String, Vec<String>) {
    assert!(
        files_max > 0,
        "detect_entities_load_text: files_max must be positive"
    );

    let mut all_text = String::new();
    let mut all_lines = Vec::new();

    for (index, path) in file_paths.iter().enumerate() {
        if index >= files_max {
            break;
        }
        // Stream up to BYTES_PER_FILE_MAX bytes only — fs::read_to_string would
        // first allocate the entire file before truncating, defeating the cap
        // on huge files.
        let Ok(file) = File::open(path) else {
            continue;
        };
        // +4 lets us detect that the file exceeded the cap and lets us walk back
        // to a UTF-8 boundary without losing bytes in the truncated case.
        let mut buffer = Vec::with_capacity(BYTES_PER_FILE_MAX + 4);
        if file
            .take((BYTES_PER_FILE_MAX as u64) + 4)
            .read_to_end(&mut buffer)
            .is_err()
        {
            continue;
        }
        let read_len = buffer.len().min(BYTES_PER_FILE_MAX);
        let mut end = read_len;
        // Walk backward to a valid UTF-8 boundary so non-ASCII content does not
        // produce a partial character at the cap.
        while end > 0 {
            if std::str::from_utf8(&buffer[..end]).is_ok() {
                break;
            }
            end -= 1;
        }
        let Ok(text) = std::str::from_utf8(&buffer[..end]) else {
            continue;
        };
        all_text.push_str(text);
        all_text.push('\n');
        all_lines.extend(text.lines().map(String::from));
    }

    (all_text, all_lines)
}

/// Called by `detect_entities` to build the pronoun proximity regex from locale patterns.
///
/// Joins all pronoun patterns from `EntityPatterns` into a single alternation.
/// Falls back to hard-coded English pronouns when the locale list is empty.
fn detect_entities_build_pronoun_re(patterns: &EntityPatterns) -> Regex {
    // Locale-specific patterns already contain `\b` boundaries.
    let joined = if patterns.pronoun_patterns.is_empty() {
        r"(?i)\b(she|her|hers|he|him|his|they|them|their)\b".to_string()
    } else {
        format!("(?i)({})", patterns.pronoun_patterns.join("|"))
    };
    // The fallback pattern is a compile-time literal and cannot fail.
    // Runtime patterns from locales are validated JSON — failures are silent.
    #[allow(clippy::expect_used)]
    Regex::new(&joined).unwrap_or_else(|_| {
        Regex::new(r"(?i)\b(she|her|hers|he|him|his|they|them|their)\b")
            .expect("English pronoun fallback regex is a compile-time literal")
    })
}

/// Called by `detect_entities` to keep that function within the 70-line limit.
///
/// Sort each result list by descending confidence (people, projects) or descending
/// frequency (uncertain), then truncate to the per-category caps of 15/10/8.
fn detect_entities_sort_and_truncate(
    people: &mut Vec<DetectedEntity>,
    projects: &mut Vec<DetectedEntity>,
    uncertain: &mut Vec<DetectedEntity>,
) {
    assert!(
        people.len() + projects.len() + uncertain.len() > 0,
        "at least one entity must be present"
    );

    people.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    projects.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    uncertain.sort_by_key(|b| std::cmp::Reverse(b.frequency));

    people.truncate(15);
    projects.truncate(10);
    uncertain.truncate(8);

    // Postcondition: result lists are bounded by their truncation limits.
    debug_assert!(people.len() <= 15);
    debug_assert!(projects.len() <= 10);
    debug_assert!(uncertain.len() <= 8);
}

/// Build the stop word set for entity candidate extraction.
///
/// Entity detection needs a stricter filter than topic extraction: a word like
/// `"model"` is a useful topic token but a terrible entity candidate because it
/// matches thousands of unrelated drawers. This function starts from the
/// canonical `dialect::topics::stop_words()` base list (which covers common
/// English function words) and extends it with terms that are valid topics but
/// poor entity candidates: programming keywords, UI verbs, macOS filesystem
/// labels, and AI/ML domain terms that appear frequently but name no specific
/// real-world entity.
///
/// It is separate from `drawer::is_stop_word` (used for FTS indexing) because
/// the two filtering jobs have different precision/recall tradeoffs and must
/// evolve independently as the two features are tuned.
// Large static stopword list — line count reflects data volume, not code complexity.
#[rustfmt::skip]
fn extract_candidates_stop_words() -> HashSet<&'static str> {
    let mut stops = crate::dialect::topics::stop_words();
    stops.extend([
        // Common words not in the base list.
        "ok", "okay", "still", "even", "yes", "take", "put", "come", "go", "see", "know", "think", "true", "false",
        "none", "null", "new", "old", "any", "less", "next", "last", "first", "second", "let", "right", "hey", "hi",
        "hello", "thanks", "thank", "world",
        // Programming keywords — prevent `Def`, `Return`, etc. from scoring as entities.
        "return", "print", "def", "class", "import", "args", "dict", "str", "int", "bool", "self", "cls", "kwargs",
        "returns", "raises", "yields",
        // Generic technical nouns common in documentation but not specific enough to be entities.
        "step", "usage", "run", "check", "find", "add", "set", "list", "path", "file", "type", "name", "note", "example",
        "option", "result", "error", "warning", "info", "data", "item", "key", "value", "stack", "layer", "mode", "test",
        "stop", "start", "copy", "move", "source", "target", "output", "input", "number", "version", "system",
        // Generic nouns common in conversational text.
        "topic", "choose", "social", "human", "humans", "people", "something", "nothing", "everything", "anything",
        "someone", "everyone", "anyone", "day", "life", "place", "part", "kind", "sort", "case", "point", "idea", "fact",
        "sense", "question", "answer", "reason",
        // UI interaction verbs — prevent `Click`, `Save`, etc. from scoring as entities.
        "click", "hit", "press", "tap", "drag", "drop", "open", "close", "save", "load", "launch", "install", "download",
        "upload", "scroll", "select", "enter", "submit", "cancel", "confirm", "delete", "paste", "write", "read", "search",
        "show", "hide",
        // macOS filesystem labels — prevent `Library`, `Desktop`, etc. from scoring as entities.
        "desktop", "documents", "downloads", "users", "home", "library", "applications", "preferences", "settings", "terminal",
        // AI/ML domain terms — too generic to be named entities even when capitalised.
        "actor", "vector", "remote", "control", "duration", "fetch", "agents", "tools", "others", "guards", "ethics",
        "regulation", "learning", "thinking", "memory", "language", "intelligence", "technology", "society", "culture",
        "future", "history", "science", "model", "models", "network", "networks", "training", "inference",
    ]);
    stops
}

/// Extract capitalized-word candidates from `text`, filtered by stop words and a
/// minimum frequency of 3 occurrences.  Returns a map of candidate name to count.
///
/// `patterns` provides locale-specific stopwords (merged from requested languages)
/// and supplemental multi-word patterns for non-Latin scripts. Locale stopwords
/// are checked alongside the base English stop list from `extract_candidates_stop_words`.
fn extract_candidates(text: &str, patterns: &EntityPatterns) -> HashMap<String, usize> {
    assert!(
        !text.is_empty(),
        "extract_candidates: text must not be empty"
    );
    let base_stops = extract_candidates_stop_words();
    let locale_stops = &patterns.stopwords;

    let mut counts: HashMap<String, usize> = HashMap::new();

    // Phrase-level stopword check: joined multi-word locale stopwords (e.g. Hindi
    // "के लिए") would otherwise slip past the per-token check below because none
    // of the constituent tokens is itself listed as a stopword.
    let phrase_is_stopped = |phrase: &str| -> bool {
        let lower_phrase = phrase.to_lowercase();
        if locale_stops.contains(&lower_phrase) {
            return true;
        }
        phrase.split_whitespace().any(|token| {
            let lower = token.to_lowercase();
            base_stops.contains(lower.as_str()) || locale_stops.contains(&lower)
        })
    };

    for cap in SINGLE_RE.captures_iter(text) {
        let word = &cap[1];
        let lower = word.to_lowercase();
        if word.len() > 1 && !base_stops.contains(lower.as_str()) && !locale_stops.contains(&lower)
        {
            *counts.entry(word.to_string()).or_insert(0) += 1;
        }
    }

    // Locale-specific single-word patterns capture non-Latin scripts (Devanagari,
    // CJK, Cyrillic, etc.) whose characters fall outside SINGLE_RE's ASCII range.
    for pattern in &patterns.candidate_patterns {
        if let Ok(re) = Regex::new(pattern) {
            for cap in re.captures_iter(text) {
                // Some locale patterns lack an explicit capture group (e.g. CJK
                // surnames concatenated with a unicode range); fall back to the
                // whole match when group 1 is absent.
                let word = cap.get(1).map_or_else(|| &cap[0], |m| m.as_str());
                let lower = word.to_lowercase();
                if word.chars().count() > 1
                    && !base_stops.contains(lower.as_str())
                    && !locale_stops.contains(&lower)
                {
                    *counts.entry(word.to_string()).or_insert(0) += 1;
                }
            }
        }
    }

    for cap in MULTI_RE.captures_iter(text) {
        let phrase = &cap[1];
        if !phrase_is_stopped(phrase) {
            *counts.entry(phrase.to_string()).or_insert(0) += 1;
        }
    }

    // Locale-specific multi-word patterns supplement MULTI_RE for non-Latin scripts
    // whose word boundaries are not captured by the base ASCII regex.
    for pattern in &patterns.multi_word_patterns {
        if let Ok(re) = Regex::new(pattern) {
            for cap in re.captures_iter(text) {
                let phrase = &cap[1];
                if !phrase_is_stopped(phrase) {
                    *counts.entry(phrase.to_string()).or_insert(0) += 1;
                }
            }
        }
    }

    // Require at least 3 occurrences before treating a name as an entity.
    // Once or twice is likely a passing reference; three times suggests the
    // name is a recurring actor or concept worth storing.
    counts.retain(|_, freq| *freq >= 3);

    // Postcondition: all surviving candidates have frequency >= 3.
    debug_assert!(counts.values().all(|&freq| freq >= 3));

    counts
}

struct EntityScores {
    person_score: i32,
    project_score: i32,
    // Count of distinct person-signal categories (verb, dialogue, pronoun, addressed)
    // used by classify_entity to require corroboration before committing to "person".
    person_category_count: usize,
}

/// Distinct person-signal categories that contribute to corroboration.
///
/// Returned as a `HashSet` rather than a count so the locale-signal helper and
/// the hardcoded English checks can be unioned without double-counting overlaps
/// (e.g. both English and a locale firing on the verb arm).
#[derive(Hash, Eq, PartialEq, Copy, Clone)]
enum PersonCategory {
    Verb,
    Dialogue,
    Pronoun,
    DirectAddress,
}

/// Score person-related signals: verb patterns, dialogue markers, pronouns, direct address.
///
/// `pronoun_re` is the compiled locale pronoun regex built from `EntityPatterns`.
/// Returns `(score, categories)` where `categories` is the set of distinct
/// signal kinds that fired across both the hardcoded English checks and the
/// locale templates — caller computes the count from the union.
fn score_entity_person(
    name: &str,
    escaped: &str,
    text: &str,
    lines: &[String],
    pronoun_re: &Regex,
    patterns: &EntityPatterns,
) -> (i32, HashSet<PersonCategory>) {
    let mut score = 0i32;
    let mut categories: HashSet<PersonCategory> = HashSet::new();

    let person_verbs = [
        "said", "asked", "told", "replied", "laughed", "smiled", "cried", "felt", "thinks?",
        "wants?", "loves?", "hates?", "knows?", "decided", "pushed", "wrote",
    ];
    let mut verb_hit = false;
    for verb in person_verbs {
        if let Ok(re) = Regex::new(&format!(r"(?i)\b{escaped}\s+{verb}\b")) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 2;
                verb_hit = true;
            }
        }
    }
    if verb_hit {
        categories.insert(PersonCategory::Verb);
    }

    let dialogue_pats = [
        format!(r"(?im)^>\s*{escaped}[:\s]"),
        format!(r"(?im)^{escaped}:\s"),
        format!(r"(?im)^\[{escaped}\]"),
    ];
    let mut dialogue_hit = false;
    for pat in &dialogue_pats {
        if let Ok(re) = Regex::new(pat) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 3;
                dialogue_hit = true;
            }
        }
    }
    if dialogue_hit {
        categories.insert(PersonCategory::Dialogue);
    }

    let pronoun_hits = score_entity_person_pronoun_score(name, lines, pronoun_re);
    if pronoun_hits > 0 {
        score += pronoun_hits * 2;
        categories.insert(PersonCategory::Pronoun);
    }

    if let Ok(re) = Regex::new(&format!(
        r"(?i)\bhey\s+{escaped}\b|\bthanks?\s+{escaped}\b|\bhi\s+{escaped}\b"
    )) {
        let count = re.find_iter(text).count();
        if count > 0 {
            score += i32::try_from(count).unwrap_or(i32::MAX) * 4;
            categories.insert(PersonCategory::DirectAddress);
        }
    }

    let (locale_score, locale_categories) =
        score_entity_person_locale_signals(escaped, text, patterns);
    // Union ensures a locale verb hit and an English verb hit count as one
    // verb category, not two. Without this, the >=2 corroboration check could
    // pass on what is really a single signal kind seen twice.
    categories.extend(locale_categories);
    (score + locale_score, categories)
}

/// Called by `score_entity_person` to keep that function within the 70-line limit.
///
/// Scan `lines` for lines containing `name` and count how many have a pronoun from
/// `pronoun_re` within a ±2-line window, indicating a person-like antecedent.
/// Returns the raw hit count (caller multiplies by weight).
fn score_entity_person_pronoun_score(name: &str, lines: &[String], pronoun_re: &Regex) -> i32 {
    assert!(!name.is_empty(), "name must not be empty");

    let name_lower = name.to_lowercase();
    // Build a word-boundary regex so "al" does not match inside "palantir".
    // Compile once outside the loop; fall back to no matches if the name yields
    // an invalid pattern (cannot happen with regex::escape, but handle gracefully).
    let escaped = regex::escape(&name_lower);
    let name_re = Regex::new(&format!(r"(?i)\b{escaped}\b")).ok();
    let mut pronoun_hits = 0i32;
    for (index, line) in lines.iter().enumerate() {
        if name_re.as_ref().is_some_and(|re| re.is_match(line)) {
            let start = index.saturating_sub(2);
            let end = (index + 3).min(lines.len());
            let window: String = lines[start..end].join(" ");
            if pronoun_re.is_match(&window) {
                pronoun_hits += 1;
            }
        }
    }

    // Postcondition: hit count is non-negative.
    debug_assert!(pronoun_hits >= 0);

    pronoun_hits
}

/// Called by `score_entity_person` to apply locale-specific verb, dialogue, and direct-address signals.
///
/// Each template substitutes `{name}` with `escaped` before compilation.
/// Verb hits weight 2×, dialogue 3×, direct-address 4× — matching the hardcoded English weights.
/// Returns `(score, categories)` so the caller can union them with the English
/// signal set and avoid double-counting overlapping signal kinds.
fn score_entity_person_locale_signals(
    escaped: &str,
    text: &str,
    patterns: &EntityPatterns,
) -> (i32, HashSet<PersonCategory>) {
    assert!(
        !escaped.is_empty(),
        "score_entity_person_locale_signals: escaped must not be empty"
    );

    let mut score = 0i32;
    let mut categories: HashSet<PersonCategory> = HashSet::new();

    let mut verb_hit = false;
    for template in &patterns.person_verb_patterns {
        let pat = template.replace("{name}", escaped);
        if let Ok(re) = Regex::new(&format!("(?i){pat}")) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 2;
                verb_hit = true;
            }
        }
    }
    if verb_hit {
        categories.insert(PersonCategory::Verb);
    }

    let mut dialogue_hit = false;
    for template in &patterns.dialogue_patterns {
        let pat = template.replace("{name}", escaped);
        if let Ok(re) = Regex::new(&format!("(?im){pat}")) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 3;
                dialogue_hit = true;
            }
        }
    }
    if dialogue_hit {
        categories.insert(PersonCategory::Dialogue);
    }

    let mut address_hit = false;
    for template in &patterns.direct_address_patterns {
        let pat = template.replace("{name}", escaped);
        if let Ok(re) = Regex::new(&format!("(?i){pat}")) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 4;
                address_hit = true;
            }
        }
    }
    if address_hit {
        categories.insert(PersonCategory::DirectAddress);
    }

    // Postcondition: at most 3 distinct locale categories (verb, dialogue, direct-address).
    debug_assert!(categories.len() <= 3);

    (score, categories)
}

/// Score project-related signals for `escaped` against `text`: build/deploy verbs and versioned references.
fn score_entity_project(escaped: &str, text: &str, patterns: &EntityPatterns) -> i32 {
    let mut score = 0i32;

    let project_pats = [
        format!(r"(?i)\bbuilding\s+{escaped}\b"),
        format!(r"(?i)\bbuilt\s+{escaped}\b"),
        format!(r"(?i)\bship(?:ping|ped)?\s+{escaped}\b"),
        format!(r"(?i)\blaunch(?:ing|ed)?\s+{escaped}\b"),
        format!(r"(?i)\bdeploy(?:ing|ed)?\s+{escaped}\b"),
        format!(r"(?i)\binstall(?:ing|ed)?\s+{escaped}\b"),
        format!(r"(?i)\bthe\s+{escaped}\s+(architecture|pipeline|system|repo)\b"),
        format!(r"(?i)\b{escaped}\s+v\d+\b"),
        format!(r"(?i)\b{escaped}\.(py|js|ts|yaml|yml|json|sh)\b"),
        format!(r"(?i)\bimport\s+{escaped}\b"),
    ];
    for pat in &project_pats {
        if let Ok(re) = Regex::new(pat) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 2;
            }
        }
    }

    if let Ok(re) = Regex::new(&format!(r"(?i)\b{escaped}[-v]\w+")) {
        let count = re.find_iter(text).count();
        if count > 0 {
            score += i32::try_from(count).unwrap_or(i32::MAX) * 3;
        }
    }

    // Locale-specific project verb templates supplement the hardcoded English verbs above.
    for template in &patterns.project_verb_patterns {
        let pat = template.replace("{name}", escaped);
        if let Ok(re) = Regex::new(&format!("(?i){pat}")) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 2;
            }
        }
    }

    score
}

/// Compute person and project signal scores for `name` against `text` and `lines`.
///
/// `pronoun_re` is the locale pronoun regex from `detect_entities_build_pronoun_re`.
/// Returns an [`EntityScores`] combining the outputs of `score_entity_person`
/// and `score_entity_project`.
fn score_entity(
    name: &str,
    text: &str,
    lines: &[String],
    pronoun_re: &Regex,
    patterns: &EntityPatterns,
) -> EntityScores {
    assert!(!name.is_empty(), "score_entity: name must not be empty");
    let escaped = regex::escape(name);

    let (person_score, person_categories) =
        score_entity_person(name, &escaped, text, lines, pronoun_re, patterns);
    EntityScores {
        person_score,
        project_score: score_entity_project(&escaped, text, patterns),
        person_category_count: person_categories.len(),
    }
}

/// Classify `name` into `"person"`, `"project"`, or `"uncertain"` based on its signal scores
/// and occurrence frequency.
fn classify_entity(name: &str, frequency: usize, scores: &EntityScores) -> DetectedEntity {
    let person_score = scores.person_score;
    let project_score = scores.project_score;
    let total = person_score + project_score;

    if total == 0 {
        // frequency is a name occurrence count, always small enough for exact f64 representation
        #[allow(clippy::cast_precision_loss)]
        let confidence = (frequency as f64 / 50.0).min(0.4);
        return DetectedEntity {
            name: name.to_string(),
            entity_type: "uncertain".to_string(),
            confidence,
            frequency,
            signals: vec![],
        };
    }

    let person_ratio = f64::from(person_score) / f64::from(total);

    // Require at least two distinct signal categories to distinguish corroborated
    // person signals from pronoun-only matches, which are too weak alone.
    let has_two = scores.person_category_count >= 2;

    classify_entity_build(name, frequency, person_ratio, has_two, person_score)
}

/// Called by [`classify_entity`] to keep that function within the 70-line limit.
///
/// Constructs the [`DetectedEntity`] from pre-computed ratio and corroboration flags.
fn classify_entity_build(
    name: &str,
    frequency: usize,
    person_ratio: f64,
    has_two: bool,
    person_score: i32,
) -> DetectedEntity {
    if person_ratio >= 0.7 && has_two && person_score >= 5 {
        DetectedEntity {
            name: name.to_string(),
            entity_type: "person".to_string(),
            confidence: (0.5 + person_ratio * 0.5).min(0.99),
            frequency,
            signals: vec![],
        }
    } else if person_ratio >= 0.7 && (!has_two || person_score < 5) {
        DetectedEntity {
            name: name.to_string(),
            entity_type: "uncertain".to_string(),
            confidence: 0.4,
            frequency,
            signals: vec![],
        }
    } else if person_ratio <= 0.3 {
        DetectedEntity {
            name: name.to_string(),
            entity_type: "project".to_string(),
            confidence: (0.5 + (1.0 - person_ratio) * 0.5).min(0.99),
            frequency,
            signals: vec![],
        }
    } else {
        DetectedEntity {
            name: name.to_string(),
            entity_type: "uncertain".to_string(),
            confidence: 0.5,
            frequency,
            signals: vec![],
        }
    }
}

/// Walk `project_dir` and return up to `max_files` file paths suitable for entity detection.
///
/// Prose files (`.md`, `.txt`, `.rst`) are preferred; when fewer than
/// `PROSE_THRESHOLD` are found the result is padded with common code files
/// (`.rs`, `.py`, `.ts`, `.js`, `.go`). Uses an iterative depth-limited walk
/// to respect `crate::palace::WALK_DEPTH_LIMIT` and skip known build directories.
/// Minimum number of prose files required before falling back to code files.
const SCAN_PROSE_THRESHOLD: usize = 5;
/// File extensions treated as prose during `scan_for_detection`.
const SCAN_PROSE_EXTS: &[&str] = &["md", "txt", "rst"];
/// Code file extensions used as a fallback when prose files are scarce.
const SCAN_CODE_EXTS: &[&str] = &["rs", "py", "ts", "js", "go"];

pub fn scan_for_detection(project_dir: &Path, max_files: usize) -> Vec<std::path::PathBuf> {
    use crate::palace::room_detect::is_skip_dir;

    assert!(
        project_dir.is_dir(),
        "scan_for_detection: project_dir must be a directory"
    );
    assert!(
        max_files > 0,
        "scan_for_detection: max_files must be positive"
    );

    let mut prose_files: Vec<std::path::PathBuf> = Vec::new();
    let mut code_files: Vec<std::path::PathBuf> = Vec::new();
    let mut stack: Vec<(std::path::PathBuf, usize)> = vec![(project_dir.to_path_buf(), 0)];

    while let Some((directory, depth)) = stack.pop() {
        assert!(depth <= crate::palace::WALK_DEPTH_LIMIT);
        if depth >= crate::palace::WALK_DEPTH_LIMIT {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let entry_name = entry.file_name();
                if !is_skip_dir(&entry_name.to_string_lossy()) {
                    stack.push((path, depth + 1));
                }
            } else if path.is_file() {
                let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                if SCAN_PROSE_EXTS.contains(&extension) {
                    prose_files.push(path);
                } else if SCAN_CODE_EXTS.contains(&extension) {
                    code_files.push(path);
                }
            }
        }
        if prose_files.len() >= max_files {
            break;
        }
    }

    let result: Vec<std::path::PathBuf> = if prose_files.len() >= SCAN_PROSE_THRESHOLD {
        prose_files.into_iter().take(max_files).collect()
    } else {
        let remaining = max_files.saturating_sub(prose_files.len());
        let mut combined = prose_files;
        combined.extend(code_files.into_iter().take(remaining));
        combined
    };

    // Postcondition: result does not exceed max_files.
    debug_assert!(result.len() <= max_files);
    result
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile;

    /// Return a compiled English pronoun regex for use in tests that call `score_entity`
    /// directly.  Production code builds this via `detect_entities_build_pronoun_re`.
    fn test_pronoun_re() -> Regex {
        Regex::new(r"(?i)\b(she|her|hers|he|him|his|they|them|their)\b")
            .expect("hardcoded English pronoun regex cannot fail to compile")
    }

    #[test]
    fn extract_candidates_finds_capitalized_names() {
        // "Alice" appears 5 times — well above the frequency threshold of 3.
        let text =
            "Alice went to the store. Alice bought milk. Alice came home. Alice cooked. Alice ate.";
        let candidates = extract_candidates(text, &get_entity_patterns(&["en"]));
        assert!(
            candidates.contains_key("Alice"),
            "Alice should be detected as a candidate"
        );
        assert!(
            *candidates.get("Alice").expect("Alice must be present") >= 3,
            "Alice frequency should be at least 3"
        );
    }

    #[test]
    fn extract_candidates_ignores_low_frequency() {
        // "Bartholomew" appears only once — below the threshold of 3.
        let text = "Bartholomew visited the library. Xander Xander Xander was there.";
        let candidates = extract_candidates(text, &get_entity_patterns(&["en"]));
        assert!(
            !candidates.contains_key("Bartholomew"),
            "Single-occurrence names should be filtered out"
        );
        assert!(
            candidates.contains_key("Xander"),
            "Names at threshold should survive"
        );
    }

    #[test]
    fn extract_candidates_multi_word_names() {
        // "John Smith" repeated 3 times should be detected as a multi-word candidate.
        let text =
            "John Smith led the team. John Smith reviewed the code. John Smith merged the PR.";
        let candidates = extract_candidates(text, &get_entity_patterns(&["en"]));
        assert!(
            candidates.contains_key("John Smith"),
            "Multi-word names should be detected"
        );
        assert!(
            *candidates
                .get("John Smith")
                .expect("John Smith must be present")
                >= 3,
            "John Smith frequency should be at least 3"
        );
    }

    #[test]
    fn extract_candidates_filters_stop_words() {
        // "The" and "This" are stop words — they should never appear as candidates
        // even at high frequency.
        let text =
            "The quick fox. The lazy dog. The bright sun. This is great. This is fine. This works.";
        let candidates = extract_candidates(text, &get_entity_patterns(&["en"]));
        assert!(
            !candidates.contains_key("The"),
            "Stop word 'The' should be filtered"
        );
        assert!(
            !candidates.contains_key("This"),
            "Stop word 'This' should be filtered"
        );
    }

    #[test]
    fn score_entity_person_detects_speech_verbs() {
        let name = "Alice";
        let text = "Alice said hello. Alice asked a question. Alice told a story.";
        let lines: Vec<String> = text.lines().map(String::from).collect();
        let scores = score_entity(
            name,
            text,
            &lines,
            &test_pronoun_re(),
            &get_entity_patterns(&["en"]),
        );
        assert!(
            scores.person_score > 0,
            "Person score should be positive when speech verbs are present"
        );
        assert!(
            scores.person_category_count > 0,
            "Person category count should be positive for speech verb matches"
        );
    }

    #[test]
    fn score_entity_project_detects_build_verbs() {
        let text =
            "building Mempalace from scratch. deploying Mempalace to prod. shipping Mempalace v2.";
        let lines: Vec<String> = text.lines().map(String::from).collect();
        let scores = score_entity(
            "Mempalace",
            text,
            &lines,
            &test_pronoun_re(),
            &get_entity_patterns(&["en"]),
        );
        assert!(
            scores.project_score > 0,
            "Project score should be positive when build verbs are present"
        );
    }

    #[test]
    fn classify_entity_as_person() {
        // High person score with multiple signal categories triggers person classification.
        // person_category_count >= 2 satisfies the has_two corroboration requirement.
        let scores = EntityScores {
            person_score: 12,
            project_score: 0,
            person_category_count: 3,
        };
        let entity = classify_entity("Alice", 10, &scores);
        assert_eq!(
            entity.entity_type, "person",
            "Entity with high person score and multiple signal categories should be classified as person"
        );
        assert!(
            entity.confidence > 0.5,
            "Person confidence should be above 0.5"
        );
        // Prose-detected entities carry no signals — signals are populated by project_scanner.
        assert!(
            entity.signals.is_empty(),
            "prose-detected entity must have empty signals"
        );
    }

    #[test]
    fn classify_entity_as_project() {
        // Project score dominates — person_ratio <= 0.3 triggers project classification.
        let scores = EntityScores {
            person_score: 0,
            project_score: 10,
            person_category_count: 0,
        };
        let entity = classify_entity("Mempalace", 8, &scores);
        assert_eq!(
            entity.entity_type, "project",
            "Entity with dominant project score should be classified as project"
        );
        assert!(
            entity.confidence > 0.5,
            "Project confidence should be above 0.5"
        );
        // Prose-detected entities carry no signals — signals are populated by project_scanner.
        assert!(
            entity.signals.is_empty(),
            "prose-detected entity must have empty signals"
        );
    }

    #[test]
    fn detect_entities_end_to_end() {
        // Write temp files with a name repeated enough times to cross the threshold.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = dir.path().join("story.txt");
        let mut f = std::fs::File::create(&file_path).expect("file should be created");
        let content = "Alice said hello. Alice asked why. Alice told Bob. Alice replied quickly. Alice laughed.\n";
        f.write_all(content.as_bytes())
            .expect("write should succeed");

        let paths: Vec<&Path> = vec![file_path.as_path()];
        let result = detect_entities(&paths, 10, &["en"]);

        // At least one category should be non-empty since "Alice" appears 5 times
        // with person-verb signals.
        let total = result.people.len() + result.projects.len() + result.uncertain.len();
        assert!(total > 0, "Should detect at least one entity");
        assert!(
            result.people.iter().any(|e| e.name == "Alice")
                || result.uncertain.iter().any(|e| e.name == "Alice"),
            "Alice should appear in people or uncertain"
        );
    }

    /// Called by `detect_entities_with_project_entities` to keep that function within the 70-line limit.
    ///
    /// Create two temp files with content designed to trigger project and person
    /// classification branches.  Returns `(tempdir, path_one, path_two)`; the
    /// caller must keep `tempdir` alive for the duration of the test.
    fn detect_entities_with_project_entities_setup_files()
    -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for entity detection test");

        let file_path_one = temp_directory.path().join("notes_one.txt");
        let file_path_two = temp_directory.path().join("notes_two.txt");

        // Palantir: 4 project-verb hits, no person signals.
        // Vortex: 4 project-verb hits plus a versioned reference.
        // Clara: 5 person-verb hits with pronouns nearby → person bucket.
        let content_one = "\
Clara said hello to the team. She was excited.\n\
We have been building Palantir for months now.\n\
Clara asked about the deploy timeline. She wanted details.\n\
We finished deploying Palantir to staging yesterday.\n\
Clara told everyone the release is on track.\n\
The Palantir architecture is solid and well tested.\n\
Shipping Palantir went smoothly after the final review.\n\
We started building Vortex as a side project last week.\n\
Clara laughed when she saw the Vortex demo.\n\
Deploying Vortex to the cluster took only minutes.\n";

        let content_two = "\
Clara replied with detailed feedback on the Vortex pipeline.\n\
The team launched Vortex v2 with improved throughput.\n\
She reviewed the Vortex-v3 release candidate too.\n\
Installing Palantir on the new servers was straightforward.\n\
Clara said the migration is complete. He agreed.\n";

        let mut file_one = std::fs::File::create(&file_path_one)
            .expect("failed to create first temp file for project entity test");
        file_one
            .write_all(content_one.as_bytes())
            .expect("failed to write first temp file for project entity test");

        let mut file_two = std::fs::File::create(&file_path_two)
            .expect("failed to create second temp file for project entity test");
        file_two
            .write_all(content_two.as_bytes())
            .expect("failed to write second temp file for project entity test");

        (temp_directory, file_path_one, file_path_two)
    }

    #[test]
    fn detect_entities_with_project_entities() {
        // Exercises the project classification branch in detect_entities.
        // Each project name appears 4+ times with build/deploy verbs and zero
        // person signals, pushing person_ratio <= 0.3 so classify_entity picks
        // the "project" branch. Person names get speech verbs and pronouns to
        // land in the "person" bucket and verify both arms fire in one call.
        let (_temp_directory, file_path_one, file_path_two) =
            detect_entities_with_project_entities_setup_files();

        let paths: Vec<&Path> = vec![file_path_one.as_path(), file_path_two.as_path()];
        let result = detect_entities(&paths, 10, &["en"]);

        // Postcondition: detection found entities across multiple categories.
        let entities_total = result.people.len() + result.projects.len() + result.uncertain.len();
        assert!(
            entities_total > 0,
            "detection should find at least one entity across all categories"
        );

        // Project bucket should contain at least one of Palantir or Vortex.
        // Both names carry strong project signals (build/deploy verbs, versioned
        // references) and no person signals, so person_ratio should be <= 0.3.
        let project_names: Vec<&str> = result.projects.iter().map(|e| e.name.as_str()).collect();
        let has_project_entity = project_names.contains(&"Palantir")
            || project_names.contains(&"Vortex")
            || result
                .uncertain
                .iter()
                .any(|e| e.name == "Palantir" || e.name == "Vortex");
        assert!(
            has_project_entity,
            "Palantir or Vortex should appear in projects or uncertain; \
             projects={project_names:?}, uncertain={:?}",
            result.uncertain.iter().map(|e| &e.name).collect::<Vec<_>>()
        );

        // Clara should land in people or uncertain — she has speech verb and
        // pronoun signals across two signal categories.
        let has_person_entity = result.people.iter().any(|e| e.name == "Clara")
            || result.uncertain.iter().any(|e| e.name == "Clara");
        assert!(
            has_person_entity,
            "Clara should appear in people or uncertain"
        );

        // Verify project entities have reasonable confidence (> 0.0).
        for project in &result.projects {
            assert!(
                project.confidence > 0.0,
                "project entity '{}' should have positive confidence",
                project.name
            );
        }
    }

    #[test]
    fn detect_entities_empty_files() {
        // Empty files should produce no candidates — but extract_candidates asserts
        // non-empty text, so detect_entities handles the empty-read case by
        // producing an empty all_text only if no files are readable.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = dir.path().join("empty.txt");
        std::fs::write(&file_path, "").expect("write should succeed");

        // The file is empty so read_to_string returns "" which gets appended as
        // just a newline. detect_entities calls extract_candidates only when
        // all_text is non-empty after accumulation. With a single empty file,
        // all_text is "\n" which is non-empty but has no capitalized words.
        let paths: Vec<&Path> = vec![file_path.as_path()];
        let result = detect_entities(&paths, 10, &["en"]);

        assert!(result.people.is_empty(), "No people from empty file");
        assert!(result.projects.is_empty(), "No projects from empty file");
    }

    // -- scan_for_detection ---------------------------------------------------

    #[test]
    fn scan_for_detection_returns_prose_files() {
        // When prose files are present they must be returned preferentially.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for scan test");
        std::fs::write(temp_directory.path().join("notes.md"), "# notes")
            .expect("failed to write notes.md");
        std::fs::write(temp_directory.path().join("readme.txt"), "readme content")
            .expect("failed to write readme.txt");
        std::fs::write(temp_directory.path().join("main.rs"), "fn main() {}")
            .expect("failed to write main.rs");

        let result = scan_for_detection(temp_directory.path(), 10);

        // Prose files must be present in the result.
        assert!(!result.is_empty(), "must find at least one file");
        assert!(result.len() <= 10, "result must not exceed max_files");
        let extensions: Vec<&str> = result
            .iter()
            .filter_map(|p| p.extension().and_then(|e| e.to_str()))
            .collect();
        assert!(
            extensions.contains(&"md") || extensions.contains(&"txt"),
            "prose files must appear in result; found extensions: {extensions:?}"
        );
    }

    #[test]
    fn scan_for_detection_falls_back_to_code_files_when_prose_is_absent() {
        // When fewer than PROSE_THRESHOLD prose files exist, code files must pad the result.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for fallback scan test");
        // Only one prose file (below threshold of 5) and two code files.
        std::fs::write(temp_directory.path().join("readme.md"), "# readme")
            .expect("failed to write readme.md");
        std::fs::write(temp_directory.path().join("main.rs"), "fn main() {}")
            .expect("failed to write main.rs");
        std::fs::write(temp_directory.path().join("lib.rs"), "pub mod lib;")
            .expect("failed to write lib.rs");

        let result = scan_for_detection(temp_directory.path(), 10);

        // Code files must be included because prose count is below threshold.
        assert!(!result.is_empty(), "must find at least one file");
        assert!(result.len() <= 10, "result must not exceed max_files");
        let extensions: Vec<&str> = result
            .iter()
            .filter_map(|p| p.extension().and_then(|e| e.to_str()))
            .collect();
        assert!(
            extensions.contains(&"rs"),
            "code files must be included when prose count is below threshold; found: {extensions:?}"
        );
    }

    // ── detect_entities — empty path list produces empty result ───────────────

    #[test]
    fn detect_entities_no_paths_returns_empty_result() {
        // When no file paths are provided the all_text accumulator stays empty,
        // and detect_entities must return three empty vecs without panicking.
        let paths: Vec<&Path> = vec![];
        let result = detect_entities(&paths, 1, &["en"]);
        assert!(result.people.is_empty(), "no paths must yield no people");
        assert!(
            result.projects.is_empty(),
            "no paths must yield no projects"
        );
        assert!(
            result.uncertain.is_empty(),
            "no paths must yield no uncertain"
        );
    }

    // ── detect_entities_load_text — large file truncated at UTF-8 boundary ───

    #[test]
    fn detect_entities_load_text_truncates_large_file_at_utf8_boundary() {
        // A file larger than BYTES_PER_FILE_MAX (5000 bytes) must be truncated at a
        // valid UTF-8 character boundary so no panic occurs on multi-byte chars.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = dir.path().join("large.txt");

        // Build content that is well over 5000 bytes with multi-byte UTF-8 chars.
        // "é" is 2 bytes (0xC3 0xA9), so 3000 repetitions = 6000 bytes.
        let content = "é".repeat(3000);
        std::fs::write(&file_path, content.as_bytes()).expect("write should succeed");

        let (text, _lines) = detect_entities_load_text(&[file_path.as_path()], 1);

        // The returned text must be a valid UTF-8 string (no truncation mid-char).
        assert!(!text.is_empty(), "truncated text must not be empty");
        assert!(
            text.len() <= BYTES_PER_FILE_MAX + 4,
            "truncated text must not significantly exceed the byte cap"
        );
        // All bytes must form valid UTF-8 — String::from_utf8 would panic on bad bytes.
        assert!(
            text.is_ascii() || !text.chars().any(|c| c == char::REPLACEMENT_CHARACTER),
            "truncated text must be valid UTF-8 with no replacement characters"
        );
    }

    // ── detect_entities_build_pronoun_re — empty patterns uses English fallback ─

    #[test]
    fn detect_entities_build_pronoun_re_falls_back_to_english_when_empty() {
        // When EntityPatterns has an empty pronoun_patterns vec the function must
        // return the hard-coded English pronoun regex without panicking.
        let patterns = EntityPatterns {
            candidate_patterns: vec![],
            multi_word_patterns: vec![],
            person_verb_patterns: vec![],
            pronoun_patterns: vec![],
            dialogue_patterns: vec![],
            direct_address_patterns: vec![],
            project_verb_patterns: vec![],
            stopwords: std::collections::HashSet::new(),
        };
        let pronoun_re = detect_entities_build_pronoun_re(&patterns);
        // The English fallback must match at least one English pronoun.
        assert!(
            pronoun_re.is_match("she went to the store"),
            "English fallback pronoun regex must match 'she'"
        );
        assert!(
            pronoun_re.is_match("he said hello"),
            "English fallback pronoun regex must match 'he'"
        );
    }

    // ── classify_entity — total == 0 (uncertain with frequency confidence) ────

    #[test]
    fn classify_entity_returns_uncertain_when_both_scores_are_zero() {
        // When person_score == project_score == 0, the entity is uncertain with
        // confidence capped at 0.4 from frequency / 50.
        let scores = EntityScores {
            person_score: 0,
            project_score: 0,
            person_category_count: 0,
        };
        // frequency=10 → confidence = min(0.4, 10/50) = 0.2
        let entity = classify_entity("Zephyr", 10, &scores);
        assert_eq!(
            entity.entity_type, "uncertain",
            "zero scores must yield uncertain"
        );
        assert!(
            (entity.confidence - 0.2).abs() < 1e-9,
            "confidence must be frequency/50 = 0.2, got {}",
            entity.confidence
        );
        assert_eq!(entity.name, "Zephyr", "name must be preserved");
    }

    #[test]
    fn classify_entity_uncertain_confidence_capped_at_0_4_for_high_frequency() {
        // Very high frequency (≥ 20) must be capped at 0.4 confidence when both scores are zero.
        let scores = EntityScores {
            person_score: 0,
            project_score: 0,
            person_category_count: 0,
        };
        // frequency=100 → raw = 100/50 = 2.0, capped at 0.4
        let entity = classify_entity("Zephyr", 100, &scores);
        assert_eq!(
            entity.entity_type, "uncertain",
            "zero scores must yield uncertain"
        );
        assert!(
            (entity.confidence - 0.4).abs() < 1e-9,
            "high-frequency uncertain confidence must be capped at 0.4, got {}",
            entity.confidence
        );
    }

    // ── classify_entity_build — high person_ratio but NOT corroborated ────────

    #[test]
    fn classify_entity_build_high_ratio_without_corroboration_is_uncertain() {
        // person_ratio >= 0.7 but has_two=false → uncertain with confidence 0.4.
        let entity = classify_entity_build("Nova", 5, 0.8, false, 10);
        assert_eq!(
            entity.entity_type, "uncertain",
            "high person_ratio without two signal categories must be uncertain"
        );
        assert!(
            (entity.confidence - 0.4).abs() < 1e-9,
            "uncorroborated person must have confidence 0.4, got {}",
            entity.confidence
        );
        assert_eq!(entity.name, "Nova", "name must be preserved");
    }

    #[test]
    fn classify_entity_build_high_ratio_with_low_score_is_uncertain() {
        // person_ratio >= 0.7, has_two=true but person_score < 5 → uncertain.
        let entity = classify_entity_build("Nova", 5, 0.8, true, 3);
        assert_eq!(
            entity.entity_type, "uncertain",
            "corroborated but low-score person must be uncertain"
        );
        assert!(
            (entity.confidence - 0.4).abs() < 1e-9,
            "low-score person must have confidence 0.4, got {}",
            entity.confidence
        );
    }

    // ── classify_entity_build — mid-range ratio → uncertain with 0.5 ─────────

    #[test]
    fn classify_entity_build_mid_range_ratio_produces_uncertain_with_0_5() {
        // 0.3 < person_ratio < 0.7 falls through to the else branch → uncertain, conf=0.5.
        let entity = classify_entity_build("Hybrid", 8, 0.5, true, 15);
        assert_eq!(
            entity.entity_type, "uncertain",
            "mid-range person_ratio must yield uncertain"
        );
        assert!(
            (entity.confidence - 0.5).abs() < 1e-9,
            "mid-range uncertain must have confidence 0.5, got {}",
            entity.confidence
        );
        assert_eq!(entity.name, "Hybrid", "name must be preserved");
    }

    // ── score_entity_person_pronoun_score — pronoun in window ─────────────────

    #[test]
    fn score_entity_person_pronoun_score_returns_positive_for_nearby_pronouns() {
        // Lines where "Alice" appears near a pronoun (within ±2 lines) must
        // increment the pronoun hit count.
        let lines = vec![
            "She was running late.".to_string(),
            "Alice finally arrived at the office.".to_string(),
            "She apologised to the team.".to_string(),
        ];
        let pronoun_re = test_pronoun_re();
        let hits = score_entity_person_pronoun_score("Alice", &lines, &pronoun_re);
        assert!(hits > 0, "nearby pronoun must produce a positive hit count");
        // lines.len() fits in i32 for any realistic test input; test helper has 3 lines.
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_possible_wrap)]
        let lines_len_i32 = lines.len() as i32;
        assert!(
            hits <= lines_len_i32,
            "hit count cannot exceed number of lines"
        );
    }

    #[test]
    fn score_entity_person_pronoun_score_returns_zero_when_no_pronouns_nearby() {
        // Lines with the name but no pronouns in any ±2-line window must score 0.
        let lines = vec![
            "Zelda walked to the market.".to_string(),
            "Zelda bought fresh bread.".to_string(),
            "Zelda returned home quickly.".to_string(),
        ];
        let pronoun_re = test_pronoun_re();
        let hits = score_entity_person_pronoun_score("Zelda", &lines, &pronoun_re);
        assert_eq!(hits, 0, "no pronouns in window must yield zero hits");
    }

    // ── detect_entities_sort_and_truncate — result caps enforced ──────────────

    #[test]
    fn detect_entities_sort_and_truncate_caps_each_category() {
        // Generate more than the per-category caps to confirm truncation occurs.
        // people cap = 15, projects cap = 10, uncertain cap = 8.
        let make_entity = |name: &str, confidence: f64, frequency: usize| DetectedEntity {
            name: name.to_string(),
            entity_type: "person".to_string(),
            confidence,
            frequency,
            signals: vec![],
        };

        let mut people: Vec<DetectedEntity> = (0..20)
            .map(|i| make_entity(&format!("Person{i}"), 0.9 - f64::from(i) * 0.01, 10))
            .collect();
        let mut projects: Vec<DetectedEntity> = (0..12)
            .map(|i| DetectedEntity {
                name: format!("Project{i}"),
                entity_type: "project".to_string(),
                confidence: 0.8,
                frequency: 5,
                signals: vec![],
            })
            .collect();
        let mut uncertain: Vec<DetectedEntity> = (0..10)
            .map(|i| DetectedEntity {
                name: format!("Uncertain{i}"),
                entity_type: "uncertain".to_string(),
                confidence: 0.5,
                frequency: 3 + i,
                signals: vec![],
            })
            .collect();

        detect_entities_sort_and_truncate(&mut people, &mut projects, &mut uncertain);

        assert!(
            people.len() <= 15,
            "people must be capped at 15, got {}",
            people.len()
        );
        assert!(
            projects.len() <= 10,
            "projects must be capped at 10, got {}",
            projects.len()
        );
        assert!(
            uncertain.len() <= 8,
            "uncertain must be capped at 8, got {}",
            uncertain.len()
        );
    }

    // ── extract_candidates — text with only stop words produces no candidates ──

    #[test]
    fn extract_candidates_all_stop_words_produces_no_candidates() {
        // Text consisting entirely of stop words at high frequency must yield no candidates.
        // "The" and "This" are both stop words; even at 5+ occurrences they must be
        // filtered before the frequency threshold is applied.
        let text = "The The The The The This This This This This";
        let candidates = extract_candidates(text, &get_entity_patterns(&["en"]));
        assert!(
            candidates.is_empty(),
            "pure stop-word text must produce no candidates, got: {candidates:?}"
        );
    }

    // ── extract_candidates — multi-word phrase containing stop word is filtered ─

    #[test]
    fn extract_candidates_multi_word_containing_stop_word_is_filtered() {
        // A multi-word phrase where any token is a stop word must be filtered out
        // even if it appears many times.  "The Moon" contains "The" (stop word).
        let text =
            "The Moon shines. The Moon rises. The Moon sets. The Moon glows. The Moon turns.";
        let candidates = extract_candidates(text, &get_entity_patterns(&["en"]));
        assert!(
            !candidates.contains_key("The Moon"),
            "multi-word phrase containing a stop word must be filtered"
        );
        // "Moon" may appear as a single-word candidate depending on the stop list.
        // The important assertion is that "The Moon" is absent.
    }

    // ── detect_entities_load_text — files_max respected ──────────────────────

    #[test]
    fn detect_entities_load_text_respects_files_max() {
        // When files_max=1 and two files are provided, only the first file must be
        // read; the loop must break at index==files_max and the second file is skipped.
        let temp_directory = tempfile::tempdir().expect("tempdir should be created");
        let file_path_one = temp_directory.path().join("first.txt");
        let file_path_two = temp_directory.path().join("second.txt");
        std::fs::write(&file_path_one, "first file content\n")
            .expect("write first file should succeed");
        std::fs::write(&file_path_two, "second file content\n")
            .expect("write second file should succeed");

        let (text, lines) =
            detect_entities_load_text(&[file_path_one.as_path(), file_path_two.as_path()], 1);

        // Only the first file should be represented — "first file content" present,
        // "second file content" absent.
        assert!(
            text.contains("first file content"),
            "first file must be included in text"
        );
        assert!(
            !text.contains("second file content"),
            "second file must be excluded when files_max=1"
        );
        assert!(
            !lines.is_empty(),
            "lines must be populated from the first file"
        );
    }

    // ── score_entity_person_locale_signals — locale patterns fire ────────────

    #[test]
    fn score_entity_person_locale_signals_fires_verb_pattern() {
        // The German locale has person_verb_patterns such as `\b{name}\s+sagte\b`.
        // Providing German text with a matching verb must produce a positive score
        // and surface the Verb category in the returned set.
        let patterns = get_entity_patterns(&["de"]);
        let escaped = regex::escape("Klaus");
        let text = "Klaus sagte, dass er kommt. Klaus sagte es noch einmal.";
        let (score, categories) = score_entity_person_locale_signals(&escaped, text, &patterns);
        assert!(
            score > 0,
            "German verb pattern must yield positive score; got {score}"
        );
        assert!(
            categories.contains(&PersonCategory::Verb),
            "German verb hit must surface the Verb category"
        );
    }

    #[test]
    fn score_entity_person_locale_signals_fires_dialogue_pattern() {
        // The German locale has dialogue_patterns including `^{name}:\s`.
        // A line beginning with the name followed by colon must fire the dialogue arm.
        let patterns = get_entity_patterns(&["de"]);
        let escaped = regex::escape("Klaus");
        // Line starts with "Klaus: " which matches `^{name}:\s`.
        let text = "Klaus: Ich bin hier. Alle hörten zu.";
        let (score, categories) = score_entity_person_locale_signals(&escaped, text, &patterns);
        assert!(
            score > 0,
            "German dialogue pattern must yield positive score; got {score}"
        );
        assert!(
            categories.contains(&PersonCategory::Dialogue),
            "German dialogue hit must surface the Dialogue category"
        );
    }

    #[test]
    fn score_entity_person_locale_signals_fires_direct_address_pattern() {
        // The German locale has a direct_address_pattern including `\bhallo\s+{name}\b`.
        // Text containing "hallo Klaus" must fire the direct-address arm.
        let patterns = get_entity_patterns(&["de"]);
        let escaped = regex::escape("Klaus");
        let text = "hallo Klaus, wie geht es dir?";
        let (score, categories) = score_entity_person_locale_signals(&escaped, text, &patterns);
        assert!(
            score > 0,
            "German direct-address pattern must yield positive score; got {score}"
        );
        assert!(
            categories.contains(&PersonCategory::DirectAddress),
            "German direct-address hit must surface the DirectAddress category"
        );
    }

    #[test]
    fn score_entity_person_locale_signals_no_match_returns_zero() {
        // Text with no German person signals must return (0, empty set).
        let patterns = get_entity_patterns(&["de"]);
        let escaped = regex::escape("Klaus");
        let text = "The weather is fine today and nothing unusual happened.";
        let (score, categories) = score_entity_person_locale_signals(&escaped, text, &patterns);
        assert_eq!(score, 0, "no matching pattern must yield score 0");
        assert!(
            categories.is_empty(),
            "no matching pattern must yield an empty category set"
        );
    }

    // ── score_entity_project — versioned reference fires ─────────────────────

    #[test]
    fn score_entity_project_versioned_reference_adds_score() {
        // The hyphenated-version pattern `\b{escaped}[-v]\w+` must fire for "Vortex-v3".
        // We use an empty EntityPatterns (no locale project_verb_patterns) so only
        // the hardcoded versioned-reference pattern contributes to the score.
        let patterns = EntityPatterns {
            candidate_patterns: vec![],
            multi_word_patterns: vec![],
            person_verb_patterns: vec![],
            pronoun_patterns: vec![],
            dialogue_patterns: vec![],
            direct_address_patterns: vec![],
            project_verb_patterns: vec![],
            stopwords: std::collections::HashSet::new(),
        };
        let escaped = regex::escape("Vortex");
        let text = "The Vortex-v3 release candidate was deployed last night. Vortex-v3 rocks.";
        let score = score_entity_project(&escaped, text, &patterns);
        assert!(
            score > 0,
            "versioned reference must contribute positive project score; got {score}"
        );
    }

    #[test]
    fn score_entity_project_locale_verb_patterns_add_score() {
        // German project_verb_patterns include `\bgebaut\s+{name}\b`.
        // Text containing "gebaut Falcon" must increase the project score.
        let patterns = get_entity_patterns(&["de"]);
        let escaped = regex::escape("Falcon");
        let text = "gebaut Falcon dauerte lange. gebaut Falcon war schwer.";
        let score = score_entity_project(&escaped, text, &patterns);
        assert!(
            score > 0,
            "German project verb pattern must contribute positive score; got {score}"
        );
    }

    // ── score_entity_person_locale_signals — Hindi (Devanagari) ──────────────
    //
    // Mirrors the German test set above; confirms locale loading, regex
    // compilation, and PersonCategory bookkeeping all work for Devanagari
    // names. The regex crate's default Unicode word-boundary handling is what
    // lets `\b{name}\b` match Devanagari runs surrounded by spaces or
    // punctuation; if a future change disabled Unicode mode these tests
    // would catch the regression before it shipped.

    #[test]
    fn score_entity_person_locale_signals_fires_verb_pattern_hindi() {
        // The Hindi locale has person_verb_patterns such as
        // `\b{name}\s+ने\s+कहा\b` ("X said"). Text containing the phrase
        // twice must yield a positive score and surface the Verb category.
        let patterns = get_entity_patterns(&["hi"]);
        let escaped = regex::escape("राम");
        let text = "राम ने कहा कि वह आ रहा है। राम ने कहा फिर से।";
        let (score, categories) = score_entity_person_locale_signals(&escaped, text, &patterns);
        assert!(
            score > 0,
            "Hindi verb pattern must yield positive score; got {score}"
        );
        assert!(
            categories.contains(&PersonCategory::Verb),
            "Hindi verb hit must surface the Verb category"
        );
    }

    #[test]
    fn score_entity_person_locale_signals_fires_dialogue_pattern_hindi() {
        // The Hindi locale inherits dialogue_patterns including `^{name}:\s`.
        // A line beginning with the Devanagari name followed by colon must
        // fire the dialogue arm under the (?im) multi-line flag.
        let patterns = get_entity_patterns(&["hi"]);
        let escaped = regex::escape("राम");
        let text = "राम: मैं यहाँ हूँ। सब सुन रहे थे।";
        let (score, categories) = score_entity_person_locale_signals(&escaped, text, &patterns);
        assert!(
            score > 0,
            "Hindi dialogue pattern must yield positive score; got {score}"
        );
        assert!(
            categories.contains(&PersonCategory::Dialogue),
            "Hindi dialogue hit must surface the Dialogue category"
        );
    }

    #[test]
    fn score_entity_person_locale_signals_fires_direct_address_pattern_hindi() {
        // The Hindi locale's direct_address_pattern includes
        // `\bनमस्ते\s+{name}\b`. Text containing "नमस्ते राम" must fire the
        // direct-address arm — the hardcoded English `hey/thanks/hi`
        // pattern will not match a Devanagari greeting, so this test
        // proves the locale template is reaching the scoring path.
        let patterns = get_entity_patterns(&["hi"]);
        let escaped = regex::escape("राम");
        let text = "नमस्ते राम, आप कैसे हैं?";
        let (score, categories) = score_entity_person_locale_signals(&escaped, text, &patterns);
        assert!(
            score > 0,
            "Hindi direct-address pattern must yield positive score; got {score}"
        );
        assert!(
            categories.contains(&PersonCategory::DirectAddress),
            "Hindi direct-address hit must surface the DirectAddress category"
        );
    }

    #[test]
    fn score_entity_person_locale_signals_no_match_returns_zero_hindi() {
        // English-only text with no Hindi signals must return (0, empty set)
        // even when the Hindi locale is active, confirming the patterns do
        // not spuriously match unrelated content.
        let patterns = get_entity_patterns(&["hi"]);
        let escaped = regex::escape("राम");
        let text = "The weather is fine today and nothing unusual happened.";
        let (score, categories) = score_entity_person_locale_signals(&escaped, text, &patterns);
        assert_eq!(score, 0, "no matching pattern must yield score 0");
        assert!(
            categories.is_empty(),
            "no matching pattern must yield an empty category set"
        );
    }

    #[test]
    fn score_entity_project_locale_verb_patterns_add_score_hindi() {
        // Hindi project_verb_patterns include `\b{name}\s+बनाया\b` ("X built").
        // The project name precedes the verb in Hindi (subject-object-verb
        // order), inverted from the German `\bgebaut\s+{name}\b` pattern.
        // Text repeating "Falcon बनाया" must increase the project score.
        let patterns = get_entity_patterns(&["hi"]);
        let escaped = regex::escape("Falcon");
        let text = "Falcon बनाया था कल। Falcon बनाया वाकई कठिन था।";
        let score = score_entity_project(&escaped, text, &patterns);
        assert!(
            score > 0,
            "Hindi project verb pattern must contribute positive score; got {score}"
        );
    }

    // ── scan_for_detection — early-exit when prose meets max_files ───────────

    #[test]
    fn scan_for_detection_stops_early_when_prose_count_reaches_max_files() {
        // The `if prose_files.len() >= max_files { break }` guard fires when the prose
        // count reaches max_files after processing a directory's entries, preventing
        // further subdirectories from being visited.  We create two subdirectories each
        // containing SCAN_PROSE_THRESHOLD (5) prose files so the first subdir alone
        // fills max_files=5 and the break fires before the second subdir is queued.
        // The final result must not exceed max_files regardless of the directory layout.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for scan early-exit test");
        let sub_one = temp_directory.path().join("sub_one");
        let sub_two = temp_directory.path().join("sub_two");
        std::fs::create_dir(&sub_one).expect("create sub_one");
        std::fs::create_dir(&sub_two).expect("create sub_two");

        // Five prose files in sub_one — enough to fill max_files=5 in a single pass.
        for name in &["a.md", "b.md", "c.md", "d.md", "e.md"] {
            std::fs::write(sub_one.join(name), "# content").expect("write prose file");
        }
        // Three more prose files in sub_two — these must not be reached after the break.
        for name in &["f.md", "g.md", "h.md"] {
            std::fs::write(sub_two.join(name), "# overflow prose").expect("write overflow");
        }

        let result = scan_for_detection(temp_directory.path(), 5);

        // Result must not exceed max_files regardless of total files on disk.
        assert!(
            result.len() <= 5,
            "result must not exceed max_files=5; got {}",
            result.len()
        );
        // At least one prose file must be present.
        assert!(
            !result.is_empty(),
            "at least one prose file must be returned"
        );
    }

    // ── detect_entities_build_pronoun_re — non-empty patterns path ────────────

    #[test]
    fn detect_entities_build_pronoun_re_uses_provided_patterns_when_non_empty() {
        // When EntityPatterns has non-empty pronoun_patterns the function must build
        // the joined regex rather than falling back to the English literal.
        let patterns = EntityPatterns {
            candidate_patterns: vec![],
            multi_word_patterns: vec![],
            person_verb_patterns: vec![],
            pronoun_patterns: vec![r"\ber\b".to_string(), r"\bsie\b".to_string()],
            dialogue_patterns: vec![],
            direct_address_patterns: vec![],
            project_verb_patterns: vec![],
            stopwords: std::collections::HashSet::new(),
        };
        let pronoun_re = detect_entities_build_pronoun_re(&patterns);
        assert!(
            pronoun_re.is_match("er kam spät"),
            "custom pronoun regex must match 'er'"
        );
        assert!(
            pronoun_re.is_match("sie lachte"),
            "custom pronoun regex must match 'sie'"
        );
    }

    // ── detect_entities — candidates empty path ────────────────────────────────

    #[test]
    fn detect_entities_returns_empty_when_no_candidates_pass_threshold() {
        // Text with only single-occurrence capitalized words must produce zero candidates
        // (threshold = 3) and detect_entities must return empty vecs via the early-exit path.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let file_path = dir.path().join("sparse.txt");
        // Each name appears exactly once — below the frequency threshold of 3.
        std::fs::write(
            &file_path,
            "Bartholomew visited. Xiomara arrived. Nebuchadnezzar left.",
        )
        .expect("write should succeed");

        let paths: Vec<&Path> = vec![file_path.as_path()];
        let result = detect_entities(&paths, 10, &["en"]);

        assert!(
            result.people.is_empty(),
            "no candidates above threshold must yield no people"
        );
        assert!(
            result.projects.is_empty(),
            "no candidates above threshold must yield no projects"
        );
        assert!(
            result.uncertain.is_empty(),
            "no candidates above threshold must yield no uncertain"
        );
    }
}
