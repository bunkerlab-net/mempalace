use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use regex::Regex;

/// A detected entity with classification.
pub struct DetectedEntity {
    pub name: String,
    pub entity_type: String, // "person", "project", or "uncertain"
    pub confidence: f64,
    pub frequency: usize,
    pub signals: Vec<String>,
}

/// Detection results grouped by type.
pub struct DetectionResult {
    pub people: Vec<DetectedEntity>,
    pub projects: Vec<DetectedEntity>,
    pub uncertain: Vec<DetectedEntity>,
}

/// Scan files and detect entity candidates.
pub fn detect_entities(file_paths: &[&Path], max_files: usize) -> DetectionResult {
    assert!(max_files > 0, "detect_entities: max_files must be positive");
    let mut all_text = String::new();
    let mut all_lines = Vec::new();
    let max_bytes_per_file = 5000;

    for (i, path) in file_paths.iter().enumerate() {
        if i >= max_files {
            break;
        }
        if let Ok(content) = fs::read_to_string(path) {
            let truncated = if content.len() > max_bytes_per_file {
                &content[..max_bytes_per_file]
            } else {
                &content
            };
            all_text.push_str(truncated);
            all_text.push('\n');
            all_lines.extend(truncated.lines().map(String::from));
        }
    }

    let candidates = extract_candidates(&all_text);
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
    sorted_candidates.sort_by(|a, b| b.1.cmp(&a.1));

    for (name, frequency) in sorted_candidates {
        let scores = score_entity(&name, &all_text, &all_lines);
        let entity = classify_entity(&name, frequency, &scores);

        match entity.entity_type.as_str() {
            "person" => people.push(entity),
            "project" => projects.push(entity),
            _ => uncertain.push(entity),
        }
    }

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
    uncertain.sort_by(|a, b| b.frequency.cmp(&a.frequency));

    people.truncate(15);
    projects.truncate(10);
    uncertain.truncate(8);

    // Postcondition: result lists are bounded by their truncation limits.
    debug_assert!(people.len() <= 15);
    debug_assert!(projects.len() <= 10);
    debug_assert!(uncertain.len() <= 8);

    DetectionResult {
        people,
        projects,
        uncertain,
    }
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

// Regex literals are compile-time constants that can never fail to compile.
#[allow(clippy::expect_used)]
fn extract_candidates(text: &str) -> HashMap<String, usize> {
    assert!(
        !text.is_empty(),
        "extract_candidates: text must not be empty"
    );
    let stops = extract_candidates_stop_words();
    // Match capitalized words of 2–20 chars. The lower bound (1 lowercase char
    // after the capital) avoids single-letter initials like "I" or "A". The
    // upper bound (19 lowercase chars) rejects long common nouns that happen
    // to be capitalized at sentence starts (e.g. "Congratulations").
    let single_re = Regex::new(r"\b([A-Z][a-z]{1,19})\b").expect(
        "single-word capitalized name regex is a compile-time literal and cannot fail to compile",
    );
    let multi_re = Regex::new(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b").expect(
        "multi-word capitalized name regex is a compile-time literal and cannot fail to compile",
    );

    let mut counts: HashMap<String, usize> = HashMap::new();

    for cap in single_re.captures_iter(text) {
        let word = &cap[1];
        if word.len() > 1 && !stops.contains(word.to_lowercase().as_str()) {
            *counts.entry(word.to_string()).or_insert(0) += 1;
        }
    }

    for cap in multi_re.captures_iter(text) {
        let phrase = &cap[1];
        if !phrase
            .split_whitespace()
            .any(|w| stops.contains(w.to_lowercase().as_str()))
        {
            *counts.entry(phrase.to_string()).or_insert(0) += 1;
        }
    }

    // Require at least 3 occurrences before treating a name as an entity.
    // Once or twice is likely a passing reference; three times suggests the
    // name is a recurring actor or concept worth storing.
    counts.retain(|_, v| *v >= 3);

    // Postcondition: all surviving candidates have frequency >= 3.
    debug_assert!(counts.values().all(|&v| v >= 3));

    counts
}

struct EntityScores {
    person_score: i32,
    project_score: i32,
    person_signals: Vec<String>,
    project_signals: Vec<String>,
}

/// Score person-related signals: verb patterns, dialogue markers, pronouns, direct address.
// Regex literals are compile-time constants that can never fail to compile.
#[allow(clippy::expect_used)]
fn score_entity_person(
    name: &str,
    escaped: &str,
    text: &str,
    lines: &[String],
) -> (i32, Vec<String>) {
    let mut score = 0i32;
    let mut signals = Vec::new();

    let person_verbs = [
        "said", "asked", "told", "replied", "laughed", "smiled", "cried", "felt", "thinks?",
        "wants?", "loves?", "hates?", "knows?", "decided", "pushed", "wrote",
    ];
    for verb in person_verbs {
        if let Ok(re) = Regex::new(&format!(r"(?i)\b{escaped}\s+{verb}\b")) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 2;
                signals.push(format!("'{name} {verb}' ({count}x)"));
            }
        }
    }

    let dialogue_pats = [
        format!(r"(?im)^>\s*{escaped}[:\s]"),
        format!(r"(?im)^{escaped}:\s"),
        format!(r"(?im)^\[{escaped}\]"),
    ];
    for pat in &dialogue_pats {
        if let Ok(re) = Regex::new(pat) {
            let count = re.find_iter(text).count();
            if count > 0 {
                score += i32::try_from(count).unwrap_or(i32::MAX) * 3;
                signals.push(format!("dialogue marker ({count}x)"));
            }
        }
    }

    let name_lower = name.to_lowercase();
    let pronoun_re = Regex::new(r"(?i)\b(she|her|hers|he|him|his|they|them|their)\b")
        .expect("pronoun regex is a compile-time literal and cannot fail to compile");
    let mut pronoun_hits = 0;
    for (i, line) in lines.iter().enumerate() {
        if line.to_lowercase().contains(&name_lower) {
            let start = i.saturating_sub(2);
            let end = (i + 3).min(lines.len());
            let window: String = lines[start..end].join(" ");
            if pronoun_re.is_match(&window) {
                pronoun_hits += 1;
            }
        }
    }
    if pronoun_hits > 0 {
        score += pronoun_hits * 2;
        signals.push(format!("pronoun nearby ({pronoun_hits}x)"));
    }

    if let Ok(re) = Regex::new(&format!(
        r"(?i)\bhey\s+{escaped}\b|\bthanks?\s+{escaped}\b|\bhi\s+{escaped}\b"
    )) {
        let count = re.find_iter(text).count();
        if count > 0 {
            score += i32::try_from(count).unwrap_or(i32::MAX) * 4;
            signals.push(format!("addressed directly ({count}x)"));
        }
    }

    signals.truncate(3);
    (score, signals)
}

/// Score project-related signals: build/deploy verbs, versioned references.
fn score_entity_project(escaped: &str, text: &str) -> (i32, Vec<String>) {
    let mut score = 0i32;
    let mut signals = Vec::new();

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
                signals.push(format!("project verb ({count}x)"));
            }
        }
    }

    if let Ok(re) = Regex::new(&format!(r"(?i)\b{escaped}[-v]\w+")) {
        let count = re.find_iter(text).count();
        if count > 0 {
            score += i32::try_from(count).unwrap_or(i32::MAX) * 3;
            signals.push(format!("versioned ({count}x)"));
        }
    }

    signals.truncate(3);
    (score, signals)
}

fn score_entity(name: &str, text: &str, lines: &[String]) -> EntityScores {
    assert!(!name.is_empty(), "score_entity: name must not be empty");
    let escaped = regex::escape(name);

    let (person_score, person_signals) = score_entity_person(name, &escaped, text, lines);
    let (project_score, project_signals) = score_entity_project(&escaped, text);

    EntityScores {
        person_score,
        project_score,
        person_signals,
        project_signals,
    }
}

fn classify_entity(name: &str, frequency: usize, scores: &EntityScores) -> DetectedEntity {
    let ps = scores.person_score;
    let prs = scores.project_score;
    let total = ps + prs;

    if total == 0 {
        // frequency is a name occurrence count, always small enough for exact f64 representation
        #[allow(clippy::cast_precision_loss)]
        let confidence = (frequency as f64 / 50.0).min(0.4);
        return DetectedEntity {
            name: name.to_string(),
            entity_type: "uncertain".to_string(),
            confidence,
            frequency,
            signals: vec![format!("appears {frequency}x, no strong type signals")],
        };
    }

    let person_ratio = f64::from(ps) / f64::from(total);

    // Count distinct signal categories to distinguish corroborated person signals
    // from pronoun-only matches, which are too weak for a confident classification.
    let mut signal_cats: HashSet<&str> = HashSet::new();
    for s in &scores.person_signals {
        if s.contains("dialogue") {
            signal_cats.insert("dialogue");
        } else if s.contains("action") || s.contains("said") || s.contains("asked") {
            signal_cats.insert("action");
        } else if s.contains("pronoun") {
            signal_cats.insert("pronoun");
        } else if s.contains("addressed") {
            signal_cats.insert("addressed");
        }
    }
    let has_two = signal_cats.len() >= 2;

    classify_entity_build(name, frequency, scores, person_ratio, has_two, ps)
}

/// Build the `DetectedEntity` once `person_ratio` and `has_two` are known.
fn classify_entity_build(
    name: &str,
    frequency: usize,
    scores: &EntityScores,
    person_ratio: f64,
    has_two: bool,
    ps: i32,
) -> DetectedEntity {
    if person_ratio >= 0.7 && has_two && ps >= 5 {
        DetectedEntity {
            name: name.to_string(),
            entity_type: "person".to_string(),
            confidence: (0.5 + person_ratio * 0.5).min(0.99),
            frequency,
            signals: if scores.person_signals.is_empty() {
                vec![format!("appears {frequency}x")]
            } else {
                scores.person_signals.clone()
            },
        }
    } else if person_ratio >= 0.7 && (!has_two || ps < 5) {
        DetectedEntity {
            name: name.to_string(),
            entity_type: "uncertain".to_string(),
            confidence: 0.4,
            frequency,
            signals: {
                let mut s = scores.person_signals.clone();
                s.push(format!("appears {frequency}x — pronoun-only match"));
                s
            },
        }
    } else if person_ratio <= 0.3 {
        DetectedEntity {
            name: name.to_string(),
            entity_type: "project".to_string(),
            confidence: (0.5 + (1.0 - person_ratio) * 0.5).min(0.99),
            frequency,
            signals: if scores.project_signals.is_empty() {
                vec![format!("appears {frequency}x")]
            } else {
                scores.project_signals.clone()
            },
        }
    } else {
        let mut signals: Vec<String> = scores.person_signals.clone();
        signals.extend(scores.project_signals.clone());
        signals.truncate(3);
        signals.push("mixed signals — needs review".to_string());
        DetectedEntity {
            name: name.to_string(),
            entity_type: "uncertain".to_string(),
            confidence: 0.5,
            frequency,
            signals,
        }
    }
}
