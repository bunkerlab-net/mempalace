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

// Large static stopword list — line count reflects data volume, not code complexity.
#[allow(clippy::too_many_lines)]
fn stopwords() -> HashSet<&'static str> {
    HashSet::from([
        "the",
        "a",
        "an",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "as",
        "is",
        "was",
        "are",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "must",
        "shall",
        "can",
        "this",
        "that",
        "these",
        "those",
        "it",
        "its",
        "they",
        "them",
        "their",
        "we",
        "our",
        "you",
        "your",
        "i",
        "my",
        "me",
        "he",
        "she",
        "his",
        "her",
        "who",
        "what",
        "when",
        "where",
        "why",
        "how",
        "which",
        "if",
        "then",
        "so",
        "not",
        "no",
        "yes",
        "ok",
        "okay",
        "just",
        "very",
        "really",
        "also",
        "already",
        "still",
        "even",
        "only",
        "here",
        "there",
        "now",
        "too",
        "up",
        "out",
        "about",
        "like",
        "use",
        "get",
        "got",
        "make",
        "made",
        "take",
        "put",
        "come",
        "go",
        "see",
        "know",
        "think",
        "true",
        "false",
        "none",
        "null",
        "new",
        "old",
        "all",
        "any",
        "some",
        "return",
        "print",
        "def",
        "class",
        "import",
        "step",
        "usage",
        "run",
        "check",
        "find",
        "add",
        "set",
        "list",
        "args",
        "dict",
        "str",
        "int",
        "bool",
        "path",
        "file",
        "type",
        "name",
        "note",
        "example",
        "option",
        "result",
        "error",
        "warning",
        "info",
        "every",
        "each",
        "more",
        "less",
        "next",
        "last",
        "first",
        "second",
        "stack",
        "layer",
        "mode",
        "test",
        "stop",
        "start",
        "copy",
        "move",
        "source",
        "target",
        "output",
        "input",
        "data",
        "item",
        "key",
        "value",
        "returns",
        "raises",
        "yields",
        "self",
        "cls",
        "kwargs",
        "world",
        "well",
        "want",
        "topic",
        "choose",
        "social",
        "human",
        "humans",
        "people",
        "things",
        "something",
        "nothing",
        "everything",
        "anything",
        "someone",
        "everyone",
        "anyone",
        "way",
        "time",
        "day",
        "life",
        "place",
        "thing",
        "part",
        "kind",
        "sort",
        "case",
        "point",
        "idea",
        "fact",
        "sense",
        "question",
        "answer",
        "reason",
        "number",
        "version",
        "system",
        "hey",
        "hi",
        "hello",
        "thanks",
        "thank",
        "right",
        "let",
        "click",
        "hit",
        "press",
        "tap",
        "drag",
        "drop",
        "open",
        "close",
        "save",
        "load",
        "launch",
        "install",
        "download",
        "upload",
        "scroll",
        "select",
        "enter",
        "submit",
        "cancel",
        "confirm",
        "delete",
        "paste",
        "write",
        "read",
        "search",
        "show",
        "hide",
        "desktop",
        "documents",
        "downloads",
        "users",
        "home",
        "library",
        "applications",
        "preferences",
        "settings",
        "terminal",
        "actor",
        "vector",
        "remote",
        "control",
        "duration",
        "fetch",
        "agents",
        "tools",
        "others",
        "guards",
        "ethics",
        "regulation",
        "learning",
        "thinking",
        "memory",
        "language",
        "intelligence",
        "technology",
        "society",
        "culture",
        "future",
        "history",
        "science",
        "model",
        "models",
        "network",
        "networks",
        "training",
        "inference",
    ])
}

fn extract_candidates(text: &str) -> HashMap<String, usize> {
    assert!(
        !text.is_empty(),
        "extract_candidates: text must not be empty"
    );
    let stops = stopwords();
    let single_re = Regex::new(r"\b([A-Z][a-z]{1,19})\b").expect("valid regex");
    let multi_re = Regex::new(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b").expect("valid regex");

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

    // Filter: must appear at least 3 times
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

fn score_entity(name: &str, text: &str, lines: &[String]) -> EntityScores {
    assert!(!name.is_empty(), "score_entity: name must not be empty");
    let escaped = regex::escape(name);
    let mut person_score = 0i32;
    let mut project_score = 0i32;
    let mut person_signals = Vec::new();
    let mut project_signals = Vec::new();

    // Person verb patterns
    let person_verbs = [
        "said", "asked", "told", "replied", "laughed", "smiled", "cried", "felt", "thinks?",
        "wants?", "loves?", "hates?", "knows?", "decided", "pushed", "wrote",
    ];
    for verb in person_verbs {
        if let Ok(re) = Regex::new(&format!(r"(?i)\b{escaped}\s+{verb}\b")) {
            let count = re.find_iter(text).count();
            if count > 0 {
                person_score += i32::try_from(count).unwrap_or(i32::MAX) * 2;
                person_signals.push(format!("'{name} {verb}' ({count}x)"));
            }
        }
    }

    // Dialogue patterns
    let dialogue_pats = [
        format!(r"(?im)^>\s*{escaped}[:\s]"),
        format!(r"(?im)^{escaped}:\s"),
        format!(r"(?im)^\[{escaped}\]"),
    ];
    for pat in &dialogue_pats {
        if let Ok(re) = Regex::new(pat) {
            let count = re.find_iter(text).count();
            if count > 0 {
                person_score += i32::try_from(count).unwrap_or(i32::MAX) * 3;
                person_signals.push(format!("dialogue marker ({count}x)"));
            }
        }
    }

    // Pronoun proximity
    let name_lower = name.to_lowercase();
    let pronoun_re =
        Regex::new(r"(?i)\b(she|her|hers|he|him|his|they|them|their)\b").expect("valid regex");
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
        person_score += pronoun_hits * 2;
        person_signals.push(format!("pronoun nearby ({pronoun_hits}x)"));
    }

    // Direct address
    if let Ok(re) = Regex::new(&format!(
        r"(?i)\bhey\s+{escaped}\b|\bthanks?\s+{escaped}\b|\bhi\s+{escaped}\b"
    )) {
        let count = re.find_iter(text).count();
        if count > 0 {
            person_score += i32::try_from(count).unwrap_or(i32::MAX) * 4;
            person_signals.push(format!("addressed directly ({count}x)"));
        }
    }

    // Project verb patterns
    let project_verbs = [
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
    for pat in &project_verbs {
        if let Ok(re) = Regex::new(pat) {
            let count = re.find_iter(text).count();
            if count > 0 {
                project_score += i32::try_from(count).unwrap_or(i32::MAX) * 2;
                project_signals.push(format!("project verb ({count}x)"));
            }
        }
    }

    // Versioned/code reference
    if let Ok(re) = Regex::new(&format!(r"(?i)\b{escaped}[-v]\w+")) {
        let count = re.find_iter(text).count();
        if count > 0 {
            project_score += i32::try_from(count).unwrap_or(i32::MAX) * 3;
            project_signals.push(format!("versioned ({count}x)"));
        }
    }

    person_signals.truncate(3);
    project_signals.truncate(3);

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

    // Count distinct signal categories
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
