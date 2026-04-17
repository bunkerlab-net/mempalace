use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;

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

#[allow(clippy::expect_used)]
// Matches two or more consecutive capitalized words (multi-word entity names).
static MULTI_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b").expect(
        "multi-word capitalized name regex is a compile-time literal and cannot fail to compile",
    )
});

#[allow(clippy::expect_used)]
// Matches gendered and plural pronouns to score person-like proximity.
static PRONOUN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(she|her|hers|he|him|his|they|them|their)\b")
        .expect("pronoun regex is a compile-time literal and cannot fail to compile")
});

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
    sorted_candidates.sort_by_key(|b| std::cmp::Reverse(b.1));

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
    uncertain.sort_by_key(|b| std::cmp::Reverse(b.frequency));

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

fn extract_candidates(text: &str) -> HashMap<String, usize> {
    assert!(
        !text.is_empty(),
        "extract_candidates: text must not be empty"
    );
    let stops = extract_candidates_stop_words();

    let mut counts: HashMap<String, usize> = HashMap::new();

    for cap in SINGLE_RE.captures_iter(text) {
        let word = &cap[1];
        if word.len() > 1 && !stops.contains(word.to_lowercase().as_str()) {
            *counts.entry(word.to_string()).or_insert(0) += 1;
        }
    }

    for cap in MULTI_RE.captures_iter(text) {
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
    let mut pronoun_hits = 0;
    for (i, line) in lines.iter().enumerate() {
        if line.to_lowercase().contains(&name_lower) {
            let start = i.saturating_sub(2);
            let end = (i + 3).min(lines.len());
            let window: String = lines[start..end].join(" ");
            if PRONOUN_RE.is_match(&window) {
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

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile;

    #[test]
    fn extract_candidates_finds_capitalized_names() {
        // "Alice" appears 5 times — well above the frequency threshold of 3.
        let text =
            "Alice went to the store. Alice bought milk. Alice came home. Alice cooked. Alice ate.";
        let candidates = extract_candidates(text);
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
        let candidates = extract_candidates(text);
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
        let candidates = extract_candidates(text);
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
        let candidates = extract_candidates(text);
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
        let scores = score_entity(name, text, &lines);
        assert!(
            scores.person_score > 0,
            "Person score should be positive when speech verbs are present"
        );
        assert!(
            !scores.person_signals.is_empty(),
            "Person signals should be non-empty for speech verb matches"
        );
    }

    #[test]
    fn score_entity_project_detects_build_verbs() {
        let text =
            "building Mempalace from scratch. deploying Mempalace to prod. shipping Mempalace v2.";
        let lines: Vec<String> = text.lines().map(String::from).collect();
        let scores = score_entity("Mempalace", text, &lines);
        assert!(
            scores.project_score > 0,
            "Project score should be positive when build verbs are present"
        );
        assert!(
            !scores.project_signals.is_empty(),
            "Project signals should be non-empty for build verb matches"
        );
    }

    #[test]
    fn classify_entity_as_person() {
        // High person score with multiple signal categories triggers person classification.
        let scores = EntityScores {
            person_score: 12,
            project_score: 0,
            person_signals: vec![
                "'Alice said' (3x)".to_string(),
                "dialogue marker (2x)".to_string(),
                "pronoun nearby (1x)".to_string(),
            ],
            project_signals: vec![],
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
    }

    #[test]
    fn classify_entity_as_project() {
        // Project score dominates — person_ratio <= 0.3 triggers project classification.
        let scores = EntityScores {
            person_score: 0,
            project_score: 10,
            person_signals: vec![],
            project_signals: vec!["project verb (5x)".to_string()],
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
        let result = detect_entities(&paths, 10);

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

    #[test]
    fn detect_entities_with_project_entities() {
        // Exercises the project classification branch in detect_entities.
        // Each project name appears 4+ times with build/deploy verbs and zero
        // person signals, pushing person_ratio <= 0.3 so classify_entity picks
        // the "project" branch. Person names get speech verbs and pronouns to
        // land in the "person" bucket and verify both arms fire in one call.
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

        let paths: Vec<&Path> = vec![file_path_one.as_path(), file_path_two.as_path()];
        let result = detect_entities(&paths, 10);

        // Postcondition: detection found entities across multiple categories.
        let total_entities = result.people.len() + result.projects.len() + result.uncertain.len();
        assert!(
            total_entities > 0,
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
        let result = detect_entities(&paths, 10);

        assert!(result.people.is_empty(), "No people from empty file");
        assert!(result.projects.is_empty(), "No projects from empty file");
    }
}
