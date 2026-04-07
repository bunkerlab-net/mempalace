//! Memory type classifier — extracts and classifies memories into five types:
//! decision, preference, milestone, problem, and emotional.

pub mod markers;

use std::collections::HashSet;

use regex::Regex;

use markers::{
    DECISION_MARKERS, EMOTION_MARKERS, MILESTONE_MARKERS, PREFERENCE_MARKERS, PROBLEM_MARKERS,
};

/// A classified memory extracted from text.
pub struct Memory {
    /// The extracted text content.
    pub content: String,
    /// Classification: `"decision"`, `"preference"`, `"milestone"`, `"problem"`, or `"emotional"`.
    pub kind: String,
    /// Sequential index among extracted memories.
    pub chunk_index: usize,
}

/// Extract memories from text, classifying into 5 types:
/// decision, preference, milestone, problem, emotional.
pub fn extract_memories(text: &str, min_confidence: f64) -> Vec<Memory> {
    let segments = split_into_segments(text);
    let mut memories = Vec::new();

    let all_markers: &[(&str, &[&str])] = &[
        ("decision", DECISION_MARKERS),
        ("preference", PREFERENCE_MARKERS),
        ("milestone", MILESTONE_MARKERS),
        ("problem", PROBLEM_MARKERS),
        ("emotional", EMOTION_MARKERS),
    ];

    for para in &segments {
        if para.trim().len() < 20 {
            continue;
        }

        let prose = extract_prose(para);

        // Score against all types
        let mut scores: Vec<(&str, f64)> = Vec::new();
        for &(mem_type, markers) in all_markers {
            let score = score_markers(&prose, markers);
            if score > 0.0 {
                scores.push((mem_type, score));
            }
        }

        if scores.is_empty() {
            continue;
        }

        // Length bonus
        let length_bonus = if para.len() > 500 {
            2.0
        } else if para.len() > 200 {
            1.0
        } else {
            0.0
        };

        let (max_type, max_score) = scores
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).expect("scores contain no NaN"))
            .expect("scores is non-empty");
        let max_score = max_score + length_bonus;

        // Disambiguate
        let score_map: std::collections::HashMap<&str, f64> = scores.iter().copied().collect();
        let final_type = disambiguate(max_type, &prose, &score_map);

        // Confidence
        let confidence = (max_score / 5.0).min(1.0);
        if confidence < min_confidence {
            continue;
        }

        memories.push(Memory {
            content: para.trim().to_string(),
            kind: final_type.to_string(),
            chunk_index: memories.len(),
        });
    }

    memories
}

/// Score text against regex markers.
fn score_markers(text: &str, markers: &[&str]) -> f64 {
    let text_lower = text.to_lowercase();
    let mut score = 0.0;
    for marker in markers {
        if let Ok(re) = Regex::new(marker) {
            let count = re.find_iter(&text_lower).count();
            // Regex match count; always small enough for exact f64 representation
            #[allow(clippy::cast_precision_loss)]
            {
                score += count as f64;
            }
        }
    }
    score
}

/// Disambiguate memory type using sentiment and resolution.
fn disambiguate<'a>(
    memory_type: &'a str,
    text: &str,
    scores: &std::collections::HashMap<&str, f64>,
) -> &'a str {
    if memory_type != "problem" {
        return memory_type;
    }

    let sentiment = get_sentiment(text);
    let has_res = has_resolution(text);

    // Resolved problems are milestones
    if has_res {
        if *scores.get("emotional").unwrap_or(&0.0) > 0.0 && sentiment == "positive" {
            return "emotional";
        }
        return "milestone";
    }

    // Problem + positive sentiment => milestone or emotional
    if sentiment == "positive" {
        if *scores.get("milestone").unwrap_or(&0.0) > 0.0 {
            return "milestone";
        }
        if *scores.get("emotional").unwrap_or(&0.0) > 0.0 {
            return "emotional";
        }
    }

    memory_type
}

fn get_sentiment(text: &str) -> &'static str {
    let positive: HashSet<&str> = [
        "pride",
        "proud",
        "joy",
        "happy",
        "love",
        "loving",
        "beautiful",
        "amazing",
        "wonderful",
        "incredible",
        "fantastic",
        "brilliant",
        "perfect",
        "excited",
        "thrilled",
        "grateful",
        "warm",
        "breakthrough",
        "success",
        "works",
        "working",
        "solved",
        "fixed",
        "nailed",
        "heart",
        "hug",
        "precious",
        "adore",
    ]
    .into();

    let negative: HashSet<&str> = [
        "bug",
        "error",
        "crash",
        "crashing",
        "crashed",
        "fail",
        "failed",
        "failing",
        "failure",
        "broken",
        "broke",
        "breaking",
        "breaks",
        "issue",
        "problem",
        "wrong",
        "stuck",
        "blocked",
        "unable",
        "impossible",
        "missing",
        "terrible",
        "horrible",
        "awful",
        "worse",
        "worst",
        "panic",
        "disaster",
        "mess",
    ]
    .into();

    let words: HashSet<String> = text
        .split(|c: char| !c.is_alphanumeric())
        .map(str::to_lowercase)
        .collect();

    let pos = words
        .iter()
        .filter(|w| positive.contains(w.as_str()))
        .count();
    let neg = words
        .iter()
        .filter(|w| negative.contains(w.as_str()))
        .count();

    match pos.cmp(&neg) {
        std::cmp::Ordering::Greater => "positive",
        std::cmp::Ordering::Less => "negative",
        std::cmp::Ordering::Equal => "neutral",
    }
}

fn has_resolution(text: &str) -> bool {
    let text_lower = text.to_lowercase();
    let patterns = [
        r"\bfixed\b",
        r"\bsolved\b",
        r"\bresolved\b",
        r"\bpatched\b",
        r"\bgot it working\b",
        r"\bit works\b",
        r"\bnailed it\b",
        r"\bfigured (it )?out\b",
        r"\bthe (fix|answer|solution)\b",
    ];
    patterns.iter().any(|p| {
        Regex::new(p)
            .map(|re| re.is_match(&text_lower))
            .unwrap_or(false)
    })
}

/// Extract only prose lines (skip code blocks and code-like lines).
fn extract_prose(text: &str) -> String {
    let code_patterns: Vec<Regex> = [
        r"^\s*[\$#]\s",
        r"^\s*(cd|source|echo|export|pip|npm|git|python|bash|curl|wget|mkdir|rm|cp|mv|ls|cat|grep|find|chmod|sudo|brew|docker)\s",
        r"^\s*```",
        r"^\s*(import|from|def|class|function|const|let|var|return)\s",
        r"^\s*[A-Z_]{2,}=",
        r"^\s*\|",
        r"^\s*[-]{2,}",
        r"^\s*[\{\}\[\]]\s*$",
        r"^\s*(if|for|while|try|except|elif|else:)\b",
        r"^\s*\w+\.\w+\(",
        r"^\s*\w+ = \w+\.\w+",
    ]
    .iter()
    .filter_map(|p| Regex::new(p).ok())
    .collect();

    let mut prose = Vec::new();
    let mut in_code = false;

    for line in text.lines() {
        let stripped = line.trim();
        if stripped.starts_with("```") {
            in_code = !in_code;
            continue;
        }
        if in_code {
            continue;
        }
        if !stripped.is_empty() && !code_patterns.iter().any(|re| re.is_match(stripped)) {
            prose.push(line);
        }
    }

    let result = prose.join("\n").trim().to_string();
    if result.is_empty() {
        text.to_string()
    } else {
        result
    }
}

/// Split text into segments for memory extraction.
fn split_into_segments(text: &str) -> Vec<String> {
    let lines: Vec<&str> = text.lines().collect();

    let turn_patterns: Vec<Regex> = [
        r"^>\s",
        r"(?i)^(Human|User|Q)\s*:",
        r"(?i)^(Assistant|AI|A|Claude|ChatGPT)\s*:",
    ]
    .iter()
    .filter_map(|p| Regex::new(p).ok())
    .collect();

    let turn_count = lines
        .iter()
        .filter(|line| {
            let stripped = line.trim();
            turn_patterns.iter().any(|re| re.is_match(stripped))
        })
        .count();

    // If enough turn markers, split by turns
    if turn_count >= 3 {
        return split_by_turns(&lines, &turn_patterns);
    }

    // Fallback: paragraph splitting
    let paragraphs: Vec<String> = text
        .split("\n\n")
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();

    // If single giant block, chunk by line groups
    if paragraphs.len() <= 1 && lines.len() > 20 {
        return lines
            .chunks(25)
            .map(|chunk| chunk.join("\n"))
            .filter(|s| !s.trim().is_empty())
            .collect();
    }

    paragraphs
}

fn split_by_turns(lines: &[&str], turn_patterns: &[Regex]) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current: Vec<&str> = Vec::new();

    for line in lines {
        let stripped = line.trim();
        let is_turn = turn_patterns.iter().any(|re| re.is_match(stripped));

        if is_turn && !current.is_empty() {
            segments.push(current.join("\n"));
            current = vec![line];
        } else {
            current.push(line);
        }
    }

    if !current.is_empty() {
        segments.push(current.join("\n"));
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_decision() {
        let text = "We decided to use GraphQL instead of REST because it gives better flexibility for our frontend queries.";
        let memories = extract_memories(text, 0.1);
        assert!(!memories.is_empty());
        assert_eq!(memories[0].kind, "decision");
    }

    #[test]
    fn test_extract_problem_resolved_becomes_milestone() {
        let text = "The bug was that the database connection was timing out. After investigation, we fixed it by increasing the pool size.";
        let memories = extract_memories(text, 0.1);
        assert!(!memories.is_empty());
        // Resolved problem should be reclassified as milestone
        assert_eq!(memories[0].kind, "milestone");
    }

    #[test]
    fn test_extract_prose_strips_code_blocks() {
        let text = "Some prose here.\n```\nlet x = 1;\n```\nMore prose.";
        let result = extract_prose(text);
        assert!(result.contains("Some prose here."));
        assert!(result.contains("More prose."));
        assert!(!result.contains("let x = 1"));
    }

    #[test]
    fn test_get_sentiment_positive() {
        assert_eq!(
            get_sentiment("I'm so proud and excited about this breakthrough"),
            "positive"
        );
    }

    #[test]
    fn test_get_sentiment_negative() {
        assert_eq!(
            get_sentiment("The bug crashed everything and it's broken"),
            "negative"
        );
    }

    #[test]
    fn test_get_sentiment_neutral() {
        assert_eq!(get_sentiment("The meeting is at three o'clock"), "neutral");
    }

    #[test]
    fn test_has_resolution_true() {
        assert!(has_resolution("We fixed the issue by updating the config"));
        assert!(has_resolution("After debugging, I figured it out"));
        assert!(has_resolution("The solution was to increase pool size"));
    }

    #[test]
    fn test_has_resolution_false() {
        assert!(!has_resolution("The system is still broken"));
        assert!(!has_resolution("We need to investigate further"));
    }

    #[test]
    fn test_split_into_segments_by_paragraphs() {
        let text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph.";
        let segments = split_into_segments(text);
        assert_eq!(segments.len(), 3);
    }

    #[test]
    fn test_split_into_segments_by_turns() {
        let text = "> question one\nanswer one\n> question two\nanswer two\n> question three\nanswer three";
        let segments = split_into_segments(text);
        assert!(segments.len() >= 3);
    }

    #[test]
    fn test_extract_emotional() {
        let text = "I'm so proud of what we built together. It's beautiful and amazing to see it all come together.";
        let memories = extract_memories(text, 0.1);
        assert!(!memories.is_empty());
        assert_eq!(memories[0].kind, "emotional");
    }
}
