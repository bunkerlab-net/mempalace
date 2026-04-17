//! Memory type classifier — extracts and classifies memories into five types:
//! decision, preference, milestone, problem, and emotional.

pub mod markers;

use std::collections::HashSet;
use std::sync::LazyLock;

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

    let all_markers: &[(&str, &[Regex])] = &[
        ("decision", DECISION_REGEXES.as_slice()),
        ("preference", PREFERENCE_REGEXES.as_slice()),
        ("milestone", MILESTONE_REGEXES.as_slice()),
        ("problem", PROBLEM_REGEXES.as_slice()),
        ("emotional", EMOTION_REGEXES.as_slice()),
    ];

    for para in &segments {
        if para.trim().len() < 20 {
            continue;
        }

        let prose = extract_prose(para);
        // Lowercase once here so score_markers can skip the allocation on each of its 5 calls.
        let prose_lower = prose.to_lowercase();

        // Score against all types
        let mut scores: Vec<(&str, f64)> = Vec::new();
        for &(mem_type, markers) in all_markers {
            let score = score_markers(&prose_lower, markers);
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

        // f64 scores come from integer match counts (count as f64); partial_cmp
        // only returns None for NaN, which cannot arise here.
        let Some(&(max_type, max_score)) = scores
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        else {
            // Unreachable: scores is non-empty (checked above at `if scores.is_empty()`).
            continue;
        };
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

    // Postcondition: all memories have non-empty content and kind.
    debug_assert!(memories.iter().all(|m| !m.content.is_empty()));
    debug_assert!(memories.iter().all(|m| !m.kind.is_empty()));

    memories
}

/// Score pre-lowercased text against pre-compiled regex markers.
///
/// Callers must pass an already-lowercased string; this function does not lowercase internally.
fn score_markers(text: &str, markers: &[Regex]) -> f64 {
    let mut score = 0.0;
    for re in markers {
        let count = re.find_iter(text).count();
        // Regex match count; always small enough for exact f64 representation
        #[allow(clippy::cast_precision_loss)]
        {
            score += count as f64;
        }
    }
    // Postcondition: score must be non-negative.
    debug_assert!(score >= 0.0);
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

/// Sentiment word lists — data, not logic.
#[rustfmt::skip]
const POSITIVE_WORDS: &[&str] = &[
    "pride", "proud", "joy", "happy", "love", "loving", "beautiful", "amazing", "wonderful", "incredible", "fantastic",
    "brilliant", "perfect", "excited", "thrilled", "grateful", "warm", "breakthrough", "success", "works", "working",
    "solved", "fixed", "nailed", "heart", "hug", "precious", "adore",
];

#[rustfmt::skip]
const NEGATIVE_WORDS: &[&str] = &[
    "bug", "error", "crash", "crashing", "crashed", "fail", "failed", "failing", "failure", "broken", "broke",
    "breaking", "breaks", "issue", "problem", "wrong", "stuck", "blocked", "unable", "impossible", "missing",
    "terrible", "horrible", "awful", "worse", "worst", "panic", "disaster", "mess",
];

// Built once at first use; POSITIVE_WORDS and NEGATIVE_WORDS are `'static` slices
// so the sets never need to be rebuilt.
static POSITIVE_SET: LazyLock<HashSet<&str>> =
    LazyLock::new(|| POSITIVE_WORDS.iter().copied().collect());
static NEGATIVE_SET: LazyLock<HashSet<&str>> =
    LazyLock::new(|| NEGATIVE_WORDS.iter().copied().collect());

// Compile a slice of pattern strings into Regexes, panicking on the first
// invalid pattern. All callers pass compile-time literals; a panic here is a
// startup invariant failure, not an operating error.
#[allow(clippy::expect_used)]
fn compile_regexes(patterns: &[&str]) -> Vec<Regex> {
    patterns
        .iter()
        .map(|p| {
            Regex::new(p)
                .expect("regex pattern is a compile-time literal and cannot fail to compile")
        })
        .collect()
}

// Marker patterns compiled once; each static serves the corresponding score_markers() call.
static DECISION_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| compile_regexes(DECISION_MARKERS));
static PREFERENCE_REGEXES: LazyLock<Vec<Regex>> =
    LazyLock::new(|| compile_regexes(PREFERENCE_MARKERS));
static MILESTONE_REGEXES: LazyLock<Vec<Regex>> =
    LazyLock::new(|| compile_regexes(MILESTONE_MARKERS));
static PROBLEM_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| compile_regexes(PROBLEM_MARKERS));
static EMOTION_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| compile_regexes(EMOTION_MARKERS));

// Patterns compiled once; used by has_resolution().
static RESOLUTION_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    compile_regexes(&[
        r"\bfixed\b",
        r"\bsolved\b",
        r"\bresolved\b",
        r"\bpatched\b",
        r"\bgot it working\b",
        r"\bit works\b",
        r"\bnailed it\b",
        r"\bfigured (it )?out\b",
        r"\bthe (fix|answer|solution)\b",
    ])
});

// Patterns compiled once; used by extract_prose().
// Note: `else\b:` matches `else:` correctly — `\b` asserts a word boundary
// between the keyword and the following `:`, which is not a word character.
static PROSE_CODE_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    compile_regexes(&[
        r"^\s*[\$#]\s",
        r"^\s*(cd|source|echo|export|pip|npm|git|python|bash|curl|wget|mkdir|rm|cp|mv|ls|cat|grep|find|chmod|sudo|brew|docker)\s",
        r"^\s*```",
        r"^\s*(import|from|def|class|function|const|let|var|return)\s",
        r"^\s*[A-Z_]{2,}=",
        r"^\s*\|",
        r"^\s*[-]{2,}",
        r"^\s*[\{\}\[\]]\s*$",
        r"^\s*(if|for|while|try|except|elif|else)\b:",
        r"^\s*\w+\.\w+\(",
        r"^\s*\w+ = \w+\.\w+",
    ])
});

// Patterns compiled once; used by split_into_segments() and split_by_turns().
static TURN_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    compile_regexes(&[
        r"^>\s",
        r"(?i)^(Human|User|Q)\s*:",
        r"(?i)^(Assistant|AI|A|Claude|ChatGPT)\s*:",
    ])
});

fn get_sentiment(text: &str) -> &'static str {
    let words: HashSet<String> = text
        .split(|c: char| !c.is_alphanumeric())
        .map(str::to_lowercase)
        .collect();

    let pos = words
        .iter()
        .filter(|w| POSITIVE_SET.contains(w.as_str()))
        .count();
    let neg = words
        .iter()
        .filter(|w| NEGATIVE_SET.contains(w.as_str()))
        .count();

    let result = match pos.cmp(&neg) {
        std::cmp::Ordering::Greater => "positive",
        std::cmp::Ordering::Less => "negative",
        std::cmp::Ordering::Equal => "neutral",
    };

    // Postcondition: result is one of the three known sentiment values.
    debug_assert!(
        result == "positive" || result == "negative" || result == "neutral",
        "get_sentiment returned unknown value: {result}"
    );

    result
}

fn has_resolution(text: &str) -> bool {
    let text_lower = text.to_lowercase();
    RESOLUTION_REGEXES.iter().any(|re| re.is_match(&text_lower))
}

/// Extract only prose lines (skip code blocks and code-like lines).
fn extract_prose(text: &str) -> String {
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
        if !stripped.is_empty() && !PROSE_CODE_REGEXES.iter().any(|re| re.is_match(stripped)) {
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

    let turn_count = lines
        .iter()
        .filter(|line| {
            let stripped = line.trim();
            TURN_REGEXES.iter().any(|re| re.is_match(stripped))
        })
        .count();

    // If enough turn markers, split by turns
    if turn_count >= 3 {
        return split_by_turns(&lines, TURN_REGEXES.as_slice());
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

    #[test]
    fn test_extract_preference() {
        // Preference markers like "I prefer" and "always use" must classify as preference.
        let text = "I prefer snake_case for all variable names. I always use functional style over imperative when possible.";
        let memories = extract_memories(text, 0.1);
        assert!(
            !memories.is_empty(),
            "preference text must produce at least one memory"
        );
        assert_eq!(
            memories[0].kind, "preference",
            "text with preference markers must classify as preference"
        );
    }

    #[test]
    fn test_extract_unresolved_problem() {
        // A problem description without resolution words must stay classified as "problem".
        let text = "The bug is causing the database connection to crash repeatedly. The system keeps failing under load and we need to investigate further.";
        let memories = extract_memories(text, 0.1);
        assert!(
            !memories.is_empty(),
            "problem text must produce at least one memory"
        );
        assert_eq!(
            memories[0].kind, "problem",
            "unresolved problem text must classify as problem, not milestone"
        );
    }

    #[test]
    fn test_short_segments_are_skipped() {
        // Segments shorter than 20 characters must be skipped entirely.
        let text = "Too short.";
        let memories = extract_memories(text, 0.0);
        assert!(
            memories.is_empty(),
            "segments under 20 characters must be skipped"
        );
        // Verify the threshold boundary: exactly 20 chars or just under.
        let text_19 = "1234567890123456789";
        assert_eq!(text_19.trim().len(), 19);
        let memories_19 = extract_memories(text_19, 0.0);
        assert!(
            memories_19.is_empty(),
            "19-character segment must be skipped"
        );
        // Boundary case: exactly 20 characters must NOT be skipped by the length
        // filter. Uses "let's use" which matches the decision regex pattern.
        let text_20 = "Let's use this tool.";
        assert_eq!(
            text_20.trim().len(),
            20,
            "test string must be exactly 20 chars"
        );
        let memories_20 = extract_memories(text_20, 0.0);
        assert!(
            !memories_20.is_empty(),
            "20-character segment with keywords must not be skipped by the length filter"
        );
    }

    #[test]
    fn test_extract_prose_all_code_falls_back_to_full_text() {
        // When all lines are code (matching code patterns), extract_prose falls
        // back to returning the original text so scoring can still proceed.
        let code_only = "```\nlet x = 1;\nlet y = 2;\n```";
        let result = extract_prose(code_only);
        // All lines are either code fences or inside a code block, so prose is
        // empty and the function falls back to the original text.
        assert!(
            !result.is_empty(),
            "extract_prose must return original text when no prose lines found"
        );
        assert!(
            result.contains("let x = 1"),
            "fallback must contain original code content"
        );
    }

    #[test]
    fn test_large_single_block_is_chunked() {
        // A single paragraph (no blank lines) exceeding 20 lines must be split
        // into chunks of 25 lines by the line-group fallback in split_into_segments.
        let lines: Vec<String> = (0..30)
            .map(|i| format!("Line number {i} with some padding text to make it longer"))
            .collect();
        let text = lines.join("\n");
        let segments = split_into_segments(&text);
        // 30 lines with no blank line separators and no turn markers: should chunk.
        assert!(
            segments.len() >= 2,
            "30-line single block must be split into at least 2 chunks"
        );
        // Each chunk must be non-empty.
        assert!(
            segments.iter().all(|segment| !segment.trim().is_empty()),
            "all chunks must be non-empty"
        );
    }

    #[test]
    fn test_disambiguate_problem_positive_milestone() {
        // A problem with resolution markers but no emotional markers must be
        // reclassified as "milestone". The `disambiguate` function checks
        // `has_resolution` first; when emotional score is 0 it returns "milestone".
        // Avoid emotional keywords (proud, love, joy) to stay off the emotional branch.
        let text = "The bug was terrible but the issue was finally fixed and it works after the long struggle.";
        let memories = extract_memories(text, 0.1);
        assert!(
            !memories.is_empty(),
            "text must produce at least one memory"
        );
        // "works" and "fixed" trigger has_resolution; no emotional keywords means
        // the emotional score is 0, so disambiguate returns "milestone".
        let kind = &memories[0].kind;
        assert_eq!(
            kind, "milestone",
            "resolved problem without emotional cues must be reclassified as milestone, got: {kind}"
        );
    }

    #[test]
    fn test_high_confidence_threshold_filters_low_scores() {
        // A very high min_confidence (1.0) should filter out memories that don't
        // have extremely high marker scores relative to the 5.0 divisor.
        let text =
            "We decided to use a new approach for the architecture because of the trade-off.";
        let memories_low = extract_memories(text, 0.1);
        let memories_high = extract_memories(text, 1.0);
        // With low threshold, the decision markers should produce a memory.
        assert!(
            !memories_low.is_empty(),
            "low threshold must produce memories from decision text"
        );
        // With maximum threshold (1.0), most text is filtered out since
        // confidence = (score / 5.0).min(1.0) requires score >= 5.0.
        assert!(
            memories_high.len() <= memories_low.len(),
            "high threshold must produce fewer or equal memories than low threshold"
        );
    }

    #[test]
    fn test_chunk_index_sequential() {
        // Extracted memories must have sequential chunk_index values starting at 0.
        let text = "> What do you prefer?\nI prefer tabs over spaces always.\n\n> Any milestones?\nWe finally shipped version 2.0 and deployed it successfully.";
        let memories = extract_memories(text, 0.1);
        // Guard: at least one memory must be produced for the loop to be meaningful.
        assert!(
            !memories.is_empty(),
            "text with preference and milestone markers must produce at least one memory"
        );
        // Verify sequential indexing for however many memories are extracted.
        for (index, memory) in memories.iter().enumerate() {
            assert_eq!(
                memory.chunk_index, index,
                "chunk_index must be sequential; expected {index}, got {}",
                memory.chunk_index
            );
        }
        // Postcondition: all memories have non-empty content and kind.
        assert!(
            memories.iter().all(|memory| !memory.content.is_empty()),
            "all memories must have non-empty content"
        );
        assert!(
            memories.iter().all(|memory| !memory.kind.is_empty()),
            "all memories must have non-empty kind"
        );
    }

    #[test]
    fn test_score_markers_returns_zero_for_no_matches() {
        // Text with no matching markers must produce a score of 0.0.
        let text = "the quick brown fox jumps over the lazy dog";
        let score = score_markers(text, DECISION_REGEXES.as_slice());
        assert!(
            (score - 0.0).abs() < f64::EPSILON,
            "non-matching text must score 0.0"
        );
        // Also verify against emotion markers for cross-type coverage.
        let emotion_score = score_markers(text, EMOTION_REGEXES.as_slice());
        assert!(
            (emotion_score - 0.0).abs() < f64::EPSILON,
            "non-matching text must score 0.0 for emotion markers"
        );
    }

    #[test]
    fn test_disambiguate_non_problem_passes_through() {
        // Non-problem types must pass through disambiguate unchanged regardless
        // of sentiment or resolution words.
        let scores: std::collections::HashMap<&str, f64> =
            [("decision", 3.0), ("milestone", 1.0)].into();
        let result = disambiguate("decision", "we fixed the breakthrough", &scores);
        assert_eq!(
            result, "decision",
            "non-problem type must not be reclassified"
        );
        // Also verify emotional passes through unchanged.
        let result_emotional = disambiguate("emotional", "we fixed it", &scores);
        assert_eq!(
            result_emotional, "emotional",
            "emotional type must not be reclassified by disambiguate"
        );
    }

    #[test]
    fn test_has_resolution_patterns() {
        // Additional resolution patterns must be detected correctly.
        assert!(
            has_resolution("I patched the config file"),
            "'patched' must be detected as resolution"
        );
        assert!(
            has_resolution("We got it working after three tries"),
            "'got it working' must be detected as resolution"
        );
        assert!(
            has_resolution("She nailed it on the first attempt"),
            "'nailed it' must be detected as resolution"
        );
        assert!(
            has_resolution("The answer was to restart the service"),
            "'the answer' must be detected as resolution"
        );
    }
}
