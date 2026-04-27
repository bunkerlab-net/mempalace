//! AAAK Dialect — compresses plain text into symbolic memory format (~30x compression).

pub mod emotions;
pub mod topics;

/// UTF-8 code points are at most 4 bytes, so a char-boundary snap never takes more than 3 steps.
const CHAR_BOUNDARY_SNAP_MAX: usize = 4;

use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;

use emotions::{emotion_signals, flag_signals};
use topics::{extract_topics, stop_words};

// Regex literal is a compile-time constant; cannot fail to compile.
#[allow(clippy::expect_used)]
static SENTENCE_SPLIT_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Splits text into sentences on punctuation or newlines.
    Regex::new(r"[.!?\n]+")
        .expect("sentence-split regex is a compile-time literal and cannot fail to compile")
});

// Regex literal is a compile-time constant; cannot fail to compile.
#[allow(clippy::expect_used)]
static NON_ALPHA_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Strips non-alphabetic characters when extracting entity codes from words.
    Regex::new(r"[^a-zA-Z]")
        .expect("non-alpha strip regex is a compile-time literal and cannot fail to compile")
});

/// AAAK Dialect encoder — compresses plain text into symbolic memory format.
pub struct Dialect {
    /// Known entity name → short code mappings.
    entity_codes: HashMap<String, String>,
}

/// Optional metadata attached to the AAAK header line.
#[derive(Default)]
pub struct CompressMetadata<'a> {
    /// Path of the original source file.
    pub source_file: &'a str,
    /// Wing (project namespace).
    pub wing: &'a str,
    /// Room (category).
    pub room: &'a str,
    /// Date string (e.g. `"2024-01-15"`).
    pub date: &'a str,
}

/// Structured representation of AAAK Dialect content after decoding.
///
/// Returned by [`Dialect::decode`]. Fields that are absent in the input
/// default to empty strings or empty vectors.
#[derive(Debug, Default)]
pub struct DecodedDialect {
    /// Wing namespace from the optional header line.
    pub wing: String,
    /// Room category from the optional header line.
    pub room: String,
    /// Date string from the optional header line.
    pub date: String,
    /// File stem from the optional header line.
    pub stem: String,
    /// Entity codes from the `0:` prefix of the content line.
    pub entities: Vec<String>,
    /// Topic keywords, split on `_` from the content line.
    pub topics: Vec<String>,
    /// Key quoted sentence (stripped of surrounding `"`).
    pub quote: String,
    /// Emotion codes (lowercase) from the content line.
    pub emotions: Vec<String>,
    /// Importance flags (uppercase) from the content line.
    pub flags: Vec<String>,
}

/// Detect emotions from plain text using keyword signals.
fn detect_emotions(text: &str) -> Vec<String> {
    let text_lower = text.to_lowercase();
    let mut detected = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for &(keyword, code) in emotion_signals() {
        if text_lower.contains(keyword) && seen.insert(code) {
            detected.push(code.to_string());
        }
        if detected.len() >= 3 {
            break;
        }
    }
    detected
}

/// Detect importance flags from plain text using keyword signals.
fn detect_flags(text: &str) -> Vec<String> {
    let text_lower = text.to_lowercase();
    let mut detected = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for &(keyword, flag) in flag_signals() {
        if text_lower.contains(keyword) && seen.insert(flag) {
            detected.push(flag.to_string());
        }
        if detected.len() >= 3 {
            break;
        }
    }
    detected
}

/// Extract the most important sentence fragment from text.
fn extract_key_sentence(text: &str) -> String {
    let sentences: Vec<&str> = SENTENCE_SPLIT_RE
        .split(text)
        .map(str::trim)
        .filter(|sentence| sentence.len() > 10)
        .collect();

    if sentences.is_empty() {
        return String::new();
    }

    let decision_words = [
        "decided",
        "because",
        "instead",
        "prefer",
        "switched",
        "chose",
        "realized",
        "important",
        "key",
        "critical",
        "discovered",
        "learned",
        "conclusion",
        "solution",
        "reason",
        "why",
        "breakthrough",
        "insight",
    ];

    let mut scored: Vec<(i32, &str)> = sentences
        .into_iter()
        .map(|sentence| {
            let sentence_lower = sentence.to_lowercase();
            let mut score: i32 = 0;
            for w in &decision_words {
                if sentence_lower.contains(w) {
                    score += 2;
                }
            }
            if sentence.len() < 80 {
                score += 1;
            }
            if sentence.len() < 40 {
                score += 1;
            }
            if sentence.len() > 150 {
                score -= 2;
            }
            (score, sentence)
        })
        .collect();

    scored.sort_by_key(|b| std::cmp::Reverse(b.0));
    let best = scored[0].1;

    if best.len() > 55 {
        let mut end = 52;
        let mut snap_steps: usize = 0;
        while end < best.len() && !best.is_char_boundary(end) {
            snap_steps += 1;
            assert!(
                snap_steps < CHAR_BOUNDARY_SNAP_MAX,
                "extract_key_sentence: exceeded CHAR_BOUNDARY_SNAP_MAX ({CHAR_BOUNDARY_SNAP_MAX}) snap steps"
            );
            end += 1;
        }
        format!("{}...", &best[..end])
    } else {
        best.to_string()
    }
}

impl Dialect {
    pub fn new(entities: &HashMap<String, String>) -> Self {
        let mut entity_codes = HashMap::new();
        for (name, code) in entities {
            entity_codes.insert(name.clone(), code.clone());
            entity_codes.insert(name.to_lowercase(), code.clone());
        }
        Self { entity_codes }
    }

    pub fn empty() -> Self {
        Self {
            entity_codes: HashMap::new(),
        }
    }

    /// Find known entities in text, or detect capitalized names.
    // Regex literal is a compile-time constant that can never fail to compile.
    #[allow(clippy::expect_used)]
    fn detect_entities(&self, text: &str) -> Vec<String> {
        assert!(!text.is_empty(), "detect_entities: text must not be empty");
        let text_lower = text.to_lowercase();
        let mut found = Vec::new();

        // Check known entities.
        for (name, code) in &self.entity_codes {
            if !name.chars().next().is_some_and(char::is_lowercase)
                && text_lower.contains(&name.to_lowercase())
                && !found.contains(code)
            {
                found.push(code.clone());
            }
        }
        if !found.is_empty() {
            return found;
        }

        // Fallback: capitalized words that look like names.
        let stops = stop_words();
        let words: Vec<&str> = text.split_whitespace().collect();
        for (i, w) in words.iter().enumerate() {
            let clean = NON_ALPHA_RE.replace_all(w, "");
            if clean.len() >= 2
                && clean.chars().next().is_some_and(char::is_uppercase)
                && clean[1..].chars().all(char::is_lowercase)
                && i > 0
                && !stops.contains(clean.to_lowercase().as_str())
            {
                let code = clean[..3.min(clean.len())].to_uppercase();
                if !found.contains(&code) {
                    found.push(code);
                }
                if found.len() >= 3 {
                    break;
                }
            }
        }
        found
    }

    /// Compress plain text into AAAK Dialect format.
    pub fn compress(&self, text: &str, metadata: Option<&CompressMetadata>) -> String {
        assert!(!text.is_empty(), "compress: text must not be empty");
        let entities = self.detect_entities(text);
        let entity_str = if entities.is_empty() {
            "???".to_string()
        } else {
            entities[..3.min(entities.len())].join("+")
        };

        let topics = extract_topics(text, 3);
        let topic_str = if topics.is_empty() {
            "misc".to_string()
        } else {
            topics.join("_")
        };

        let quote = extract_key_sentence(text);
        let emotions = detect_emotions(text);
        let flags = detect_flags(text);

        let mut lines = Vec::new();

        // Header line (if metadata available).
        if let Some(meta) = metadata
            && (!meta.source_file.is_empty() || !meta.wing.is_empty())
        {
            let stem = if meta.source_file.is_empty() {
                "?"
            } else {
                Path::new(meta.source_file)
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("?")
            };
            let header = format!(
                "{}|{}|{}|{}",
                if meta.wing.is_empty() { "?" } else { meta.wing },
                if meta.room.is_empty() { "?" } else { meta.room },
                if meta.date.is_empty() { "?" } else { meta.date },
                stem,
            );
            lines.push(header);
        }

        // Content line.
        let mut parts = vec![format!("0:{entity_str}"), topic_str];
        if !quote.is_empty() {
            parts.push(format!("\"{quote}\""));
        }
        if !emotions.is_empty() {
            parts.push(emotions.join("+"));
        }
        if !flags.is_empty() {
            parts.push(flags.join("+"));
        }
        lines.push(parts.join("|"));

        let result = lines.join("\n");

        // Postcondition: compressed output is never empty.
        debug_assert!(!result.is_empty());

        result
    }

    /// Estimate the token count of `text` using the ~1.3 tokens/word heuristic.
    ///
    /// Matches the Python reference `count_tokens` static method. Useful for
    /// estimating LLM context usage before sending content to a token-budgeted
    /// endpoint.
    pub fn count_tokens(text: &str) -> usize {
        assert!(!text.is_empty(), "count_tokens: text must not be empty");
        let word_count = text.split_ascii_whitespace().count();
        // 1.3 tokens per word: (wc * 13 + 5) / 10 gives standard rounding.
        let tokens = (word_count * 13 + 5) / 10;
        assert!(
            tokens > 0,
            "count_tokens: non-empty text must yield positive token count"
        );
        tokens
    }

    /// Parse AAAK-encoded text back into a [`DecodedDialect`].
    ///
    /// The first line is treated as a header (`WING|ROOM|DATE|STEM`) when it
    /// does not start with `"0:"`. The content line (`0:ENTITIES|topics|...`)
    /// must be present. Unknown or missing segments default to empty.
    pub fn decode(aaak: &str) -> DecodedDialect {
        assert!(!aaak.is_empty(), "decode: aaak must not be empty");

        let lines: Vec<&str> = aaak.lines().filter(|l| !l.is_empty()).collect();
        assert!(
            !lines.is_empty(),
            "decode: aaak must contain at least one non-empty line"
        );

        // The content line always starts with "0:"; any preceding line is the header.
        let (header_opt, content) = if lines.len() >= 2 && !lines[0].starts_with("0:") {
            (Some(lines[0]), lines[1])
        } else {
            (None, lines[0])
        };

        let mut result = DecodedDialect::default();
        if let Some(header) = header_opt {
            decode_fill_header(header, &mut result);
        }
        decode_fill_content(content, &mut result);

        result
    }
}

/// Called by [`Dialect::decode`] to populate header fields of `decoded`.
///
/// Expects a pipe-separated string of the form `WING|ROOM|DATE|STEM`.
/// Missing segments leave the corresponding field as an empty string.
fn decode_fill_header(header: &str, decoded: &mut DecodedDialect) {
    assert!(
        !header.is_empty(),
        "decode_fill_header: header must not be empty"
    );

    let parts: Vec<&str> = header.split('|').collect();
    assert!(
        !parts.is_empty(),
        "decode_fill_header: split must produce at least one part"
    );

    decoded.wing = parts.first().copied().unwrap_or("").to_string();
    decoded.room = parts.get(1).copied().unwrap_or("").to_string();
    decoded.date = parts.get(2).copied().unwrap_or("").to_string();
    decoded.stem = parts.get(3).copied().unwrap_or("").to_string();
}

/// Called by [`Dialect::decode`] to populate content fields of `decoded`.
///
/// Parses a content line of the form `0:ENTITIES|topics|"quote"|emotions|FLAGS`.
/// Segments after topics are optional; quoted strings become the key quote,
/// uppercase-leading segments become flags, lowercase-leading ones become emotions.
fn decode_fill_content(content: &str, decoded: &mut DecodedDialect) {
    assert!(
        !content.is_empty(),
        "decode_fill_content: content must not be empty"
    );

    let parts: Vec<&str> = content.split('|').collect();

    // Part 0: "0:ENTITIES" — strip the leading "0:" and split entities on "+".
    if let Some(entity_part) = parts.first() {
        let entity_str = entity_part.trim_start_matches("0:");
        if entity_str != "???" && !entity_str.is_empty() {
            decoded.entities = entity_str.split('+').map(str::to_string).collect();
        }
    }

    // Part 1: topics joined with "_".
    if let Some(topic_str) = parts.get(1)
        && *topic_str != "misc"
        && !topic_str.is_empty()
    {
        decoded.topics = topic_str.split('_').map(str::to_string).collect();
    }

    // Parts 2+: quoted string → quote; uppercase-leading → flags; else → emotions.
    for part in parts.iter().skip(2) {
        if part.is_empty() {
            continue;
        }
        if part.starts_with('"') {
            decoded.quote = part.trim_matches('"').to_string();
        } else if part.chars().next().is_some_and(char::is_uppercase) {
            decoded.flags.extend(part.split('+').map(str::to_string));
        } else {
            decoded.emotions.extend(part.split('+').map(str::to_string));
        }
    }

    // AAAK content lines may legitimately have zero entities (??? placeholder)
    // and zero topics (misc) for degenerate input; just assert the split worked.
    assert!(
        parts.len() <= 10,
        "decode_fill_content: content line must have at most 10 pipe-separated segments"
    );
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_basic() {
        let dialect = Dialect::empty();
        let result = dialect.compress(
            "We decided to use GraphQL instead of REST because it gives better flexibility",
            None,
        );
        assert!(result.contains("0:"));
        assert!(result.contains("DECISION"));
    }

    #[test]
    fn test_compress_with_metadata() {
        let dialect = Dialect::empty();
        let meta = CompressMetadata {
            source_file: "notes/meeting.txt",
            wing: "wing_project",
            room: "architecture",
            date: "2024-01-15",
        };
        let result = dialect.compress("Alice decided to switch from REST to GraphQL", Some(&meta));
        assert!(result.contains("wing_project|architecture|2024-01-15|meeting"));
    }

    #[test]
    fn test_detect_emotions() {
        let emotions = detect_emotions("I'm really excited but also worried about the deadline");
        assert!(emotions.contains(&"excite".to_string()));
        assert!(emotions.contains(&"anx".to_string()));
    }

    #[test]
    fn test_detect_flags() {
        let flags = detect_flags("We decided to switch because the old API was too slow");
        assert!(flags.contains(&"DECISION".to_string()));
        assert!(flags.contains(&"TECHNICAL".to_string()));
    }

    #[test]
    fn test_known_entities() {
        let mut entities = HashMap::new();
        entities.insert("Alice".to_string(), "ALC".to_string());
        entities.insert("Bob".to_string(), "BOB".to_string());
        let dialect = Dialect::new(&entities);
        let found = dialect.detect_entities("Alice told Bob about the new architecture");
        assert!(found.contains(&"ALC".to_string()));
        assert!(found.contains(&"BOB".to_string()));
    }

    #[test]
    fn detect_emotions_caps_at_three() {
        // detect_emotions must break after collecting 3 distinct emotion codes.
        // This exercises the `detected.len() >= 3` early-exit branch.
        // Input has 4+ distinct emotion keywords: love→love, excited→excite,
        // worried→anx, frustrated→frust — only the first 3 unique codes kept.
        let text = "I love this, I'm excited, but worried and frustrated about the deadline";
        let emotions = detect_emotions(text);
        assert_eq!(
            emotions.len(),
            3,
            "emotion count must saturate at exactly 3"
        );
        assert!(
            !emotions.is_empty(),
            "text with emotion keywords must produce at least one"
        );
    }

    #[test]
    fn detect_flags_caps_at_three() {
        // detect_flags must break after collecting 3 distinct flag codes.
        // This exercises the `detected.len() >= 3` early-exit branch.
        // Input has 4+ distinct flag codes: decided→DECISION, first time→ORIGIN,
        // core→CORE, api→TECHNICAL — only the first 3 unique codes kept.
        let text = "We decided for the first time that the core api needs a rewrite";
        let flags = detect_flags(text);
        assert_eq!(flags.len(), 3, "flag count must saturate at exactly 3");
        assert!(
            !flags.is_empty(),
            "text with flag keywords must produce at least one"
        );
    }

    #[test]
    fn extract_key_sentence_empty_on_short_text() {
        // Text with no sentences longer than 10 chars must return an empty string.
        // This exercises the `sentences.is_empty()` early return.
        let result = extract_key_sentence("hi. ok.");
        assert!(
            result.is_empty(),
            "text with only short fragments must produce empty key sentence"
        );
    }

    #[test]
    fn extract_key_sentence_truncates_long_sentence() {
        // A key sentence longer than 55 characters must be truncated with "...".
        // This exercises the `best.len() > 55` truncation branch.
        let long_text = "We decided to completely restructure the entire backend architecture because the legacy system was causing too many production failures and nobody understood the codebase anymore";
        let result = extract_key_sentence(long_text);
        assert!(
            result.ends_with("..."),
            "long key sentence must be truncated with ellipsis"
        );
        assert!(
            result.len() <= 60,
            "truncated key sentence must not exceed ~55 chars plus ellipsis"
        );
    }

    // ── count_tokens ─────────────────────────────────────────────────

    #[test]
    fn count_tokens_basic_word_count() {
        // 10-word sentence: 10 * 1.3 = 13.0 → 13 tokens.
        let tokens = Dialect::count_tokens("one two three four five six seven eight nine ten");
        assert_eq!(tokens, 13, "10 words must yield 13 estimated tokens");
        assert!(tokens > 0, "token count must be positive");
    }

    #[test]
    fn count_tokens_single_word() {
        // Minimum case: one word yields at least 1 token.
        let tokens = Dialect::count_tokens("hello");
        assert_eq!(tokens, 1, "one word must yield at least 1 token");
        assert!(tokens > 0);
    }

    // ── decode ───────────────────────────────────────────────────────

    #[test]
    fn decode_roundtrip_with_metadata() {
        // A compress/decode round-trip must recover wing, room, date, stem,
        // and produce non-empty entities and topics.
        let dialect = Dialect::empty();
        let meta = CompressMetadata {
            source_file: "docs/meeting.md",
            wing: "projects",
            room: "planning",
            date: "2025-01-10",
        };
        let aaak = dialect.compress(
            "Alice decided to switch from REST to GraphQL for better performance",
            Some(&meta),
        );
        let decoded = Dialect::decode(&aaak);
        assert_eq!(decoded.wing, "projects", "wing must round-trip");
        assert_eq!(decoded.room, "planning", "room must round-trip");
        assert_eq!(decoded.date, "2025-01-10", "date must round-trip");
        assert_eq!(decoded.stem, "meeting", "stem must be the file stem");
        assert!(!decoded.entities.is_empty() || !decoded.topics.is_empty());
    }

    #[test]
    fn decode_roundtrip_without_metadata() {
        // Without metadata there is no header line; entities/topics must still decode.
        let dialect = Dialect::empty();
        let aaak = dialect.compress(
            "Bob discovered that the core api needs a security fix urgently",
            None,
        );
        let decoded = Dialect::decode(&aaak);
        assert!(decoded.wing.is_empty(), "no metadata means empty wing");
        assert!(decoded.stem.is_empty(), "no metadata means empty stem");
        assert!(!decoded.entities.is_empty() || !decoded.topics.is_empty());
    }

    #[test]
    fn decode_emotions_and_flags_classified_separately() {
        // Emotion codes (lowercase) and flag codes (uppercase) must land in
        // the right fields after decode.
        let dialect = Dialect::empty();
        let aaak = dialect.compress(
            "I was excited but decided for the first time to solve the core problem",
            None,
        );
        let decoded = Dialect::decode(&aaak);
        // Verify that any detected emotions are lowercase-coded.
        for emotion in &decoded.emotions {
            assert!(
                emotion.chars().next().is_some_and(char::is_lowercase),
                "emotion codes must be lowercase-leading: {emotion}"
            );
        }
        // Verify that any detected flags are uppercase-coded.
        for flag in &decoded.flags {
            assert!(
                flag.chars().next().is_some_and(char::is_uppercase),
                "flag codes must be uppercase-leading: {flag}"
            );
        }
    }

    #[test]
    fn decode_unknown_entities_returns_empty_vec() {
        // "0:???" is the placeholder compress emits when no entities are detected.
        // decode must map it to an empty entities vec; topics and flags still parse.
        let aaak = "0:???|project_tech|DECISION";
        let decoded = Dialect::decode(aaak);
        assert!(
            decoded.entities.is_empty(),
            "??? placeholder must decode to empty entities vec"
        );
        assert!(
            !decoded.topics.is_empty() || !decoded.flags.is_empty(),
            "other fields must still decode from the content line"
        );
    }
}
