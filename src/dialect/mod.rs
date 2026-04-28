//! AAAK Dialect — compresses plain text into symbolic memory format (~30x compression).

pub mod emotions;
pub mod topics;

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

/// Compile the current locale's `quote_pattern` regex on demand.
///
/// Returns `None` when the locale JSON omits `quote_pattern` or the pattern
/// fails to compile. Compiled per call rather than cached at first use so a
/// runtime locale change is honored — if compilation cost becomes a concern,
/// add a per-locale cache, but a single regex compile is cheap relative to
/// the surrounding text scan.
fn current_locale_quote_re() -> Option<Regex> {
    let pattern = crate::i18n::get_regex("quote_pattern")?;
    // Locale JSON contracts non-empty patterns; an empty string would silently
    // compile into a regex that matches every position.
    assert!(
        !pattern.is_empty(),
        "current_locale_quote_re: quote_pattern must not be empty"
    );
    let compiled = Regex::new(&pattern).ok()?;
    // Pair assertion: a successful compile must produce a non-trivial pattern
    // (the locale JSON should never define an empty regex).
    debug_assert!(!compiled.as_str().is_empty());
    Some(compiled)
}

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

/// Extract the most memorable sentence from `text` for the AAAK quote field.
///
/// Tries the locale's `quote_pattern` first (verbatim quoted strings are ideal
/// key sentences). Falls back to scoring all sentences by decision-word density
/// via `extract_key_sentence_score`.
fn extract_key_sentence(text: &str) -> String {
    // Prefer a verbatim quoted string when the current locale defines a quote
    // pattern. The regex is compiled per call so a runtime locale switch is
    // honored — caching by locale would freeze whatever was active at first use.
    let raw = extract_key_sentence_raw(text);
    // Pipe is the AAAK field separator and `\n`/`\r` end the content line —
    // either character in the quote would corrupt the encoded line and break
    // `decode_fill_content`. Strip rather than escape: the format has no
    // in-band escape mechanism today, and a lossy substitution keeps the
    // grammar single-byte and round-trip-safe.
    raw.replace('|', "/").replace(['\n', '\r'], " ")
}

/// Resolve the unfiltered key sentence — caller is responsible for sanitizing
/// AAAK-reserved characters before encoding.
fn extract_key_sentence_raw(text: &str) -> String {
    if let Some(re) = current_locale_quote_re() {
        // Spanish (and potentially other locales) declare two alternations in
        // `quote_pattern` — one for `"..."` and one for `«...»` — so the
        // matching capture may land in either group. Iterate all matches to
        // find the longest qualifying quote; `chars().count()` handles
        // multibyte characters correctly where `.len()` would count bytes.
        let best = re
            .captures_iter(text)
            .filter_map(|cap| cap.get(1).or_else(|| cap.get(2)))
            .map(|m| m.as_str().trim())
            .filter(|quoted| quoted.chars().count() > 10)
            .max_by_key(|quoted| quoted.chars().count());
        if let Some(quoted) = best {
            return quoted.to_string();
        }
    }

    let sentences: Vec<&str> = SENTENCE_SPLIT_RE
        .split(text)
        .map(str::trim)
        .filter(|sentence| sentence.len() > 10)
        .collect();

    if sentences.is_empty() {
        return String::new();
    }

    extract_key_sentence_score(sentences)
}

/// Called by `extract_key_sentence` to score, rank, and truncate sentences.
///
/// Scores each sentence by decision-word density and length. Returns the
/// highest-scoring sentence, truncated to ≤55 characters when needed.
fn extract_key_sentence_score(sentences: Vec<&str>) -> String {
    assert!(
        !sentences.is_empty(),
        "extract_key_sentence_score: sentences must not be empty"
    );

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
            let char_count = sentence.chars().count();
            if char_count < 80 {
                score += 1;
            }
            if char_count < 40 {
                score += 1;
            }
            if char_count > 150 {
                score -= 2;
            }
            (score, sentence)
        })
        .collect();

    scored.sort_by_key(|b| std::cmp::Reverse(b.0));
    let best = scored[0].1;

    assert!(
        !best.is_empty(),
        "extract_key_sentence_score: best sentence must not be empty"
    );

    if best.chars().count() > 55 {
        // Resolve byte index of the 52nd character so the slice is always on a
        // char boundary without a manual snap loop.
        let end = best.char_indices().nth(52).map_or(best.len(), |(b, _)| b);
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

        // Header line (if any metadata field is populated). Previously the
        // condition only consulted `source_file` and `wing`, dropping the
        // header — and therefore `room` and `date` — whenever the caller
        // passed them but left the other two empty.
        if let Some(meta) = metadata
            && (!meta.source_file.is_empty()
                || !meta.wing.is_empty()
                || !meta.room.is_empty()
                || !meta.date.is_empty())
        {
            let stem_raw = if meta.source_file.is_empty() {
                ""
            } else {
                Path::new(meta.source_file)
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("")
            };
            // Encode each field so a `|`, `?`, or newline in metadata cannot
            // corrupt the header layout on round-trip. Without escaping a
            // filename like `quarter|q1.md` would split into 5 header parts
            // and trip `decode_fill_header`'s arity assertion. Decoder pairs
            // with `header_field_decode`.
            let header = format!(
                "{}|{}|{}|{}",
                header_field_encode(meta.wing),
                header_field_encode(meta.room),
                header_field_encode(meta.date),
                header_field_encode(stem_raw),
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
        // Use Unicode whitespace so non-ASCII separators (e.g. NBSP, ideographic
        // space) split words consistently with Python's str.split().
        let word_count = text.split_whitespace().count();
        // 1.3 tokens per word: (wc * 13 + 5) / 10 gives standard rounding.
        // Floor to 1: whitespace-only input yields 0 words, but 1 token is a safer
        // estimate than 0 for LLM context budgeting.
        let tokens = ((word_count * 13 + 5) / 10).max(1);
        assert!(tokens > 0, "count_tokens: result must be positive");
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

        // The encoder produces exactly one (content-only) or two (header +
        // content) non-empty lines. Anything beyond that means the input has
        // been hand-edited or concatenated with another AAAK record — fail
        // fast so the bug surfaces here instead of as silently dropped data.
        let expected_line_count = if header_opt.is_some() { 2 } else { 1 };
        assert!(
            lines.len() == expected_line_count,
            "decode: aaak must contain only header and content lines"
        );

        // Programmer-error precondition: callers must hand `decode` a
        // well-formed AAAK string. A header-only input (e.g. just
        // `wing|room|date|stem`) would otherwise fall through to
        // `decode_fill_content` and silently produce garbage — assert here
        // so the bug surfaces at the boundary instead of downstream.
        assert!(
            content.starts_with("0:"),
            "decode: content line must start with `0:`; got {content:?}"
        );

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
    // The encoder writes exactly four header positions (`WING|ROOM|DATE|STEM`,
    // with `?` for empties). A higher count means a stem or wing contained an
    // unescaped `|` — better to fail loudly than to silently drop the tail.
    assert!(
        parts.len() <= 4,
        "decode_fill_header: expected at most 4 header parts, got {}",
        parts.len()
    );

    // `header_field_decode` reverses `header_field_encode`: `?` → empty,
    // and the five percent-escapes (`%25`, `%7C`, `%0A`, `%0D`, `%3F`) are
    // turned back into `%`, `|`, newline, CR, and `?` respectively. Other
    // `%XX` sequences pass through verbatim so a stem like `30%discount`
    // written by an older build still decodes to its literal form.
    decoded.wing = parts
        .first()
        .map(|part| header_field_decode(part))
        .unwrap_or_default();
    decoded.room = parts
        .get(1)
        .map(|part| header_field_decode(part))
        .unwrap_or_default();
    decoded.date = parts
        .get(2)
        .map(|part| header_field_decode(part))
        .unwrap_or_default();
    decoded.stem = parts
        .get(3)
        .map(|part| header_field_decode(part))
        .unwrap_or_default();
}

/// Percent-encode an AAAK header field so it survives round-tripping.
///
/// Replaces `%`, `|`, newline, and CR with their `%XX` escapes. Empty input
/// emits the `?` sentinel; a literal `?` field is escaped to `%3F` so it is
/// not later mistaken for empty by `header_field_decode`.
///
/// Encoding `%` first prevents double-encoding the escape introducer.
fn header_field_encode(field: &str) -> String {
    if field.is_empty() {
        return "?".to_string();
    }
    if field == "?" {
        return "%3F".to_string();
    }
    let mut out = String::with_capacity(field.len());
    for character in field.chars() {
        match character {
            '%' => out.push_str("%25"),
            '|' => out.push_str("%7C"),
            '\n' => out.push_str("%0A"),
            '\r' => out.push_str("%0D"),
            _ => out.push(character),
        }
    }
    out
}

/// Inverse of [`header_field_encode`].
///
/// `?` decodes to empty; the five known `%XX` escapes decode to their literal
/// characters; any other `%XX` (or a trailing `%` with too few following
/// characters) is preserved verbatim so legacy un-escaped data containing
/// percent signs round-trips unchanged.
fn header_field_decode(field: &str) -> String {
    if field == "?" {
        return String::new();
    }
    let bytes = field.as_bytes();
    let mut out = String::with_capacity(field.len());
    let mut index: usize = 0;
    while index < bytes.len() {
        // Compare on raw bytes so a `%` followed by a multi-byte UTF-8 lead
        // byte cannot trigger a mid-codepoint `&str` slice panic.
        if bytes[index] == b'%' && index + 3 <= bytes.len() {
            let triple = [bytes[index], bytes[index + 1], bytes[index + 2]];
            let decoded_char = match &triple {
                b"%25" => Some('%'),
                b"%7C" => Some('|'),
                b"%0A" => Some('\n'),
                b"%0D" => Some('\r'),
                b"%3F" => Some('?'),
                _ => None,
            };
            if let Some(character) = decoded_char {
                out.push(character);
                index += 3;
                continue;
            }
        }
        // Unknown escape (or plain char): advance one full UTF-8 char so we
        // never split a multi-byte sequence mid-way.
        let remainder = &field[index..];
        if let Some(character) = remainder.chars().next() {
            out.push(character);
            index += character.len_utf8();
        } else {
            break;
        }
    }
    out
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
            // strip_prefix + strip_suffix removes exactly one wrapping quote from
            // each end, preserving embedded quotes that trim_matches would eat.
            decoded.quote = part
                .strip_prefix('"')
                .and_then(|without_prefix| without_prefix.strip_suffix('"'))
                .unwrap_or(part)
                .to_string();
        } else if part.chars().next().is_some_and(char::is_uppercase) {
            decoded.flags.extend(part.split('+').map(str::to_string));
        } else {
            decoded.emotions.extend(part.split('+').map(str::to_string));
        }
    }

    // The encoder produces at most five pipe-separated segments:
    // `0:ENTITIES|topics|"quote"|emotions|FLAGS`. A higher count means the
    // input was hand-edited or corrupted — fail fast rather than letting the
    // skip(2) loop above accumulate stray segments into emotions/flags/quote.
    assert!(
        parts.len() <= 5,
        "decode_fill_content: content line must have at most 5 pipe-separated segments"
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

    #[test]
    fn count_tokens_unicode_whitespace() {
        // `str::split_whitespace` also splits on Unicode separators (em-space
        // U+2003, non-breaking space U+00A0). A 5-word Unicode-separated
        // string must produce the same estimate as ASCII spaces would: 5 * 1.3
        // = 6.5 → 7 (with the half-up rounding `(wc * 13 + 5) / 10`).
        let text = "alpha\u{2003}beta\u{00a0}gamma\u{2009}delta\u{205f}epsilon";
        let tokens = Dialect::count_tokens(text);
        assert_eq!(
            tokens, 7,
            "5 unicode-separated words must yield 7 estimated tokens"
        );
        assert!(tokens > 0);
    }

    #[test]
    fn count_tokens_whitespace_only() {
        // Whitespace-only input yields 0 words; we floor to 1 token so callers
        // never see a 0 estimate when budgeting LLM context.
        let tokens = Dialect::count_tokens("   \t\n  ");
        assert_eq!(tokens, 1, "whitespace-only input must floor to 1 token");
        assert!(tokens > 0);
    }

    // ── header field escape/unescape ────────────────────────────────

    #[test]
    fn header_field_encode_empty_emits_sentinel() {
        // Empty fields must serialize as the `?` sentinel so decoders can
        // distinguish "absent" from "literal value".
        assert_eq!(header_field_encode(""), "?");
    }

    #[test]
    fn header_field_encode_literal_question_mark_disambiguates() {
        // A field whose value really is `"?"` must not be mistaken for empty
        // on the way back; encode it as `%3F`.
        assert_eq!(header_field_encode("?"), "%3F");
        assert_eq!(header_field_decode("%3F"), "?");
    }

    #[test]
    fn header_field_encode_escapes_pipe_and_newlines() {
        // The four characters that would corrupt the header layout — `|`,
        // newline, CR, and the percent introducer itself — must all be
        // percent-encoded so split-on-`|` stays correct.
        assert_eq!(header_field_encode("a|b"), "a%7Cb");
        assert_eq!(header_field_encode("a\nb"), "a%0Ab");
        assert_eq!(header_field_encode("a\rb"), "a%0Db");
        assert_eq!(header_field_encode("100%"), "100%25");
    }

    #[test]
    fn header_field_decode_handles_unknown_percent_escapes() {
        // Unknown `%XX` sequences must pass through untouched so legacy
        // un-escaped data containing literal `%` (e.g. a stem like
        // `30%discount`) still round-trips through the new decoder.
        assert_eq!(header_field_decode("30%XX"), "30%XX");
        assert_eq!(header_field_decode("trailing%"), "trailing%");
        assert_eq!(header_field_decode("short%2"), "short%2");
    }

    #[test]
    fn header_field_round_trip_preserves_special_chars() {
        // The canonical correctness test: encode → decode is identity for
        // any string, including ones with delimiters, sentinels, percent
        // signs, multi-byte UTF-8, and CR/LF.
        for sample in [
            "projects",
            "wing|with|pipes",
            "report%2Bdraft",
            "?",
            "line one\nline two",
            "alpha\rbeta",
            "100% complete",
            "café\u{2003}süß",
        ] {
            let encoded = header_field_encode(sample);
            let decoded = header_field_decode(&encoded);
            assert_eq!(
                decoded, sample,
                "round-trip must preserve {sample:?} (encoded as {encoded:?})"
            );
        }
    }

    #[test]
    fn decode_roundtrip_preserves_pipe_in_stem() {
        // A filename with a literal `|` would previously over-split the
        // header and trip `decode_fill_header`'s arity assertion. With
        // percent-encoding the stem round-trips losslessly.
        let dialect = Dialect::empty();
        let meta = CompressMetadata {
            source_file: "/notes/quarter|q1.md",
            wing: "projects",
            room: "planning",
            date: "2025-01-10",
        };
        let aaak = dialect.compress("Some content with enough words to compress", Some(&meta));
        let decoded = Dialect::decode(&aaak);
        assert_eq!(decoded.wing, "projects");
        assert_eq!(decoded.room, "planning");
        assert_eq!(decoded.date, "2025-01-10");
        assert_eq!(decoded.stem, "quarter|q1");
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

    #[test]
    fn extract_key_sentence_returns_verbatim_quoted_string_via_locale_pattern() {
        // Pin the locale to English so current_locale_quote_re uses the English
        // quote_pattern (`"([^"]{20,200})"`) regardless of the process environment.
        // Without pinning, a non-English MEMPALACE_LANG would change or omit the
        // regex and make this test flaky.
        temp_env::with_vars(
            [
                ("MEMPALACE_LANG", Some("en")),
                ("MEMPAL_LANG", None::<&str>),
            ],
            || {
                // When current_locale_quote_re returns Some, extract_key_sentence
                // must return the quoted content directly instead of scoring sentences.
                // The quoted span must be >10 chars to pass the inner length guard.
                let text =
                    r#"She said "We should switch to GraphQL for better performance" and move on."#;
                let result = extract_key_sentence(text);
                // "We should switch to GraphQL for better performance" is 49 chars > 10.
                assert_eq!(
                    result, "We should switch to GraphQL for better performance",
                    "extract_key_sentence must return the quoted content directly"
                );
                assert!(
                    result.len() > 10,
                    "returned quote must exceed the 10-char length guard"
                );
            },
        );
    }

    #[test]
    fn extract_key_sentence_score_snaps_char_boundary_for_multibyte_char() {
        // Lines 215-220: when byte 52 falls inside a multi-byte UTF-8 character,
        // the snap loop increments `end` until a char boundary is reached.
        // Construct a sentence that is >55 bytes where byte 51 begins a 2-byte char (é).
        // "We decided to restructure all of the backend system" = 51 ASCII bytes (0-50),
        // then 'é' (U+00E9) at bytes 51-52, then "s overall therefore" to exceed 55 bytes.
        let long_sentence =
            "We decided to restructure all of the backend systemé overall therefore complete";
        // Verify that the é straddles byte position 51-52 as designed.
        assert_eq!(
            &long_sentence[..51],
            "We decided to restructure all of the backend system",
            "first 51 bytes must be ASCII confirming é starts at byte 51"
        );
        // No quotes in the text so current_locale_quote_re does not short-circuit.
        let result = extract_key_sentence(long_sentence);
        // The result must be truncated with "..." because len > 55 bytes.
        assert!(
            result.ends_with("..."),
            "sentence truncated after char-boundary snap must end with ellipsis"
        );
        assert!(
            result.len() <= 60,
            "truncated result must not exceed ~55 chars plus ellipsis"
        );
    }

    #[test]
    fn detect_entities_fallback_extracts_capitalized_names_not_at_position_zero() {
        // Lines 276-282: when entity_codes is empty, detect_entities falls back to
        // scanning for capitalized words. The guard `i > 0` excludes the first word,
        // so a name that appears after the first word must be detected.
        let dialect = Dialect::empty();
        // "Alice" is the second word (i=1 > 0), uppercase first char, all lowercase rest,
        // length >= 2, and "alice" is not a stop word — so code "ALI" must be pushed.
        let found = dialect.detect_entities("Yesterday Alice went to the market to buy things");
        assert!(
            !found.is_empty(),
            "fallback entity detection must find capitalized names after position 0"
        );
        assert!(
            found.contains(&"ALI".to_string()),
            "fallback must produce the 3-char uppercase code for Alice"
        );
    }

    #[test]
    fn compress_produces_misc_when_text_has_only_stop_words() {
        // Line 300: when extract_topics returns empty (all words are stop words or too short),
        // compress must substitute "misc" as the topic string.
        let dialect = Dialect::empty();
        // All words are in the stop_words list, so no topics are extracted.
        let result = dialect.compress("the to and is are but for of", None);
        // The content line must contain "misc" as the topic segment.
        assert!(
            result.contains("misc"),
            "compress must produce 'misc' topic when no words pass the topic filter"
        );
        assert!(result.contains("0:"), "content line prefix must be present");
    }

    #[test]
    fn compress_with_wing_but_empty_source_file_uses_question_mark_stem() {
        // Line 316: when metadata.source_file is empty but wing is non-empty,
        // the header line is still emitted, and the stem field must be "?".
        let dialect = Dialect::empty();
        let meta = CompressMetadata {
            // Empty source_file triggers the `"?"` branch at line 316.
            source_file: "",
            wing: "test_wing",
            room: "planning",
            date: "2025-01-01",
        };
        let result = dialect.compress(
            "We decided to switch to GraphQL because it is more flexible for clients",
            Some(&meta),
        );
        // Header must contain the wing and must use "?" as the stem.
        assert!(
            result.contains("test_wing"),
            "header must include the wing name"
        );
        assert!(
            result.contains("test_wing|planning|2025-01-01|?"),
            "stem must be '?' when source_file is empty"
        );
    }

    #[test]
    fn decode_fill_content_skips_empty_pipe_segments() {
        // Line 455: an empty segment between pipes (e.g. `||`) must be skipped via
        // the `continue` statement rather than being treated as a quote, flag, or emotion.
        // We craft an AAAK content string with a deliberate empty segment.
        let aaak_with_empty_segment = "0:ALC|project||excite";
        let decoded = Dialect::decode(aaak_with_empty_segment);
        // The empty pipe segment must be skipped entirely: no quote field
        // populated, no flags fabricated. The previous assertion was
        // tautological (`a || !a`) and would have passed even if the
        // decoder synthesized garbage from the empty segment.
        assert!(
            decoded.quote.is_empty(),
            "empty pipe segment must not populate the quote field"
        );
        assert!(
            decoded.flags.is_empty(),
            "empty pipe segment must not produce flags"
        );
        // "excite" is the emotion (not empty, lowercase-leading).
        assert!(
            decoded.emotions.contains(&"excite".to_string()),
            "emotion 'excite' must be decoded correctly after the empty segment"
        );
        // Entities must be decoded from "ALC".
        assert!(
            decoded.entities.contains(&"ALC".to_string()),
            "entity 'ALC' must be decoded from the non-??? entity segment"
        );
    }

    #[test]
    fn decode_fill_content_with_real_entity_codes_populates_entities_vec() {
        // Lines 440-442: entity_str that is neither "???" nor empty must be split on "+"
        // and assigned to decoded.entities. This covers the `if entity_str != "???"` branch.
        let dialect = {
            let mut entities = std::collections::HashMap::new();
            entities.insert("Alice".to_string(), "ALC".to_string());
            Dialect::new(&entities)
        };
        // Compress text that mentions Alice — entity_str will be "ALC", not "???".
        let aaak = dialect.compress("Alice decided to switch to GraphQL for the project", None);
        let decoded = Dialect::decode(&aaak);
        // Entity codes must be populated (not empty, since "ALC" was detected).
        assert!(
            !decoded.entities.is_empty(),
            "entities vec must be non-empty when real entity codes are present"
        );
        assert!(
            decoded.entities.contains(&"ALC".to_string()),
            "ALC code must appear in decoded entities"
        );
    }
}
