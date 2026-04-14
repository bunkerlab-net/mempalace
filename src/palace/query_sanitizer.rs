//! Query sanitizer — mitigate system prompt contamination in search queries.
//!
//! Problem: AI agents sometimes prepend system prompts (2000+ chars) to search
//! queries. Embedding models represent the full string as a single vector where
//! the system prompt overwhelms the actual question (typically 10–50 chars),
//! causing near-total retrieval failure. See mempalace-py issue #333.
//!
//! Approach: four-step extraction, in order of precision:
//!   1. Short-query passthrough (≤ 200 chars) — no action needed.
//!   2. Question extraction — find a sentence ending with `?`.
//!   3. Tail sentence — take the last meaningful newline-delimited segment.
//!   4. Tail truncation — fallback, take the last 250 chars.

use std::sync::LazyLock;

use regex::Regex;

const MAX_QUERY_LEN: usize = 250;
const SAFE_QUERY_LEN: usize = 200;
const MIN_QUESTION_SEGMENT_LEN: usize = 3;

#[allow(clippy::expect_used)]
// Matches a sentence ending with a question mark (including Unicode `？`),
// with optional trailing quote/whitespace. Compiled once at first use.
static QUESTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"[?？]\s*["']?\s*$"#)
        .expect("question regex is a compile-time literal and cannot fail to compile")
});

/// Result of [`sanitize_query`].
pub struct SanitizedQuery {
    /// The cleaned query to use for search.
    pub clean_query: String,
    /// Whether any sanitization was applied.
    pub was_sanitized: bool,
    /// Char count of the trimmed input (see the `trim()` call at the top of
    /// [`sanitize_query`] — the raw string is trimmed before this is measured).
    pub original_length: usize,
    /// Char count of the cleaned output.
    pub clean_length: usize,
    /// Name of the method used.
    pub method: &'static str,
}

/// Extract the actual search intent from a potentially contaminated query.
///
/// Logs a warning to stderr (not stdout — MCP servers must not pollute stdout)
/// when sanitization is applied.
#[must_use]
pub fn sanitize_query(raw: &str) -> SanitizedQuery {
    let raw = raw.trim();
    let original_length = raw.chars().count();

    if raw.is_empty() {
        return passthrough(String::new(), 0);
    }
    assert!(original_length > 0);

    // Step 1: short query — almost certainly not contaminated.
    if original_length <= SAFE_QUERY_LEN {
        return passthrough(raw.to_owned(), original_length);
    }

    let segments: Vec<&str> = raw
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    // Step 2/3: treat the trailing segment as the primary intent carrier.
    if let Some(last_seg) = segments.last().copied() {
        let last_len = last_seg.chars().count();
        if QUESTION_RE.is_match(last_seg) && last_len >= MIN_QUESTION_SEGMENT_LEN {
            let candidate = tail_guard(last_seg);
            eprintln!(
                "mempalace: query sanitized {original_length} → {} chars (method=question_extraction)",
                candidate.chars().count()
            );
            return sanitized(candidate, original_length, "question_extraction");
        }
        if last_len >= MIN_QUESTION_SEGMENT_LEN {
            let candidate = tail_guard(last_seg);
            eprintln!(
                "mempalace: query sanitized {original_length} → {} chars (method=tail_sentence)",
                candidate.chars().count()
            );
            return sanitized(candidate, original_length, "tail_sentence");
        }
    }

    // Step 4: nothing usable found — truncate to the tail.
    let candidate = tail_guard(raw);
    eprintln!(
        "mempalace: query sanitized {original_length} → {} chars (method=tail_truncation)",
        candidate.chars().count()
    );
    sanitized(candidate, original_length, "tail_truncation")
}

fn passthrough(s: String, len: usize) -> SanitizedQuery {
    SanitizedQuery {
        clean_length: len,
        clean_query: s,
        was_sanitized: false,
        original_length: len,
        method: "passthrough",
    }
}

fn sanitized(clean_query: String, original_length: usize, method: &'static str) -> SanitizedQuery {
    let clean_length = clean_query.chars().count();
    SanitizedQuery {
        clean_query,
        was_sanitized: true,
        original_length,
        clean_length,
        method,
    }
}

/// Return the last [`MAX_QUERY_LEN`] chars of `s`.
fn tail_guard(s: &str) -> String {
    assert!(!s.is_empty(), "tail_guard: input must not be empty");

    let total = s.chars().count();
    if total <= MAX_QUERY_LEN {
        return s.to_owned();
    }
    let skip = total - MAX_QUERY_LEN;
    let byte_start = s.char_indices().nth(skip).map_or(0, |(i, _)| i);
    let result = s[byte_start..].to_owned();

    // Postcondition: output is bounded by MAX_QUERY_LEN.
    debug_assert!(result.chars().count() <= MAX_QUERY_LEN);

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_short() {
        let r = sanitize_query("what is the capital of France?");
        assert!(!r.was_sanitized);
        assert_eq!(r.method, "passthrough");
        assert_eq!(r.clean_query, "what is the capital of France?");
    }

    #[test]
    fn passthrough_empty() {
        let r = sanitize_query("   ");
        assert!(!r.was_sanitized);
        assert_eq!(r.clean_query, "");
    }

    #[test]
    fn question_extraction() {
        let prompt = format!(
            "{}\nwhat did we decide about the database schema?",
            "x".repeat(300)
        );
        let r = sanitize_query(&prompt);
        assert!(r.was_sanitized);
        assert_eq!(r.method, "question_extraction");
        assert_eq!(
            r.clean_query,
            "what did we decide about the database schema?"
        );
    }

    #[test]
    fn question_extraction_short_question_segment() {
        let prompt = format!("{}\nETA?", "x".repeat(300));
        let r = sanitize_query(&prompt);
        assert!(r.was_sanitized);
        assert_eq!(r.method, "question_extraction");
        assert_eq!(r.clean_query, "ETA?");
    }

    #[test]
    fn tail_sentence() {
        let prompt = format!("{}\nchromadb locking bug", "x".repeat(300));
        let r = sanitize_query(&prompt);
        assert!(r.was_sanitized);
        assert_eq!(r.method, "tail_sentence");
        assert_eq!(r.clean_query, "chromadb locking bug");
    }

    #[test]
    fn tail_truncation() {
        // All newline-segments are tiny (only 2 chars each), forcing fallback to tail_truncation.
        let prompt = "ab\n".repeat(100); // 300 chars; each segment "ab" is only 2 chars
        let r = sanitize_query(&prompt);
        assert!(r.was_sanitized);
        assert_eq!(r.method, "tail_truncation");
    }

    #[test]
    fn tail_sentence_long_line() {
        // Single long line with no newlines → tail_sentence via the last (only) segment.
        let prompt = "a".repeat(600);
        let r = sanitize_query(&prompt);
        assert!(r.was_sanitized);
        assert_eq!(r.method, "tail_sentence");
        assert_eq!(r.clean_length, MAX_QUERY_LEN);
    }

    #[test]
    fn utf8_boundary_safe() {
        // Force truncation with multi-byte chars to validate UTF-8-safe slicing.
        let prompt = "é".repeat(550);
        let r = sanitize_query(&prompt);
        assert_eq!(r.clean_length, MAX_QUERY_LEN);
        assert!(std::str::from_utf8(r.clean_query.as_bytes()).is_ok());
    }
}
