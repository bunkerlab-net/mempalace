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
//!   4. Tail truncation — fallback, take the last 500 chars.

use regex::Regex;

const MAX_QUERY_LEN: usize = 500;
const SAFE_QUERY_LEN: usize = 200;
const MIN_SEGMENT_LEN: usize = 10;

/// Result of [`sanitize_query`].
pub struct SanitizedQuery {
    /// The cleaned query to use for search.
    pub clean_query: String,
    /// Whether any sanitization was applied.
    pub was_sanitized: bool,
    /// Byte length of the original input.
    pub original_length: usize,
    /// Byte length of the cleaned output.
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
    let original_length = raw.len();

    if raw.is_empty() {
        return passthrough(String::new(), 0);
    }

    // Step 1: short query — almost certainly not contaminated.
    if original_length <= SAFE_QUERY_LEN {
        return passthrough(raw.to_owned(), original_length);
    }

    let segments: Vec<&str> = raw
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    // Step 2: find the last newline-segment that ends with `?` or `？`.
    let question_re = Regex::new(r#"[?？]\s*["']?\s*$"#)
        .expect("valid regex: question_re pattern is a compile-time constant");

    for seg in segments.iter().rev() {
        if question_re.is_match(seg) && seg.len() >= MIN_SEGMENT_LEN {
            let candidate = tail_guard(seg);
            eprintln!(
                "mempalace: query sanitized {original_length} → {} chars (method=question_extraction)",
                candidate.len()
            );
            return sanitized(candidate, original_length, "question_extraction");
        }
    }

    // Step 3: take the last meaningful segment (system prompts are prepended,
    // so the actual query is at the end of the string).
    for seg in segments.iter().rev() {
        if seg.len() >= MIN_SEGMENT_LEN {
            let candidate = tail_guard(seg);
            eprintln!(
                "mempalace: query sanitized {original_length} → {} chars (method=tail_sentence)",
                candidate.len()
            );
            return sanitized(candidate, original_length, "tail_sentence");
        }
    }

    // Step 4: nothing usable found — truncate to the tail.
    let candidate = tail_guard(raw);
    eprintln!(
        "mempalace: query sanitized {original_length} → {} chars (method=tail_truncation)",
        candidate.len()
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
    let clean_length = clean_query.len();
    SanitizedQuery {
        clean_query,
        was_sanitized: true,
        original_length,
        clean_length,
        method,
    }
}

/// Return the last [`MAX_QUERY_LEN`] bytes of `s`, adjusted to a valid UTF-8
/// char boundary so slicing never panics.
fn tail_guard(s: &str) -> String {
    if s.len() <= MAX_QUERY_LEN {
        return s.to_owned();
    }
    let byte_start = s.len() - MAX_QUERY_LEN;
    // Advance to the next valid char boundary at or after `byte_start`.
    let boundary = (byte_start..=s.len())
        .find(|&i| s.is_char_boundary(i))
        .unwrap_or(s.len());
    s[boundary..].to_owned()
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
    fn tail_sentence() {
        let prompt = format!("{}\nchromadb locking bug", "x".repeat(300));
        let r = sanitize_query(&prompt);
        assert!(r.was_sanitized);
        assert_eq!(r.method, "tail_sentence");
        assert_eq!(r.clean_query, "chromadb locking bug");
    }

    #[test]
    fn tail_truncation() {
        // All newline-segments are tiny (< MIN_SEGMENT_LEN), forcing fallback.
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
        // 300 bytes of ASCII + a 3-byte UTF-8 char pushes total over SAFE_QUERY_LEN
        let prompt = format!("{}{}", "a".repeat(300), "é".repeat(50));
        let r = sanitize_query(&prompt);
        assert!(std::str::from_utf8(r.clean_query.as_bytes()).is_ok());
    }
}
