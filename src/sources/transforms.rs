//! Reference content transformations for RFC 002 §1.4.
//!
//! Adapters declare which of these transformations they apply via
//! [`SourceAdapter::declared_transformations`]. Core may validate that declared
//! transformations were actually applied by re-running them and comparing output.
//!
//! All functions are pure (no I/O) and operate on `&[u8]` or `&str`.

// ─── Byte-level transforms ─────────────────────────────────────────────────

/// Replace invalid UTF-8 byte sequences with the Unicode replacement character (U+FFFD).
///
/// Valid UTF-8 input is returned unchanged. Adapters should apply this as the
/// first transform in their pipeline to guarantee all subsequent string ops are safe.
pub fn utf8_replace_invalid(input: &[u8]) -> String {
    let result = String::from_utf8_lossy(input).into_owned();
    // If the input was already valid UTF-8 the result must be byte-for-byte equal.
    if let Ok(valid_str) = std::str::from_utf8(input) {
        assert_eq!(
            result, valid_str,
            "utf8_replace_invalid: valid UTF-8 input must be preserved exactly"
        );
    }
    // Each invalid byte is replaced by U+FFFD (3 bytes), so output ≤ 3× input length.
    assert!(
        result.len() <= input.len().saturating_mul(3).saturating_add(1),
        "utf8_replace_invalid: output length cannot exceed 3× input length"
    );
    result
}

// ─── String-level transforms ───────────────────────────────────────────────

/// Normalize line endings to `\n` only.
///
/// Converts CRLF (`\r\n`) and bare CR (`\r`) to LF (`\n`). Adapters should
/// apply this early so downstream code can assume `\n` as the only line separator.
pub fn newline_normalize(input: &str) -> String {
    // Two-pass: replace CRLF first, then remaining bare CR.
    let crlf_removed = input.replace("\r\n", "\n");
    let result = crlf_removed.replace('\r', "\n");
    // Postcondition: no CR characters remain.
    assert!(
        !result.contains('\r'),
        "newline_normalize: CR must not remain after normalization"
    );
    // Normalizing line endings can only shrink or preserve byte length.
    assert!(
        result.len() <= input.len(),
        "newline_normalize: output cannot be longer than input"
    );
    result
}

/// Remove ASCII control characters except horizontal tab (`\t`) and newline (`\n`).
///
/// All non-ASCII bytes are preserved so multi-byte UTF-8 sequences are not corrupted.
/// Apply after [`newline_normalize`] so `\r` has already been converted to `\n`.
pub fn strip_control_chars(input: &str) -> String {
    let result: String = input
        .chars()
        .filter(|&c| c == '\t' || c == '\n' || !c.is_ascii_control())
        .collect();
    // Stripping characters can only shrink or preserve length.
    assert!(
        result.len() <= input.len(),
        "strip_control_chars: output cannot exceed input length"
    );
    // Postcondition: no ASCII control characters except tab/newline remain.
    assert!(
        !result
            .chars()
            .any(|c| c.is_ascii_control() && c != '\t' && c != '\n'),
        "strip_control_chars: no control chars except tab/newline should remain"
    );
    result
}

// ─── Pipeline validation ───────────────────────────────────────────────────

/// Validate that `content` is non-empty after the standard RFC-002 §1.4 transforms.
///
/// Returns `Err(TransformationViolation(...))` if the content would be empty or
/// whitespace-only after [`newline_normalize`] and [`strip_control_chars`] are
/// applied, which the spec disallows for filed drawers.
pub fn validate_content(
    content: &str,
) -> std::result::Result<(), crate::error::SourceAdapterError> {
    let normalized = newline_normalize(content);
    let stripped = strip_control_chars(&normalized);

    if stripped.trim().is_empty() {
        return Err(crate::error::SourceAdapterError::TransformationViolation(
            "content is empty or whitespace-only after RFC-002 §1.4 transforms".to_string(),
        ));
    }

    // Postcondition: no CR sequences remain — newline_normalize was applied.
    assert!(
        !stripped.contains('\r'),
        "validate_content: CR must be absent after newline_normalize"
    );
    // Postcondition: trimmed content is non-empty on the success path.
    assert!(
        !stripped.trim().is_empty(),
        "validate_content: trimmed content must be non-empty on Ok path"
    );
    Ok(())
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── utf8_replace_invalid ───────────────────────────────────────────────

    #[test]
    fn utf8_replace_invalid_preserves_valid_ascii() {
        // Pure ASCII is valid UTF-8 and must be preserved byte-for-byte.
        let input = b"hello world";
        let result = utf8_replace_invalid(input);
        assert_eq!(result, "hello world");
        assert_eq!(result.len(), input.len());
    }

    #[test]
    fn utf8_replace_invalid_replaces_invalid_bytes() {
        // 0xFF is not valid UTF-8 and must be replaced by U+FFFD.
        let input = b"hello\xffworld";
        let result = utf8_replace_invalid(input);
        assert!(
            result.contains('\u{FFFD}'),
            "invalid byte must be replaced by U+FFFD"
        );
        assert!(
            !result.contains('\u{00FF}'),
            "raw 0xFF must not appear in result"
        );
    }

    #[test]
    fn utf8_replace_invalid_preserves_valid_utf8_multibyte() {
        // Multi-byte sequences that form valid UTF-8 must be preserved.
        let input = "café".as_bytes(); // 'é' is 2 bytes in UTF-8
        let result = utf8_replace_invalid(input);
        assert_eq!(result, "café");
        assert_eq!(result.chars().count(), 4);
    }

    // ── newline_normalize ──────────────────────────────────────────────────

    #[test]
    fn newline_normalize_converts_crlf_to_lf() {
        // Windows CRLF must be replaced by a single LF.
        let input = "line1\r\nline2\r\nline3";
        let result = newline_normalize(input);
        assert_eq!(result, "line1\nline2\nline3");
        assert!(
            !result.contains('\r'),
            "no CR must remain after normalization"
        );
    }

    #[test]
    fn newline_normalize_converts_bare_cr_to_lf() {
        // Old Mac CR-only line endings must also become LF.
        let input = "line1\rline2\rline3";
        let result = newline_normalize(input);
        assert_eq!(result, "line1\nline2\nline3");
        assert!(
            !result.contains('\r'),
            "no CR must remain after normalization"
        );
    }

    #[test]
    fn newline_normalize_leaves_lf_only_unchanged() {
        // Input that already uses LF must be returned unchanged.
        let input = "line1\nline2\nline3";
        let result = newline_normalize(input);
        assert_eq!(result, input, "LF-only input must be unchanged");
        assert_eq!(result.len(), input.len());
    }

    // ── strip_control_chars ────────────────────────────────────────────────

    #[test]
    fn strip_control_chars_removes_null_and_bell() {
        // NUL (0x00) and BEL (0x07) must be removed.
        let input = "hello\x00world\x07";
        let result = strip_control_chars(input);
        assert_eq!(result, "helloworld");
        assert!(!result.contains('\x00'), "NUL must be stripped");
    }

    #[test]
    fn strip_control_chars_preserves_tab_and_newline() {
        // Tab and newline are the only control chars that must be preserved.
        let input = "col1\tcol2\nrow2";
        let result = strip_control_chars(input);
        assert_eq!(result, input, "tab and newline must be preserved");
        assert!(result.contains('\t') && result.contains('\n'));
    }

    // ── validate_content ───────────────────────────────────────────────────

    #[test]
    fn validate_content_accepts_non_empty_content() {
        // Content with printable text must pass validation.
        let result = validate_content("Hello, world!");
        assert!(result.is_ok(), "non-empty content must pass validation");
        // Pair: whitespace-padded non-empty content also passes.
        assert!(
            validate_content("  Hello  ").is_ok(),
            "content with surrounding whitespace but non-empty core must pass"
        );
    }

    #[test]
    fn validate_content_rejects_empty_and_whitespace_content() {
        // Empty string must fail with TransformationViolation.
        let empty_result = validate_content("");
        assert!(empty_result.is_err(), "empty content must fail validation");
        // Pair: whitespace-only content must also fail.
        let whitespace_result = validate_content("   \n\t  ");
        assert!(
            whitespace_result.is_err(),
            "whitespace-only content must fail validation"
        );
    }

    #[test]
    fn strip_control_chars_preserves_multibyte_unicode() {
        // Non-ASCII bytes must not be touched by control-char stripping.
        let input = "héllo\x00wörld";
        let result = strip_control_chars(input);
        assert!(result.contains('é'), "multibyte 'é' must be preserved");
        assert!(result.contains('ö'), "multibyte 'ö' must be preserved");
        assert!(!result.contains('\x00'), "NUL must still be stripped");
    }
}
