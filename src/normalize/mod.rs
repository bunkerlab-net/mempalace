//! Chat export format detection and normalization to `> user\nresponse` transcript format.

pub mod chatgpt;
pub mod claude_ai;
pub mod claude_code;
pub mod codex;
pub mod slack;

use std::path::Path;

use crate::error::Result;

/// Normalize a file to transcript format.
/// Detects format automatically and converts to `> user\nresponse\n\n` format.
pub fn normalize(filepath: &Path) -> Result<String> {
    const MAX_SIZE: u64 = 500 * 1024 * 1024; // 500 MB safety limit
    if !filepath.exists() {
        return Err(crate::error::Error::Other(format!(
            "normalize: file not found: {}",
            filepath.display()
        )));
    }
    let file_size = std::fs::metadata(filepath)
        .map_err(|e| {
            crate::error::Error::Other(format!("could not read {}: {e}", filepath.display()))
        })?
        .len();
    if file_size > MAX_SIZE {
        return Err(crate::error::Error::Other(format!(
            "file too large ({} MB): {}",
            file_size / (1024 * 1024),
            filepath.display()
        )));
    }
    let content = std::fs::read_to_string(filepath).or_else(|_| {
        std::fs::read(filepath).map(|bytes| String::from_utf8_lossy(&bytes).to_string())
    })?;

    let content = content.trim().to_string();
    if content.is_empty() {
        return Ok(content);
    }

    // If the file already contains ≥ 3 lines with `>` markers it is already
    // in transcript format (or close enough). The threshold of 3 avoids
    // misidentifying Markdown blockquotes (which commonly appear once or twice)
    // as a transcript.
    let quote_count = content
        .lines()
        .filter(|l| l.trim_start().starts_with('>'))
        .count();
    if quote_count >= 3 {
        return Ok(content);
    }

    // JSONL formats (Claude Code, Codex) are tried before serde_json::from_str
    // because they are not valid JSON — serde would reject them. Try them first
    // so we never pass a multi-line JSONL file to the JSON parser.
    let extension = filepath
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    if (extension == "json"
        || extension == "jsonl"
        || content.starts_with('{')
        || content.starts_with('['))
        && let Some(normalized) = try_normalize_json(&content)
    {
        return Ok(normalized);
    }

    Ok(content)
}

fn try_normalize_json(content: &str) -> Option<String> {
    // Try Claude Code JSONL first
    if let Some(result) = claude_code::try_parse(content) {
        return Some(result);
    }

    // Try Codex CLI JSONL
    if let Some(result) = codex::try_parse(content) {
        return Some(result);
    }

    // Try parsing as JSON
    let data: serde_json::Value = serde_json::from_str(content).ok()?;

    // Try each format
    for parser in [claude_ai::try_parse, chatgpt::try_parse, slack::try_parse] {
        if let Some(result) = parser(&data) {
            return Some(result);
        }
    }

    None
}

/// Convert [(role, text), ...] to transcript format with > markers.
pub fn messages_to_transcript(messages: &[(&str, &str)]) -> String {
    // Precondition: all roles are non-empty.
    debug_assert!(messages.iter().all(|(role, _)| !role.is_empty()));
    let mut lines = Vec::new();
    let mut i = 0;

    // Upper bound: i advances by at least 1 each iteration, bounded by messages.len().
    while i < messages.len() {
        assert!(i < messages.len());
        let (role, text) = messages[i];
        if role == "user" {
            lines.push(format!("> {text}"));
            if i + 1 < messages.len() && messages[i + 1].0 == "assistant" {
                lines.push(messages[i + 1].1.to_string());
                i += 2;
            } else {
                i += 1;
            }
        } else {
            lines.push(text.to_string());
            i += 1;
        }
        lines.push(String::new());
    }

    lines.join("\n")
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn messages_to_transcript_pairs() {
        let msgs = vec![("user", "hello"), ("assistant", "hi there")];
        let result = messages_to_transcript(&msgs);
        assert!(result.contains("> hello"));
        assert!(result.contains("hi there"));
    }

    #[test]
    fn messages_to_transcript_user_only() {
        let msgs = vec![("user", "question")];
        let result = messages_to_transcript(&msgs);
        assert!(result.contains("> question"));
    }

    #[test]
    fn messages_to_transcript_empty() {
        let msgs: Vec<(&str, &str)> = vec![];
        let result = messages_to_transcript(&msgs);
        assert!(result.is_empty());
    }

    #[test]
    fn normalize_passthrough_with_markers() {
        // Text with >= 3 lines starting with > should pass through
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("test.txt");
        std::fs::write(
            &path,
            "> hello\nresponse\n\n> second\nreply\n\n> third\nanother",
        )
        .expect("write test file");
        let result = normalize(&path).expect("normalize");
        assert!(result.contains("> hello"));
        // TempDir auto-cleans up when dropped
    }

    #[test]
    fn normalize_file_not_found_returns_error() {
        // normalize must return Err when the file does not exist.
        // Use a temp directory to produce a deterministic nonexistent path.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for not-found test");
        let nonexistent_path = temp_directory.path().join("no_such_file.txt");
        let result = normalize(&nonexistent_path);
        assert!(result.is_err(), "missing file must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("not found")),
            "error must mention 'not found'"
        );
    }

    #[test]
    fn normalize_empty_file_returns_ok_empty() {
        // An empty file must normalize to an empty string without error.
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("empty.txt");
        std::fs::write(&path, "").expect("write empty file");
        let result = normalize(&path).expect("empty file must return Ok");
        assert!(
            result.is_empty(),
            "empty file content must normalize to empty string"
        );
        assert_eq!(result.len(), 0, "result length must be zero");
    }

    #[test]
    fn normalize_plain_text_passthrough() {
        // Plain text (not JSON, not enough > markers) must be returned as-is.
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("plain.txt");
        let content = "This is just plain text content.\nNo JSON, no > markers here at all.\n";
        std::fs::write(&path, content).expect("write plain text file");
        let result = normalize(&path).expect("plain text must return Ok");
        // Exact equality after trim: normalize trims whitespace but must not alter content.
        assert_eq!(
            result.trim(),
            content.trim(),
            "normalized plain text must exactly equal the original trimmed content"
        );
        assert!(!result.is_empty(), "plain text result must not be empty");
    }

    #[test]
    fn normalize_unrecognized_json_returns_raw_content() {
        // Valid JSON that does not match any known chat format (Claude Code, Codex,
        // Claude AI, ChatGPT, Slack) must fall through and return the raw content.
        // Table-driven: each case uses a different JSON shape and file extension to
        // exercise distinct code paths (.json extension vs content-sniffing).
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for unrecognized-JSON test");
        let cases: &[(&str, &str)] = &[
            // (.json extension — triggers extension-based JSON path)
            (
                "unknown.json",
                r#"{"unknown_key": "value", "data": [1, 2, 3]}"#,
            ),
            // (.json extension — different JSON shape)
            (
                "weather.json",
                r#"{"weather": "sunny", "temperature": 72, "units": "fahrenheit"}"#,
            ),
            // (non-.json extension — triggers content-sniffing via `content.starts_with('{')`)
            (
                "config.txt",
                r#"{"sniffed_key": "detected by content start", "value": true}"#,
            ),
        ];
        for &(filename, json_content) in cases {
            let filepath = temp_directory.path().join(filename);
            std::fs::write(&filepath, json_content)
                .expect("failed to write unrecognized JSON test file");
            let result =
                normalize(&filepath).expect("normalize must succeed for unrecognized JSON");
            assert!(
                !result.is_empty(),
                "result must not be empty for {filename}"
            );
            assert_eq!(
                result.trim(),
                json_content.trim(),
                "unrecognized JSON must be returned as raw content unchanged ({filename})"
            );
        }
    }

    #[test]
    fn normalize_two_quote_lines_do_not_short_circuit() {
        // Fewer than 3 lines starting with > must not short-circuit to the
        // transcript passthrough; format detection runs but finds no match,
        // so the raw content is returned unchanged (after trim).
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("two_quotes.txt");
        let content = "> first line\n> second line\nplain text here";
        std::fs::write(&path, content).expect("write two-quote file");
        let result = normalize(&path).expect("two-quote file must return Ok");
        assert_eq!(
            result.trim(),
            content.trim(),
            "content with only two > lines must be returned unchanged"
        );
        assert!(!result.is_empty(), "result must not be empty");
    }

    #[test]
    fn normalize_jsonl_extension_triggers_json_path() {
        // A .jsonl file with Claude Code JSONL content must be detected via the
        // extension check even though .jsonl is not .json. This exercises the
        // `extension == "jsonl"` branch in normalize().
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for normalize test");
        let filepath = temp_directory.path().join("conversation.jsonl");
        let content = r#"{"type":"human","message":{"content":"hello from jsonl"}}
{"type":"assistant","message":{"content":"reply from assistant"}}"#;
        std::fs::write(&filepath, content).expect("failed to write .jsonl test file");
        let result = normalize(&filepath).expect("normalize must succeed for valid .jsonl file");
        // The Claude Code JSONL parser must have matched; verify transcript markers.
        assert!(
            result.contains("> hello from jsonl"),
            "user turn must be prefixed with > marker"
        );
        assert!(
            result.contains("reply from assistant"),
            "assistant turn must appear in transcript"
        );
    }

    #[test]
    fn normalize_json_content_without_json_extension() {
        // A file without .json/.jsonl extension but whose content starts with '{'
        // must still trigger the JSON detection path. This exercises the
        // `content.starts_with('{')` branch.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for normalize test");
        let filepath = temp_directory.path().join("conversation.txt");
        // Claude AI format as a JSON object — starts with '{' but has .txt extension.
        let content = r#"{"messages":[{"role":"user","content":"detected by content"},{"role":"assistant","content":"yes indeed"}]}"#;
        std::fs::write(&filepath, content).expect("failed to write JSON-content .txt test file");
        let result = normalize(&filepath).expect("normalize must succeed for JSON content in .txt");
        assert!(
            result.contains("> detected by content"),
            "user turn must be parsed from JSON content in non-.json file"
        );
        assert!(
            result.contains("yes indeed"),
            "assistant turn must be parsed from JSON content in non-.json file"
        );
    }

    #[test]
    fn normalize_json_array_content_without_json_extension() {
        // A file without .json extension but whose content starts with '[' must
        // still trigger the JSON detection path. This exercises the
        // `content.starts_with('[')` branch.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for normalize test");
        let filepath = temp_directory.path().join("chat.log");
        // Claude AI flat array format — starts with '[' but has .log extension.
        let content = r#"[{"role":"user","content":"array start"},{"role":"assistant","content":"array reply"}]"#;
        std::fs::write(&filepath, content).expect("failed to write JSON-array .log test file");
        let result = normalize(&filepath).expect("normalize must succeed for JSON array in .log");
        assert!(
            result.contains("> array start"),
            "user turn must be parsed from JSON array in non-.json file"
        );
        assert!(
            result.contains("array reply"),
            "assistant turn must be parsed from JSON array in non-.json file"
        );
    }

    #[test]
    fn normalize_file_too_large_returns_error() {
        // Files exceeding the 500 MB safety limit must be rejected. Create a sparse
        // file (no disk space consumed) whose metadata reports a size above MAX_SIZE.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for size-limit test");
        let filepath = temp_directory.path().join("huge.json");
        let file = std::fs::File::create(&filepath)
            .expect("failed to create sparse file for size-limit test");
        // 500 MB + 1 byte exceeds the MAX_SIZE constant (500 * 1024 * 1024).
        file.set_len(500 * 1024 * 1024 + 1)
            .expect("failed to set sparse file length above MAX_SIZE");
        drop(file);

        let result = normalize(&filepath);
        assert!(result.is_err(), "file exceeding 500 MB must be rejected");
        assert!(
            result
                .as_ref()
                .err()
                .is_some_and(|error| error.to_string().contains("too large")),
            "error message must mention 'too large'"
        );
    }
}
