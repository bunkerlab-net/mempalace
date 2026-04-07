//! Chat export format detection and normalization to `> user\nresponse` transcript format.

pub mod chatgpt;
pub mod claude_ai;
pub mod claude_code;
pub mod slack;

use std::path::Path;

use crate::error::Result;

/// Normalize a file to transcript format.
/// Detects format automatically and converts to `> user\nresponse\n\n` format.
pub fn normalize(filepath: &Path) -> Result<String> {
    let content = std::fs::read_to_string(filepath).or_else(|_| {
        std::fs::read(filepath).map(|bytes| String::from_utf8_lossy(&bytes).to_string())
    })?;

    let content = content.trim().to_string();
    if content.is_empty() {
        return Ok(content);
    }

    // Already has > markers — pass through
    let quote_count = content
        .lines()
        .filter(|l| l.trim_start().starts_with('>'))
        .count();
    if quote_count >= 3 {
        return Ok(content);
    }

    // Try JSON normalization
    let ext = filepath
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    if (ext == "json" || ext == "jsonl" || content.starts_with('{') || content.starts_with('['))
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
    let mut lines = Vec::new();
    let mut i = 0;

    while i < messages.len() {
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
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("test.txt");
        std::fs::write(
            &path,
            "> hello\nresponse\n\n> second\nreply\n\n> third\nanother",
        )
        .expect("write test file");
        let result = normalize(&path).expect("normalize");
        assert!(result.contains("> hello"));
        // TempDir auto-cleans up when dropped
    }
}
