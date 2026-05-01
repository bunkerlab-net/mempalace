//! Parser for Gemini CLI `JSONL` session exports.
//!
//! Gemini CLI writes sessions to `~/.gemini/tmp/<project_hash>/chats/` as
//! `session-*.jsonl`. Each line is a JSON object with a `type` field.
//! We require a `session_metadata` entry (not present in Claude Code or
//! Codex JSONL) so this parser does not false-positive in the dispatch chain.
//!
//! Any `user`/`gemini` turns that appear before `session_metadata` are
//! discarded — they are preamble noise. `message_update` entries carry only
//! token counts and are skipped. Multiple text blocks in a message's
//! `content` array are concatenated with `\n`.

use super::messages_to_transcript;

/// Try to parse a Gemini CLI JSONL session into transcript text.
///
/// Returns `None` when the content lacks a `session_metadata` entry, fewer
/// than 2 messages were collected after the sentinel, or the format does
/// not match the expected Gemini CLI schema.
pub fn try_parse(content: &str) -> Option<String> {
    assert!(!content.is_empty(), "try_parse: content must not be empty");

    let mut messages: Vec<(String, String)> = Vec::new();
    let mut has_session_metadata = false;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: serde_json::Value = match serde_json::from_str(line) {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };
        let Some(obj) = entry.as_object() else {
            continue;
        };

        let entry_type = obj
            .get("type")
            .and_then(|type_val| type_val.as_str())
            .unwrap_or("");

        if entry_type == "session_metadata" {
            has_session_metadata = true;
            continue;
        }

        // Discard everything — including user/gemini turns — until the
        // session_metadata sentinel has been seen.
        if !has_session_metadata {
            continue;
        }

        if entry_type != "user" && entry_type != "gemini" {
            continue;
        }

        let text = try_parse_content_blocks(obj);
        if text.is_empty() {
            continue;
        }

        let role = if entry_type == "user" {
            "user"
        } else {
            "assistant"
        };
        messages.push((role.to_string(), text));
    }

    // Negative-space: at least 2 messages required (mirrors Codex guard).
    if has_session_metadata && messages.len() >= 2 {
        let refs: Vec<(&str, &str)> = messages
            .iter()
            .map(|(role, text)| (role.as_str(), text.as_str()))
            .collect();
        Some(messages_to_transcript(&refs))
    } else {
        None
    }
}

/// Extract and join non-empty text blocks from a Gemini CLI message entry's
/// `content` array of `{"text": "..."}` objects.
fn try_parse_content_blocks(obj: &serde_json::Map<String, serde_json::Value>) -> String {
    let Some(content_array) = obj.get("content").and_then(|val| val.as_array()) else {
        return String::new();
    };

    let parts: Vec<&str> = content_array
        .iter()
        .filter_map(|block| {
            let text = block.get("text")?.as_str()?;
            if text.trim().is_empty() {
                None
            } else {
                Some(text)
            }
        })
        .collect();

    assert!(
        parts.len() <= content_array.len(),
        "try_parse_content_blocks: parts must not exceed blocks"
    );

    parts.join("\n")
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_gemini_session() {
        let jsonl = r#"{"type":"session_metadata","sessionId":"s1","projectHash":"abc"}
{"type":"user","id":"m1","content":[{"text":"Hello Gemini"}]}
{"type":"gemini","id":"m2","content":[{"text":"Hello! How can I help?"}]}"#;
        let result = try_parse(jsonl);
        assert!(result.is_some(), "valid session must parse");
        let text = result.expect("must be Some");
        assert!(
            text.contains("> Hello Gemini"),
            "user turn must have > prefix"
        );
        assert!(
            text.contains("Hello! How can I help?"),
            "assistant turn must appear"
        );
    }

    #[test]
    fn returns_none_without_session_metadata() {
        // Missing session_metadata means the content could be Claude Code or
        // Codex JSONL; parser must return None to avoid false-positive.
        let jsonl = r#"{"type":"user","content":[{"text":"hello"}]}
{"type":"gemini","content":[{"text":"hi"}]}"#;
        assert!(
            try_parse(jsonl).is_none(),
            "missing session_metadata must return None"
        );
    }

    #[test]
    fn pre_sentinel_turns_are_discarded() {
        // user/gemini turns before session_metadata must not appear in output.
        let jsonl = r#"{"type":"user","content":[{"text":"before sentinel — discard me"}]}
{"type":"session_metadata","sessionId":"s2"}
{"type":"user","id":"a","content":[{"text":"after sentinel — keep me"}]}
{"type":"gemini","id":"b","content":[{"text":"reply"}]}"#;
        let result = try_parse(jsonl).expect("must parse");
        assert!(
            !result.contains("discard me"),
            "pre-sentinel turn must be discarded"
        );
        assert!(
            result.contains("after sentinel"),
            "post-sentinel turn must be present"
        );
    }

    #[test]
    fn message_update_entries_are_skipped() {
        let jsonl = r#"{"type":"session_metadata","sessionId":"s3"}
{"type":"user","content":[{"text":"question"}]}
{"type":"message_update","id":"m1","tokens":{"input":10,"output":5}}
{"type":"gemini","content":[{"text":"answer"}]}"#;
        let result = try_parse(jsonl).expect("must parse");
        assert!(result.contains("> question"), "user turn must be present");
        assert!(result.contains("answer"), "assistant turn must be present");
    }

    #[test]
    fn returns_none_for_single_message() {
        let jsonl = r#"{"type":"session_metadata","sessionId":"s4"}
{"type":"user","content":[{"text":"only one message"}]}"#;
        assert!(
            try_parse(jsonl).is_none(),
            "fewer than 2 messages must return None"
        );
    }

    #[test]
    fn multi_block_content_is_concatenated() {
        let jsonl = r#"{"type":"session_metadata","sessionId":"s5"}
{"type":"user","content":[{"text":"part one"},{"text":"part two"}]}
{"type":"gemini","content":[{"text":"combined reply"}]}"#;
        let result = try_parse(jsonl).expect("must parse");
        assert!(result.contains("part one"), "first block must be present");
        assert!(result.contains("part two"), "second block must be present");
    }

    #[test]
    fn empty_text_blocks_are_skipped() {
        let jsonl = r#"{"type":"session_metadata","sessionId":"s6"}
{"type":"user","content":[{"text":""},{"text":"   "},{"text":"real content"}]}
{"type":"gemini","content":[{"text":"ok"}]}"#;
        let result = try_parse(jsonl).expect("must parse");
        assert!(
            result.contains("real content"),
            "non-empty block must be present"
        );
    }

    #[test]
    fn malformed_lines_are_skipped() {
        let jsonl = r#"{"type":"session_metadata","sessionId":"s7"}
not json at all
{"type":"user","content":[{"text":"valid"}]}
also not json
{"type":"gemini","content":[{"text":"reply"}]}"#;
        let result = try_parse(jsonl).expect("must parse despite malformed lines");
        assert!(result.contains("> valid"), "user turn must be present");
    }

    #[test]
    fn does_not_match_codex_jsonl() {
        // Codex JSONL uses session_meta (not session_metadata); Gemini parser
        // must return None so the dispatch chain continues to the Codex parser.
        let codex_jsonl = r#"{"type":"session_meta","session_id":"abc123"}
{"type":"event_msg","payload":{"type":"user_message","message":"hello codex"}}
{"type":"event_msg","payload":{"type":"agent_message","message":"hi there"}}"#;
        assert!(
            try_parse(codex_jsonl).is_none(),
            "Codex JSONL must not be matched by Gemini parser"
        );
    }

    #[test]
    fn multi_turn_conversation_preserves_order() {
        let jsonl = r#"{"type":"session_metadata","sessionId":"s8"}
{"type":"user","content":[{"text":"first"}]}
{"type":"gemini","content":[{"text":"second"}]}
{"type":"user","content":[{"text":"third"}]}
{"type":"gemini","content":[{"text":"fourth"}]}"#;
        let result = try_parse(jsonl).expect("must parse");
        let pos_first = result.find("first").expect("first must be present");
        let pos_second = result.find("second").expect("second must be present");
        let pos_third = result.find("third").expect("third must be present");
        let pos_fourth = result.find("fourth").expect("fourth must be present");
        assert!(pos_first < pos_second, "turns must be in order");
        assert!(pos_second < pos_third, "turns must be in order");
        assert!(pos_third < pos_fourth, "turns must be in order");
    }
}
