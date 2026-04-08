//! Parser for `OpenAI` Codex CLI `JSONL` session exports.
//!
//! Codex CLI writes sessions to `~/.codex/sessions/*/rollout-*.jsonl`.
//! Each line is a JSON object. We process only `event_msg` entries and
//! require at least one `session_meta` entry for format validation.

use super::messages_to_transcript;

/// Try to parse a Codex CLI JSONL session into transcript text.
///
/// Returns `None` if the file lacks a `session_meta` entry, contains
/// fewer than 2 messages, or doesn't match the expected format.
pub fn try_parse(content: &str) -> Option<String> {
    let mut messages: Vec<(String, String)> = Vec::new();
    let mut has_session_meta = false;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(obj) = entry.as_object() else {
            continue;
        };

        let entry_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");

        if entry_type == "session_meta" {
            has_session_meta = true;
            continue;
        }

        if entry_type != "event_msg" {
            continue;
        }

        let Some(payload) = obj.get("payload").and_then(|v| v.as_object()) else {
            continue;
        };

        let payload_type = payload.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let text = match payload.get("message").and_then(|v| v.as_str()) {
            Some(s) => s.trim().to_string(),
            None => continue,
        };

        if text.is_empty() {
            continue;
        }

        match payload_type {
            "user_message" => messages.push(("user".to_string(), text)),
            "agent_message" => messages.push(("assistant".to_string(), text)),
            _ => {}
        }
    }

    if has_session_meta && messages.len() >= 2 {
        let refs: Vec<(&str, &str)> = messages
            .iter()
            .map(|(r, t)| (r.as_str(), t.as_str()))
            .collect();
        Some(messages_to_transcript(&refs))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_codex_session() {
        let jsonl = r#"{"type":"session_meta","session_id":"abc123"}
{"type":"event_msg","payload":{"type":"user_message","message":"hello codex"}}
{"type":"event_msg","payload":{"type":"agent_message","message":"hi there"}}"#;
        let result = try_parse(jsonl);
        assert!(result.is_some());
        let text = result.expect("should parse");
        assert!(text.contains("> hello codex"));
        assert!(text.contains("hi there"));
    }

    #[test]
    fn returns_none_without_session_meta() {
        let jsonl = r#"{"type":"event_msg","payload":{"type":"user_message","message":"hello"}}
{"type":"event_msg","payload":{"type":"agent_message","message":"hi"}}"#;
        assert!(try_parse(jsonl).is_none());
    }

    #[test]
    fn skips_response_item_entries() {
        let jsonl = r#"{"type":"session_meta","session_id":"abc"}
{"type":"response_item","payload":{"type":"user_message","message":"duplicate"}}
{"type":"event_msg","payload":{"type":"user_message","message":"real user"}}
{"type":"event_msg","payload":{"type":"agent_message","message":"real agent"}}"#;
        let result = try_parse(jsonl).expect("should parse");
        assert!(result.contains("> real user"));
        assert!(!result.contains("duplicate"));
    }

    #[test]
    fn returns_none_for_single_message() {
        let jsonl = r#"{"type":"session_meta"}
{"type":"event_msg","payload":{"type":"user_message","message":"just me"}}"#;
        assert!(try_parse(jsonl).is_none());
    }

    #[test]
    fn returns_none_for_empty_content() {
        assert!(try_parse("").is_none());
        assert!(try_parse("not json at all\nnot json either").is_none());
    }
}
