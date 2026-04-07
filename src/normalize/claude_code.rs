//! Parser for Claude Code JSONL conversation exports.

use super::messages_to_transcript;

/// Try to parse Claude Code JSONL format into transcript text.
///
/// Each line is a JSON object with `"type"` (`"human"` or `"assistant"`)
/// and `"message"` containing a `"content"` field. Returns `None` if
/// fewer than 2 messages are found.
pub fn try_parse(content: &str) -> Option<String> {
    let mut messages: Vec<(String, String)> = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: serde_json::Value = serde_json::from_str(line).ok()?;
        let obj = entry.as_object()?;

        let msg_type = obj.get("type")?.as_str()?;
        let message = obj.get("message")?.as_object()?;
        let text = extract_content(message.get("content")?);

        if text.is_empty() {
            continue;
        }

        match msg_type {
            "human" => messages.push(("user".to_string(), text)),
            "assistant" => messages.push(("assistant".to_string(), text)),
            _ => {}
        }
    }

    if messages.len() >= 2 {
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
    fn parse_valid_jsonl() {
        let jsonl = r#"{"type":"human","message":{"content":"hello world"}}
{"type":"assistant","message":{"content":"hi there"}}"#;
        let result = try_parse(jsonl);
        assert!(result.is_some());
        let text = result.expect("should parse");
        assert!(text.contains("> hello world"));
        assert!(text.contains("hi there"));
    }

    #[test]
    fn parse_single_message_returns_none() {
        let jsonl = r#"{"type":"human","message":{"content":"hello"}}"#;
        assert!(try_parse(jsonl).is_none());
    }

    #[test]
    fn parse_invalid_json_returns_none() {
        assert!(try_parse("not json at all").is_none());
    }

    #[test]
    fn parse_array_content() {
        let jsonl = r#"{"type":"human","message":{"content":[{"type":"text","text":"array msg"}]}}
{"type":"assistant","message":{"content":"reply"}}"#;
        let result = try_parse(jsonl).expect("should parse");
        assert!(result.contains("> array msg"));
    }
}

fn extract_content(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.trim().to_string(),
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|item| {
                if let Some(s) = item.as_str() {
                    Some(s.to_string())
                } else if let Some(obj) = item.as_object() {
                    if obj.get("type").and_then(|t| t.as_str()) == Some("text") {
                        obj.get("text")
                            .and_then(|t| t.as_str())
                            .map(std::string::ToString::to_string)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string(),
        serde_json::Value::Object(obj) => obj
            .get("text")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        _ => String::new(),
    }
}
