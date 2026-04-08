//! Parser for Claude.ai JSON conversation exports.

use super::messages_to_transcript;

/// Try to parse Claude.ai JSON export into transcript text.
///
/// Accepts three formats:
/// - JSON array of message objects (`[{"role":…,"content":…}]`)
/// - Object with a `"messages"` or `"chat_messages"` key
/// - Privacy export: top-level array of conversation objects where each object
///   has a `"chat_messages"` key — all conversations are flattened into one transcript
///
/// Returns `None` if fewer than 2 messages are found.
pub fn try_parse(data: &serde_json::Value) -> Option<String> {
    let items = if let Some(arr) = data.as_array() {
        // Privacy export: array of conversation objects, each with a chat_messages key.
        // Check if ANY element contains "chat_messages" to detect privacy export format.
        if arr.iter().any(|v| v.get("chat_messages").is_some()) {
            arr.iter()
                .filter_map(|conv| conv.get("chat_messages")?.as_array())
                .flatten()
                .cloned()
                .collect()
        } else {
            arr.clone()
        }
    } else if let Some(obj) = data.as_object() {
        obj.get("messages")
            .or_else(|| obj.get("chat_messages"))
            .and_then(|v| v.as_array())
            .cloned()?
    } else {
        return None;
    };

    let mut messages: Vec<(String, String)> = Vec::new();

    for item in &items {
        let obj = item.as_object()?;
        let role = obj.get("role")?.as_str()?;
        let content = obj.get("content")?;
        let text = extract_content(content);
        if text.is_empty() {
            continue;
        }

        match role {
            "user" | "human" => messages.push(("user".to_string(), text)),
            "assistant" | "ai" => messages.push(("assistant".to_string(), text)),
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
    fn parse_array_format() {
        let data: serde_json::Value = serde_json::from_str(
            r#"[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"}]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("> hi"));
        assert!(result.contains("hello"));
    }

    #[test]
    fn parse_object_with_messages_key() {
        let data: serde_json::Value = serde_json::from_str(
            r#"{"messages":[{"role":"human","content":"q"},{"role":"ai","content":"a"}]}"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("> q"));
        assert!(result.contains("a"));
    }

    #[test]
    fn parse_privacy_export_format() {
        let data: serde_json::Value = serde_json::from_str(
            r#"[{"chat_messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"}]}]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("> hi"));
        assert!(result.contains("hello"));
    }

    #[test]
    fn returns_none_for_unrecognized_format() {
        let data: serde_json::Value =
            serde_json::from_str(r#"{"something":"else"}"#).expect("valid json");
        assert!(try_parse(&data).is_none());
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
        _ => String::new(),
    }
}
