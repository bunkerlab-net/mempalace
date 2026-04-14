//! Parser for Claude.ai JSON conversation exports.

use super::messages_to_transcript;

/// Try to parse Claude.ai JSON export into transcript text.
///
/// Accepts three formats:
/// - JSON array of message objects (`[{"role":…,"content":…}]`)
/// - Object with a `"messages"` or `"chat_messages"` key
/// - Privacy export: top-level array of conversation objects where each object
///   has a `"chat_messages"` or `"messages"` key — each conversation becomes
///   its own transcript, joined by blank lines (preserves conversation boundaries)
///
/// Both `"role"` and `"sender"` are accepted as the author field (the privacy
/// export uses `"sender"` while the API format uses `"role"`).  A top-level
/// `"text"` key is used as fallback when `"content"` is absent or empty.
///
/// Returns `None` if no conversation yields at least 2 messages (the threshold
/// is per conversation — a conversation with fewer than 2 messages is silently
/// dropped; if all conversations are dropped the result is `None`).
pub fn try_parse(data: &serde_json::Value) -> Option<String> {
    if let Some(arr) = data.as_array() {
        // Privacy export: array of conversation objects, each with a chat_messages
        // or messages key.  Only treat as privacy export if the first element looks
        // like a conversation object (has chat_messages or messages) to avoid
        // misclassifying a plain flat message array.
        let first_is_convo = arr.first().is_some_and(|v| {
            v.get("chat_messages")
                .or_else(|| v.get("messages"))
                .and_then(|m| m.as_array())
                .is_some()
        });

        if first_is_convo {
            // Process each conversation separately; join transcripts with blank lines.
            let transcripts: Vec<String> = arr
                .iter()
                .filter_map(|conv| {
                    let msgs = conv
                        .get("chat_messages")
                        .or_else(|| conv.get("messages"))
                        .and_then(|v| v.as_array())?;
                    collect_messages(msgs)
                })
                .collect();

            return if transcripts.is_empty() {
                None
            } else {
                Some(transcripts.join("\n\n"))
            };
        }

        // Flat array of message objects.
        collect_messages(arr)
    } else if let Some(obj) = data.as_object() {
        let items = obj
            .get("messages")
            .or_else(|| obj.get("chat_messages"))
            .and_then(|v| v.as_array())?;
        collect_messages(items)
    } else {
        None
    }
}

/// Extract (role, text) pairs from a message list and return a transcript.
///
/// Accepts both `"role"` (API format) and `"sender"` (privacy export) as the
/// author field, and falls back to a top-level `"text"` key when `"content"`
/// blocks are absent or empty.  Returns `None` if fewer than 2 messages found.
fn collect_messages(items: &[serde_json::Value]) -> Option<String> {
    let mut messages: Vec<(String, String)> = Vec::new();

    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };

        // Accept "role" (API) or "sender" (privacy export).
        let role = obj
            .get("role")
            .or_else(|| obj.get("sender"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Primary: content blocks. Fallback: top-level "text" key.
        let text = {
            let from_content = obj.get("content").map(extract_content).unwrap_or_default();
            if from_content.is_empty() {
                obj.get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string()
            } else {
                from_content
            }
        };

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

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
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
            r#"{"messages":[{"role":"human","content":"q"},{"role":"ai","content":"assistant_reply_42"}]}"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("> q"));
        assert!(result.contains("assistant_reply_42"));
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
    fn parse_privacy_export_multiple_conversations() {
        let data: serde_json::Value = serde_json::from_str(
            r#"[{"chat_messages":[{"role":"user","content":"first"},{"role":"assistant","content":"reply1"}]},{"chat_messages":[{"role":"user","content":"second"},{"role":"assistant","content":"reply2"}]}]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("> first"));
        assert!(result.contains("reply1"));
        assert!(result.contains("> second"));
        assert!(result.contains("reply2"));
    }

    #[test]
    fn returns_none_for_unrecognized_format() {
        let data: serde_json::Value =
            serde_json::from_str(r#"{"something":"else"}"#).expect("valid json");
        assert!(try_parse(&data).is_none());
    }

    #[test]
    fn parse_privacy_export_with_sender_field() {
        // Privacy exports use "sender" instead of "role".
        let data: serde_json::Value = serde_json::from_str(
            r#"[{"chat_messages":[{"sender":"human","content":"question"},{"sender":"assistant","content":"answer_42"}]}]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("> question"), "user turn preserved");
        assert!(result.contains("answer_42"), "assistant turn preserved");
    }

    #[test]
    fn parse_with_top_level_text_fallback() {
        // Some export variants have a top-level "text" key instead of "content".
        let data: serde_json::Value = serde_json::from_str(
            r#"[{"role":"user","text":"fallback question"},{"role":"assistant","text":"fallback_answer"}]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse with text fallback");
        assert!(
            result.contains("> fallback question"),
            "user turn from text key"
        );
        assert!(
            result.contains("fallback_answer"),
            "assistant turn from text key"
        );
    }

    #[test]
    fn parse_privacy_export_each_convo_separate() {
        // Each conversation must produce a separate transcript block joined by \n\n.
        let data: serde_json::Value = serde_json::from_str(
            r#"[
                {"chat_messages":[{"role":"user","content":"convo1_q"},{"role":"assistant","content":"convo1_a"}]},
                {"chat_messages":[{"role":"user","content":"convo2_q"},{"role":"assistant","content":"convo2_a"}]}
            ]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        // Both conversations present.
        assert!(result.contains("convo1_q"), "first convo user turn");
        assert!(result.contains("convo1_a"), "first convo assistant turn");
        assert!(result.contains("convo2_q"), "second convo user turn");
        assert!(result.contains("convo2_a"), "second convo assistant turn");
    }

    #[test]
    fn parse_privacy_export_messages_key_variant() {
        // Some privacy exports use "messages" instead of "chat_messages".
        let data: serde_json::Value = serde_json::from_str(
            r#"[{"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"}]}]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse messages key variant");
        assert!(result.contains("> hi"), "user turn");
        assert!(result.contains("hello"), "assistant turn");
    }
}
