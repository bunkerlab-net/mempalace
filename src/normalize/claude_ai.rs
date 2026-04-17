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

    /// Parse a JSON string literal into a `serde_json::Value`.
    /// Reduces boilerplate across tests that build JSON fixtures inline.
    fn json(source: &str) -> serde_json::Value {
        serde_json::from_str(source).expect("test JSON fixture must be valid JSON")
    }

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

    #[test]
    fn extract_content_array_with_plain_strings() {
        // Content arrays can contain plain strings (not wrapped in {type: "text"}).
        // This exercises the `item.as_str()` branch inside extract_content.
        let data = json(
            r#"[{"role":"user","content":["plain string one","plain string two"]},{"role":"assistant","content":"reply"}]"#,
        );
        let result = try_parse(&data).expect("must parse content array with plain strings");
        assert!(
            result.contains("plain string one"),
            "first plain string must appear in transcript"
        );
        assert!(
            result.contains("plain string two"),
            "second plain string must appear in transcript"
        );
    }

    #[test]
    fn extract_content_non_string_non_array_returns_empty() {
        // When "content" is a number or boolean (neither string nor array), extract_content
        // returns an empty string. The message is skipped if the text fallback is also absent.
        // With only one valid message (the assistant), try_parse returns None because the
        // minimum threshold is 2 messages.
        let data =
            json(r#"[{"role":"user","content":42},{"role":"assistant","content":"only valid"}]"#);
        // User message has numeric content (empty after extraction), only assistant
        // is valid — fewer than 2 messages means None.
        let result = try_parse(&data);
        assert!(
            result.is_none(),
            "must return None when numeric content produces fewer than 2 messages"
        );
    }

    #[test]
    fn privacy_export_drops_short_conversations() {
        // A privacy export conversation with fewer than 2 messages must be silently
        // dropped. If all conversations are too short, try_parse returns None.
        let data = json(r#"[{"chat_messages":[{"role":"user","content":"solo question"}]}]"#);
        let result = try_parse(&data);
        assert!(
            result.is_none(),
            "single-message conversation must be dropped"
        );
    }

    #[test]
    fn privacy_export_mixed_short_and_full_conversations() {
        // A privacy export with one short conversation (dropped) and one full
        // conversation (kept) must return only the full conversation's transcript.
        let data = json(
            r#"[
            {"chat_messages":[{"role":"user","content":"orphan question"}]},
            {"chat_messages":[{"role":"user","content":"full question"},{"role":"assistant","content":"full answer"}]}
        ]"#,
        );
        let result = try_parse(&data).expect("must parse when at least one conversation is valid");
        // The short conversation's content must not appear.
        assert!(
            !result.contains("orphan question"),
            "short conversation must be dropped from output"
        );
        assert!(
            result.contains("> full question"),
            "full conversation user turn must appear"
        );
        assert!(
            result.contains("full answer"),
            "full conversation assistant turn must appear"
        );
    }

    #[test]
    fn messages_with_empty_text_are_skipped() {
        // Messages where both content and text fallback are empty must be skipped.
        let data = json(
            r#"[{"role":"user","content":""},{"role":"assistant","content":""},{"role":"user","content":"actual question"},{"role":"assistant","content":"actual answer"}]"#,
        );
        let result = try_parse(&data).expect("must parse when enough non-empty messages exist");
        assert!(
            result.contains("> actual question"),
            "non-empty user message must appear"
        );
        assert!(
            result.contains("actual answer"),
            "non-empty assistant message must appear"
        );
    }

    #[test]
    fn non_object_items_in_array_are_skipped() {
        // Non-object items (strings, numbers, nulls) in the message array must be
        // silently skipped without breaking parsing.
        let data = json(
            r#"["not an object", null, 42, {"role":"user","content":"real user"}, {"role":"assistant","content":"real assistant"}]"#,
        );
        let result = try_parse(&data).expect("must parse despite non-object items in array");
        assert!(
            result.contains("> real user"),
            "valid user message must survive non-object neighbors"
        );
        assert!(
            result.contains("real assistant"),
            "valid assistant message must survive non-object neighbors"
        );
    }

    #[test]
    fn returns_none_for_non_object_non_array_data() {
        // When the top-level JSON value is neither an object nor an array (e.g. a
        // string or number), try_parse must return None.
        let data_string = json(r#""just a string""#);
        let data_number = json("42");
        assert!(
            try_parse(&data_string).is_none(),
            "string value must return None"
        );
        assert!(
            try_parse(&data_number).is_none(),
            "number value must return None"
        );
    }

    #[test]
    fn object_with_chat_messages_key() {
        // An object with "chat_messages" key (not just "messages") must be parsed.
        // This exercises the obj.get("chat_messages") fallback in the object branch.
        let data: serde_json::Value = serde_json::from_str(
            r#"{"chat_messages":[{"role":"user","content":"chat_msg question"},{"role":"assistant","content":"chat_msg answer"}]}"#,
        )
        .expect("failed to parse JSON fixture for chat_messages object test");
        let result = try_parse(&data).expect("must parse object with chat_messages key");
        assert!(
            result.contains("> chat_msg question"),
            "user turn from chat_messages key must appear"
        );
        assert!(
            result.contains("chat_msg answer"),
            "assistant turn from chat_messages key must appear"
        );
    }

    #[test]
    fn unknown_role_messages_are_skipped() {
        // Messages with roles other than user/human/assistant/ai must be skipped.
        // If only unknown-role messages remain, try_parse returns None.
        let data: serde_json::Value = serde_json::from_str(
            r#"[{"role":"system","content":"system prompt"},{"role":"tool","content":"tool output"}]"#,
        )
        .expect("failed to parse JSON fixture for unknown role test");
        let result = try_parse(&data);
        assert!(
            result.is_none(),
            "messages with unknown roles must be skipped, producing None when no valid messages remain"
        );
    }
}
