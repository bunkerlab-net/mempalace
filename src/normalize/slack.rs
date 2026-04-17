//! Parser for Slack JSON message exports.

use std::collections::HashMap;

use super::messages_to_transcript;

/// Provenance footer appended to Slack transcript output so downstream consumers
/// know the speaker roles are positionally assigned, not verified.
const PROVENANCE_FOOTER: &str = "\n[source: slack-export | multi-party chat \u{2014} speaker roles are positional, not verified]";

/// Sanitize a Slack speaker ID for safe embedding in transcript text.
///
/// Replaces bracket characters and control characters (U+0000..U+001F) with `_`
/// to prevent chunk-boundary injection via crafted exports (issue #812).
/// Trims surrounding whitespace after substitution.
fn slack_sanitize_user_id(raw_user_id: &str) -> String {
    let sanitized: String = raw_user_id
        .chars()
        .map(|c| {
            if c == '[' || c == ']' || (c as u32) < 0x20 {
                '_'
            } else {
                c
            }
        })
        .collect();
    sanitized.trim().to_string()
}

/// Try to parse a Slack JSON message export into transcript text.
///
/// Valid messages are JSON objects with `type == "message"`, non-empty `text`,
/// and non-empty `user` (or `username`) id. Non-objects, non-message types,
/// empty text, and empty user fields are silently skipped.
///
/// Assigns the first user as `"user"` and alternates role assignment for
/// subsequent users. Each message is prefixed with the speaker ID so the
/// original author is preserved. A provenance footer is appended to mark
/// the transcript as a Slack import. Returns `None` if fewer than 2 valid
/// messages.
pub fn try_parse(data: &serde_json::Value) -> Option<String> {
    let items = data.as_array()?;
    let mut messages: Vec<(String, String)> = Vec::new();
    let mut seen_users: HashMap<String, String> = HashMap::new();
    let mut last_role: Option<String> = None;

    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        if obj.get("type").and_then(|t| t.as_str()) != Some("message") {
            continue;
        }

        let raw_user_id = obj
            .get("user")
            .or_else(|| obj.get("username"))
            .and_then(|u| u.as_str())
            .unwrap_or("");
        // Sanitize speaker ID before any use — prevents bracket/control-char injection.
        let user_id = slack_sanitize_user_id(raw_user_id);
        let text = obj
            .get("text")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .trim()
            .to_string();

        if text.is_empty() || user_id.is_empty() {
            continue;
        }

        let role = if let Some(existing_role) = seen_users.get(&user_id) {
            existing_role.clone()
        } else {
            let new_role = if seen_users.is_empty() {
                "user".to_string()
            } else if last_role.as_deref() == Some("user") {
                "assistant".to_string()
            } else {
                "user".to_string()
            };
            seen_users.insert(user_id.clone(), new_role.clone());
            last_role = Some(new_role.clone());
            new_role
        };
        // Prefix with speaker ID so the original author is preserved in the transcript.
        messages.push((role, format!("[{user_id}] {text}")));
    }

    if messages.len() >= 2 {
        let refs: Vec<(&str, &str)> = messages
            .iter()
            .map(|(r, t)| (r.as_str(), t.as_str()))
            .collect();
        Some(messages_to_transcript(&refs) + PROVENANCE_FOOTER)
    } else {
        None
    }
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_slack_messages() {
        let data: serde_json::Value = serde_json::from_str(
            r#"[
                {"type":"message","user":"U1","text":"hello team"},
                {"type":"message","user":"U2","text":"hi there"}
            ]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("[U1] hello team"));
        assert!(result.contains("[U2] hi there"));
        assert!(result.contains("slack-export"));
    }

    #[test]
    fn returns_none_for_single_message() {
        let data: serde_json::Value =
            serde_json::from_str(r#"[{"type":"message","user":"U1","text":"alone"}]"#)
                .expect("valid json");
        assert!(try_parse(&data).is_none());
    }

    #[test]
    fn multi_user_role_alternation() {
        let data: serde_json::Value = serde_json::from_str(
            r#"[
                {"type":"message","user":"U1","text":"first message"},
                {"type":"message","user":"U2","text":"second message"},
                {"type":"message","user":"U3","text":"third message"},
                {"type":"message","user":"U1","text":"back to first"},
                {"type":"message","user":"U2","text":"back to second"}
            ]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        let lines: Vec<&str> = result.lines().collect();

        // Filter out empty lines to check role markers
        let non_empty_lines: Vec<&str> = lines.iter().copied().filter(|l| !l.is_empty()).collect();
        assert!(
            non_empty_lines.len() >= 5,
            "expected at least 5 non-empty lines, got {}",
            non_empty_lines.len()
        );

        // Verify role alternation by checking > markers
        // U1 is "user" (has > marker)
        assert!(
            non_empty_lines[0].starts_with('>') && non_empty_lines[0].contains("first message")
        );
        // U2 is "assistant" (no > marker)
        assert!(
            !non_empty_lines[1].starts_with('>') && non_empty_lines[1].contains("second message")
        );
        // U3 is "user" (has > marker)
        assert!(
            non_empty_lines[2].starts_with('>') && non_empty_lines[2].contains("third message")
        );
        // U1 remains "user" (has > marker)
        assert!(
            non_empty_lines[3].starts_with('>') && non_empty_lines[3].contains("back to first")
        );
        // U2 remains "assistant" (no > marker)
        assert!(
            !non_empty_lines[4].starts_with('>') && non_empty_lines[4].contains("back to second")
        );
    }

    #[test]
    fn skips_malformed_entries() {
        let data: serde_json::Value = serde_json::from_str(
            r#"[
                {"type":"message","user":"U1","text":"first valid"},
                "not an object",
                null,
                42,
                {"type":"message","user":"U2","text":"second valid"}
            ]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("first valid"));
        assert!(result.contains("second valid"));
    }

    #[test]
    fn sanitizes_injection_in_user_id() {
        // Brackets and control chars in user ID must be replaced with '_' to prevent
        // chunk-boundary injection via crafted exports (issue #812).
        let data: serde_json::Value = serde_json::from_str(
            r#"[
                {"type":"message","user":"[U1\n]","text":"injected"},
                {"type":"message","user":"U2","text":"clean"}
            ]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(!result.contains("[U1\n]"), "raw injection must not appear");
        assert!(result.contains("_U1__"), "brackets/newline replaced with _");
        assert!(result.contains("slack-export"));
    }

    #[test]
    fn appends_provenance_footer() {
        let data: serde_json::Value = serde_json::from_str(
            r#"[
                {"type":"message","user":"A","text":"hello"},
                {"type":"message","user":"B","text":"world"}
            ]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("slack-export"));
        assert!(result.contains("speaker roles are positional, not verified"));
    }
}
