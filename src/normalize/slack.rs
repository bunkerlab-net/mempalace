//! Parser for Slack JSON message exports.

use std::collections::HashMap;

use super::messages_to_transcript;

/// Try to parse a Slack JSON message export into transcript text.
///
/// Valid messages are JSON objects with `type == "message"`, non-empty `text`,
/// and non-empty `user` (or `username`) id. Non-objects, non-message types,
/// empty text, and empty user fields are silently skipped.
///
/// Assigns the first user as `"user"` and alternates role assignment for
/// subsequent users. Returns `None` if fewer than 2 valid messages.
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

        let user_id = obj
            .get("user")
            .or_else(|| obj.get("username"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
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
        messages.push((role, text));
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
    fn parse_slack_messages() {
        let data: serde_json::Value = serde_json::from_str(
            r#"[
                {"type":"message","user":"U1","text":"hello team"},
                {"type":"message","user":"U2","text":"hi there"}
            ]"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("> hello team"));
        assert!(result.contains("hi there"));
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
}
