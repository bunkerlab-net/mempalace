//! Parser for Slack JSON message exports.

use std::collections::HashMap;

use super::messages_to_transcript;

/// Try to parse a Slack JSON message export into transcript text.
///
/// Assigns the first user as `"user"` and alternates role assignment for
/// subsequent users. Returns `None` if fewer than 2 messages.
pub fn try_parse(data: &serde_json::Value) -> Option<String> {
    let items = data.as_array()?;
    let mut messages: Vec<(String, String)> = Vec::new();
    let mut seen_users: HashMap<String, String> = HashMap::new();
    let mut last_role: Option<String> = None;

    for item in items {
        let obj = item.as_object()?;
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

        if !seen_users.contains_key(&user_id) {
            let role = if seen_users.is_empty() {
                "user".to_string()
            } else if last_role.as_deref() == Some("user") {
                "assistant".to_string()
            } else {
                "user".to_string()
            };
            seen_users.insert(user_id.clone(), role);
        }

        let role = seen_users[&user_id].clone();
        last_role = Some(role.clone());
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
}
