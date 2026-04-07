//! Parser for `ChatGPT` conversation JSON exports.

use std::collections::HashSet;

use super::messages_to_transcript;

/// Try to parse a `ChatGPT` conversations.json mapping tree into transcript text.
///
/// Traverses the node tree from root through `children` links, extracting
/// user and assistant messages. Returns `None` if fewer than 2 messages.
pub fn try_parse(data: &serde_json::Value) -> Option<String> {
    let mapping = data.as_object()?.get("mapping")?.as_object()?;

    // Find root node (parent=null, no message)
    let mut root_id: Option<&str> = None;
    let mut fallback_root: Option<&str> = None;

    for (node_id, node) in mapping {
        if node.get("parent").is_some_and(serde_json::Value::is_null) {
            if node.get("message").is_none_or(serde_json::Value::is_null) {
                root_id = Some(node_id.as_str());
                break;
            } else if fallback_root.is_none() {
                fallback_root = Some(node_id.as_str());
            }
        }
    }

    let root = root_id.or(fallback_root)?;
    let mut messages: Vec<(String, String)> = Vec::new();
    let mut current_id = root.to_string();
    let mut visited = HashSet::new();

    while !visited.contains(&current_id) {
        visited.insert(current_id.clone());
        let node = mapping.get(&current_id)?;

        if let Some(msg) = node.get("message")
            && !msg.is_null()
        {
            let role = msg.get("author")?.get("role")?.as_str()?;
            let content = msg.get("content")?;
            let parts = content.get("parts").and_then(|p| p.as_array());

            let text = parts
                .map(|ps| {
                    ps.iter()
                        .filter_map(|p| p.as_str())
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default()
                .trim()
                .to_string();

            if !text.is_empty() {
                match role {
                    "user" => messages.push(("user".to_string(), text)),
                    "assistant" => messages.push(("assistant".to_string(), text)),
                    _ => {}
                }
            }
        }

        let children = node.get("children").and_then(|c| c.as_array());
        if let Some(kids) = children {
            if let Some(first) = kids.first().and_then(|k| k.as_str()) {
                current_id = first.to_string();
            } else {
                break;
            }
        } else {
            break;
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
    fn parse_chatgpt_mapping_tree() {
        let data: serde_json::Value = serde_json::from_str(
            r#"{
                "mapping": {
                    "root": {
                        "parent": null,
                        "message": null,
                        "children": ["msg1"]
                    },
                    "msg1": {
                        "parent": "root",
                        "message": {
                            "author": {"role": "user"},
                            "content": {"parts": ["what is rust?"]}
                        },
                        "children": ["msg2"]
                    },
                    "msg2": {
                        "parent": "msg1",
                        "message": {
                            "author": {"role": "assistant"},
                            "content": {"parts": ["Rust is a systems programming language."]}
                        },
                        "children": []
                    }
                }
            }"#,
        )
        .expect("valid json");
        let result = try_parse(&data).expect("should parse");
        assert!(result.contains("> what is rust?"));
        assert!(result.contains("Rust is a systems programming language."));
    }

    #[test]
    fn returns_none_without_mapping() {
        let data: serde_json::Value =
            serde_json::from_str(r#"{"title":"chat"}"#).expect("valid json");
        assert!(try_parse(&data).is_none());
    }
}
