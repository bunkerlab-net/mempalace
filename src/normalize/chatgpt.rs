//! Parser for `ChatGPT` conversation JSON exports.

use std::collections::HashSet;

use super::messages_to_transcript;

/// Try to parse a `ChatGPT` conversations.json mapping tree into transcript text.
///
/// Traverses the node tree from root through `children` links, extracting
/// user and assistant messages. Returns `None` unless at least one message
/// with role `"user"` and at least one with role `"assistant"` are present
/// — one-sided transcripts (e.g. system-prompt-only files) are rejected.
pub fn try_parse(data: &serde_json::Value) -> Option<String> {
    let mapping = data.as_object()?.get("mapping")?.as_object()?;
    let root = try_parse_find_root(mapping)?;
    let messages = try_parse_walk_tree(root, mapping)?;

    // Require at least one user turn AND at least one assistant turn so we
    // never store a one-sided transcript (e.g. a file with only system prompts).
    let has_user = messages.iter().any(|(role, _)| role == "user");
    let has_assistant = messages.iter().any(|(role, _)| role == "assistant");
    if has_user && has_assistant {
        let refs: Vec<(&str, &str)> = messages
            .iter()
            .map(|(role, text)| (role.as_str(), text.as_str()))
            .collect();
        Some(messages_to_transcript(&refs))
    } else {
        None
    }
}

/// Find the root node ID in a `ChatGPT` mapping — the node with `parent=null`
/// and no message body. Falls back to the first null-parent node with a message.
fn try_parse_find_root(mapping: &serde_json::Map<String, serde_json::Value>) -> Option<&str> {
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

    root_id.or(fallback_root)
}

/// Walk the linear path of the message tree from `root`, collecting (role, text) pairs.
///
/// Returns `None` if any node lookup fails (malformed export). Always follows the
/// first child so branching edits are ignored — see the comment in `try_parse`.
fn try_parse_walk_tree(
    root: &str,
    mapping: &serde_json::Map<String, serde_json::Value>,
) -> Option<Vec<(String, String)>> {
    let mut messages: Vec<(String, String)> = Vec::new();
    let mut id_current = root.to_string();
    let mut visited = HashSet::new();

    // Upper bound: each node in `mapping` can be visited at most once, so this
    // loop runs at most mapping.len() times regardless of the tree structure.
    while !visited.contains(&id_current) {
        assert!(
            visited.len() <= mapping.len(),
            "visited set cannot exceed mapping size — cycle guard is broken"
        );
        visited.insert(id_current.clone());
        let node = mapping.get(&id_current)?;

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

        // ChatGPT exports can represent branching conversations (e.g. message
        // edits produce sibling branches). We always follow the first child,
        // which corresponds to the linear path of the original conversation
        // before any edits. Branching paths are ignored — they are rare and
        // would require a tree walk that could produce confusing transcripts.
        let children = node.get("children").and_then(|c| c.as_array());
        if let Some(kids) = children {
            if let Some(first) = kids.first().and_then(|k| k.as_str()) {
                id_current = first.to_string();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Some(messages)
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
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
        .expect("hardcoded test fixture is valid JSON and must parse without error");
        let result = try_parse(&data)
            .expect("try_parse should succeed for well-formed ChatGPT export JSON fixture");
        assert!(result.contains("> what is rust?"));
        assert!(result.contains("Rust is a systems programming language."));
    }

    #[test]
    fn returns_none_without_mapping() {
        let data: serde_json::Value = serde_json::from_str(r#"{"title":"chat"}"#)
            .expect("hardcoded test fixture is valid JSON and must parse without error");
        assert!(try_parse(&data).is_none());
    }
}
