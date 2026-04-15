use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;

use turso::Connection;

use crate::config;
use crate::db;
use crate::error::Result;

// L1 is injected into AI context windows. 15 drawers at ~200 chars each is
// ~3 000 chars — enough for a meaningful summary without crowding the prompt.
const MAX_DRAWERS: usize = 15;
// Hard character cap so a wing with many long drawers cannot overflow the
// context budget. 3 200 chars is roughly 800 tokens at the 4-chars/token
// rule of thumb, leaving headroom for L0 and the user's own message.
const MAX_CHARS: usize = 3200;

/// Layer 0: Identity text from ~/.mempalace/identity.txt
pub fn layer0() -> String {
    let path = config::config_dir().join("identity.txt");
    if path.exists() {
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                let text = text.trim().to_string();
                if text.is_empty() {
                    "## L0 — IDENTITY\nNo identity configured. Create ~/.mempalace/identity.txt"
                        .to_string()
                } else {
                    format!("## L0 — IDENTITY\n{text}")
                }
            }
            Err(_) => "## L0 — IDENTITY\nNo identity configured. Create ~/.mempalace/identity.txt"
                .to_string(),
        }
    } else {
        "## L0 — IDENTITY\nNo identity configured. Create ~/.mempalace/identity.txt".to_string()
    }
}

/// Layer 1: Essential story — top drawers grouped by room.
pub async fn layer1(connection: &Connection, wing: Option<&str>) -> Result<String> {
    let sql = if let Some(w) = wing {
        format!(
            "SELECT content, wing, room, source_file FROM drawers WHERE wing = '{}' LIMIT 1000",
            w.replace('\'', "''")
        )
    } else {
        "SELECT content, wing, room, source_file FROM drawers LIMIT 1000".to_string()
    };

    let rows = db::query_all(connection, &sql, ()).await?;

    if rows.is_empty() {
        return Ok("## L1 — No memories yet.".to_string());
    }

    // Use insertion order as a proxy for recency. A proper recency sort would
    // require a timestamp column; insertion order is good enough until that
    // column exists.
    let top = &rows[..rows.len().min(MAX_DRAWERS)];
    let by_room = layer1_build_room_map(top);

    let mut lines = vec!["## L1 — ESSENTIAL STORY".to_string()];
    let mut total_len = 0usize;

    let mut sorted_rooms: Vec<_> = by_room.keys().cloned().collect();
    sorted_rooms.sort();

    for room in sorted_rooms {
        let entries = &by_room[&room];
        let room_line = format!("\n[{room}]");
        lines.push(room_line.clone());
        total_len += room_line.len();

        for (content, source) in entries {
            let snippet: String = content.chars().take(200).collect();
            let snippet = snippet.replace('\n', " ");
            let snippet = if content.len() > 200 {
                format!("{snippet}...")
            } else {
                snippet
            };

            let mut entry = format!("  - {snippet}");
            if !source.is_empty() {
                let _ = write!(entry, "  ({source})");
            }

            if total_len + entry.len() > MAX_CHARS {
                lines.push("  ... (more in L3 search)".to_string());
                return Ok(lines.join("\n"));
            }

            total_len += entry.len();
            lines.push(entry);
        }
    }

    Ok(lines.join("\n"))
}

/// Build a room → [(content, `source_name`)] map from drawer rows.
fn layer1_build_room_map(rows: &[turso::Row]) -> HashMap<String, Vec<(String, String)>> {
    let mut by_room: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for row in rows {
        let content = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let room = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let source = row
            .get_value(3)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let source_name = Path::new(&source)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        by_room
            .entry(room)
            .or_default()
            .push((content, source_name));
    }
    by_room
}

/// Generate full wake-up text (L0 + L1).
pub async fn wake_up(connection: &Connection, wing: Option<&str>) -> Result<String> {
    let l0 = layer0();
    let l1 = layer1(connection, wing).await?;
    let text = format!("{l0}\n\n{l1}");
    // 4 chars per token is a standard rule-of-thumb for English text with GPT-family models.
    let tokens = text.len() / 4;
    Ok(format!("{text}\n\n(~{tokens} tokens)"))
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::palace::drawer::{self, DrawerParams};

    #[test]
    fn layer0_returns_string() {
        let result = layer0();
        assert!(
            !result.is_empty(),
            "layer0 should return a non-empty string"
        );
        assert!(
            result.contains("L0"),
            "layer0 output should contain the L0 section header"
        );
    }

    #[tokio::test]
    async fn layer1_empty_palace() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = layer1(&connection, None)
            .await
            .expect("layer1 should succeed on empty DB");
        assert!(
            result.contains("No memories yet"),
            "Empty palace should indicate no memories"
        );
        assert!(
            result.contains("L1"),
            "layer1 output should contain the L1 section header"
        );
    }

    #[tokio::test]
    async fn layer1_with_seeded_drawers() {
        let (_db, connection) = crate::test_helpers::test_db().await;

        // Seed two drawers in different rooms.
        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_layer_1",
                wing: "testwing",
                room: "engineering",
                content: "Rust compiler internals and borrow checker details for the project",
                source_file: "notes.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("first add_drawer should succeed");

        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_layer_2",
                wing: "testwing",
                room: "design",
                content: "UI mockups and wireframes for the dashboard redesign project",
                source_file: "design.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("second add_drawer should succeed");

        let result = layer1(&connection, Some("testwing"))
            .await
            .expect("layer1 should succeed with seeded drawers");

        assert!(
            result.contains("ESSENTIAL STORY"),
            "layer1 output should contain the essential story header"
        );
        assert!(
            result.contains("Rust compiler"),
            "layer1 output should contain content from the first seeded drawer"
        );
        assert!(
            result.contains("UI mockups"),
            "layer1 output should contain content from the second seeded drawer"
        );
    }

    #[tokio::test]
    async fn wake_up_includes_token_estimate() {
        let (_db, connection) = crate::test_helpers::test_db().await;

        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_wake_1",
                wing: "testwing",
                room: "general",
                content: "Important context about the mempalace architecture and design decisions",
                source_file: "arch.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer should succeed");

        let result = wake_up(&connection, Some("testwing"))
            .await
            .expect("wake_up should succeed with seeded data");

        assert!(
            result.contains("token"),
            "wake_up output should contain a token estimate"
        );
        assert!(
            result.contains("L0") && result.contains("L1"),
            "wake_up output should contain both L0 and L1 sections"
        );
    }
}
