use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;

use turso::Connection;

use crate::config;
use crate::db;
use crate::error::Result;

const MAX_DRAWERS: usize = 15;
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
pub async fn layer1(conn: &Connection, wing: Option<&str>) -> Result<String> {
    let sql = if let Some(w) = wing {
        format!(
            "SELECT content, wing, room, source_file FROM drawers WHERE wing = '{}' LIMIT 1000",
            w.replace('\'', "''")
        )
    } else {
        "SELECT content, wing, room, source_file FROM drawers LIMIT 1000".to_string()
    };

    let rows = db::query_all(conn, &sql, ()).await?;

    if rows.is_empty() {
        return Ok("## L1 — No memories yet.".to_string());
    }

    // Take first MAX_DRAWERS (they're already stored in insertion order — good enough for v1)
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
pub async fn wake_up(conn: &Connection, wing: Option<&str>) -> Result<String> {
    let l0 = layer0();
    let l1 = layer1(conn, wing).await?;
    let text = format!("{l0}\n\n{l1}");
    let tokens = text.len() / 4;
    Ok(format!("{text}\n\n(~{tokens} tokens)"))
}
