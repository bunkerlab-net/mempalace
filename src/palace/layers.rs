use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;

use turso::Connection;

use crate::config;
use crate::db;
use crate::error::Result;
use crate::palace::search;

// L1 is injected into AI context windows. 15 drawers at ~200 chars each is
// ~3 000 chars — enough for a meaningful summary without crowding the prompt.
const DRAWERS_MAX: usize = 15;
// Hard character cap so a wing with many long drawers cannot overflow the
// context budget. 3 200 chars is roughly 800 tokens at the 4-chars/token
// rule of thumb, leaving headroom for L0 and the user's own message.
const CHARS_MAX: usize = 3200;
// L2 retrieval caps. 100 is generous; callers should pass a smaller value
// (typically 10-20) to keep context injection manageable.
const LAYER2_RESULTS_MAX: usize = 100;
// Snippet length for L2/L3 display — 300 chars fits comfortably within a
// single context line and preserves readability without truncating too early.
const SNIPPET_LEN_MAX: usize = 300;

const _: () = assert!(LAYER2_RESULTS_MAX <= 1000);
const _: () = assert!(SNIPPET_LEN_MAX > 0);

/// Layer 0: Identity text from `$XDG_DATA_HOME/mempalace/identity.txt`.
pub fn layer0() -> String {
    let path = config::config_dir().join("identity.txt");
    let missing = format!(
        "## L0 — IDENTITY\nNo identity configured. Create {}",
        path.display()
    );
    if path.exists() {
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                let text = text.trim().to_string();
                if text.is_empty() {
                    missing
                } else {
                    format!("## L0 — IDENTITY\n{text}")
                }
            }
            Err(_) => missing,
        }
    } else {
        missing
    }
}

/// Layer 1: Essential story — top drawers grouped by room.
pub async fn layer1(connection: &Connection, wing: Option<&str>) -> Result<String> {
    let sql = if let Some(wing_name) = wing {
        format!(
            "SELECT content, wing, room, source_file FROM drawers WHERE wing = '{}' LIMIT 1000",
            wing_name.replace('\'', "''")
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
    let top_drawers = &rows[..rows.len().min(DRAWERS_MAX)];
    let by_room = layer1_build_room_map(top_drawers);

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

            if total_len + entry.len() > CHARS_MAX {
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
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let room = row
            .get_value(2)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let source = row
            .get_value(3)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
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

/// Layer 2: On-demand retrieval filtered by wing and/or room.
///
/// Returns the most recent `results` drawers matching the given filters,
/// formatted as a compact context block for injection into an AI prompt.
pub async fn layer2(
    connection: &Connection,
    wing: Option<&str>,
    room: Option<&str>,
    results: usize,
) -> Result<String> {
    assert!(results > 0, "results must be positive");
    assert!(
        results <= LAYER2_RESULTS_MAX,
        "results must not exceed {LAYER2_RESULTS_MAX}"
    );

    let rows = layer2_query(connection, wing, room, results).await?;

    if rows.is_empty() {
        return Ok(format!(
            "## L2 — ON-DEMAND\nNo drawers found{}.",
            layer2_empty_label(wing, room)
        ));
    }

    let mut lines = vec![format!("## L2 — ON-DEMAND ({} drawers)", rows.len())];
    for row in &rows {
        let entry = layer2_format_row(row);
        lines.push(entry);
    }

    let result = lines.join("\n");
    assert!(!result.is_empty(), "layer2 output must not be empty");
    Ok(result)
}

/// Execute the SQL for layer2, returning raw rows.
async fn layer2_query(
    connection: &Connection,
    wing: Option<&str>,
    room: Option<&str>,
    results: usize,
) -> Result<Vec<turso::Row>> {
    assert!(results > 0);

    let mut sql = "SELECT content, wing, room, source_file FROM drawers".to_string();
    let mut params: Vec<turso::Value> = Vec::new();

    if let Some(w) = wing {
        sql.push_str(" WHERE wing = ?1");
        params.push(turso::Value::from(w));
    }
    if let Some(room_filter) = room {
        if params.is_empty() {
            sql.push_str(" WHERE room = ?1");
        } else {
            sql.push_str(" AND room = ?2");
        }
        params.push(turso::Value::from(room_filter));
    }
    let limit_index = params.len() + 1;
    let _ = write!(sql, " ORDER BY filed_at DESC LIMIT ?{limit_index}");
    // SQLite LIMIT requires a signed integer; saturate at i64::MAX for safety.
    let results_i64 = i64::try_from(results).unwrap_or(i64::MAX);
    params.push(turso::Value::Integer(results_i64));

    db::query_all(connection, &sql, turso::params_from_iter(params)).await
}

/// Format one layer2 result row into a display line.
fn layer2_format_row(row: &turso::Row) -> String {
    let content = row
        .get_value(0)
        .ok()
        .and_then(|c| c.as_text().cloned())
        .unwrap_or_default();
    let room = row
        .get_value(2)
        .ok()
        .and_then(|c| c.as_text().cloned())
        .unwrap_or_default();
    let source = row
        .get_value(3)
        .ok()
        .and_then(|c| c.as_text().cloned())
        .unwrap_or_default();
    let source_name = Path::new(&source)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let snippet: String = content.chars().take(SNIPPET_LEN_MAX).collect();
    let snippet = snippet.replace('\n', " ");
    let snippet = if content.len() > SNIPPET_LEN_MAX {
        format!("{snippet}...")
    } else {
        snippet
    };

    let mut entry = format!("  [{room}] {snippet}");
    if !source_name.is_empty() {
        let _ = write!(entry, "  ({source_name})");
    }
    entry
}

/// Build a parenthetical filter label for the layer2 empty message.
fn layer2_empty_label(wing: Option<&str>, room: Option<&str>) -> String {
    match (wing, room) {
        (Some(w), Some(room_name)) => format!(" for wing={w} room={room_name}"),
        (Some(w), None) => format!(" for wing={w}"),
        (None, Some(room_name)) => format!(" for room={room_name}"),
        (None, None) => String::new(),
    }
}

/// Layer 3: Deep keyword search across the full palace.
///
/// Delegates to the inverted-index search (`palace::search::search_memories`)
/// and formats the results as a compact context block.
pub async fn layer3(
    connection: &Connection,
    query: &str,
    wing: Option<&str>,
    room: Option<&str>,
    results: usize,
) -> Result<String> {
    assert!(!query.is_empty(), "query must not be empty");
    assert!(results > 0, "results must be positive");

    let hits = search::search_memories(connection, query, wing, room, results).await?;

    if hits.is_empty() {
        return Ok(format!(
            "## L3 — SEARCH RESULTS for \"{query}\"\nNo results found."
        ));
    }

    let mut lines = vec![format!("## L3 — SEARCH RESULTS for \"{query}\"")];
    for (index, hit) in hits.iter().enumerate() {
        let snippet: String = hit.text.chars().take(SNIPPET_LEN_MAX).collect();
        let snippet = snippet.replace('\n', " ");
        let snippet = if hit.text.len() > SNIPPET_LEN_MAX {
            format!("{snippet}...")
        } else {
            snippet
        };
        // 1-based index for human readability in the context block.
        lines.push(format!(
            "  [{}] {}/{} (score={:.1}, chunk={})",
            index + 1,
            hit.wing,
            hit.room,
            hit.relevance,
            hit.chunk_index
        ));
        lines.push(format!("      {snippet}"));
        if !hit.source_path.is_empty() {
            lines.push(format!("      src: {}", hit.source_path));
        }
    }

    // Expand the top result's neighbors for additional context if it's part of
    // a multi-chunk source (chunk_index > 0 or other chunks exist nearby).
    if let Some(top_hit) = hits.first()
        && !top_hit.source_path.is_empty()
    {
        layer3_add_neighbor_context(connection, top_hit, &mut lines).await?;
    }

    let result = lines.join("\n");
    assert!(!result.is_empty(), "layer3 output must not be empty");
    Ok(result)
}

/// Expand the anchor result's adjacent chunks and append them to the output lines.
///
/// Radius of 2 means up to 4 adjacent chunks are fetched (2 before, 2 after).
/// A no-op when the source has only a single chunk.
async fn layer3_add_neighbor_context(
    connection: &Connection,
    anchor: &search::SearchResult,
    lines: &mut Vec<String>,
) -> Result<()> {
    assert!(!anchor.source_path.is_empty());

    let neighbors =
        search::search_expand_neighbors(connection, &anchor.source_path, anchor.chunk_index, 2)
            .await?;

    if neighbors.is_empty() {
        return Ok(());
    }

    lines.push("      nearby context:".to_string());
    for neighbor in &neighbors {
        let snippet: String = neighbor.text.chars().take(SNIPPET_LEN_MAX).collect();
        let snippet = snippet.replace('\n', " ");
        lines.push(format!(
            "        chunk {}: {}",
            neighbor.chunk_index, snippet
        ));
    }

    // Postcondition: adding neighbors must produce more lines.
    assert!(!lines.is_empty());
    Ok(())
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

    #[tokio::test]
    async fn layer2_empty_palace_returns_no_drawers_message() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = layer2(&connection, None, None, 10)
            .await
            .expect("layer2 should succeed on empty DB");
        assert!(
            result.contains("No drawers found"),
            "empty palace must report no drawers"
        );
        assert!(result.contains("L2"), "output must contain L2 header");
    }

    #[tokio::test]
    async fn layer2_with_wing_filter_returns_matching_drawers() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_l2_1",
                wing: "wing_alpha",
                room: "notes",
                content: "Layer two on-demand context retrieval test",
                source_file: "notes.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for layer2 wing filter test");

        let result = layer2(&connection, Some("wing_alpha"), None, 10)
            .await
            .expect("layer2 must succeed with wing filter");

        assert!(result.contains("L2"), "output must contain L2 header");
        assert!(
            result.contains("on-demand context"),
            "output must contain the seeded drawer content"
        );
        // Pair assertion: wrong wing returns nothing.
        let empty = layer2(&connection, Some("wing_beta"), None, 10)
            .await
            .expect("layer2 must succeed for non-matching wing");
        assert!(
            empty.contains("No drawers found"),
            "wrong wing must return empty result"
        );
    }

    #[tokio::test]
    async fn layer3_empty_palace_returns_no_results_message() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = layer3(&connection, "elephant", None, None, 5)
            .await
            .expect("layer3 should succeed on empty DB");
        assert!(
            result.contains("No results found"),
            "empty palace must report no results"
        );
        assert!(result.contains("L3"), "output must contain L3 header");
    }

    #[tokio::test]
    async fn layer3_returns_matching_drawer_content() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_l3_1",
                wing: "research",
                room: "notes",
                content: "Quantum computing fundamentals and qubit entanglement",
                source_file: "quantum.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for layer3 search test");

        let result = layer3(&connection, "quantum computing", None, None, 5)
            .await
            .expect("layer3 must succeed when matching drawers exist");

        assert!(result.contains("L3"), "output must contain L3 header");
        assert!(
            result.contains("quantum"),
            "output must surface the matching drawer content"
        );
        // Pair assertion: output must include wing/room attribution.
        assert!(
            result.contains("research"),
            "output must include the wing name"
        );
    }
}
