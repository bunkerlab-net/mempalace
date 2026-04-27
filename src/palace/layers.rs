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

    let kg_fact_lines = layer1_append_kg_facts(connection).await?;
    for fact_line in kg_fact_lines {
        if total_len + fact_line.len() > CHARS_MAX {
            break;
        }
        total_len += fact_line.len();
        lines.push(fact_line);
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

/// Query active KG facts and format them as display lines for L1 context injection.
///
/// Called by `layer1` to append entity-relationship context below the room-based
/// drawer summaries. Capped at 20 distinct (subject, predicate) pairs to bound
/// the number of follow-up `query_relationship` calls and output size.
async fn layer1_append_kg_facts(connection: &Connection) -> Result<Vec<String>> {
    const PAIRS_LIMIT: usize = 20;

    let pair_rows = db::query_all(
        connection,
        "SELECT DISTINCT s.name, t.predicate \
         FROM triples t JOIN entities s ON t.subject = s.id \
         WHERE t.valid_to IS NULL LIMIT 20",
        (),
    )
    .await?;

    if pair_rows.is_empty() {
        return Ok(Vec::new());
    }

    // Precondition: LIMIT 20 in the SQL bounds this slice to PAIRS_LIMIT.
    assert!(
        pair_rows.len() <= PAIRS_LIMIT,
        "layer1_append_kg_facts: pair_rows.len() {} must not exceed {PAIRS_LIMIT}",
        pair_rows.len()
    );

    let mut lines = vec!["\n[knowledge graph]".to_string()];
    for row in &pair_rows {
        let subject_name = row
            .get_value(0)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let predicate = row
            .get_value(1)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();

        if subject_name.is_empty() || predicate.is_empty() {
            continue;
        }

        let facts =
            crate::kg::query::query_relationship(connection, &subject_name, &predicate).await?;
        for fact in &facts {
            lines.push(format!(
                "  - {} {} {}",
                fact.subject, fact.predicate, fact.object
            ));
        }
    }

    // Postcondition: at least the header line is always present.
    assert!(
        !lines.is_empty(),
        "layer1_append_kg_facts: lines must contain at least the header"
    );
    Ok(lines)
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

    // ── layer0 empty identity file (lines 40-41) ─────────────────────────

    #[test]
    fn layer0_returns_missing_message_when_identity_file_is_empty() {
        // When identity.txt exists but contains only whitespace, layer0 must
        // return the "missing" placeholder rather than the empty trimmed text.
        // MEMPALACE_DIR redirects config_dir() to our temp directory so the test
        // is hermetic and does not touch the real user config directory.
        //
        // We also write known_entities.json (empty array) into the same temp dir
        // so that if this test races with the fact_checker test (which also sets
        // MEMPALACE_DIR via temp_env), the registry lookup still returns a valid
        // (empty) list rather than a parse error — preventing a spurious failure.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temp dir for layer0 empty-file test");
        let identity_path = temp_directory.path().join("identity.txt");
        std::fs::write(&identity_path, "   \n  ")
            .expect("failed to write empty identity.txt for layer0 test");
        std::fs::write(temp_directory.path().join("known_entities.json"), "[]")
            .expect("failed to write empty known_entities.json for layer0 test");

        temp_env::with_var(
            "MEMPALACE_DIR",
            Some(temp_directory.path().to_str().expect("valid UTF-8 path")),
            || {
                let result = layer0();
                assert!(
                    result.contains("No identity configured"),
                    "empty identity.txt must produce the missing placeholder"
                );
                assert!(
                    result.contains("L0"),
                    "missing identity output must still include the L0 section header"
                );
            },
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
    async fn layer1_includes_kg_facts_when_triples_exist() {
        // When active KG triples are present, layer1 must append a knowledge graph block.
        // A drawer must also exist to bypass the no-drawers early return.
        let (_db, connection) = crate::test_helpers::test_db().await;

        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_kg_blend_1",
                wing: "research",
                room: "general",
                content: "Background context for knowledge graph blending test",
                source_file: "kg_blend.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer should succeed for KG blending test setup");

        crate::kg::add_triple(
            &connection,
            &crate::kg::TripleParams {
                subject: "Grace",
                predicate: "works_at",
                object: "DeepMind",
                valid_from: None,
                valid_to: None,
                confidence: 1.0,
                source_closet: None,
                source_file: None,
                source_drawer_id: None,
                adapter_name: None,
            },
        )
        .await
        .expect("add_triple should succeed for layer1 KG blending test");

        let result = layer1(&connection, None)
            .await
            .expect("layer1 should succeed with seeded drawer and triple");

        assert!(
            result.contains("knowledge graph"),
            "layer1 must include a knowledge graph section when triples exist"
        );
        assert!(
            result.contains("Grace"),
            "layer1 KG section must include the subject entity name"
        );
        // Pair assertion: result must include the drawer content too.
        assert!(
            result.contains("Background context"),
            "layer1 must still include the seeded drawer content alongside KG facts"
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

    // ── layer1 CHARS_MAX budget overflow (lines 102-105) ────────────────

    #[tokio::test]
    async fn layer1_truncates_when_content_exceeds_char_budget() {
        // When accumulated drawer content exceeds CHARS_MAX the function must
        // return early with a truncation notice rather than continuing the loop.
        // CHARS_MAX is 3200; inserting many 300-char drawers will exceed it.
        let (_db, connection) = crate::test_helpers::test_db().await;

        // Each drawer contributes ~300 chars. 15 drawers = ~4500 chars > CHARS_MAX.
        // We insert up to DRAWERS_MAX (15) which layer1 slices to top_drawers.
        let long_chunk: String = "Xylophone trumpet synthesizer percussion ".repeat(8);
        assert!(
            long_chunk.len() > 200,
            "fixture chunk must exceed the 200-char per-drawer snippet limit"
        );

        for index in 0..15_usize {
            drawer::add_drawer(
                &connection,
                &DrawerParams {
                    id: &format!("d_overflow_{index}"),
                    wing: "overflow_wing",
                    room: "general",
                    content: &long_chunk,
                    source_file: &format!("file_{index}.md"),
                    chunk_index: 0,
                    added_by: "test",
                    ingest_mode: "projects",
                    source_mtime: None,
                },
            )
            .await
            .expect("add_drawer must succeed for CHARS_MAX overflow test fixture");
        }

        let result = layer1(&connection, None)
            .await
            .expect("layer1 must succeed even when content exceeds the char budget");

        assert!(
            result.contains("more in L3 search"),
            "overflow must emit the truncation notice"
        );
        assert!(
            result.contains("L1"),
            "truncated output must still include the L1 header"
        );
    }

    // ── layer3 neighbor context expansion (lines 373-436) ────────────────

    #[tokio::test]
    async fn layer3_appends_neighbor_context_for_multi_chunk_source() {
        // When the top search hit belongs to a multi-chunk source, layer3 must
        // call layer3_add_neighbor_context and append a "nearby context" block.
        let (_db, connection) = crate::test_helpers::test_db().await;

        // Insert three consecutive chunks from the same source file.
        // The middle chunk (index 1) is the likely top hit; neighbors 0 and 2
        // should be fetched and appended as nearby context.
        for index in 0..3_usize {
            let content = if index == 1 {
                // Make index 1 a unique strong match for the query.
                "nebula astrophysics photometry spectroscopy".to_string()
            } else {
                format!("supporting context chunk number {index} of the astrophysics series")
            };
            drawer::add_drawer(
                &connection,
                &DrawerParams {
                    id: &format!("neighbor_chunk_{index}"),
                    wing: "science",
                    room: "astronomy",
                    content: &content,
                    source_file: "astrophysics.md",
                    chunk_index: index,
                    added_by: "test",
                    ingest_mode: "projects",
                    source_mtime: None,
                },
            )
            .await
            .expect("add_drawer must succeed for neighbor context expansion test");
        }

        let result = layer3(&connection, "nebula astrophysics", None, None, 5)
            .await
            .expect("layer3 must succeed for multi-chunk source search");

        assert!(
            result.contains("L3"),
            "layer3 output must contain the L3 header"
        );
        assert!(
            result.contains("astrophysics"),
            "layer3 output must include the matching content"
        );
        // The nearby context block is appended only when neighbors exist.
        assert!(
            result.contains("nearby context"),
            "layer3 output must include the nearby context block for multi-chunk source"
        );
    }

    // ── layer2_empty_label ────────────────────────────────────────────

    #[test]
    fn layer2_empty_label_all_arms() {
        // Verify all four match arms of layer2_empty_label produce the expected text.
        let both = layer2_empty_label(Some("mywing"), Some("myroom"));
        assert!(
            both.contains("mywing"),
            "both-filter label must include wing"
        );
        assert!(
            both.contains("myroom"),
            "both-filter label must include room"
        );

        let wing_only = layer2_empty_label(Some("mywing"), None);
        assert!(
            wing_only.contains("mywing"),
            "wing-only label must include wing"
        );
        assert!(
            !wing_only.contains("room"),
            "wing-only label must not mention room"
        );

        let room_only = layer2_empty_label(None, Some("myroom"));
        assert!(
            room_only.contains("myroom"),
            "room-only label must include room"
        );
        assert!(
            !room_only.contains("wing"),
            "room-only label must not mention wing"
        );

        let none_label = layer2_empty_label(None, None);
        assert!(
            none_label.is_empty(),
            "no-filter label must be empty string"
        );
    }

    // ── layer2 with room filter ───────────────────────────────────────

    #[tokio::test]
    async fn layer2_with_room_filter_returns_matching_drawers() {
        // layer2 with room=Some must surface drawers from that room only.
        let (_db, connection) = crate::test_helpers::test_db().await;
        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_l2_room_1",
                wing: "research",
                room: "notes",
                content: "Room-filtered drawer about biochemistry research notes",
                source_file: "bio.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for room-filter test");

        let result = layer2(&connection, None, Some("notes"), 10)
            .await
            .expect("layer2 must succeed with room filter");

        assert!(result.contains("L2"), "output must contain L2 header");
        assert!(
            result.contains("biochemistry"),
            "output must contain the seeded drawer content"
        );

        // Pair assertion: a different room returns nothing.
        let empty = layer2(&connection, None, Some("nonexistent_room"), 10)
            .await
            .expect("layer2 must succeed for non-matching room");
        assert!(
            empty.contains("No drawers found"),
            "non-matching room must return empty result"
        );
    }

    #[tokio::test]
    async fn layer2_with_wing_and_room_filter_returns_matching_drawers() {
        // Both wing and room filters applied together must narrow results correctly.
        let (_db, connection) = crate::test_helpers::test_db().await;
        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_l2_both_1",
                wing: "science",
                room: "physics",
                content: "Quantum entanglement and superposition principles in modern physics",
                source_file: "phys.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for wing+room filter test");

        let result = layer2(&connection, Some("science"), Some("physics"), 10)
            .await
            .expect("layer2 must succeed with wing+room filter");

        assert!(result.contains("L2"), "output must contain L2 header");
        assert!(
            result.contains("superposition"),
            "output must contain the seeded drawer content"
        );

        // Pair assertion: correct wing but wrong room returns nothing.
        let empty = layer2(&connection, Some("science"), Some("chemistry"), 10)
            .await
            .expect("layer2 must succeed for non-matching room");
        assert!(
            empty.contains("No drawers found"),
            "wrong room must return empty result even with correct wing"
        );
    }

    // ── layer2_empty_label in context — wing+room empty ──────────────

    #[tokio::test]
    async fn layer2_empty_message_includes_filter_context() {
        // The empty message must include wing and room when both are specified.
        let (_db, connection) = crate::test_helpers::test_db().await;

        let result = layer2(&connection, Some("alpha"), Some("beta"), 10)
            .await
            .expect("layer2 must succeed on empty DB");

        assert!(
            result.contains("No drawers found"),
            "empty result must say no drawers found"
        );
        assert!(
            result.contains("alpha"),
            "empty result label must include the wing name"
        );
        assert!(
            result.contains("beta"),
            "empty result label must include the room name"
        );
    }

    // ── layer1 snippet truncation ─────────────────────────────────────

    #[tokio::test]
    async fn layer1_long_content_is_truncated_with_ellipsis() {
        // Content longer than 200 chars must be truncated and suffixed with '...'.
        let (_db, connection) = crate::test_helpers::test_db().await;

        // Build content longer than the 200-char snippet limit.
        let long_content = "The quick brown fox jumps over the lazy dog. "
            .repeat(6)
            .trim()
            .to_string();
        assert!(
            long_content.len() > 200,
            "fixture must exceed the 200-char snippet limit"
        );

        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_long_snip",
                wing: "research",
                room: "notes",
                content: &long_content,
                source_file: "long.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for snippet truncation test");

        let result = layer1(&connection, None)
            .await
            .expect("layer1 must succeed with long drawer content");

        assert!(
            result.contains("ESSENTIAL STORY"),
            "layer1 output must contain the essential story header"
        );
        assert!(
            result.contains("..."),
            "truncated snippet must end with ellipsis"
        );
    }

    // ── wake_up without wing filter ───────────────────────────────────

    #[tokio::test]
    async fn wake_up_without_wing_filter_includes_all_wings() {
        // wake_up with wing=None must include drawers from all wings.
        let (_db, connection) = crate::test_helpers::test_db().await;

        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_wake_global",
                wing: "general",
                room: "inbox",
                content: "Global wake_up context about distributed systems design",
                source_file: "dist.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for global wake_up test");

        let result = wake_up(&connection, None)
            .await
            .expect("wake_up must succeed without wing filter");

        assert!(result.contains("L0"), "result must include L0 section");
        assert!(result.contains("L1"), "result must include L1 section");
        assert!(
            result.contains("distributed systems"),
            "result must include drawer content from global call"
        );
    }

    // ── layer2_format_row long content truncation ─────────────────────

    #[tokio::test]
    async fn layer2_long_drawer_content_is_truncated() {
        // Drawer content longer than SNIPPET_LEN_MAX must be truncated with '...'.
        let (_db, connection) = crate::test_helpers::test_db().await;

        // Build content longer than the 300-char L2 snippet limit.
        let long_content = "Abcdefghijklmnopqrstuvwxyz ".repeat(15).trim().to_string();
        assert!(
            long_content.len() > SNIPPET_LEN_MAX,
            "fixture must exceed SNIPPET_LEN_MAX"
        );

        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "d_l2_long",
                wing: "data",
                room: "archive",
                content: &long_content,
                source_file: "archive.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for L2 truncation test");

        let result = layer2(&connection, None, None, 10)
            .await
            .expect("layer2 must succeed with long drawer content");

        assert!(result.contains("L2"), "output must contain L2 header");
        assert!(
            result.contains("..."),
            "long drawer content must be truncated with ellipsis in L2"
        );
    }
}
