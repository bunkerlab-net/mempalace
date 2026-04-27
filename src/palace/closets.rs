//! Closet storage and search-boost operations on the `compressed` table.
//!
//! Closets are compact topic summaries (topics, quotes, summary text) stored
//! in `compressed` and used to rank-boost BM25 search results.  When query
//! terms appear in a closet, drawers from the same source file receive a
//! multiplicative score boost.
//!
//! Ports `palace.py::{upsert_closet_lines,purge_file_closets}` from the
//! Python reference, adapted for the `TursoDB` SQL backend (no `ChromaDB`).

use std::cmp::Reverse;
use std::collections::HashMap;
use std::fmt::Write as _;

use turso::Connection;

use crate::db;
use crate::error::Result;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Rank-based multiplicative boosts for the top five closet-matching source files.
///
/// Applied as `relevance *= (1.0 + boost)` so the effect is proportional to
/// the existing BM25 score. Values mirror the Python reference boosts for
/// cosine distance, rescaled to work reasonably with BM25 magnitude.
const CLOSET_BOOSTS: &[f64] = &[0.40, 0.25, 0.15, 0.08, 0.04];
const _: () = assert!(CLOSET_BOOSTS.len() == 5);

/// Fallback boost applied to ranked closet hits beyond the top five.
const CLOSET_BOOST_FALLBACK: f64 = 0.02;
const _: () = assert!(CLOSET_BOOST_FALLBACK > 0.0);
const _: () = assert!(CLOSET_BOOST_FALLBACK < CLOSET_BOOSTS[4]);

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Insert or replace closet content for a single drawer in the `compressed` table.
///
/// Called after LLM-driven or regex-driven closet generation for a drawer.
/// `lines` holds the full generated text (topics, quotes, summary).
pub async fn upsert_closet_lines(
    connection: &Connection,
    drawer_id: &str,
    source_file: &str,
    lines: &str,
) -> Result<()> {
    assert!(
        !drawer_id.is_empty(),
        "upsert_closet_lines: drawer_id must not be empty"
    );
    assert!(
        !lines.is_empty(),
        "upsert_closet_lines: lines must not be empty"
    );

    connection
        .execute(
            "INSERT OR REPLACE INTO compressed (id, content, source_file) VALUES (?, ?, ?)",
            (drawer_id, lines, source_file),
        )
        .await?;

    Ok(())
}

/// Delete all `compressed` entries whose `source_file` matches `source_file`.
///
/// Call before re-mining or re-compressing a file so stale closet topics do
/// not accumulate. Returns the number of rows deleted.
pub async fn purge_file_closets(connection: &Connection, source_file: &str) -> Result<u64> {
    assert!(
        !source_file.is_empty(),
        "purge_file_closets: source_file must not be empty"
    );
    assert!(
        source_file.len() < 4096,
        "purge_file_closets: source_file path must be bounded"
    );

    let rows_affected = connection
        .execute(
            "DELETE FROM compressed WHERE source_file = ?",
            (source_file,),
        )
        .await?;

    // Deletion count must be bounded by realistic palace sizes.
    assert!(
        rows_affected < 1_000_000,
        "purge_file_closets: rows affected must be bounded"
    );
    Ok(rows_affected)
}

/// Return a multiplicative closet rank-boost per source file.
///
/// Queries the `compressed` table for entries whose `source_file` is in
/// `source_paths` and whose content contains at least one word from
/// `query_words`.  Returns a map of `source_file → boost` for matching files,
/// ranked by hit count.  Callers apply the boost as:
///
/// ```text
/// result.relevance *= 1.0 + boost;
/// ```
///
/// Returns an empty map when `source_paths` is empty or nothing matches.
pub async fn search_closet_boost(
    connection: &Connection,
    source_paths: &[String],
    query_words: &[String],
) -> Result<HashMap<String, f64>> {
    assert!(
        !query_words.is_empty(),
        "search_closet_boost: query_words must not be empty"
    );

    if source_paths.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = search_closet_boost_fetch(connection, source_paths).await?;

    let mut scored = search_closet_boost_score(&rows, query_words);
    scored.sort_unstable_by_key(|item| Reverse(item.1));

    let mut boosts: HashMap<String, f64> = HashMap::with_capacity(scored.len());
    for (rank, (source_file, _)) in scored.iter().enumerate() {
        let boost = CLOSET_BOOSTS
            .get(rank)
            .copied()
            .unwrap_or(CLOSET_BOOST_FALLBACK);
        boosts.insert(source_file.clone(), boost);
    }

    assert!(boosts.len() <= source_paths.len());
    Ok(boosts)
}

// ─────────────────────────────────────────────────────────────────────────────
// Private helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Fetch `(source_file, content)` pairs from `compressed` for the given paths.
///
/// Helper for [`search_closet_boost`]. Skips rows with empty content.
async fn search_closet_boost_fetch(
    connection: &Connection,
    source_paths: &[String],
) -> Result<Vec<turso::Row>> {
    assert!(
        !source_paths.is_empty(),
        "search_closet_boost_fetch: source_paths must not be empty"
    );

    let mut placeholders = String::with_capacity(source_paths.len() * 4);
    for i in 1..=source_paths.len() {
        if i > 1 {
            placeholders.push_str(", ");
        }
        // write! on String is infallible.
        let _ = write!(placeholders, "?{i}");
    }

    let sql = format!(
        "SELECT source_file, content FROM compressed \
         WHERE source_file IN ({placeholders}) AND content != ''"
    );
    let params: Vec<turso::Value> = source_paths
        .iter()
        .map(|path| turso::Value::from(path.as_str()))
        .collect();

    assert!(!sql.is_empty());
    db::query_all(connection, &sql, turso::params_from_iter(params)).await
}

/// Score each row by how many query words appear in its content.
///
/// Helper for [`search_closet_boost`]. Returns `(source_file, hit_count)` for
/// rows that contain at least one query word. Empty or repeated source files
/// are merged by keeping the highest hit count.
fn search_closet_boost_score(rows: &[turso::Row], query_words: &[String]) -> Vec<(String, usize)> {
    assert!(
        !query_words.is_empty(),
        "search_closet_boost_score: query_words must not be empty"
    );

    let mut best: HashMap<String, usize> = HashMap::new();

    for row in rows {
        let source_file: String = row.get(0).unwrap_or_default();
        let content: String = row.get(1).unwrap_or_default();
        if source_file.is_empty() || content.is_empty() {
            continue;
        }
        let content_lower = content.to_lowercase();
        let hit_count = query_words
            .iter()
            .filter(|word| content_lower.contains(word.as_str()))
            .count();
        if hit_count == 0 {
            continue;
        }
        // Keep the best score if a source file appears in multiple closet entries.
        let entry = best.entry(source_file).or_insert(0);
        if hit_count > *entry {
            *entry = hit_count;
        }
    }

    let result: Vec<(String, usize)> = best.into_iter().collect();
    assert!(result.len() <= rows.len());
    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // ── upsert_closet_lines ───────────────────────────────────────────────

    #[tokio::test]
    async fn upsert_closet_lines_inserts_row() {
        // A fresh upsert must create a row in compressed with the given content.
        let (_db, connection) = crate::test_helpers::test_db().await;
        upsert_closet_lines(&connection, "drawer_1", "notes.md", "Rust | memory | Alice")
            .await
            .expect("upsert must succeed");

        let rows = db::query_all(
            &connection,
            "SELECT content, source_file FROM compressed WHERE id = 'drawer_1'",
            (),
        )
        .await
        .expect("select must succeed");
        assert_eq!(rows.len(), 1, "one row must be inserted");
        let content: String = rows[0].get(0).expect("content column must be readable");
        let source_file: String = rows[0].get(1).expect("source_file column must be readable");
        assert_eq!(content, "Rust | memory | Alice");
        assert_eq!(source_file, "notes.md");
    }

    #[tokio::test]
    async fn upsert_closet_lines_replaces_existing_row() {
        // A second upsert for the same drawer_id must replace the previous content.
        let (_db, connection) = crate::test_helpers::test_db().await;
        upsert_closet_lines(&connection, "drawer_1", "notes.md", "old content")
            .await
            .expect("first upsert must succeed");
        upsert_closet_lines(&connection, "drawer_1", "notes.md", "new content")
            .await
            .expect("second upsert must succeed");

        let rows = db::query_all(
            &connection,
            "SELECT content FROM compressed WHERE id = 'drawer_1'",
            (),
        )
        .await
        .expect("select must succeed");
        assert_eq!(rows.len(), 1, "only one row must exist after replace");
        let content: String = rows[0].get(0).expect("content must be readable");
        assert_eq!(content, "new content", "content must be updated");
    }

    // ── purge_file_closets ────────────────────────────────────────────────

    #[tokio::test]
    async fn purge_file_closets_removes_all_rows_for_file() {
        // Purge must delete all rows matching source_file, leave others intact.
        let (_db, connection) = crate::test_helpers::test_db().await;
        upsert_closet_lines(&connection, "d1", "file_a.md", "content a1")
            .await
            .expect("upsert d1");
        upsert_closet_lines(&connection, "d2", "file_a.md", "content a2")
            .await
            .expect("upsert d2");
        upsert_closet_lines(&connection, "d3", "file_b.md", "content b1")
            .await
            .expect("upsert d3");

        let deleted = purge_file_closets(&connection, "file_a.md")
            .await
            .expect("purge must succeed");
        assert_eq!(deleted, 2, "two rows for file_a.md must be deleted");

        let remaining = db::query_all(&connection, "SELECT id FROM compressed", ())
            .await
            .expect("select must succeed");
        assert_eq!(remaining.len(), 1, "only d3 must remain");
    }

    #[tokio::test]
    async fn purge_file_closets_returns_zero_for_unknown_file() {
        // Purging a file with no closet entries must return 0 without error.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let deleted = purge_file_closets(&connection, "nonexistent.md")
            .await
            .expect("purge of unknown file must succeed");
        assert_eq!(deleted, 0, "zero rows deleted for unknown file");
    }

    // ── search_closet_boost ───────────────────────────────────────────────

    #[tokio::test]
    async fn search_closet_boost_empty_source_paths_returns_empty_map() {
        // No source paths = no closet lookup = empty boost map.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let boosts = search_closet_boost(&connection, &[], &["rust".to_string()])
            .await
            .expect("boost must succeed");
        assert!(
            boosts.is_empty(),
            "empty source_paths must return empty map"
        );
    }

    #[tokio::test]
    async fn search_closet_boost_match_returns_top_boost() {
        // A single matching source file must receive the top-rank boost.
        let (_db, connection) = crate::test_helpers::test_db().await;
        upsert_closet_lines(
            &connection,
            "d1",
            "/notes/rust.md",
            "Rust | memory | borrow checker",
        )
        .await
        .expect("upsert must succeed");

        let boosts = search_closet_boost(
            &connection,
            &["/notes/rust.md".to_string()],
            &["rust".to_string()],
        )
        .await
        .expect("boost must succeed");

        assert_eq!(
            boosts.len(),
            1,
            "one matching source file must produce one boost"
        );
        let boost = boosts["/notes/rust.md"];
        // Top-ranked match must receive CLOSET_BOOSTS[0] = 0.40.
        assert!(
            (boost - CLOSET_BOOSTS[0]).abs() < f64::EPSILON,
            "top match must get CLOSET_BOOSTS[0]"
        );
    }

    #[tokio::test]
    async fn search_closet_boost_no_match_returns_empty_map() {
        // A source file in compressed with unrelated content must not be boosted.
        let (_db, connection) = crate::test_helpers::test_db().await;
        upsert_closet_lines(
            &connection,
            "d1",
            "/notes/cooking.md",
            "pasta | olive oil | tomato",
        )
        .await
        .expect("upsert must succeed");

        let boosts = search_closet_boost(
            &connection,
            &["/notes/cooking.md".to_string()],
            &["rust".to_string()],
        )
        .await
        .expect("boost must succeed");
        assert!(
            boosts.is_empty(),
            "no query word match must produce empty boost map"
        );
    }

    // ── search_closet_boost_score unit tests ─────────────────────────────

    #[test]
    fn boost_score_returns_empty_for_no_matching_rows() {
        // When no rows contain query words, scored must be empty.
        let scored = search_closet_boost_score(&[], &["rust".to_string()]);
        assert!(
            scored.is_empty(),
            "empty rows must produce empty scored vec"
        );
    }
}
