//! Structured palace statistics for library callers.
//!
//! Unlike `cli::status::run`, which prints directly to stdout, this module
//! returns a [`PalaceStats`] struct so MCP tools and integrations can format
//! the data themselves.

use turso::Connection;

use crate::db;
use crate::error::Result;

/// Aggregated statistics about the palace.
pub struct PalaceStats {
    /// Total drawers across all wings and rooms.
    pub total_drawers: i64,
    /// Per-wing drawer counts, sorted by count descending: `(wing_name, count)`.
    pub wing_counts: Vec<(String, i64)>,
    /// Per-room drawer counts, sorted by wing then count descending:
    /// `(wing_name, room_name, count)`.
    pub room_counts: Vec<(String, String, i64)>,
    /// Number of entities in the knowledge graph.
    pub entity_count: i64,
    /// Number of triples in the knowledge graph.
    pub triple_count: i64,
}

/// Query `COUNT(*) FROM drawers`.
async fn query_stats_total_drawers(connection: &Connection) -> Result<i64> {
    let rows = db::query_all(connection, "SELECT COUNT(*) FROM drawers", ()).await?;
    let count = rows
        .first()
        .and_then(|row| row.get_value(0).ok())
        .and_then(|cell| cell.as_integer().copied())
        .unwrap_or(0);
    assert!(
        count >= 0,
        "query_stats_total_drawers: count must be non-negative"
    );
    Ok(count)
}

/// Query per-wing drawer counts, ordered by count descending.
async fn query_stats_wing_counts(connection: &Connection) -> Result<Vec<(String, i64)>> {
    let rows = db::query_all(
        connection,
        "SELECT wing, COUNT(*) as cnt FROM drawers GROUP BY wing ORDER BY cnt DESC",
        (),
    )
    .await?;
    let wing_counts: Vec<(String, i64)> = rows
        .iter()
        .map(|row| {
            let wing = row
                .get_value(0)
                .ok()
                .and_then(|cell| cell.as_text().cloned())
                .unwrap_or_default();
            let count = row
                .get_value(1)
                .ok()
                .and_then(|cell| cell.as_integer().copied())
                .unwrap_or(0);
            (wing, count)
        })
        .collect();
    assert!(
        wing_counts.iter().all(|(_, c)| *c >= 0),
        "query_stats_wing_counts: all per-wing counts must be non-negative"
    );
    Ok(wing_counts)
}

/// Query per-room drawer counts, ordered by wing then count descending.
async fn query_stats_room_counts(connection: &Connection) -> Result<Vec<(String, String, i64)>> {
    let rows = db::query_all(
        connection,
        "SELECT wing, room, COUNT(*) as cnt \
         FROM drawers GROUP BY wing, room ORDER BY wing, cnt DESC",
        (),
    )
    .await?;
    let room_counts: Vec<(String, String, i64)> = rows
        .iter()
        .map(|row| {
            let wing = row
                .get_value(0)
                .ok()
                .and_then(|cell| cell.as_text().cloned())
                .unwrap_or_default();
            let room = row
                .get_value(1)
                .ok()
                .and_then(|cell| cell.as_text().cloned())
                .unwrap_or_default();
            let count = row
                .get_value(2)
                .ok()
                .and_then(|cell| cell.as_integer().copied())
                .unwrap_or(0);
            (wing, room, count)
        })
        .collect();
    assert!(
        room_counts.iter().all(|(_, _, c)| *c >= 0),
        "query_stats_room_counts: all per-room counts must be non-negative"
    );
    Ok(room_counts)
}

/// Query `COUNT(*) FROM entities`.
async fn query_stats_entity_count(connection: &Connection) -> Result<i64> {
    let rows = db::query_all(connection, "SELECT COUNT(*) FROM entities", ()).await?;
    let count = rows
        .first()
        .and_then(|row| row.get_value(0).ok())
        .and_then(|cell| cell.as_integer().copied())
        .unwrap_or(0);
    assert!(
        count >= 0,
        "query_stats_entity_count: count must be non-negative"
    );
    Ok(count)
}

/// Query `COUNT(*) FROM triples`.
async fn query_stats_triple_count(connection: &Connection) -> Result<i64> {
    let rows = db::query_all(connection, "SELECT COUNT(*) FROM triples", ()).await?;
    let count = rows
        .first()
        .and_then(|row| row.get_value(0).ok())
        .and_then(|cell| cell.as_integer().copied())
        .unwrap_or(0);
    assert!(
        count >= 0,
        "query_stats_triple_count: count must be non-negative"
    );
    Ok(count)
}

/// Return aggregated palace statistics without printing anything.
///
/// Callers such as `cli::status::run` and MCP tools use this instead of issuing
/// direct database queries, keeping the query logic in one place.
pub async fn query_stats(connection: &Connection) -> Result<PalaceStats> {
    let total_drawers = query_stats_total_drawers(connection).await?;
    let wing_counts = query_stats_wing_counts(connection).await?;
    let room_counts = query_stats_room_counts(connection).await?;
    let entity_count = query_stats_entity_count(connection).await?;
    let triple_count = query_stats_triple_count(connection).await?;

    assert!(
        total_drawers >= 0,
        "query_stats: total_drawers must be non-negative"
    );
    assert!(
        entity_count >= 0,
        "query_stats: entity_count must be non-negative"
    );

    let stats = PalaceStats {
        total_drawers,
        wing_counts,
        room_counts,
        entity_count,
        triple_count,
    };

    // Pair assertion: sum of per-wing counts must equal the overall total.
    let wing_total: i64 = stats.wing_counts.iter().map(|(_, c)| c).sum();
    assert!(
        wing_total == stats.total_drawers,
        "query_stats: per-wing total {wing_total} must equal total_drawers {}",
        stats.total_drawers
    );

    Ok(stats)
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn query_stats_empty_palace_returns_zeros() {
        // An empty palace must return zeros for all counts with empty lists.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let stats = query_stats(&connection)
            .await
            .expect("query_stats must succeed on empty palace");
        assert_eq!(
            stats.total_drawers, 0,
            "empty palace must have zero drawers"
        );
        assert!(
            stats.wing_counts.is_empty(),
            "empty palace must have no wing counts"
        );
        assert!(
            stats.room_counts.is_empty(),
            "empty palace must have no room counts"
        );
        assert_eq!(
            stats.entity_count, 0,
            "empty palace must have zero entities"
        );
        assert_eq!(stats.triple_count, 0, "empty palace must have zero triples");
    }

    #[tokio::test]
    async fn query_stats_with_drawers_reports_counts() {
        // Wing, room, and total counts must reflect all added drawers.
        let (_db, connection) = crate::test_helpers::test_db().await;

        for (id, wing, room) in [
            ("stats-001", "alpha", "general"),
            ("stats-002", "alpha", "notes"),
            ("stats-003", "beta", "general"),
        ] {
            crate::palace::drawer::add_drawer(
                &connection,
                &crate::palace::drawer::DrawerParams {
                    id,
                    wing,
                    room,
                    content: "content for stats library test",
                    source_file: "a.rs",
                    chunk_index: 0,
                    added_by: "test",
                    ingest_mode: "projects",
                    source_mtime: None,
                },
            )
            .await
            .expect("add_drawer must succeed for stats test");
        }

        let stats = query_stats(&connection)
            .await
            .expect("query_stats must succeed with drawers present");

        assert_eq!(stats.total_drawers, 3, "three drawers must be counted");
        assert_eq!(stats.wing_counts.len(), 2, "two wings must appear");
        assert_eq!(
            stats.room_counts.len(),
            3,
            "three wing/room pairs must appear"
        );

        // alpha has 2 drawers — it must appear first (sorted by count desc).
        let (first_wing, first_count) = &stats.wing_counts[0];
        assert_eq!(first_wing, "alpha", "alpha must be the highest-count wing");
        assert_eq!(*first_count, 2, "alpha must have 2 drawers");
    }
}
