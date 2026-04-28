//! `MemoryStack` — aggregator façade over the palace layer functions.
//!
//! Provides a stateful object that bundles a `Connection` with `recall`,
//! `browse`, `search`, and `status` operations, mirroring the Python
//! `MemoryStack` API for library callers that want a single entry point
//! rather than calling layer functions individually.

use turso::Connection;

use crate::error::Result;
use crate::palace::{layers, stats};

/// Aggregates the palace layer operations behind a single connection-bound object.
pub struct MemoryStack<'a> {
    connection: &'a Connection,
}

impl<'a> MemoryStack<'a> {
    /// Create a new `MemoryStack` bound to `connection`.
    pub fn new(connection: &'a Connection) -> Self {
        MemoryStack { connection }
    }

    /// Return the full wake-up context (L0 identity + L1 essential story).
    ///
    /// Equivalent to `layers::wake_up`. Pass `wing` to scope L1 to a single
    /// wing.
    pub async fn recall(&self, wing: Option<&str>) -> Result<String> {
        if let Some(name) = wing {
            assert!(
                !name.is_empty(),
                "MemoryStack::recall: wing must not be empty if provided"
            );
        }
        let output = layers::wake_up(self.connection, wing).await?;
        assert!(
            !output.is_empty(),
            "MemoryStack::recall: wake_up must return non-empty context"
        );
        Ok(output)
    }

    /// Return on-demand L2 drawer recall scoped to `wing` and/or `room`.
    pub async fn browse(
        &self,
        wing: Option<&str>,
        room: Option<&str>,
        results: usize,
    ) -> Result<String> {
        assert!(results > 0, "MemoryStack::browse: results must be positive");
        let output = layers::layer2(self.connection, wing, room, results).await?;
        assert!(
            !output.is_empty(),
            "MemoryStack::browse: layer2 must return non-empty output"
        );
        Ok(output)
    }

    /// Return L3 keyword search results.
    pub async fn search(
        &self,
        query: &str,
        wing: Option<&str>,
        room: Option<&str>,
        results: usize,
    ) -> Result<String> {
        assert!(
            !query.is_empty(),
            "MemoryStack::search: query must not be empty"
        );
        assert!(results > 0, "MemoryStack::search: results must be positive");
        layers::layer3(self.connection, query, wing, room, results).await
    }

    /// Return aggregated palace statistics.
    pub async fn status(&self) -> Result<stats::PalaceStats> {
        let palace_stats = stats::query_stats(self.connection).await?;
        assert!(
            palace_stats.total_drawers >= 0,
            "MemoryStack::status: total_drawers must be non-negative"
        );
        assert!(
            palace_stats.entity_count >= 0,
            "MemoryStack::status: entity_count must be non-negative"
        );
        Ok(palace_stats)
    }
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::palace::drawer::{self, DrawerParams};

    #[tokio::test]
    async fn recall_empty_palace_returns_no_memories_message() {
        // recall on an empty palace must succeed and include the L1 header.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let stack = MemoryStack::new(&connection);
        let result = stack
            .recall(None)
            .await
            .expect("MemoryStack::recall must succeed on empty palace");
        assert!(
            !result.is_empty(),
            "recall must return non-empty output on empty palace"
        );
        assert!(
            result.contains("L0") || result.contains("L1"),
            "recall must contain at least one layer header"
        );
    }

    #[tokio::test]
    async fn status_empty_palace_returns_zero_counts() {
        // status on an empty palace must return zero for all counts.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let stack = MemoryStack::new(&connection);
        let palace_stats = stack
            .status()
            .await
            .expect("MemoryStack::status must succeed on empty palace");
        assert_eq!(
            palace_stats.total_drawers, 0,
            "empty palace must report zero drawers"
        );
        assert_eq!(
            palace_stats.entity_count, 0,
            "empty palace must report zero entities"
        );
    }

    #[tokio::test]
    async fn browse_empty_palace_returns_no_drawers_message() {
        // browse on an empty palace must return the no-drawers message.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let stack = MemoryStack::new(&connection);
        let result = stack
            .browse(None, None, 5)
            .await
            .expect("MemoryStack::browse must succeed on empty palace");
        assert!(
            result.contains("No drawers found"),
            "browse on empty palace must report no drawers"
        );
        // Pair assertion: result must include the L2 section header.
        assert!(
            result.contains("L2"),
            "browse output must contain L2 header"
        );
    }

    #[tokio::test]
    async fn search_returns_results_for_matching_query() {
        // search must surface drawers whose content matches the query term.
        let (_db, connection) = crate::test_helpers::test_db().await;
        drawer::add_drawer(
            &connection,
            &DrawerParams {
                id: "stack-search-001",
                wing: "research",
                room: "notes",
                content: "Distributed systems consensus protocols and Raft algorithm",
                source_file: "notes.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for MemoryStack search test");

        let stack = MemoryStack::new(&connection);
        let result = stack
            .search("consensus", None, None, 5)
            .await
            .expect("MemoryStack::search must succeed for matching query");

        assert!(
            result.contains("L3"),
            "search output must contain L3 section header"
        );
        assert!(
            result.contains("consensus"),
            "search must surface the matching drawer content"
        );
    }
}
