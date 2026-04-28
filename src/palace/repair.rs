//! Library entry point for rebuilding the inverted word index.
//!
//! Unlike `cli::repair::run`, this module does not print progress output or
//! create a file-system backup. It performs the DB operation only, making it
//! safe to call from MCP tools and library integrations.

use turso::Connection;

use crate::db::query_all;
use crate::error::Result;
use crate::palace::drawer;

/// Rebuild the full-text inverted word index from all drawer content.
///
/// Wraps the full operation in a `BEGIN IMMEDIATE` transaction. Returns the
/// number of drawers that were re-indexed. On failure, attempts a `ROLLBACK`
/// before propagating the error.
pub async fn rebuild_index(connection: &Connection) -> Result<usize> {
    connection.execute("BEGIN IMMEDIATE", ()).await?;
    match rebuild_index_execute(connection).await {
        Ok(total) => {
            connection.execute("COMMIT", ()).await?;
            Ok(total)
        }
        Err(error) => {
            if let Err(rollback_error) = connection.execute("ROLLBACK", ()).await {
                eprintln!("Rollback failed: {rollback_error}");
            }
            Err(error)
        }
    }
}

/// Collect all drawers and rebuild the index inside an already-open transaction.
///
/// Called exclusively by `rebuild_index`. Errors propagate to the caller, which
/// issues the `ROLLBACK`.
async fn rebuild_index_execute(connection: &Connection) -> Result<usize> {
    let rows = query_all(connection, "SELECT id, content FROM drawers", ()).await?;
    let total = rows.len();

    let drawers: Vec<(String, String)> = rows
        .iter()
        .map(|row| -> Result<(String, String)> {
            let id: String = row.get(0)?;
            let content: String = row.get(1)?;
            Ok((id, content))
        })
        .collect::<Result<Vec<_>>>()?;

    // Precondition: collected count must match the row count.
    assert!(
        drawers.len() == total,
        "rebuild_index_execute: drawers.len() {} must equal row count {total}",
        drawers.len()
    );

    connection.execute("DELETE FROM drawer_words", ()).await?;

    let mut indexed_count = 0usize;
    for (id, content) in &drawers {
        drawer::index_words(connection, id, content).await?;
        indexed_count += 1;
    }

    // Pair assertion: every collected drawer must have been indexed.
    assert!(
        indexed_count == total,
        "rebuild_index_execute: indexed {indexed_count} but expected {total} drawers"
    );

    Ok(total)
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rebuild_index_empty_palace_returns_zero() {
        // An empty palace must succeed and report zero drawers reindexed.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let total = rebuild_index(&connection)
            .await
            .expect("rebuild_index must succeed on an empty palace");
        assert_eq!(total, 0, "empty palace must report 0 drawers reindexed");
        // Pair assertion: drawer_words must be empty after reindexing an empty palace.
        let rows = crate::db::query_all(&connection, "SELECT COUNT(*) FROM drawer_words", ())
            .await
            .expect("drawer_words count query must succeed");
        let index_count: i64 = rows
            .first()
            .and_then(|r| r.get_value(0).ok())
            .and_then(|c| c.as_integer().copied())
            .unwrap_or(0);
        assert_eq!(
            index_count, 0,
            "drawer_words must be empty when no drawers exist"
        );
    }

    #[tokio::test]
    async fn rebuild_index_with_drawers_returns_count() {
        // A palace with drawers must return the correct reindexed count.
        let (_db, connection) = crate::test_helpers::test_db().await;

        for (id, wing) in [("repair-lib-001", "alpha"), ("repair-lib-002", "beta")] {
            crate::palace::drawer::add_drawer(
                &connection,
                &crate::palace::drawer::DrawerParams {
                    id,
                    wing,
                    room: "general",
                    content: "content for library rebuild_index test",
                    source_file: "a.rs",
                    chunk_index: 0,
                    added_by: "test",
                    ingest_mode: "projects",
                    source_mtime: None,
                },
            )
            .await
            .expect("add_drawer must succeed for rebuild_index test");
        }

        let total = rebuild_index(&connection)
            .await
            .expect("rebuild_index must succeed with two drawers present");
        assert_eq!(total, 2, "two drawers must report count of 2");
        // Pair assertion: drawer_words must have entries after reindexing non-empty palace.
        let rows = crate::db::query_all(&connection, "SELECT COUNT(*) FROM drawer_words", ())
            .await
            .expect("drawer_words count query must succeed after rebuild");
        let index_count: i64 = rows
            .first()
            .and_then(|r| r.get_value(0).ok())
            .and_then(|c| c.as_integer().copied())
            .unwrap_or(0);
        assert!(
            index_count > 0,
            "drawer_words must have entries after reindexing two drawers"
        );
    }
}
