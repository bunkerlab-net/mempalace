//! Database connection helpers for the embedded turso/SQLite engine.

use std::time::Duration;

use turso::{Builder, Connection, Database};

use crate::error::Result;

/// Open (or create) a local turso database and return a connection.
///
/// For file-backed databases, WAL journal mode is enabled for better concurrency
/// and crash recovery when multiple MCP clients write simultaneously.
/// In-memory databases skip the WAL pragma since it is not applicable.
///
/// Callers are expected to have set `LIMBO_DISABLE_FILE_LOCK` before the Tokio
/// runtime starts (see `main()`).
pub async fn open_db(path: &str) -> Result<(Database, Connection)> {
    assert!(!path.is_empty(), "database path must not be empty");

    let db = Builder::new_local(path)
        .experimental_triggers(true)
        .build()
        .await?;
    let connection = db.connect()?;

    // Only enable WAL for file-backed databases; in-memory DBs do not support it.
    let is_in_memory = path == ":memory:"
        || path.starts_with("file::memory:")
        || (path.starts_with("file:") && path.contains("mode=memory"));
    if !is_in_memory {
        let mut wal_rows = connection.query("PRAGMA journal_mode=WAL", ()).await?;
        // Upper bound: PRAGMA journal_mode returns exactly one row; drain it.
        while wal_rows.next().await?.is_some() {}
    }

    // Allow waiting up to 5 seconds for write locks when another process is
    // writing, instead of failing immediately.
    connection.busy_timeout(Duration::from_secs(5))?;

    // Postcondition: verify the connection is usable.
    let mut check = connection.query("SELECT 1", ()).await?;
    assert!(
        check.next().await?.is_some(),
        "connection must be usable after open"
    );

    Ok((db, connection))
}

/// Collect all rows from a query into a Vec.
pub async fn query_all(
    connection: &Connection,
    sql: &str,
    params: impl turso::IntoParams,
) -> Result<Vec<turso::Row>> {
    assert!(!sql.is_empty(), "SQL query must not be empty");

    let mut rows = connection.query(sql, params).await?;
    let mut results = Vec::new();
    while let Some(row) = rows.next().await? {
        results.push(row);
    }
    Ok(results)
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_db_creates_and_connects() {
        let dir = tempfile::tempdir().expect("tempdir creation should succeed");
        let db_path = dir.path().join("test.db");
        let path_str = db_path
            .to_str()
            .expect("tempdir path should be valid UTF-8");

        let (_db, conn) = open_db(path_str)
            .await
            .expect("open_db should succeed for a fresh file path");

        // Verify the connection is usable by running a trivial query.
        let rows = query_all(&conn, "SELECT 42 AS answer", ())
            .await
            .expect("trivial SELECT should succeed on newly opened connection");
        assert_eq!(rows.len(), 1, "SELECT 42 should return exactly 1 row");
        let val: i64 = rows[0].get(0).expect("column 0 should be readable as i64");
        assert_eq!(val, 42);
    }

    #[tokio::test]
    async fn query_all_returns_rows() {
        let (_db, conn) = crate::test_helpers::test_db().await;

        // Insert a row into the drawers table (schema is already applied).
        conn.execute(
            "INSERT INTO drawers (id, wing, room, content) VALUES ('d1', 'w', 'r', 'hello')",
            (),
        )
        .await
        .expect("INSERT into drawers should succeed");

        let rows = query_all(&conn, "SELECT id, content FROM drawers WHERE id = 'd1'", ())
            .await
            .expect("SELECT from drawers should succeed after insert");
        assert_eq!(rows.len(), 1, "should find exactly the inserted row");
        let id: String = rows[0]
            .get(0)
            .expect("column 0 (id) should be readable as String");
        assert_eq!(id, "d1");
    }

    #[tokio::test]
    async fn query_all_empty_result() {
        let (_db, conn) = crate::test_helpers::test_db().await;

        let rows = query_all(
            &conn,
            "SELECT id FROM drawers WHERE wing = 'nonexistent'",
            (),
        )
        .await
        .expect("SELECT with no matching rows should still succeed");
        assert!(rows.is_empty(), "no rows should match a nonexistent wing");
        assert_eq!(rows.len(), 0);
    }
}
