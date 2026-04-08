//! Database connection helpers for the embedded turso/SQLite engine.

use std::time::Duration;

use turso::{Builder, Connection, Database};

use crate::error::Result;

/// Open (or create) a local turso database and return a connection.
///
/// For file-backed databases, WAL journal mode is enabled for better concurrency
/// and crash recovery when multiple MCP clients write simultaneously.
/// In-memory databases skip the WAL pragma since it is not applicable.
#[allow(unsafe_code)]
pub async fn open_db(path: &str) -> Result<(Database, Connection)> {
    // Disable turso/limbo's exclusive file lock so multiple processes (e.g.
    // concurrent MCP servers or CLI commands) can access the same database.
    // WAL mode provides the necessary concurrency control at the protocol level.
    // See: https://github.com/bunkerlab-net/mempalace/issues/9
    //
    // SAFETY: set_var is unsafe due to thread-safety concerns, but this runs
    // before any other threads access the environment.
    unsafe {
        std::env::set_var("LIMBO_DISABLE_FILE_LOCK", "1");
    }

    let db = Builder::new_local(path)
        .experimental_triggers(true)
        .build()
        .await?;
    let conn = db.connect()?;

    // Only enable WAL for file-backed databases; in-memory DBs do not support it
    let is_in_memory = path.is_empty()
        || path == ":memory:"
        || path.starts_with("file::memory:")
        || (path.starts_with("file:") && path.contains("mode=memory"));
    if !is_in_memory {
        let mut wal_rows = conn.query("PRAGMA journal_mode=WAL", ()).await?;
        while wal_rows.next().await?.is_some() {}
    }

    // Allow waiting up to 5 seconds for write locks when another process is
    // writing, instead of failing immediately.
    conn.busy_timeout(Duration::from_secs(5))?;

    Ok((db, conn))
}

/// Collect all rows from a query into a Vec.
pub async fn query_all(
    conn: &Connection,
    sql: &str,
    params: impl turso::IntoParams,
) -> Result<Vec<turso::Row>> {
    let mut rows = conn.query(sql, params).await?;
    let mut results = Vec::new();
    while let Some(row) = rows.next().await? {
        results.push(row);
    }
    Ok(results)
}
