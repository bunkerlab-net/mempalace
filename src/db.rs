//! Database connection helpers for the embedded turso/SQLite engine.

use turso::{Builder, Connection, Database};

use crate::error::Result;

/// Open (or create) a local turso database and return a connection.
pub async fn open_db(path: &str) -> Result<(Database, Connection)> {
    let db = Builder::new_local(path)
        .experimental_triggers(true)
        .build()
        .await?;
    let conn = db.connect()?;
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
