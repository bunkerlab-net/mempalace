//! Repair command — rebuild the inverted index from all stored drawers.

use std::path::Path;

use turso::Connection;

use crate::db::query_all;
use crate::error::Result;
use crate::palace::drawer;

/// Backup the palace database and rebuild the inverted word index.
pub async fn run(conn: &Connection, palace_path: &Path) -> Result<()> {
    // Backup: copy main DB file and WAL sidecars to ensure a consistent snapshot
    let backup_path = palace_path.with_extension("db.bak");
    std::fs::copy(palace_path, &backup_path)?;

    let wal_path = palace_path.with_extension("db-wal");
    let shm_path = palace_path.with_extension("db-shm");
    if wal_path.exists() {
        let backup_wal = backup_path.with_extension("db-wal");
        std::fs::copy(&wal_path, &backup_wal)?;
    }
    if shm_path.exists() {
        let backup_shm = backup_path.with_extension("db-shm");
        std::fs::copy(&shm_path, &backup_shm)?;
    }

    println!("Backup created: {}", backup_path.display());

    // Read all drawers
    let rows = query_all(conn, "SELECT id, content FROM drawers", ()).await?;
    let total = rows.len();
    println!("Found {total} drawers to re-index");

    // Collect id+content before clearing index (borrow lifetime)
    let drawers: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            let id: String = row.get(0).unwrap_or_default();
            let content: String = row.get(1).unwrap_or_default();
            (id, content)
        })
        .collect();

    // Clear and rebuild within a transaction for atomicity
    conn.execute("BEGIN TRANSACTION", ()).await?;

    if let Err(e) = async {
        conn.execute("DELETE FROM drawer_words", ()).await?;
        println!("Cleared existing index");

        // Rebuild
        for (i, (id, content)) in drawers.iter().enumerate() {
            drawer::index_words(conn, id, content).await?;
            if (i + 1) % 100 == 0 || i + 1 == total {
                println!("  [{}/{}] re-indexed", i + 1, total);
            }
        }
        Ok::<(), crate::error::Error>(())
    }
    .await
    {
        conn.execute("ROLLBACK", ()).await?;
        return Err(e);
    }

    conn.execute("COMMIT", ()).await?;

    println!("\nRepair complete: {total} drawers re-indexed");
    Ok(())
}
