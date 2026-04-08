//! Repair command — rebuild the inverted index from all stored drawers.

use std::path::Path;

use turso::Connection;

use crate::db::query_all;
use crate::error::Result;
use crate::palace::drawer;

/// Backup the palace database and rebuild the inverted word index.
pub async fn run(conn: &Connection, palace_path: &Path) -> Result<()> {
    // Backup
    let backup_path = palace_path.with_extension("db.bak");
    std::fs::copy(palace_path, &backup_path)?;
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

    // Clear existing index
    conn.execute("DELETE FROM drawer_words", ()).await?;
    println!("Cleared existing index");

    // Rebuild
    for (i, (id, content)) in drawers.iter().enumerate() {
        drawer::index_words(conn, id, content).await?;
        if (i + 1) % 100 == 0 || i + 1 == total {
            println!("  [{}/{}] re-indexed", i + 1, total);
        }
    }

    println!("\nRepair complete: {total} drawers re-indexed");
    Ok(())
}
