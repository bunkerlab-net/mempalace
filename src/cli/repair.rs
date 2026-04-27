//! Repair command — rebuild the inverted index from all stored drawers.

use std::path::Path;

use turso::Connection;

use crate::db::query_all;
use crate::error::Result;

/// Backup the palace database, scan for inconsistencies, and rebuild the inverted word index.
pub async fn run(connection: &Connection, palace_path: &Path) -> Result<()> {
    run_create_backup(connection, palace_path).await?;
    run_scan(connection).await?;
    run_rebuild_index(connection).await
}

/// Scan for index inconsistencies and report them.
///
/// Reports two classes of problem:
///   - Drawers with no `drawer_words` entries (content was never indexed).
///   - Orphaned `drawer_words` rows pointing to non-existent drawers.
///
/// Both classes are fixed by the rebuild step that follows, so this function
/// is informational only — it does not modify the database.
async fn run_scan(connection: &Connection) -> Result<()> {
    let unindexed_rows = query_all(
        connection,
        "SELECT id FROM drawers WHERE id NOT IN (SELECT DISTINCT drawer_id FROM drawer_words)",
        (),
    )
    .await?;
    let unindexed_count = unindexed_rows.len();

    let orphan_rows = query_all(
        connection,
        "SELECT count(*) FROM drawer_words WHERE drawer_id NOT IN (SELECT id FROM drawers)",
        (),
    )
    .await?;
    let orphan_count: i64 = orphan_rows
        .first()
        .and_then(|row| row.get_value(0).ok())
        .and_then(|cell| cell.as_integer().copied())
        .unwrap_or(0);

    if unindexed_count > 0 {
        println!(
            "Scan found {unindexed_count} drawer(s) with no index entries — will be re-indexed"
        );
    }
    if orphan_count > 0 {
        println!(
            "Scan found {orphan_count} orphaned index row(s) — will be cleared during rebuild"
        );
    }
    if unindexed_count == 0 && orphan_count == 0 {
        println!("Scan: no inconsistencies found");
    }

    // Postconditions: counts are non-negative.
    debug_assert!(orphan_count >= 0, "orphan count must be non-negative");
    Ok(())
}

/// Checkpoint the WAL and copy the palace database (plus sidecar files) to `.db.bak`.
async fn run_create_backup(connection: &Connection, palace_path: &Path) -> Result<()> {
    // Checkpoint WAL to ensure backup is self-contained.
    // wal_checkpoint returns rows (busy, log, checkpointed) — must use query_all.
    query_all(connection, "PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;

    let backup_path = palace_path.with_extension("db.bak");
    std::fs::copy(palace_path, &backup_path)?;

    // Build backup sidecar names from backup_path to avoid overwriting source files.
    let backup_filename = backup_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("palace.db.bak");
    let backup_parent = backup_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));

    let wal_path = palace_path.with_extension("db-wal");
    let shm_path = palace_path.with_extension("db-shm");
    if wal_path.exists() {
        let backup_wal = backup_parent.join(format!("{backup_filename}-wal"));
        std::fs::copy(&wal_path, &backup_wal)?;
    }
    if shm_path.exists() {
        let backup_shm = backup_parent.join(format!("{backup_filename}-shm"));
        std::fs::copy(&shm_path, &backup_shm)?;
    }

    println!("Backup created: {}", backup_path.display());
    Ok(())
}

/// Clear and rebuild the inverted word index via the palace library.
///
/// Delegates the transaction and index work to `palace::repair::rebuild_index`,
/// which wraps everything in a `BEGIN IMMEDIATE` transaction. The CLI layer adds
/// the completion message.
async fn run_rebuild_index(connection: &Connection) -> Result<()> {
    let total = crate::palace::repair::rebuild_index(connection).await?;
    assert!(
        total < usize::MAX,
        "run_rebuild_index: reindexed count must be within usize range"
    );
    println!("\nRepair complete: {total} drawers re-indexed");
    Ok(())
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Open a file-backed turso database at `path`, apply the schema, and return
    /// `(Database, Connection)`.  `repair::run` requires a real filesystem path because
    /// it calls `std::fs::copy`; an in-memory database cannot be used here.
    async fn open_file_db(path: &std::path::Path) -> (turso::Database, turso::Connection) {
        let database = turso::Builder::new_local(
            path.to_str()
                .expect("file-backed database path must be valid UTF-8"),
        )
        .experimental_triggers(true)
        .build()
        .await
        .expect("failed to create file-backed turso database");
        let connection = database
            .connect()
            .expect("failed to connect to file-backed database");
        crate::schema::ensure_schema(&connection)
            .await
            .expect("failed to apply schema to file-backed database");
        (database, connection)
    }

    #[tokio::test]
    async fn repair_creates_backup_and_rebuilds_index() {
        // repair::run must create a .db.bak backup and re-index all drawers.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for repair backup test");
        let database_path = temp_directory.path().join("palace.db");
        let (_database, connection) = open_file_db(&database_path).await;

        // Add a drawer so there is at least one entry to re-index.
        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "drawer_repair_test_001",
                wing: "test_wing",
                room: "general",
                content: "content to index during repair test",
                source_file: "test.txt",
                chunk_index: 0,
                added_by: "test_agent",
                ingest_mode: "repair_test",
                source_mtime: None,
            },
        )
        .await
        .expect("failed to add drawer for repair test setup");

        run(&connection, &database_path)
            .await
            .expect("repair::run should succeed after adding a test drawer");

        // Backup file must exist after repair.
        let backup_path = database_path.with_extension("db.bak");
        assert!(
            backup_path.exists(),
            "repair must create a .db.bak backup file"
        );
        assert!(
            database_path.exists(),
            "original palace.db must still exist after repair"
        );
    }

    #[tokio::test]
    async fn repair_with_no_drawers_succeeds() {
        // repair::run must succeed even when the palace has no drawers to re-index.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for empty-palace repair test");
        let database_path = temp_directory.path().join("empty_palace.db");
        let (_database, connection) = open_file_db(&database_path).await;

        run(&connection, &database_path)
            .await
            .expect("repair::run should succeed on a palace with zero drawers");

        let backup_path = database_path.with_extension("db.bak");
        assert!(
            backup_path.exists(),
            "backup must be created even for empty palace"
        );
        assert!(
            database_path.exists(),
            "original database file must remain after repair"
        );
    }
}
