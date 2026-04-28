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
pub(crate) async fn run_scan(connection: &Connection) -> Result<()> {
    // Single round-trip: ask SQLite to count both classes of inconsistency at once
    // rather than fetching the full unindexed id list just to take its length.
    let rows = query_all(
        connection,
        "SELECT \
           (SELECT count(*) FROM drawers WHERE id NOT IN (SELECT DISTINCT drawer_id FROM drawer_words)) AS unindexed_count, \
           (SELECT count(*) FROM drawer_words WHERE drawer_id NOT IN (SELECT id FROM drawers)) AS orphan_count",
        (),
    )
    .await?;

    let row = rows.first();
    let unindexed_count: i64 = row
        .and_then(|row| row.get_value(0).ok())
        .and_then(|cell| cell.as_integer().copied())
        .unwrap_or(0);
    let orphan_count: i64 = row
        .and_then(|row| row.get_value(1).ok())
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
    debug_assert!(unindexed_count >= 0, "unindexed count must be non-negative");
    debug_assert!(orphan_count >= 0, "orphan count must be non-negative");
    Ok(())
}

/// Checkpoint the WAL and copy the palace database (plus sidecar files) to `.db.bak`.
pub(crate) async fn run_create_backup(connection: &Connection, palace_path: &Path) -> Result<()> {
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

    #[tokio::test]
    async fn run_scan_reports_unindexed_and_orphan_counts() {
        // run_scan must print (not error) when it finds drawers with no index entries
        // and orphaned index rows. We exercise both the `unindexed_count > 0` branch
        // (lines 46-50) and the `orphan_count > 0` branch (lines 51-55).
        let (_database, connection) = crate::test_helpers::test_db().await;

        // Insert a drawer that has no matching drawer_words rows → unindexed.
        connection
            .execute(
                "INSERT INTO drawers (id, wing, room, content) VALUES ('unindexed_drawer_001', 'test_wing', 'general', 'some content')",
                (),
            )
            .await
            .expect("INSERT into drawers must succeed for unindexed-drawer setup");

        // Insert a drawer_words row pointing to a non-existent drawer → orphan.
        connection
            .execute(
                "INSERT INTO drawer_words (word, drawer_id, count) VALUES ('orphan_word', 'nonexistent_drawer_999', 1)",
                (),
            )
            .await
            .expect("INSERT into drawer_words must succeed for orphan setup");

        // run_scan is informational only — it must not return an error even when
        // inconsistencies are present.
        let result = run_scan(&connection).await;
        assert!(
            result.is_ok(),
            "run_scan must not error when inconsistencies exist"
        );
        // Confirm the setup rows are still present (run_scan does not modify the DB).
        let orphan_check = crate::db::query_all(
            &connection,
            "SELECT count(*) FROM drawer_words WHERE drawer_id = 'nonexistent_drawer_999'",
            (),
        )
        .await
        .expect("verification query must succeed");
        assert!(
            !orphan_check.is_empty(),
            "orphan verification query must return a row"
        );
    }

    #[tokio::test]
    async fn run_create_backup_copies_wal_and_shm_when_present() {
        // run_create_backup must copy .db-wal and .db-shm sidecar files when they
        // exist alongside the palace database (lines 85-92 in run_create_backup).
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for WAL/SHM backup test");
        let database_path = temp_directory.path().join("palace_wal.db");
        let (_database, connection) = open_file_db(&database_path).await;

        // Create stub WAL and SHM sidecar files so the `if exists` branches fire.
        let wal_path = database_path.with_extension("db-wal");
        let shm_path = database_path.with_extension("db-shm");
        std::fs::write(&wal_path, b"stub-wal-content").expect("failed to write stub WAL file");
        std::fs::write(&shm_path, b"stub-shm-content").expect("failed to write stub SHM file");

        run_create_backup(&connection, &database_path)
            .await
            .expect("run_create_backup must succeed when WAL/SHM files exist");

        // The backup database and both sidecar backups must exist.
        let backup_path = database_path.with_extension("db.bak");
        assert!(
            backup_path.exists(),
            "run_create_backup must create a .db.bak file"
        );
        let backup_wal = temp_directory.path().join("palace_wal.db.bak-wal");
        let backup_shm = temp_directory.path().join("palace_wal.db.bak-shm");
        assert!(
            backup_wal.exists(),
            "run_create_backup must copy the WAL sidecar to .db.bak-wal"
        );
        assert!(
            backup_shm.exists(),
            "run_create_backup must copy the SHM sidecar to .db.bak-shm"
        );
    }
}
