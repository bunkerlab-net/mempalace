//! `SQLite` schema DDL — creates the five core tables and their indexes.

use turso::Connection;

use crate::error::Result;

const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS drawers (
    id TEXT PRIMARY KEY,
    wing TEXT NOT NULL,
    room TEXT NOT NULL,
    content TEXT NOT NULL,
    source_file TEXT,
    chunk_index INTEGER DEFAULT 0,
    added_by TEXT DEFAULT 'mempalace',
    ingest_mode TEXT,
    extract_mode TEXT,
    source_mtime REAL,
    filed_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_drawers_wing ON drawers(wing);
CREATE INDEX IF NOT EXISTS idx_drawers_room ON drawers(room);
CREATE INDEX IF NOT EXISTS idx_drawers_wing_room ON drawers(wing, room);
CREATE INDEX IF NOT EXISTS idx_drawers_source ON drawers(source_file);

-- Inverted index for keyword search (replaces FTS5 which turso doesn't support)
CREATE TABLE IF NOT EXISTS drawer_words (
    word TEXT NOT NULL,
    drawer_id TEXT NOT NULL,
    count INTEGER DEFAULT 1,
    PRIMARY KEY (word, drawer_id)
);

CREATE INDEX IF NOT EXISTS idx_words_word ON drawer_words(word);
CREATE INDEX IF NOT EXISTS idx_words_drawer ON drawer_words(drawer_id);

CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT DEFAULT 'unknown',
    properties TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS triples (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    confidence REAL DEFAULT 1.0,
    source_closet TEXT,
    source_file TEXT,
    extracted_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_triples_subject ON triples(subject);
CREATE INDEX IF NOT EXISTS idx_triples_object ON triples(object);
CREATE INDEX IF NOT EXISTS idx_triples_predicate ON triples(predicate);

CREATE TABLE IF NOT EXISTS compressed (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    compression_ratio REAL,
    wing TEXT,
    room TEXT,
    filed_at TEXT DEFAULT (datetime('now'))
);

-- Explicit cross-wing tunnels created by agents when they notice a connection
-- between two specific rooms in different wings/projects.  Stored in SQLite
-- rather than a JSON file so they survive palace rebuilds only if the file
-- is preserved — but the DB is always available without extra state.
CREATE TABLE IF NOT EXISTS explicit_tunnels (
    id TEXT PRIMARY KEY,
    source_wing TEXT NOT NULL,
    source_room TEXT NOT NULL,
    target_wing TEXT NOT NULL,
    target_room TEXT NOT NULL,
    source_drawer_id TEXT,
    target_drawer_id TEXT,
    label TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_tunnels_source ON explicit_tunnels(source_wing, source_room);
CREATE INDEX IF NOT EXISTS idx_tunnels_target ON explicit_tunnels(target_wing, target_room);
";

/// Create all tables and indexes if they don't already exist.
///
/// Also runs lightweight migrations for existing databases (columns that were
/// added after initial release).  Each migration is expected to be idempotent —
/// `SQLite` returns an error when a column already exists, which we deliberately
/// ignore.
pub async fn ensure_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(SCHEMA).await?;

    // Migration: add source_mtime column (introduced to support re-mining
    // modified files).  Silently ignored for databases that already have it.
    let _ = connection
        .execute("ALTER TABLE drawers ADD COLUMN source_mtime REAL", ())
        .await;

    // Pair assertion: verify all six core tables were created.
    let rows = crate::db::query_all(
        connection,
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
        (),
    )
    .await?;
    let table_names: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get::<String>(0).ok())
        .collect();
    assert!(
        table_names.contains(&"drawers".to_string()),
        "drawers table must exist"
    );
    assert!(
        table_names.contains(&"drawer_words".to_string()),
        "drawer_words table must exist"
    );
    assert!(
        table_names.contains(&"entities".to_string()),
        "entities table must exist"
    );
    assert!(
        table_names.contains(&"triples".to_string()),
        "triples table must exist"
    );
    assert!(
        table_names.contains(&"compressed".to_string()),
        "compressed table must exist"
    );
    assert!(
        table_names.contains(&"explicit_tunnels".to_string()),
        "explicit_tunnels table must exist"
    );

    Ok(())
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const EXPECTED_TABLES: &[&str] = &[
        "compressed",
        "drawer_words",
        "drawers",
        "entities",
        "explicit_tunnels",
        "triples",
    ];

    #[tokio::test]
    async fn ensure_schema_creates_all_tables() {
        let (_db, conn) = crate::test_helpers::test_db().await;

        let rows = crate::db::query_all(
            &conn,
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
            (),
        )
        .await
        .expect("sqlite_master query should succeed after ensure_schema");

        let table_names: Vec<String> = rows
            .iter()
            .filter_map(|r| r.get::<String>(0).ok())
            .collect();

        // All 6 core tables must be present.
        for &expected in EXPECTED_TABLES {
            assert!(
                table_names.contains(&expected.to_string()),
                "table '{expected}' must exist after ensure_schema"
            );
        }
        assert!(
            table_names.len() >= EXPECTED_TABLES.len(),
            "at least {} tables should exist, found {}",
            EXPECTED_TABLES.len(),
            table_names.len()
        );
    }

    #[tokio::test]
    async fn ensure_schema_idempotent() {
        let (_db, conn) = crate::test_helpers::test_db().await;

        // Record the table count after the first ensure_schema (called by test_db).
        let count_query = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'";
        let rows_before = crate::db::query_all(&conn, count_query, ())
            .await
            .expect("table count query before second call should succeed");
        let count_before: i64 = rows_before[0]
            .get(0)
            .expect("COUNT(*) column 0 should be readable as i64");
        let expected_table_count =
            i64::try_from(EXPECTED_TABLES.len()).expect("table count fits in i64");
        assert!(
            count_before >= expected_table_count,
            "at least {} core tables should exist before second call, got {count_before}",
            EXPECTED_TABLES.len()
        );

        // Call ensure_schema a second time — must not add tables.
        ensure_schema(&conn)
            .await
            .expect("second ensure_schema call should succeed (idempotent)");

        let rows_after = crate::db::query_all(&conn, count_query, ())
            .await
            .expect("table count query after second call should succeed");
        let count_after: i64 = rows_after[0]
            .get(0)
            .expect("COUNT(*) column 0 should be readable as i64");

        assert_eq!(
            count_before, count_after,
            "ensure_schema must not add or remove tables on second call"
        );
    }
}
