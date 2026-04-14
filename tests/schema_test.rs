// Integration test — .expect() is acceptable with a descriptive message.
// Integration test functions are allowed to be long for narrative clarity.
#![allow(clippy::expect_used, clippy::too_many_lines)]

/// Verify full schema creation works.
#[tokio::test]
async fn test_schema_creation() {
    let db = turso::Builder::new_local(":memory:")
        .build()
        .await
        .expect("failed to create db");

    let conn = db.connect().expect("failed to connect");

    // Run the schema SQL (copied from schema.rs since we can't import bin crate in integration tests)
    conn.execute_batch(
        r"
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
            filed_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_drawers_wing ON drawers(wing);
        CREATE INDEX IF NOT EXISTS idx_drawers_room ON drawers(room);
        CREATE INDEX IF NOT EXISTS idx_drawers_wing_room ON drawers(wing, room);
        CREATE INDEX IF NOT EXISTS idx_drawers_source ON drawers(source_file);
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
    ",
    )
    .await
    .expect("schema creation failed");

    // Verify tables exist
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            (),
        )
        .await
        .expect("failed to query tables");

    let mut tables = Vec::new();
    while let Some(row) = rows.next().await.expect("failed to advance rows") {
        tables.push(
            row.get_value(0)
                .expect("missing column 0")
                .as_text()
                .expect("column 0 not text")
                .clone(),
        );
    }

    assert!(tables.contains(&"drawers".to_string()));
    assert!(tables.contains(&"drawer_words".to_string()));
    assert!(tables.contains(&"entities".to_string()));
    assert!(tables.contains(&"triples".to_string()));
    assert!(tables.contains(&"compressed".to_string()));

    println!("All tables created: {tables:?}");
}
