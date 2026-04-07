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
";

/// Create all tables and indexes if they don't already exist.
pub async fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA).await?;
    Ok(())
}
