use std::collections::HashMap;

use turso::Connection;

use crate::db;
use crate::error::Result;

/// Index the words in a drawer's content into the `drawer_words` table.
pub async fn index_words(conn: &Connection, drawer_id: &str, content: &str) -> Result<()> {
    let mut word_counts: HashMap<String, i32> = HashMap::new();
    for word in tokenize(content) {
        *word_counts.entry(word).or_insert(0) += 1;
    }

    for (word, count) in &word_counts {
        conn.execute(
            "INSERT OR IGNORE INTO drawer_words (word, drawer_id, count) VALUES (?1, ?2, ?3)",
            turso::params![word.as_str(), drawer_id, *count],
        )
        .await?;
    }

    Ok(())
}

/// Tokenize text into lowercase words, filtering stop words and short words.
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|w| w.len() >= 3)
        .map(str::to_lowercase)
        .filter(|w| !is_stop_word(w))
        .collect()
}

fn is_stop_word(word: &str) -> bool {
    matches!(
        word,
        "the"
            | "and"
            | "for"
            | "are"
            | "but"
            | "not"
            | "you"
            | "all"
            | "can"
            | "had"
            | "her"
            | "was"
            | "one"
            | "our"
            | "out"
            | "has"
            | "have"
            | "from"
            | "they"
            | "been"
            | "said"
            | "each"
            | "which"
            | "their"
            | "will"
            | "other"
            | "about"
            | "many"
            | "then"
            | "them"
            | "these"
            | "some"
            | "would"
            | "make"
            | "like"
            | "into"
            | "time"
            | "very"
            | "when"
            | "come"
            | "could"
            | "more"
            | "than"
            | "that"
            | "this"
            | "with"
            | "what"
            | "just"
            | "also"
            | "there"
            | "where"
            | "after"
            | "back"
            | "only"
            | "most"
            | "over"
            | "such"
            | "here"
            | "should"
            | "because"
            | "does"
            | "did"
            | "get"
            | "how"
            | "its"
            | "may"
            | "let"
            | "new"
            | "now"
            | "old"
            | "see"
            | "way"
            | "who"
            | "use"
            | "being"
            | "well"
    )
}

/// Return the agreed-upon `source_mtime` for all drawers from `source_file`,
/// or `None` when the file has never been mined or its mtime cannot be trusted.
///
/// A multi-chunk file produces multiple drawer rows.  All chunks written in the
/// same mine pass share an identical mtime, so this function verifies that every
/// row carries the same non-NULL value.  Any NULL or disagreement between rows
/// signals that the data is inconsistent and the file should be re-mined.
async fn stored_mtime(conn: &Connection, source_file: &str) -> Result<Option<f64>> {
    let rows = db::query_all(
        conn,
        "SELECT source_mtime FROM drawers WHERE source_file = ?1",
        turso::params![source_file],
    )
    .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let mut agreed: Option<f64> = None;
    for row in &rows {
        // `get()` returns Err for NULL — map to None and force a re-mine.
        let Some(m): Option<f64> = row.get(0).ok() else {
            return Ok(None);
        };
        // mtime values come from the OS (no floating-point arithmetic), so
        // bitwise equality is safe for detecting inconsistency between rows.
        #[allow(clippy::float_cmp)]
        let disagrees = agreed.is_some_and(|a| a != m);
        if disagrees {
            return Ok(None);
        }
        agreed = Some(m);
    }
    Ok(agreed)
}

/// Return the modification time of a file as seconds since the Unix epoch.
///
/// Returns `None` when the file cannot be stat-ed or the platform does not
/// support modification times.
fn file_mtime(path: &str) -> Option<f64> {
    std::fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs_f64())
}

/// Check whether a file has already been mined *and is unchanged* since it was
/// last filed.
///
/// Returns `false` (triggering a re-mine) when:
/// - No drawer for the file exists.
/// - The drawer was created before `source_mtime` tracking was added (NULL).
/// - The file's current mtime differs from the stored mtime (file was modified).
pub async fn file_already_mined(conn: &Connection, source_file: &str) -> Result<bool> {
    let Some(stored) = stored_mtime(conn, source_file).await? else {
        return Ok(false);
    };

    let Some(current) = file_mtime(source_file) else {
        return Ok(false);
    };

    // Both values are produced by the same OS syscall (stat mtime) converted to
    // f64 via `as_secs_f64()` — bitwise equality is the correct check here.
    // An epsilon comparison would incorrectly treat genuinely-equal timestamps
    // as different.
    #[allow(clippy::float_cmp)]
    Ok(stored == current)
}

/// Parameters for inserting a drawer into the palace.
pub struct DrawerParams<'a> {
    /// Unique drawer identifier.
    pub id: &'a str,
    /// Wing (project namespace).
    pub wing: &'a str,
    /// Room (category within the wing).
    pub room: &'a str,
    /// Text content of the drawer.
    pub content: &'a str,
    /// Path of the original source file.
    pub source_file: &'a str,
    /// Zero-based chunk position within the source file.
    pub chunk_index: usize,
    /// Agent or process that created this drawer.
    pub added_by: &'a str,
    /// Ingestion mode: `"projects"` or `"convos"`.
    pub ingest_mode: &'a str,
    /// Modification time of the source file at mine time (seconds since Unix
    /// epoch).  `None` for drawers that have no on-disk source (e.g. MCP or
    /// conversation imports).
    pub source_mtime: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_splits_and_lowercases() {
        let tokens = tokenize("Hello World! Rust_lang programming");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
        assert!(tokens.contains(&"rust_lang".to_string()));
        assert!(tokens.contains(&"programming".to_string()));
    }

    #[test]
    fn tokenize_filters_short_words() {
        let tokens = tokenize("I am OK hi no");
        // All words are < 3 chars
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenize_filters_stop_words() {
        let tokens = tokenize("the and for are but not you");
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenize_preserves_underscores() {
        let tokens = tokenize("my_variable another_one");
        assert!(tokens.contains(&"my_variable".to_string()));
        assert!(tokens.contains(&"another_one".to_string()));
    }

    #[test]
    fn is_stop_word_known_words() {
        assert!(is_stop_word("the"));
        assert!(is_stop_word("and"));
        assert!(is_stop_word("should"));
        assert!(is_stop_word("because"));
    }

    #[test]
    fn is_stop_word_content_words() {
        assert!(!is_stop_word("rust"));
        assert!(!is_stop_word("database"));
        assert!(!is_stop_word("function"));
    }
}

#[cfg(test)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn add_drawer_inserts_row() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let p = DrawerParams {
            id: "d1",
            wing: "test_wing",
            room: "general",
            content: "hello world from rust programming",
            source_file: "test.rs",
            chunk_index: 0,
            added_by: "test",
            ingest_mode: "projects",
            source_mtime: None,
        };
        let inserted = add_drawer(&conn, &p).await.expect("add_drawer");
        assert!(inserted);

        let rows = crate::db::query_all(
            &conn,
            "SELECT content FROM drawers WHERE id = ?1",
            turso::params!["d1"],
        )
        .await
        .expect("query");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn add_drawer_duplicate_returns_false() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let p = DrawerParams {
            id: "dup1",
            wing: "w",
            room: "r",
            content: "some content here for testing",
            source_file: "f.rs",
            chunk_index: 0,
            added_by: "test",
            ingest_mode: "projects",
            source_mtime: None,
        };
        let first = add_drawer(&conn, &p).await.expect("first insert");
        assert!(first);
        let second = add_drawer(&conn, &p).await.expect("second insert");
        assert!(!second);
    }

    #[tokio::test]
    async fn add_drawer_stores_mtime() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let p = DrawerParams {
            id: "mt1",
            wing: "w",
            room: "r",
            content: "content with mtime",
            source_file: "mtime_test.rs",
            chunk_index: 0,
            added_by: "test",
            ingest_mode: "projects",
            source_mtime: Some(1_700_000_000.5),
        };
        add_drawer(&conn, &p).await.expect("add_drawer");

        let rows = crate::db::query_all(
            &conn,
            "SELECT source_mtime FROM drawers WHERE id = ?1",
            turso::params!["mt1"],
        )
        .await
        .expect("query");
        assert_eq!(rows.len(), 1);
        let stored: Option<f64> = rows[0].get(0).ok();
        assert_eq!(stored, Some(1_700_000_000.5));
    }

    #[tokio::test]
    async fn index_words_creates_entries() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        // Insert a drawer first
        conn.execute(
            "INSERT INTO drawers (id, wing, room, content) VALUES ('iw1', 'w', 'r', 'test')",
            (),
        )
        .await
        .expect("insert drawer");

        index_words(&conn, "iw1", "rust rust programming")
            .await
            .expect("index_words");

        let rows = crate::db::query_all(
            &conn,
            "SELECT word, count FROM drawer_words WHERE drawer_id = ?1 ORDER BY word",
            turso::params!["iw1"],
        )
        .await
        .expect("query");

        // "rust" (count 2) and "programming" (count 1)
        assert_eq!(rows.len(), 2);
    }

    // --- file_already_mined tests ---

    #[tokio::test]
    async fn file_already_mined_no_row_returns_false() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        assert!(
            !file_already_mined(&conn, "nonexistent.rs")
                .await
                .expect("check")
        );
    }

    /// Drawers mined before mtime tracking was added have NULL `source_mtime`.
    /// They must be re-mined so the mtime gets recorded.
    #[tokio::test]
    async fn file_already_mined_null_mtime_returns_false() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        conn.execute(
            "INSERT INTO drawers (id, wing, room, content, source_file) \
             VALUES ('fm_null', 'w', 'r', 'c', 'exists.rs')",
            (),
        )
        .await
        .expect("insert");

        // NULL mtime → treat as not yet mined (forces re-mine to record mtime).
        assert!(!file_already_mined(&conn, "exists.rs").await.expect("check"));
    }

    /// A drawer whose stored mtime matches the file's current mtime is skipped.
    #[tokio::test]
    async fn file_already_mined_matching_mtime_returns_true() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let tmp = tempfile::NamedTempFile::new().expect("tmp file");
        let path = tmp.path().to_string_lossy().to_string();
        let mtime = file_mtime(&path).expect("mtime");

        conn.execute(
            "INSERT INTO drawers (id, wing, room, content, source_file, source_mtime) \
             VALUES ('fm_match', 'w', 'r', 'c', ?1, ?2)",
            turso::params![path.as_str(), mtime],
        )
        .await
        .expect("insert");

        assert!(file_already_mined(&conn, &path).await.expect("check"));
    }

    /// A drawer whose stored mtime differs from the file's current mtime must be
    /// re-mined (the file was modified after it was last filed).
    #[tokio::test]
    async fn file_already_mined_stale_mtime_returns_false() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let tmp = tempfile::NamedTempFile::new().expect("tmp file");
        let path = tmp.path().to_string_lossy().to_string();
        // Store an obviously wrong mtime.
        let stale_mtime: f64 = 0.0;

        conn.execute(
            "INSERT INTO drawers (id, wing, room, content, source_file, source_mtime) \
             VALUES ('fm_stale', 'w', 'r', 'c', ?1, ?2)",
            turso::params![path.as_str(), stale_mtime],
        )
        .await
        .expect("insert");

        assert!(!file_already_mined(&conn, &path).await.expect("check"));
    }

    /// When two chunks of the same file disagree on their stored mtime, the
    /// file must be re-mined (inconsistent state should never happen in
    /// practice but the guard ensures correctness).
    #[tokio::test]
    async fn file_already_mined_disagreeing_mtimes_returns_false() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let tmp = tempfile::NamedTempFile::new().expect("tmp file");
        let path = tmp.path().to_string_lossy().to_string();

        conn.execute(
            "INSERT INTO drawers (id, wing, room, content, source_file, source_mtime) \
             VALUES ('chunk0', 'w', 'r', 'c0', ?1, 1000.0)",
            turso::params![path.as_str()],
        )
        .await
        .expect("insert chunk0");
        conn.execute(
            "INSERT INTO drawers (id, wing, room, content, source_file, source_mtime) \
             VALUES ('chunk1', 'w', 'r', 'c1', ?1, 2000.0)",
            turso::params![path.as_str()],
        )
        .await
        .expect("insert chunk1");

        assert!(
            !file_already_mined(&conn, &path).await.expect("check"),
            "disagreeing mtimes must trigger re-mine"
        );
    }
}

/// Add a drawer and index its words.
///
/// Returns `true` when the drawer was inserted, `false` when a drawer with the
/// same `id` already exists (idempotent — no error is raised).
///
/// The INSERT and word indexing are wrapped in a savepoint so that a failed
/// `index_words` call rolls back the drawer too, leaving no unsearchable
/// orphans.  Savepoints nest correctly if the caller is already inside a
/// transaction.
pub async fn add_drawer(conn: &Connection, p: &DrawerParams<'_>) -> Result<bool> {
    // SQLite only has i64 integers, so we cast chunk_index (usize) to i32 at the SQL boundary.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let chunk_index_sql = p.chunk_index as i32;

    conn.execute("SAVEPOINT add_drawer", ()).await?;

    let rows_affected = conn
        .execute(
            "INSERT OR IGNORE INTO drawers \
             (id, wing, room, content, source_file, chunk_index, added_by, ingest_mode, source_mtime) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            turso::params![
                p.id,
                p.wing,
                p.room,
                p.content,
                p.source_file,
                chunk_index_sql,
                p.added_by,
                p.ingest_mode,
                p.source_mtime
            ],
        )
        .await;

    let rows_affected = match rows_affected {
        Ok(n) => n,
        Err(e) => {
            let _ = conn.execute("ROLLBACK TO SAVEPOINT add_drawer", ()).await;
            let _ = conn.execute("RELEASE SAVEPOINT add_drawer", ()).await;
            return Err(e.into());
        }
    };

    if rows_affected == 0 {
        // Already exists — nothing was written; release the savepoint and report.
        conn.execute("RELEASE SAVEPOINT add_drawer", ()).await?;
        return Ok(false);
    }

    match index_words(conn, p.id, p.content).await {
        Ok(()) => {
            conn.execute("RELEASE SAVEPOINT add_drawer", ()).await?;
            Ok(true)
        }
        Err(e) => {
            let _ = conn.execute("ROLLBACK TO SAVEPOINT add_drawer", ()).await;
            let _ = conn.execute("RELEASE SAVEPOINT add_drawer", ()).await;
            Err(e)
        }
    }
}
