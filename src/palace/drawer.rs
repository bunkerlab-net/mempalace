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

/// Check if a file has already been mined.
pub async fn file_already_mined(conn: &Connection, source_file: &str) -> Result<bool> {
    let rows = db::query_all(
        conn,
        "SELECT 1 FROM drawers WHERE source_file = ?1 LIMIT 1",
        turso::params![source_file],
    )
    .await?;
    Ok(!rows.is_empty())
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
        };
        let first = add_drawer(&conn, &p).await.expect("first insert");
        assert!(first);
        let second = add_drawer(&conn, &p).await.expect("second insert");
        assert!(!second);
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

    #[tokio::test]
    async fn file_already_mined_returns_correctly() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        assert!(
            !file_already_mined(&conn, "nonexistent.rs")
                .await
                .expect("check")
        );

        conn.execute(
            "INSERT INTO drawers (id, wing, room, content, source_file) VALUES ('fm1', 'w', 'r', 'c', 'exists.rs')",
            (),
        )
        .await
        .expect("insert");

        assert!(file_already_mined(&conn, "exists.rs").await.expect("check"));
    }
}

/// Add a drawer and index its words.
pub async fn add_drawer(conn: &Connection, p: &DrawerParams<'_>) -> Result<bool> {
    // SQLite only has i64 integers, so we cast chunk_index (usize) to i32 at the SQL boundary.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let chunk_index_sql = p.chunk_index as i32;
    let result = conn
        .execute(
            "INSERT OR IGNORE INTO drawers (id, wing, room, content, source_file, chunk_index, added_by, ingest_mode) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            turso::params![p.id, p.wing, p.room, p.content, p.source_file, chunk_index_sql, p.added_by, p.ingest_mode],
        )
        .await?;

    if result > 0 {
        index_words(conn, p.id, p.content).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}
