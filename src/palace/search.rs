use std::fmt::Write as _;
use std::path::Path;

use turso::Connection;

use crate::db;
use crate::error::Result;

/// A single search result from the inverted index.
pub struct SearchResult {
    /// The drawer's text content.
    pub text: String,
    /// The wing (project namespace) this drawer belongs to.
    pub wing: String,
    /// The room (category) this drawer belongs to.
    pub room: String,
    /// Original source filename (basename only).
    pub source_file: String,
    /// Relevance score — sum of matched word counts.
    pub relevance: f64,
}

/// Search the palace using the inverted index (keyword matching with relevance scoring).
pub async fn search_memories(
    conn: &Connection,
    query: &str,
    wing: Option<&str>,
    room: Option<&str>,
    n_results: usize,
) -> Result<Vec<SearchResult>> {
    let words = tokenize_query(query);
    if words.is_empty() {
        return Ok(vec![]);
    }

    // Build placeholders for IN clause
    let placeholders: Vec<String> = (1..=words.len()).map(|i| format!("?{i}")).collect();
    let in_clause = placeholders.join(", ");

    // Build optional wing/room filters
    let mut filters = String::new();
    let mut param_offset = words.len();
    if wing.is_some() {
        param_offset += 1;
        let _ = write!(filters, " AND d.wing = ?{param_offset}");
    }
    if room.is_some() {
        param_offset += 1;
        let _ = write!(filters, " AND d.room = ?{param_offset}");
    }

    let sql = format!(
        "SELECT d.id, d.content, d.wing, d.room, d.source_file, SUM(dw.count) as relevance \
         FROM drawers d \
         JOIN drawer_words dw ON d.id = dw.drawer_id \
         WHERE dw.word IN ({in_clause}){filters} \
         GROUP BY d.id \
         ORDER BY relevance DESC \
         LIMIT ?{}",
        param_offset + 1
    );

    // Build params
    let mut params: Vec<turso::Value> = words
        .iter()
        .map(|w| turso::Value::from(w.as_str()))
        .collect();
    if let Some(w) = wing {
        params.push(turso::Value::from(w));
    }
    if let Some(r) = room {
        params.push(turso::Value::from(r));
    }
    let n_results_i32 = i32::try_from(n_results).unwrap_or(i32::MAX);
    params.push(turso::Value::from(n_results_i32));

    let rows = db::query_all(conn, &sql, turso::params_from_iter(params)).await?;

    let mut results = Vec::new();
    for row in &rows {
        let text = row
            .get_value(1)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let wing = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let room = row
            .get_value(3)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let source = row
            .get_value(4)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let raw_relevance = row
            .get_value(5)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .unwrap_or(0);
        let relevance = f64::from(i32::try_from(raw_relevance).unwrap_or(i32::MAX));

        let source_name = Path::new(&source)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        results.push(SearchResult {
            text,
            wing,
            room,
            source_file: source_name,
            relevance,
        });
    }

    Ok(results)
}

/// Tokenize a query string into searchable words.
fn tokenize_query(query: &str) -> Vec<String> {
    query
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|w| w.len() >= 3)
        .map(str::to_lowercase)
        .filter(|w| !is_stop_word(w))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_query_basic() {
        let tokens = tokenize_query("rust programming language");
        assert!(tokens.contains(&"rust".to_string()));
        assert!(tokens.contains(&"programming".to_string()));
        assert!(tokens.contains(&"language".to_string()));
    }

    #[test]
    fn tokenize_query_filters_stop_words() {
        let tokens = tokenize_query("the and for");
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenize_query_empty_input() {
        assert!(tokenize_query("").is_empty());
        assert!(tokenize_query("   ").is_empty());
    }

    #[test]
    fn tokenize_query_mixed_content_and_stop_words() {
        let tokens = tokenize_query("the quick brown fox");
        // "the" is stop word, "fox" is < 3? no it's 3 chars so it passes
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
        assert!(tokens.contains(&"fox".to_string()));
    }
}

#[cfg(test)]
mod async_tests {
    use super::*;

    async fn seed_drawers(conn: &Connection) {
        // Insert two drawers with indexed words
        crate::palace::drawer::add_drawer(
            conn,
            &crate::palace::drawer::DrawerParams {
                id: "s1",
                wing: "project_a",
                room: "backend",
                content: "rust programming language is fast and safe",
                source_file: "main.rs",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("seed s1");

        crate::palace::drawer::add_drawer(
            conn,
            &crate::palace::drawer::DrawerParams {
                id: "s2",
                wing: "project_b",
                room: "frontend",
                content: "react programming framework with components",
                source_file: "app.tsx",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("seed s2");
    }

    #[tokio::test]
    async fn search_single_word() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_drawers(&conn).await;
        let results = search_memories(&conn, "rust", None, None, 10)
            .await
            .expect("search");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].wing, "project_a");
        assert_eq!(results[0].source_file, "main.rs");
        assert!(results[0].relevance > 0.0);
    }

    #[tokio::test]
    async fn search_multi_word_relevance() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_drawers(&conn).await;
        // "programming" appears in both, but searching "rust programming" should rank s1 higher
        let results = search_memories(&conn, "rust programming", None, None, 10)
            .await
            .expect("search");
        assert!(!results.is_empty());
        assert_eq!(results[0].wing, "project_a");
        assert_eq!(results[0].source_file, "main.rs");
        assert!(results[0].relevance > 0.0);
    }

    #[tokio::test]
    async fn search_with_wing_filter() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_drawers(&conn).await;
        let results = search_memories(&conn, "programming", Some("project_b"), None, 10)
            .await
            .expect("search");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].room, "frontend");
        assert_eq!(results[0].source_file, "app.tsx");
        assert!(results[0].relevance > 0.0);
    }

    #[tokio::test]
    async fn search_with_room_filter() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_drawers(&conn).await;
        let results = search_memories(&conn, "programming", None, Some("backend"), 10)
            .await
            .expect("search");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].wing, "project_a");
        assert_eq!(results[0].room, "backend");
        assert_eq!(results[0].source_file, "main.rs");
        assert!(results[0].relevance > 0.0);
    }

    #[tokio::test]
    async fn search_no_results() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_drawers(&conn).await;
        let results = search_memories(&conn, "elephant", None, None, 10)
            .await
            .expect("search");
        assert!(results.is_empty());
    }
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
