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
    /// ISO-8601 timestamp when this drawer was filed; empty string if not recorded.
    pub created_at: String,
}

/// Search the palace using the inverted index (keyword matching with relevance scoring).
pub async fn search_memories(
    connection: &Connection,
    query: &str,
    wing: Option<&str>,
    room: Option<&str>,
    n_results: usize,
) -> Result<Vec<SearchResult>> {
    assert!(n_results > 0, "n_results must be positive");

    let words = tokenize_query(query);
    if words.is_empty() {
        return Ok(vec![]);
    }

    // turso does not support dynamic-length prepared statements, so the SQL
    // is built at call time. The word list comes from tokenize_query, which
    // only produces lowercase alphanumeric tokens, so no sanitization is needed.
    let placeholders: Vec<String> = (1..=words.len()).map(|i| format!("?{i}")).collect();
    let in_clause = placeholders.join(", ");

    // Wing and room filters are appended after the word placeholders, so their
    // parameter indices must be offset past the word count.
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
        "SELECT d.id, d.content, d.wing, d.room, d.source_file, SUM(dw.count) as relevance, d.filed_at \
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
    // SQLite LIMIT expects a signed integer. Callers are unlikely to request
    // more than i32::MAX results, but we saturate rather than panic.
    let n_results_i32 = i32::try_from(n_results).unwrap_or(i32::MAX);
    params.push(turso::Value::from(n_results_i32));

    let rows = db::query_all(connection, &sql, turso::params_from_iter(params)).await?;
    let results = search_memories_parse_rows(&rows);

    // Postcondition: result count bounded by the SQL LIMIT.
    debug_assert!(results.len() <= n_results);

    Ok(results)
}

/// Map query result rows (columns: id, content, wing, room, `source_file`, relevance, `filed_at`)
/// into `SearchResult` values.
fn search_memories_parse_rows(rows: &[turso::Row]) -> Vec<SearchResult> {
    let mut results = Vec::new();
    for row in rows {
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
        let created_at = row
            .get_value(6)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();

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
            created_at,
        });
    }
    results
}

/// Tokenize a query string into searchable words.
///
/// The minimum length of 3 matches the indexing threshold in `index_words`:
/// shorter tokens (single letters, two-letter words) are almost always noise
/// and would fan out to enormous result sets, hurting relevance.
fn tokenize_query(query: &str) -> Vec<String> {
    query
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|w| w.len() >= 3)
        .map(str::to_lowercase)
        .filter(|w| !crate::palace::drawer::is_stop_word(w))
        .collect()
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
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
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod async_tests {
    use super::*;

    async fn seed_drawers(connection: &Connection) {
        // Insert two drawers with indexed words
        crate::palace::drawer::add_drawer(
            connection,
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
        .expect("add_drawer should succeed when seeding test drawer s1 (rust/project_a)");

        crate::palace::drawer::add_drawer(
            connection,
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
        .expect("add_drawer should succeed when seeding test drawer s2 (react/project_b)");
    }

    #[tokio::test]
    async fn search_single_word() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_drawers(&connection).await;
        let results = search_memories(&connection, "rust", None, None, 10)
            .await
            .expect("search_memories should not error when searching for 'rust'");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].wing, "project_a");
        assert_eq!(results[0].source_file, "main.rs");
        assert!(results[0].relevance > 0.0);
    }

    #[tokio::test]
    async fn search_multi_word_relevance() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_drawers(&connection).await;
        // "programming" appears in both, but searching "rust programming" should rank s1 higher
        let results = search_memories(&connection, "rust programming", None, None, 10)
            .await
            .expect("search_memories should not error when searching for 'rust programming'");
        assert!(!results.is_empty());
        assert_eq!(results[0].wing, "project_a");
        assert_eq!(results[0].source_file, "main.rs");
        assert!(results[0].relevance > 0.0);
    }

    #[tokio::test]
    async fn search_with_wing_filter() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_drawers(&connection).await;
        let results = search_memories(&connection, "programming", Some("project_b"), None, 10)
            .await
            .expect("search_memories should not error when filtering by wing 'project_b'");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].room, "frontend");
        assert_eq!(results[0].source_file, "app.tsx");
        assert!(results[0].relevance > 0.0);
    }

    #[tokio::test]
    async fn search_with_room_filter() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_drawers(&connection).await;
        let results = search_memories(&connection, "programming", None, Some("backend"), 10)
            .await
            .expect("search_memories should not error when filtering by room 'backend'");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].wing, "project_a");
        assert_eq!(results[0].room, "backend");
        assert_eq!(results[0].source_file, "main.rs");
        assert!(results[0].relevance > 0.0);
    }

    #[tokio::test]
    async fn search_no_results() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_drawers(&connection).await;
        let results = search_memories(&connection, "elephant", None, None, 10)
            .await
            .expect("search_memories should not error when query matches no drawers");
        assert!(results.is_empty());
    }
}
