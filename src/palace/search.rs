use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;

use turso::Connection;

use crate::db;
use crate::error::Result;

/// BM25 saturation parameter: controls how quickly TF saturates (k1=1.5 is standard).
const BM25_K1: f64 = 1.5;
/// BM25 length normalisation parameter (b=0.75 is standard).
const BM25_B: f64 = 0.75;
/// Overfetch multiplier: candidate pool = `n_results` × `BM25_OVERFETCH` before BM25 re-rank.
const BM25_OVERFETCH: usize = 3;

const _: () = assert!(BM25_K1 > 0.0);
const _: () = assert!(BM25_B > 0.0 && BM25_B < 1.0);
const _: () = assert!(BM25_OVERFETCH >= 1);

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
    /// Full source path as stored in the drawers table.
    pub source_path: String,
    /// Chunk index within the source file (0-based).
    pub chunk_index: i32,
    /// BM25 relevance score.
    pub relevance: f64,
    /// ISO-8601 timestamp when this drawer was filed; empty string if not recorded.
    pub created_at: String,
}

/// Internal candidate row from the first-pass query.
struct Candidate {
    id: String,
    text: String,
    wing: String,
    room: String,
    source_path: String,
    chunk_index: i32,
    created_at: String,
}

/// Search the palace using BM25-ranked inverted-index search.
///
/// Two-pass approach: the first pass selects up to `n_results × BM25_OVERFETCH`
/// candidates by raw TF sum; the second pass re-ranks them by BM25 score.
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

    let n_candidates = n_results.saturating_mul(BM25_OVERFETCH).max(n_results);
    let candidates =
        search_memories_candidates(connection, &words, wing, room, n_candidates).await?;

    if candidates.is_empty() {
        return Ok(vec![]);
    }

    let candidate_ids: Vec<String> = candidates.iter().map(|c| c.id.clone()).collect();
    let (tf_data, doc_lengths) = tokio::try_join!(
        search_memories_tf_data(connection, &candidate_ids, &words),
        search_memories_doc_lengths(connection, &candidate_ids),
    )?;

    let results =
        search_memories_compute_bm25(candidates, &tf_data, &doc_lengths, &words, n_results);

    // Postcondition: result count bounded by the requested limit.
    debug_assert!(results.len() <= n_results);

    Ok(results)
}

/// Fetch adjacent chunks from the same source file for context expansion.
///
/// Returns drawers whose `source_file` matches `source_path` and whose
/// `chunk_index` falls within `[chunk_index - radius, chunk_index + radius]`,
/// excluding the anchor chunk itself. Results are ordered by `chunk_index`.
pub async fn search_expand_neighbors(
    connection: &Connection,
    source_path: &str,
    chunk_index: i32,
    radius: i32,
) -> Result<Vec<SearchResult>> {
    assert!(!source_path.is_empty(), "source_path must not be empty");
    assert!(radius > 0, "radius must be positive");

    let min_chunk = chunk_index.saturating_sub(radius);
    let max_chunk = chunk_index.saturating_add(radius);
    assert!(min_chunk <= max_chunk);

    let sql = "SELECT id, content, wing, room, source_file, chunk_index, filed_at \
               FROM drawers \
               WHERE source_file = ?1 AND chunk_index BETWEEN ?2 AND ?3 \
               AND chunk_index != ?4 \
               ORDER BY chunk_index ASC";

    let rows = db::query_all(
        connection,
        sql,
        turso::params![source_path, min_chunk, max_chunk, chunk_index],
    )
    .await?;

    Ok(search_expand_parse_rows(&rows, source_path))
}

/// First-pass query: select top `n_candidates` drawers by raw TF sum.
async fn search_memories_candidates(
    connection: &Connection,
    words: &[String],
    wing: Option<&str>,
    room: Option<&str>,
    n_candidates: usize,
) -> Result<Vec<Candidate>> {
    assert!(!words.is_empty());
    assert!(n_candidates > 0);

    let placeholders: Vec<String> = (1..=words.len()).map(|i| format!("?{i}")).collect();
    let in_clause = placeholders.join(", ");

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
        "SELECT d.id, d.content, d.wing, d.room, d.source_file, d.chunk_index, d.filed_at \
         FROM drawers d \
         JOIN drawer_words dw ON d.id = dw.drawer_id \
         WHERE dw.word IN ({in_clause}){filters} \
         GROUP BY d.id \
         ORDER BY SUM(dw.count) DESC \
         LIMIT ?{}",
        param_offset + 1
    );

    let mut params: Vec<turso::Value> = words
        .iter()
        .map(|w| turso::Value::from(w.as_str()))
        .collect();
    if let Some(w) = wing {
        params.push(turso::Value::from(w));
    }
    if let Some(room_filter) = room {
        params.push(turso::Value::from(room_filter));
    }
    let n_i32 = i32::try_from(n_candidates).unwrap_or(i32::MAX);
    params.push(turso::Value::from(n_i32));

    let rows = db::query_all(connection, &sql, turso::params_from_iter(params)).await?;
    Ok(search_memories_parse_candidates(&rows))
}

/// Second-pass query: per-term TF for the candidate set.
///
/// Returns `drawer_id → word → count` nested map.
async fn search_memories_tf_data(
    connection: &Connection,
    candidate_ids: &[String],
    words: &[String],
) -> Result<HashMap<String, HashMap<String, i64>>> {
    assert!(!candidate_ids.is_empty());
    assert!(!words.is_empty());

    let id_phs: Vec<String> = (1..=candidate_ids.len()).map(|i| format!("?{i}")).collect();
    let word_offset = candidate_ids.len();
    let word_phs: Vec<String> = (word_offset + 1..=word_offset + words.len())
        .map(|i| format!("?{i}"))
        .collect();

    let sql = format!(
        "SELECT drawer_id, word, count FROM drawer_words \
         WHERE drawer_id IN ({}) AND word IN ({})",
        id_phs.join(", "),
        word_phs.join(", ")
    );

    let mut params: Vec<turso::Value> = candidate_ids
        .iter()
        .map(|id| turso::Value::from(id.as_str()))
        .collect();
    params.extend(words.iter().map(|w| turso::Value::from(w.as_str())));

    let rows = db::query_all(connection, &sql, turso::params_from_iter(params)).await?;

    let mut tf_map: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for row in &rows {
        let drawer_id = row
            .get_value(0)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let word = row
            .get_value(1)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let count = row
            .get_value(2)
            .ok()
            .and_then(|cell| cell.as_integer().copied())
            .unwrap_or(0);
        tf_map.entry(drawer_id).or_default().insert(word, count);
    }
    Ok(tf_map)
}

/// Document-length query: total word count per candidate.
///
/// Returns `drawer_id → doc_len` map.
async fn search_memories_doc_lengths(
    connection: &Connection,
    candidate_ids: &[String],
) -> Result<HashMap<String, i64>> {
    assert!(!candidate_ids.is_empty());

    let placeholders: Vec<String> = (1..=candidate_ids.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT drawer_id, SUM(count) as doc_len FROM drawer_words \
         WHERE drawer_id IN ({}) GROUP BY drawer_id",
        placeholders.join(", ")
    );
    let params: Vec<turso::Value> = candidate_ids
        .iter()
        .map(|id| turso::Value::from(id.as_str()))
        .collect();

    let rows = db::query_all(connection, &sql, turso::params_from_iter(params)).await?;

    let mut doc_lengths: HashMap<String, i64> = HashMap::new();
    for row in &rows {
        let drawer_id = row
            .get_value(0)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let doc_len = row
            .get_value(1)
            .ok()
            .and_then(|cell| cell.as_integer().copied())
            .unwrap_or(0);
        doc_lengths.insert(drawer_id, doc_len);
    }

    // Postcondition: every candidate must have a length entry (defaulting to 0 is safe).
    debug_assert!(
        candidate_ids
            .iter()
            .all(|id| doc_lengths.contains_key(id.as_str()))
    );
    Ok(doc_lengths)
}

/// Re-rank candidates by BM25 score and return the top `n_results`.
fn search_memories_compute_bm25(
    candidates: Vec<Candidate>,
    tf_data: &HashMap<String, HashMap<String, i64>>,
    doc_lengths: &HashMap<String, i64>,
    words: &[String],
    n_results: usize,
) -> Vec<SearchResult> {
    assert!(n_results > 0);

    if candidates.is_empty() {
        return vec![];
    }

    let idf = search_memories_compute_idf(&candidates, tf_data, words);
    let avgdl = search_memories_avgdl(&candidates, doc_lengths);

    let mut scored: Vec<(f64, Candidate)> = candidates
        .into_iter()
        .map(|c| {
            let score = search_memories_score_one(&c, tf_data, doc_lengths, words, &idf, avgdl);
            (score, c)
        })
        .collect();

    // Stable descending sort so equal scores preserve insertion order.
    scored.sort_by(|(a, _), (b, _)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(n_results);

    scored
        .into_iter()
        .map(|(score, c)| search_memories_to_result(score, c))
        .collect()
}

/// Compute candidate-scoped IDF for each query term.
// BM25 IDF requires casting candidate counts (usize) and df (usize) to f64.
// Precision loss is acceptable: candidate pools never approach 2^53.
#[allow(clippy::cast_precision_loss)]
fn search_memories_compute_idf(
    candidates: &[Candidate],
    tf_data: &HashMap<String, HashMap<String, i64>>,
    words: &[String],
) -> HashMap<String, f64> {
    assert!(!candidates.is_empty());
    let total = candidates.len() as f64;
    words
        .iter()
        .map(|word| {
            let df = candidates
                .iter()
                .filter(|c| tf_data.get(&c.id).and_then(|m| m.get(word)).is_some())
                .count() as f64;
            // Robertson-Sparck Jones IDF with smoothing to avoid negative values.
            let idf = if df > 0.0 {
                ((total - df + 0.5) / (df + 0.5) + 1.0).ln()
            } else {
                0.0
            };
            (word.clone(), idf)
        })
        .collect()
}

/// Average document length across the candidate set.
// i64 and usize to f64 casts are for BM25 length normalisation.
// Precision loss is acceptable; drawers never accumulate 2^53 word-count tokens.
#[allow(clippy::cast_precision_loss)]
fn search_memories_avgdl(candidates: &[Candidate], doc_lengths: &HashMap<String, i64>) -> f64 {
    assert!(!candidates.is_empty());
    let total_len: i64 = candidates
        .iter()
        .map(|c| doc_lengths.get(&c.id).copied().unwrap_or(1))
        .sum();
    // candidates.len() > 0 guaranteed by the assert above.
    total_len as f64 / candidates.len() as f64
}

/// BM25 score for a single candidate.
// i64 doc_len and tf casts to f64 are for BM25 formula; values never exceed 2^53.
#[allow(clippy::cast_precision_loss)]
fn search_memories_score_one(
    candidate: &Candidate,
    tf_data: &HashMap<String, HashMap<String, i64>>,
    doc_lengths: &HashMap<String, i64>,
    words: &[String],
    idf: &HashMap<String, f64>,
    avgdl: f64,
) -> f64 {
    assert!(avgdl > 0.0, "avgdl must be positive");
    let doc_len = doc_lengths.get(&candidate.id).copied().unwrap_or(1) as f64;
    let term_tfs = tf_data.get(&candidate.id);

    words
        .iter()
        .map(|word| {
            let tf = term_tfs.and_then(|m| m.get(word)).copied().unwrap_or(0) as f64;
            let idf_score = idf.get(word).copied().unwrap_or(0.0);
            let length_norm = 1.0 - BM25_B + BM25_B * doc_len / avgdl;
            let denominator = tf + BM25_K1 * length_norm;
            if denominator == 0.0 {
                0.0
            } else {
                idf_score * tf * (BM25_K1 + 1.0) / denominator
            }
        })
        .sum()
}

/// Convert a `(score, Candidate)` pair to a `SearchResult`.
fn search_memories_to_result(score: f64, candidate: Candidate) -> SearchResult {
    let source_file = Path::new(&candidate.source_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    assert!(!candidate.id.is_empty());
    SearchResult {
        text: candidate.text,
        wing: candidate.wing,
        room: candidate.room,
        source_file,
        source_path: candidate.source_path,
        chunk_index: candidate.chunk_index,
        relevance: score,
        created_at: candidate.created_at,
    }
}

/// Parse candidate rows (columns: id, content, wing, room, `source_file`, `chunk_index`, `filed_at`).
fn search_memories_parse_candidates(rows: &[turso::Row]) -> Vec<Candidate> {
    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row
            .get_value(0)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let text = row
            .get_value(1)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let wing = row
            .get_value(2)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let room = row
            .get_value(3)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let source_path = row
            .get_value(4)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let chunk_index = row
            .get_value(5)
            .ok()
            .and_then(|cell| cell.as_integer().copied())
            .and_then(|int_val| i32::try_from(int_val).ok())
            .unwrap_or(0);
        let created_at = row
            .get_value(6)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        if !id.is_empty() {
            candidates.push(Candidate {
                id,
                text,
                wing,
                room,
                source_path,
                chunk_index,
                created_at,
            });
        }
    }
    candidates
}

/// Parse neighbor expansion rows into `SearchResult` values with zero relevance score.
///
/// The `source_path` argument is the path from the anchor drawer used as the WHERE filter;
/// it fills `source_path` when the column itself is empty (shouldn't happen, but defensive).
fn search_expand_parse_rows(rows: &[turso::Row], source_path: &str) -> Vec<SearchResult> {
    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let text = row
            .get_value(1)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let wing = row
            .get_value(2)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let room = row
            .get_value(3)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let stored_path = row
            .get_value(4)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let chunk_index = row
            .get_value(5)
            .ok()
            .and_then(|cell| cell.as_integer().copied())
            .and_then(|int_val| i32::try_from(int_val).ok())
            .unwrap_or(0);
        let created_at = row
            .get_value(6)
            .ok()
            .and_then(|cell| cell.as_text().cloned())
            .unwrap_or_default();
        let effective_path = if stored_path.is_empty() {
            source_path.to_string()
        } else {
            stored_path
        };
        let source_file = Path::new(&effective_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        results.push(SearchResult {
            text,
            wing,
            room,
            source_file,
            source_path: effective_path,
            chunk_index,
            relevance: 0.0,
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
        .split(|character: char| !character.is_alphanumeric() && character != '_')
        .filter(|token| token.len() >= 3)
        .map(str::to_lowercase)
        .filter(|token| !crate::palace::drawer::is_stop_word(token))
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
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
        assert!(tokens.contains(&"fox".to_string()));
    }

    #[test]
    fn search_memories_compute_idf_unique_term_scores_higher() {
        // A term present in only 1 of N candidates has higher IDF than one in all N.
        let candidates = vec![
            Candidate {
                id: "a".into(),
                text: String::new(),
                wing: String::new(),
                room: String::new(),
                source_path: String::new(),
                chunk_index: 0,
                created_at: String::new(),
            },
            Candidate {
                id: "b".into(),
                text: String::new(),
                wing: String::new(),
                room: String::new(),
                source_path: String::new(),
                chunk_index: 0,
                created_at: String::new(),
            },
        ];
        let mut tf_data: HashMap<String, HashMap<String, i64>> = HashMap::new();
        tf_data
            .entry("a".into())
            .or_default()
            .insert("unique".into(), 1);
        tf_data
            .entry("a".into())
            .or_default()
            .insert("common".into(), 1);
        tf_data
            .entry("b".into())
            .or_default()
            .insert("common".into(), 1);

        let words = vec!["unique".to_string(), "common".to_string()];
        let idf = search_memories_compute_idf(&candidates, &tf_data, &words);

        assert!(
            idf["unique"] > idf["common"],
            "unique term must have higher IDF"
        );
        assert!(idf["common"] >= 0.0, "common term IDF must be non-negative");
    }

    #[test]
    fn search_memories_avgdl_correct() {
        // Two candidates: lengths 10 and 20 → avgdl = 15.
        let candidates = vec![
            Candidate {
                id: "x".into(),
                text: String::new(),
                wing: String::new(),
                room: String::new(),
                source_path: String::new(),
                chunk_index: 0,
                created_at: String::new(),
            },
            Candidate {
                id: "y".into(),
                text: String::new(),
                wing: String::new(),
                room: String::new(),
                source_path: String::new(),
                chunk_index: 0,
                created_at: String::new(),
            },
        ];
        let mut doc_lengths = HashMap::new();
        doc_lengths.insert("x".to_string(), 10_i64);
        doc_lengths.insert("y".to_string(), 20_i64);
        let avgdl = search_memories_avgdl(&candidates, &doc_lengths);
        assert!(
            (avgdl - 15.0).abs() < 1e-9,
            "avgdl must equal 15.0 for lengths 10 and 20"
        );
        assert!(avgdl > 0.0);
    }
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod async_tests {
    use super::*;

    async fn seed_drawers(connection: &Connection) {
        // Insert two drawers with indexed words.
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

    async fn seed_chunked_source(connection: &Connection) {
        // Three consecutive chunks from the same source file.
        for i in 0..3_usize {
            crate::palace::drawer::add_drawer(
                connection,
                &crate::palace::drawer::DrawerParams {
                    id: &format!("chunk-{i}"),
                    wing: "project_c",
                    room: "general",
                    content: &format!("chunk content number {i} with unique identifier"),
                    source_file: "chunked.rs",
                    chunk_index: i,
                    added_by: "test",
                    ingest_mode: "projects",
                    source_mtime: None,
                },
            )
            .await
            .expect("add_drawer must succeed for chunked source seeding");
        }
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
        // "programming" appears in both, but "rust programming" should rank s1 higher
        // because s1 matches the unique "rust" term (higher IDF contribution).
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

    #[tokio::test]
    async fn search_result_has_chunk_index_and_source_path() {
        // SearchResult must expose chunk_index and source_path for neighbor expansion.
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_drawers(&connection).await;
        let results = search_memories(&connection, "rust", None, None, 5)
            .await
            .expect("search for 'rust' must succeed");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].chunk_index, 0);
        assert!(
            !results[0].source_path.is_empty(),
            "source_path must be set"
        );
    }

    #[tokio::test]
    async fn search_expand_neighbors_returns_adjacent_chunks() {
        // Neighbor expansion for chunk_index=1 with radius=1 must return chunks 0 and 2.
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_chunked_source(&connection).await;

        let neighbors = search_expand_neighbors(&connection, "chunked.rs", 1, 1)
            .await
            .expect("expand_neighbors must succeed for chunked source");

        assert_eq!(
            neighbors.len(),
            2,
            "radius=1 around index 1 must return chunks 0 and 2"
        );
        let indices: Vec<i32> = neighbors.iter().map(|r| r.chunk_index).collect();
        assert!(indices.contains(&0), "chunk 0 must be in neighbors");
        assert!(indices.contains(&2), "chunk 2 must be in neighbors");
        assert!(!indices.contains(&1), "anchor chunk must be excluded");
    }

    #[tokio::test]
    async fn search_expand_neighbors_empty_when_no_adjacent_chunks() {
        // A single-chunk source has no neighbors.
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_drawers(&connection).await;

        let neighbors = search_expand_neighbors(&connection, "main.rs", 0, 2)
            .await
            .expect("expand_neighbors must succeed even when there are no neighbors");
        // s1 has chunk_index=0 but there are no other chunks from main.rs.
        assert!(
            neighbors.is_empty(),
            "single-chunk source must have no neighbors"
        );
    }
}
