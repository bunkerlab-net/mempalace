//! Near-duplicate drawer detection using Jaccard similarity (Band C).
//!
//! Groups drawers by `source_file`, computes pairwise Jaccard similarity on
//! their word bags (from `drawer_words`), and removes the shorter member of
//! any pair that exceeds `threshold`. The longest drawer in a duplicate pair
//! is always kept.
//!
//! Python's `dedup.py` uses `ChromaDB` cosine similarity on embeddings. This
//! implementation replaces that with Jaccard on the existing inverted index,
//! avoiding any vector dependency.

use std::collections::{HashMap, HashSet};

use turso::Connection;

use crate::db::query_all;
use crate::error::Result;

/// Default Jaccard similarity threshold above which two drawers are duplicates.
pub const DEFAULT_THRESHOLD: f64 = 0.85;

/// Minimum drawers in a source-file group to bother checking for duplicates.
const MIN_GROUP_SIZE: usize = 2;

/// Maximum pairwise comparisons per group before we stop (guards against
/// pathological source files with hundreds of chunks).
const MAX_PAIRS_PER_GROUP: usize = 10_000;

const _: () = assert!(DEFAULT_THRESHOLD > 0.0);
const _: () = assert!(MIN_GROUP_SIZE >= 2);
const _: () = assert!(MAX_PAIRS_PER_GROUP > 0);

/// Statistics returned by [`dedup_drawers`].
#[derive(Debug, Default)]
pub struct DedupStats {
    /// Number of source-file groups examined.
    pub groups_scanned: usize,
    /// Duplicate drawers identified (above threshold).
    pub duplicates_found: usize,
    /// Drawers actually deleted (0 in dry-run mode).
    pub deleted: usize,
}

/// Lightweight drawer record for grouping logic.
struct DrawerInfo {
    id: String,
    content_len: usize,
}

/// Find and remove near-duplicate drawers across the palace.
///
/// Groups drawers by `source_file`, computes pairwise Jaccard similarity
/// using `drawer_words`, and deletes the shorter drawer in each pair whose
/// similarity exceeds `threshold`. Dry-run mode reports without deleting.
pub async fn dedup_drawers(
    connection: &Connection,
    wing: Option<&str>,
    threshold: f64,
    dry_run: bool,
) -> Result<DedupStats> {
    assert!(threshold > 0.0, "dedup_drawers: threshold must be positive");
    assert!(
        threshold <= 1.0,
        "dedup_drawers: threshold must be at most 1.0"
    );

    let groups = dedup_fetch_groups(connection, wing).await?;
    let mut stats = DedupStats::default();

    for drawers in groups.values() {
        if drawers.len() < MIN_GROUP_SIZE {
            continue;
        }
        stats.groups_scanned += 1;
        let ids: Vec<&str> = drawers.iter().map(|info| info.id.as_str()).collect();
        let word_sets = dedup_fetch_word_sets(connection, &ids).await?;
        let to_delete = dedup_find_duplicates(drawers, &word_sets, threshold);
        stats.duplicates_found += to_delete.len();

        if !dry_run {
            for id in &to_delete {
                dedup_delete_drawer(connection, id).await?;
                stats.deleted += 1;
            }
        }
    }

    assert!(
        stats.deleted <= stats.duplicates_found,
        "dedup_drawers: deleted must not exceed duplicates_found"
    );
    Ok(stats)
}

/// Fetch all source-file groups with 2+ drawers (optionally filtered by wing).
///
/// Returns a map from `source_file` → Vec<DrawerInfo>, ordered by content length
/// descending so the keeper (longest) is always index 0.
async fn dedup_fetch_groups(
    connection: &Connection,
    wing: Option<&str>,
) -> Result<HashMap<String, Vec<DrawerInfo>>> {
    let rows = if let Some(wing_filter) = wing {
        assert!(
            !wing_filter.is_empty(),
            "dedup_fetch_groups: wing must not be empty"
        );
        query_all(
            connection,
            "SELECT id, source_file, LENGTH(content) AS content_len \
             FROM drawers WHERE wing = ? ORDER BY source_file, content_len DESC",
            (wing_filter,),
        )
        .await?
    } else {
        query_all(
            connection,
            "SELECT id, source_file, LENGTH(content) AS content_len \
             FROM drawers ORDER BY source_file, content_len DESC",
            (),
        )
        .await?
    };

    let mut groups: HashMap<String, Vec<DrawerInfo>> = HashMap::new();
    for row in &rows {
        let id: String = row.get(0).unwrap_or_default();
        let source_file: String = row.get(1).unwrap_or_default();
        // content_len from SQLite LENGTH() is i64 in the turso driver.
        let content_len: i64 = row.get(2).unwrap_or_default();
        if id.is_empty() || source_file.is_empty() {
            continue;
        }
        // SQLite LENGTH() returns i64; content is never > usize::MAX in practice.
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        groups.entry(source_file).or_default().push(DrawerInfo {
            id,
            content_len: content_len as usize,
        });
    }

    groups.retain(|_, drawers| drawers.len() >= MIN_GROUP_SIZE);

    debug_assert!(
        groups
            .values()
            .all(|drawers| drawers.len() >= MIN_GROUP_SIZE),
        "all groups must have at least MIN_GROUP_SIZE drawers"
    );
    Ok(groups)
}

/// Fetch word sets for the given drawer IDs from `drawer_words`.
///
/// Returns a map from `drawer_id` → set of words.
async fn dedup_fetch_word_sets(
    connection: &Connection,
    ids: &[&str],
) -> Result<HashMap<String, HashSet<String>>> {
    assert!(
        !ids.is_empty(),
        "dedup_fetch_word_sets: ids must not be empty"
    );

    // Build a parameterized IN clause. turso requires one bind per param.
    // We fall back to a per-ID query if the group is unusually large.
    let mut word_sets: HashMap<String, HashSet<String>> = HashMap::new();
    for id in ids {
        let rows = query_all(
            connection,
            "SELECT word FROM drawer_words WHERE drawer_id = ?",
            (*id,),
        )
        .await?;
        let words: HashSet<String> = rows
            .iter()
            .map(|row| row.get::<String>(0).unwrap_or_default())
            .filter(|word| !word.is_empty())
            .collect();
        word_sets.insert((*id).to_string(), words);
    }

    assert_eq!(
        word_sets.len(),
        ids.len(),
        "dedup_fetch_word_sets: word_sets count must equal ids count"
    );
    Ok(word_sets)
}

/// Compute pairwise Jaccard similarity and return IDs of drawers to delete.
///
/// For each pair (i, j) where i < j: if Jaccard ≥ `threshold`, the shorter
/// drawer is marked for deletion. The longer one (index with more content)
/// is always kept. Each drawer is marked for deletion at most once.
fn dedup_find_duplicates(
    drawers: &[DrawerInfo],
    word_sets: &HashMap<String, HashSet<String>>,
    threshold: f64,
) -> Vec<String> {
    assert!(!drawers.is_empty());
    assert!(threshold > 0.0);
    assert!(threshold <= 1.0);

    let mut to_delete: HashSet<String> = HashSet::new();
    let pair_limit = MAX_PAIRS_PER_GROUP.min(drawers.len() * (drawers.len() - 1) / 2);
    let mut pairs_checked = 0usize;
    let empty_words: HashSet<String> = HashSet::new();

    'outer: for (index_i, drawer_i) in drawers.iter().enumerate() {
        for drawer_j in drawers.iter().skip(index_i + 1) {
            if pairs_checked >= pair_limit {
                break 'outer;
            }
            pairs_checked += 1;

            if to_delete.contains(&drawer_i.id) && to_delete.contains(&drawer_j.id) {
                continue;
            }

            let words_i = word_sets.get(&drawer_i.id).unwrap_or(&empty_words);
            let words_j = word_sets.get(&drawer_j.id).unwrap_or(&empty_words);
            let similarity = dedup_jaccard(words_i, words_j);

            if similarity >= threshold {
                let shorter = if drawer_i.content_len <= drawer_j.content_len {
                    &drawer_i.id
                } else {
                    &drawer_j.id
                };
                to_delete.insert(shorter.clone());
            }
        }
    }

    let result: Vec<String> = to_delete.into_iter().collect();
    assert!(
        result.len() < drawers.len(),
        "dedup_find_duplicates: cannot delete all drawers in a group"
    );
    result
}

/// Compute Jaccard similarity between two word sets.
///
/// Returns 0.0 when both sets are empty, and 1.0 when sets are identical.
fn dedup_jaccard(set_a: &HashSet<String>, set_b: &HashSet<String>) -> f64 {
    let union_size = set_a.len() + set_b.len();
    if union_size == 0 {
        return 0.0;
    }

    let intersection_size = set_a.intersection(set_b).count();
    // Jaccard = |A ∩ B| / |A ∪ B|  =  |A ∩ B| / (|A| + |B| - |A ∩ B|)
    let union_actual = union_size - intersection_size;

    assert!(
        union_actual > 0,
        "dedup_jaccard: union must be non-zero when at least one set is non-empty"
    );

    // Both counts are usize; precision loss is acceptable for a similarity score.
    #[allow(clippy::cast_precision_loss)]
    let result = intersection_size as f64 / union_actual as f64;

    assert!(result >= 0.0);
    assert!(result <= 1.0);
    result
}

/// Delete one drawer and its associated `drawer_words` and `compressed` rows.
async fn dedup_delete_drawer(connection: &Connection, id: &str) -> Result<()> {
    assert!(!id.is_empty(), "dedup_delete_drawer: id must not be empty");
    assert!(
        id.starts_with("drawer_"),
        "dedup_delete_drawer: id must have drawer_ prefix"
    );

    connection
        .execute("DELETE FROM drawers WHERE id = ?", (id,))
        .await?;
    connection
        .execute("DELETE FROM drawer_words WHERE drawer_id = ?", (id,))
        .await?;
    // Clean up closet LLM entries — compressed.id == drawer id.
    connection
        .execute("DELETE FROM compressed WHERE id = ?", (id,))
        .await?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // ── dedup_jaccard ──────────────────────────────────────────────────────

    #[test]
    fn jaccard_identical_sets_returns_one() {
        let set_a: HashSet<String> = ["rust", "memory"].iter().map(ToString::to_string).collect();
        let set_b: HashSet<String> = ["rust", "memory"].iter().map(ToString::to_string).collect();
        let similarity = dedup_jaccard(&set_a, &set_b);
        // Identical sets: intersection = 2, union = 2, Jaccard = 1.0
        assert!(
            (similarity - 1.0).abs() < f64::EPSILON,
            "identical sets must return 1.0"
        );
    }

    #[test]
    fn jaccard_disjoint_sets_returns_zero() {
        let set_a: HashSet<String> = ["rust"].iter().map(ToString::to_string).collect();
        let set_b: HashSet<String> = ["python"].iter().map(ToString::to_string).collect();
        let similarity = dedup_jaccard(&set_a, &set_b);
        assert!(similarity < f64::EPSILON, "disjoint sets must return 0.0");
    }

    #[test]
    fn jaccard_partial_overlap_is_between_zero_and_one() {
        let set_a: HashSet<String> = ["a", "b", "c"].iter().map(ToString::to_string).collect();
        let set_b: HashSet<String> = ["b", "c", "d"].iter().map(ToString::to_string).collect();
        let similarity = dedup_jaccard(&set_a, &set_b);
        // intersection = {b, c} = 2, union = {a, b, c, d} = 4 → 0.5
        assert!(
            (similarity - 0.5).abs() < 1e-9,
            "partial overlap must be 0.5"
        );
    }

    #[test]
    fn jaccard_empty_sets_returns_zero() {
        let set_a: HashSet<String> = HashSet::new();
        let set_b: HashSet<String> = HashSet::new();
        let similarity = dedup_jaccard(&set_a, &set_b);
        assert!(similarity < f64::EPSILON, "empty sets must return 0.0");
    }

    // ── dedup_find_duplicates ──────────────────────────────────────────────

    #[test]
    fn find_duplicates_marks_shorter_drawer() {
        let drawers = vec![
            DrawerInfo {
                id: "drawer_long".to_string(),
                content_len: 200,
            },
            DrawerInfo {
                id: "drawer_short".to_string(),
                content_len: 100,
            },
        ];
        let mut word_sets: HashMap<String, HashSet<String>> = HashMap::new();
        let shared_words: HashSet<String> = ["rust", "memory", "palace"]
            .iter()
            .map(ToString::to_string)
            .collect();
        word_sets.insert("drawer_long".to_string(), shared_words.clone());
        word_sets.insert("drawer_short".to_string(), shared_words);

        let to_delete = dedup_find_duplicates(&drawers, &word_sets, 0.9);
        assert_eq!(to_delete.len(), 1, "one duplicate must be found");
        assert_eq!(
            to_delete[0], "drawer_short",
            "shorter drawer must be deleted"
        );
    }

    #[test]
    fn find_duplicates_below_threshold_keeps_both() {
        let drawers = vec![
            DrawerInfo {
                id: "drawer_a".to_string(),
                content_len: 100,
            },
            DrawerInfo {
                id: "drawer_b".to_string(),
                content_len: 100,
            },
        ];
        let mut word_sets: HashMap<String, HashSet<String>> = HashMap::new();
        word_sets.insert(
            "drawer_a".to_string(),
            ["rust", "memory"].iter().map(ToString::to_string).collect(),
        );
        word_sets.insert(
            "drawer_b".to_string(),
            ["python", "palace"]
                .iter()
                .map(ToString::to_string)
                .collect(),
        );

        let to_delete = dedup_find_duplicates(&drawers, &word_sets, 0.9);
        assert!(
            to_delete.is_empty(),
            "disjoint drawers must not be marked for deletion"
        );
    }

    // ── dedup_fetch_groups (integration) ──────────────────────────────────────

    #[tokio::test]
    async fn dedup_fetch_groups_filters_single_drawer_sources() {
        // A source_file with only one drawer is below MIN_GROUP_SIZE and must be excluded.
        let (_db, connection) = crate::test_helpers::test_db().await;
        connection
            .execute(
                "INSERT INTO drawers (id, wing, room, content, source_file, added_by) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "drawer_solo_src",
                    "test",
                    "room",
                    "single drawer in this source",
                    "solo_source.md",
                    "test",
                ),
            )
            .await
            .expect("insert solo drawer");

        let groups = dedup_fetch_groups(&connection, None)
            .await
            .expect("dedup_fetch_groups must succeed");

        // solo_source.md has only 1 drawer — below MIN_GROUP_SIZE=2.
        assert!(
            !groups.contains_key("solo_source.md"),
            "single-drawer source_file must be filtered out by MIN_GROUP_SIZE"
        );
    }

    #[tokio::test]
    async fn dedup_fetch_groups_wing_filter_restricts_scope() {
        // Only drawers in the specified wing must be considered.
        let (_db, connection) = crate::test_helpers::test_db().await;

        // Two drawers in "wing_alpha" with the same source_file → a valid group.
        for (id, content) in [
            ("drawer_wa_1", "rust memory palace alpha version long"),
            ("drawer_wa_2", "rust memory palace alpha version"),
        ] {
            connection
                .execute(
                    "INSERT INTO drawers (id, wing, room, content, source_file, added_by) \
                     VALUES (?, ?, ?, ?, ?, ?)",
                    (id, "wing_alpha", "room", content, "alpha.md", "test"),
                )
                .await
                .expect("insert wing_alpha drawer");
        }
        // One drawer in "wing_beta" — cannot form a group on its own.
        connection
            .execute(
                "INSERT INTO drawers (id, wing, room, content, source_file, added_by) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "drawer_wb_1",
                    "wing_beta",
                    "room",
                    "beta only drawer",
                    "beta.md",
                    "test",
                ),
            )
            .await
            .expect("insert wing_beta drawer");

        // Without filter: wing_alpha's group appears.
        let all_groups = dedup_fetch_groups(&connection, None)
            .await
            .expect("fetch all groups");
        assert!(
            all_groups.contains_key("alpha.md"),
            "alpha.md group must appear without wing filter"
        );

        // With wing_alpha filter: alpha.md group appears.
        let alpha_groups = dedup_fetch_groups(&connection, Some("wing_alpha"))
            .await
            .expect("fetch wing_alpha groups");
        assert!(
            alpha_groups.contains_key("alpha.md"),
            "alpha.md must appear when filtering by wing_alpha"
        );

        // With wing_beta filter: beta.md has only 1 drawer → no groups.
        let beta_groups = dedup_fetch_groups(&connection, Some("wing_beta"))
            .await
            .expect("fetch wing_beta groups");
        assert!(
            beta_groups.is_empty(),
            "wing_beta has no multi-drawer source_file groups"
        );
    }

    // ── dedup_drawers (integration) ─────────────────────────────────────────

    #[tokio::test]
    async fn dedup_dry_run_finds_but_does_not_delete() {
        let (_db, connection) = crate::test_helpers::test_db().await;

        // Seed two near-identical drawers from the same source_file.
        connection
            .execute(
                "INSERT INTO drawers (id, wing, room, content, source_file, added_by) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "drawer_dedup_long",
                    "test",
                    "notes",
                    "Rust memory palace project notes long version",
                    "notes.md",
                    "test",
                ),
            )
            .await
            .expect("seed long drawer");
        connection
            .execute(
                "INSERT INTO drawers (id, wing, room, content, source_file, added_by) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "drawer_dedup_short",
                    "test",
                    "notes",
                    "Rust memory palace project notes",
                    "notes.md",
                    "test",
                ),
            )
            .await
            .expect("seed short drawer");

        // Seed matching word sets.
        for word in &["rust", "memory", "palace", "project", "notes"] {
            connection
                .execute(
                    "INSERT INTO drawer_words (word, drawer_id) VALUES (?, ?)",
                    (*word, "drawer_dedup_long"),
                )
                .await
                .expect("seed long words");
            connection
                .execute(
                    "INSERT INTO drawer_words (word, drawer_id) VALUES (?, ?)",
                    (*word, "drawer_dedup_short"),
                )
                .await
                .expect("seed short words");
        }

        let stats = dedup_drawers(&connection, None, 0.9, true)
            .await
            .expect("dry_run dedup must succeed");

        assert_eq!(stats.groups_scanned, 1, "one group must be scanned");
        assert_eq!(stats.duplicates_found, 1, "one duplicate must be found");
        assert_eq!(stats.deleted, 0, "dry_run must not delete anything");

        // Pair assertion: both drawers must still exist.
        let rows = query_all(
            &connection,
            "SELECT id FROM drawers WHERE source_file = 'notes.md' ORDER BY id",
            (),
        )
        .await
        .expect("query after dry_run must succeed");
        assert_eq!(rows.len(), 2, "dry_run must not delete any drawers");
    }
}
