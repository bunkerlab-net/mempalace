//! Temporal knowledge graph — entities, relationship triples, and time-bounded facts.

pub mod query;

use turso::Connection;

use crate::config::sanitize_iso_temporal;
use crate::db;
use crate::error::Result;

/// Normalize an entity name to an ID: lowercase, spaces→underscores, strip apostrophes.
pub fn entity_id(name: &str) -> String {
    let result = name.to_lowercase().replace(' ', "_").replace('\'', "");

    // Postcondition: result has no spaces and no uppercase.
    debug_assert!(!result.contains(' '));
    debug_assert!(result.chars().all(|c| !c.is_uppercase()));
    // Note: non-empty input does NOT guarantee non-empty output — names
    // consisting entirely of apostrophes (e.g. "'") normalize to "".
    // Callers that need a non-empty ID must validate before calling.

    result
}

/// Return `true` when `value` is a date-only ISO temporal (`YYYY-MM-DD`).
fn is_date_only_temporal(value: &str) -> bool {
    value.len() == 10 && value.as_bytes()[4] == b'-' && value.as_bytes()[7] == b'-'
}

/// Return the comparable instant for a `valid_from`-style temporal value.
///
/// Date-only inputs are widened to the start of the day so a date-only `valid_from`
/// compares correctly against a canonical UTC datetime `valid_to` of the same day.
/// Mirrors `_temporal_start_key` in `mempalace/knowledge_graph.py`.
pub(crate) fn temporal_start_key(value: &str) -> String {
    if is_date_only_temporal(value) {
        format!("{value}T00:00:00Z")
    } else {
        value.to_string()
    }
}

/// Return the comparable instant for a `valid_to`-style temporal value.
///
/// Date-only inputs are widened to end-of-day so legacy date-only `valid_to`
/// values remain compatible with canonical UTC datetime queries on the same day.
/// Mirrors `_temporal_end_key` in `mempalace/knowledge_graph.py`.
pub(crate) fn temporal_end_key(value: &str) -> String {
    if is_date_only_temporal(value) {
        format!("{value}T23:59:59Z")
    } else {
        value.to_string()
    }
}

/// `SQLite` expression for comparing a `valid_from`-style column with mixed temporal shapes.
///
/// Widens date-only stored values (`length = 10`, `-` at positions 4 and 7) to
/// start-of-day (`T00:00:00Z`) so they compare correctly against canonical UTC
/// datetime query parameters. Mirrors `_sql_temporal_start_expr` in Python.
pub(crate) fn sql_temporal_start_expr(column: &str) -> String {
    format!(
        "CASE WHEN length({column}) = 10 \
         AND substr({column}, 5, 1) = '-' \
         AND substr({column}, 8, 1) = '-' \
         THEN {column} || 'T00:00:00Z' ELSE {column} END"
    )
}

/// `SQLite` expression for comparing a `valid_to`-style column with mixed temporal shapes.
///
/// Widens date-only stored values to end-of-day (`T23:59:59Z`) so legacy date-only
/// `valid_to` values remain inclusive when queried with a same-day datetime.
/// Mirrors `_sql_temporal_end_expr` in Python.
/// Mirrors `_sql_temporal_end_expr` in Python.
pub(crate) fn sql_temporal_end_expr(column: &str) -> String {
    format!(
        "CASE WHEN length({column}) = 10 \
         AND substr({column}, 5, 1) = '-' \
         AND substr({column}, 8, 1) = '-' \
         THEN {column} || 'T23:59:59Z' ELSE {column} END"
    )
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn entity_id_lowercases_and_replaces_spaces() {
        assert_eq!(entity_id("John Doe"), "john_doe");
    }

    #[test]
    fn entity_id_strips_apostrophes() {
        assert_eq!(entity_id("O'Brien"), "obrien");
    }

    #[test]
    fn entity_id_already_normalized() {
        assert_eq!(entity_id("simple"), "simple");
    }

    #[test]
    fn entity_id_empty_string() {
        assert_eq!(entity_id(""), "");
    }

    #[test]
    fn add_triple_validate_params_rejects_nan_confidence() {
        // NaN confidence must be rejected — it would corrupt sort comparisons downstream.
        let params = TripleParams {
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            valid_from: None,
            valid_to: None,
            confidence: f64::NAN,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let result = add_triple_validate_params(&params);
        assert!(result.is_err(), "NaN confidence must be rejected");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("confidence")),
            "error message must mention confidence"
        );
    }

    #[test]
    fn add_triple_validate_params_rejects_negative_confidence() {
        // Negative confidence is outside the valid [0.0, 1.0] range.
        let params = TripleParams {
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            valid_from: None,
            valid_to: None,
            confidence: -0.1,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let result = add_triple_validate_params(&params);
        assert!(result.is_err(), "negative confidence must be rejected");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("confidence")),
            "error message must mention confidence"
        );
    }

    #[test]
    fn add_triple_validate_params_rejects_confidence_greater_than_one() {
        // Confidence above 1.0 is outside the valid [0.0, 1.0] range.
        let params = TripleParams {
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            valid_from: None,
            valid_to: None,
            confidence: 1.1,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let result = add_triple_validate_params(&params);
        assert!(result.is_err(), "confidence > 1.0 must be rejected");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("confidence")),
            "error message must mention confidence"
        );
    }

    #[test]
    fn add_triple_validate_params_rejects_invalid_date_format() {
        // A non-ISO date in valid_from must be rejected with a clear error.
        let params = TripleParams {
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            valid_from: Some("not-a-date"),
            valid_to: None,
            confidence: 0.5,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let result = add_triple_validate_params(&params);
        assert!(result.is_err(), "invalid date format must be rejected");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("valid_from")),
            "error message must mention valid_from"
        );
    }

    #[test]
    fn add_triple_validate_params_rejects_from_after_to() {
        // valid_from after valid_to is logically impossible and must be rejected.
        let params = TripleParams {
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            valid_from: Some("2025-06-01"),
            valid_to: Some("2025-01-01"),
            confidence: 0.8,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let result = add_triple_validate_params(&params);
        assert!(
            result.is_err(),
            "valid_from after valid_to must be rejected"
        );
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("inverted interval")),
            "error message must explain the temporal ordering constraint"
        );
    }

    #[test]
    fn temporal_start_key_widens_date_only_to_start_of_day() {
        assert_eq!(temporal_start_key("2026-05-15"), "2026-05-15T00:00:00Z");
    }

    #[test]
    fn temporal_start_key_passes_through_datetime() {
        assert_eq!(
            temporal_start_key("2026-05-15T12:30:45Z"),
            "2026-05-15T12:30:45Z"
        );
    }

    #[test]
    fn temporal_end_key_widens_date_only_to_end_of_day() {
        assert_eq!(temporal_end_key("2026-05-15"), "2026-05-15T23:59:59Z");
    }

    #[test]
    fn temporal_end_key_passes_through_datetime() {
        assert_eq!(
            temporal_end_key("2026-05-15T12:30:45Z"),
            "2026-05-15T12:30:45Z"
        );
    }

    #[test]
    fn sql_temporal_start_expr_emits_case_when_block() {
        let sql = sql_temporal_start_expr("t.valid_from");
        assert!(sql.contains("CASE WHEN length(t.valid_from) = 10"));
        assert!(sql.contains("|| 'T00:00:00Z'"));
    }

    #[test]
    fn sql_temporal_end_expr_emits_case_when_block() {
        let sql = sql_temporal_end_expr("t.valid_to");
        assert!(sql.contains("CASE WHEN length(t.valid_to) = 10"));
        assert!(sql.contains("|| 'T23:59:59Z'"));
    }

    #[test]
    fn is_date_only_temporal_recognizes_iso_date() {
        assert!(is_date_only_temporal("2026-05-15"));
    }

    #[test]
    fn is_date_only_temporal_rejects_datetime() {
        assert!(!is_date_only_temporal("2026-05-15T12:30:45Z"));
        assert!(!is_date_only_temporal("2026-05"));
    }

    #[test]
    fn add_triple_validate_params_accepts_canonical_datetime() {
        // Canonical UTC datetime (Z-suffixed) must pass validation.
        let params = TripleParams {
            subject: "Alice",
            predicate: "joined",
            object: "Acme",
            valid_from: Some("2026-05-15T12:30:45Z"),
            valid_to: None,
            confidence: 0.9,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let result = add_triple_validate_params(&params);
        assert!(
            result.is_ok(),
            "canonical UTC datetime must validate successfully"
        );
    }

    #[test]
    fn add_triple_validate_params_normalizes_zero_offset_to_z() {
        // +00:00 → Z, so the stored row uses the canonical shape.
        let params = TripleParams {
            subject: "Alice",
            predicate: "joined",
            object: "Acme",
            valid_from: Some("2026-05-15T12:30:45+00:00"),
            valid_to: None,
            confidence: 0.9,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let validated = add_triple_validate_params(&params).expect("must validate");
        assert_eq!(
            validated.valid_from.as_deref(),
            Some("2026-05-15T12:30:45Z"),
            "+00:00 must normalize to Z"
        );
    }

    #[test]
    fn add_triple_validate_params_rejects_invalid_valid_to_date() {
        // A non-ISO date in valid_to must also be rejected.
        let params = TripleParams {
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            valid_from: None,
            valid_to: Some("garbage"),
            confidence: 0.5,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let result = add_triple_validate_params(&params);
        assert!(result.is_err(), "invalid valid_to date must be rejected");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("valid_to")),
            "error message must mention valid_to"
        );
    }
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn add_entity_rejects_all_apostrophe_name() {
        // A name consisting entirely of apostrophes normalizes to an empty entity id.
        // add_entity must reject this with an Err rather than inserting a blank key.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = add_entity(&connection, "'''", "person", None).await;
        assert!(
            result.is_err(),
            "all-apostrophe name must produce an empty entity id and be rejected"
        );
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("empty")),
            "error message must mention empty entity id"
        );
    }

    #[tokio::test]
    async fn add_entity_inserts_and_returns_id() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let id = add_entity(&connection, "Alice Smith", "person", None)
            .await
            .expect("add_entity should succeed for valid name and entity type");
        assert_eq!(id, "alice_smith");

        let rows = crate::db::query_all(
            &connection,
            "SELECT name FROM entities WHERE id = ?1",
            turso::params!["alice_smith"],
        )
        .await
        .expect("SELECT FROM entities should succeed after add_entity");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn add_triple_creates_entities_automatically() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let _tid = add_triple(
            &connection,
            &TripleParams {
                subject: "Alice",
                predicate: "works at",
                object: "Acme Corp",
                valid_from: Some("2024-01-01"),
                valid_to: None,
                confidence: 0.9,
                source_closet: None,
                source_file: None,
                source_drawer_id: None,
                adapter_name: None,
            },
        )
        .await
        .expect("add_triple should succeed for valid subject/predicate/object params");

        // Both entities should exist.
        let entities = crate::db::query_all(&connection, "SELECT id FROM entities ORDER BY id", ())
            .await
            .expect(
                "SELECT FROM entities should succeed after add_triple with auto-entity creation",
            );
        assert_eq!(entities.len(), 2);
    }

    #[tokio::test]
    async fn add_triple_dedup_returns_existing_id() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let params = TripleParams {
            subject: "Bob",
            predicate: "likes",
            object: "Rust",
            valid_from: None,
            valid_to: None,
            confidence: 1.0,
            source_closet: None,
            source_file: None,
            source_drawer_id: None,
            adapter_name: None,
        };
        let id1 = add_triple(&connection, &params)
            .await
            .expect("first add_triple should succeed for new triple");
        let id2 = add_triple(&connection, &params)
            .await
            .expect("second add_triple on same params should return existing id without error");
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn seed_from_entity_facts_inserts_all_triples() {
        // seed_from_entity_facts must insert one triple per input pair and return the count.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let count = seed_from_entity_facts(
            &connection,
            "Dave",
            &[("knows", "Eve"), ("works_at", "Initech")],
        )
        .await
        .expect("seed_from_entity_facts should succeed for valid subject and pairs");
        assert_eq!(count, 2, "two pairs must produce two processed triples");
        // Pair assertion: both triples must exist in the graph.
        let rows = crate::db::query_all(&connection, "SELECT COUNT(*) FROM triples", ())
            .await
            .expect("SELECT COUNT from triples must succeed after seed");
        let triple_count: i64 = rows
            .first()
            .and_then(|r| r.get_value(0).ok())
            .and_then(|c| c.as_integer().copied())
            .unwrap_or(0);
        assert_eq!(
            triple_count, 2,
            "triples table must contain exactly 2 rows after seed"
        );
    }

    #[tokio::test]
    async fn invalidate_sets_valid_to() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let _tid = add_triple(
            &connection,
            &TripleParams {
                subject: "Carol",
                predicate: "works at",
                object: "OldCo",
                valid_from: None,
                valid_to: None,
                confidence: 1.0,
                source_closet: None,
                source_file: None,
                source_drawer_id: None,
                adapter_name: None,
            },
        )
        .await
        .expect("add_triple should succeed for Carol/works at/OldCo");

        invalidate(
            &connection,
            "Carol",
            "works at",
            "OldCo",
            Some("2024-06-01"),
        )
        .await
        .expect("invalidate should succeed for existing Carol/works at/OldCo triple");

        let rows = crate::db::query_all(
            &connection,
            "SELECT valid_to FROM triples WHERE subject = 'carol' AND predicate = 'works_at'",
            (),
        )
        .await
        .expect("SELECT valid_to should succeed after invalidate");
        assert_eq!(rows.len(), 1);
        let valid_to: String = rows[0]
            .get(0)
            .expect("valid_to column 0 must be present in triples result row");
        assert_eq!(valid_to, "2024-06-01");
    }
}

/// Add or update an entity node.
#[cfg(test)]
pub async fn add_entity(
    connection: &Connection,
    name: &str,
    entity_type: &str,
    properties: Option<&str>,
) -> Result<String> {
    let entity_identifier = entity_id(name);
    // entity_id can return "" for inputs like "'"; reject before writing a blank key.
    if entity_identifier.is_empty() {
        return Err(crate::error::Error::Other(
            "empty normalized entity id".to_string(),
        ));
    }
    let properties_json = properties.unwrap_or("{}");
    connection
        .execute(
            "INSERT OR REPLACE INTO entities (id, name, type, properties) VALUES (?1, ?2, ?3, ?4)",
            turso::params![
                entity_identifier.as_str(),
                name,
                entity_type,
                properties_json
            ],
        )
        .await?;
    Ok(entity_identifier)
}

/// Bulk-seed KG triples for a subject from a `(predicate, object)` slice.
///
/// Each pair is inserted via `add_triple` with default confidence and no date
/// bounds. Returns the number of fact pairs processed. Used in tests to build
/// entity relationship graphs without calling `add_triple` individually.
#[cfg(test)]
async fn seed_from_entity_facts(
    connection: &Connection,
    subject: &str,
    facts: &[(&str, &str)],
) -> Result<usize> {
    assert!(
        !subject.is_empty(),
        "seed_from_entity_facts: subject must not be empty"
    );
    assert!(
        !facts.is_empty(),
        "seed_from_entity_facts: facts slice must not be empty"
    );

    let mut processed = 0usize;
    for &(predicate, object) in facts {
        assert!(
            !predicate.is_empty(),
            "seed_from_entity_facts: predicate must not be empty"
        );
        assert!(
            !object.is_empty(),
            "seed_from_entity_facts: object must not be empty"
        );
        add_triple(
            connection,
            &TripleParams {
                subject,
                predicate,
                object,
                valid_from: None,
                valid_to: None,
                confidence: 1.0,
                source_closet: None,
                source_file: None,
                source_drawer_id: None,
                adapter_name: None,
            },
        )
        .await?;
        processed += 1;
    }

    // Postcondition: one triple must have been processed per input pair.
    assert!(
        processed == facts.len(),
        "seed_from_entity_facts: processed {processed} but expected {}",
        facts.len()
    );
    Ok(processed)
}

/// Parameters for [`add_triple`].
pub struct TripleParams<'a> {
    /// The entity performing or originating the relationship.
    pub subject: &'a str,
    /// The relationship type (e.g. `"works_at"`, `"likes"`).
    pub predicate: &'a str,
    /// The target entity of the relationship.
    pub object: &'a str,
    /// When this fact became true (ISO date string, or `None` for unknown).
    pub valid_from: Option<&'a str>,
    /// When this fact stopped being true (`None` if still current).
    pub valid_to: Option<&'a str>,
    /// Confidence score between 0.0 and 1.0.
    pub confidence: f64,
    /// Optional closet (drawer) that sourced this fact.
    pub source_closet: Option<&'a str>,
    /// Optional file path that sourced this fact.
    pub source_file: Option<&'a str>,
    /// RFC 002 §5.5: drawer ID of the source record, set by adapters that
    /// advertise `supports_kg_triples`.  `None` for all non-adapter paths.
    pub source_drawer_id: Option<&'a str>,
    /// RFC 002 §5.5: adapter that produced this triple.
    /// `None` for all non-adapter paths.
    pub adapter_name: Option<&'a str>,
}

/// Canonicalized temporal fields returned by [`add_triple_validate_params`].
///
/// Holds the sanitized `valid_from` / `valid_to` strings so the caller writes the
/// canonical form (e.g. `+00:00` normalized to `Z`) to the database rather than
/// the raw caller-supplied string.
pub(crate) struct ValidatedTemporals {
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
}

/// Validate confidence and ISO temporal fields on a `TripleParams`.
///
/// Both `valid_from` and `valid_to` are validated via
/// [`crate::config::sanitize_iso_temporal`], so date and canonical UTC datetime
/// forms are accepted. Inverted intervals are rejected using temporal comparison
/// keys so a date-only `valid_to` of the same day as a datetime `valid_from`
/// resolves to the end of that day instead of midnight. Called by `add_triple`
/// to keep that function within the 70-line limit.
fn add_triple_validate_params(params: &TripleParams<'_>) -> Result<ValidatedTemporals> {
    if params.confidence.is_nan() || params.confidence < 0.0 || params.confidence > 1.0 {
        return Err(crate::error::Error::Other(format!(
            "confidence must be between 0.0 and 1.0, got {}",
            params.confidence
        )));
    }
    let valid_from = sanitize_iso_temporal(params.valid_from, "valid_from")?;
    let valid_to = sanitize_iso_temporal(params.valid_to, "valid_to")?;

    // Reject inverted intervals using temporal keys: a date-only valid_to
    // resolves to end-of-day, so `valid_to=2026-05-15` with
    // `valid_from=2026-05-15T15:00:00Z` is NOT inverted.
    if let (Some(from), Some(to)) = (valid_from.as_deref(), valid_to.as_deref())
        && !from.is_empty()
        && !to.is_empty()
        && temporal_end_key(to) < temporal_start_key(from)
    {
        return Err(crate::error::Error::Other(format!(
            "valid_to={to:?} is before valid_from={from:?}; \
             an inverted interval would be invisible to every KG query"
        )));
    }
    Ok(ValidatedTemporals {
        valid_from,
        valid_to,
    })
}

/// Upsert subject and object entities into the graph (INSERT OR IGNORE).
/// Called by `add_triple` to keep that function within the 70-line limit.
async fn add_triple_ensure_entities(
    connection: &Connection,
    subject_id: &str,
    subject_name: &str,
    object_id: &str,
    object_name: &str,
) -> Result<()> {
    assert!(!subject_id.is_empty(), "subject_id must not be empty");
    assert!(!object_id.is_empty(), "object_id must not be empty");

    connection
        .execute(
            "INSERT OR IGNORE INTO entities (id, name) VALUES (?1, ?2)",
            turso::params![subject_id, subject_name],
        )
        .await?;
    connection
        .execute(
            "INSERT OR IGNORE INTO entities (id, name) VALUES (?1, ?2)",
            turso::params![object_id, object_name],
        )
        .await?;
    Ok(())
}

/// Check whether an active triple already exists and return its ID if so.
/// Called by `add_triple` to keep that function within the 70-line limit.
async fn add_triple_check_duplicate(
    connection: &Connection,
    subject_id: &str,
    predicate: &str,
    object_id: &str,
) -> Result<Option<String>> {
    assert!(!subject_id.is_empty(), "subject_id must not be empty");
    assert!(!predicate.is_empty(), "predicate must not be empty");
    assert!(!object_id.is_empty(), "object_id must not be empty");

    let existing = db::query_all(
        connection,
        "SELECT id FROM triples WHERE subject=?1 AND predicate=?2 AND object=?3 AND valid_to IS NULL",
        turso::params![subject_id, predicate, object_id],
    )
    .await?;

    if let Some(row) = existing.first()
        && let Ok(val) = row.get_value(0)
        && let Some(id) = val.as_text()
    {
        return Ok(Some(id.clone()));
    }
    Ok(None)
}

/// Add a relationship triple. Auto-creates entities if they don't exist.
/// Returns the triple ID.
pub async fn add_triple(connection: &Connection, params: &TripleParams<'_>) -> Result<String> {
    // Preconditions: subject, predicate, and object must all be non-empty.
    assert!(
        !params.subject.is_empty(),
        "triple subject must not be empty"
    );
    assert!(
        !params.predicate.is_empty(),
        "triple predicate must not be empty"
    );
    assert!(!params.object.is_empty(), "triple object must not be empty");

    let validated = add_triple_validate_params(params)?;

    let subject_id = entity_id(params.subject);
    let object_id = entity_id(params.object);
    let predicate = params.predicate.to_lowercase().replace(' ', "_");

    // Pair assertions: entity_id can return "" for inputs like "'".
    // An empty normalized ID would silently corrupt the graph with a blank key.
    assert!(
        !subject_id.is_empty(),
        "triple subject normalizes to empty ID"
    );
    assert!(
        !object_id.is_empty(),
        "triple object normalizes to empty ID"
    );
    assert!(
        !predicate.is_empty(),
        "triple predicate normalizes to empty ID"
    );

    add_triple_ensure_entities(
        connection,
        &subject_id,
        params.subject,
        &object_id,
        params.object,
    )
    .await?;

    if let Some(existing_id) =
        add_triple_check_duplicate(connection, &subject_id, &predicate, &object_id).await?
    {
        return Ok(existing_id);
    }

    let triple_id = format!(
        "t_{subject_id}_{predicate}_{object_id}_{}",
        &uuid::Uuid::new_v4().to_string().replace('-', "")[..8]
    );

    // Postcondition: triple ID follows naming convention.
    assert!(triple_id.starts_with("t_"), "triple_id must start with t_");

    add_triple_insert_row(
        connection,
        &triple_id,
        &subject_id,
        &predicate,
        &object_id,
        params,
        &validated,
    )
    .await?;

    Ok(triple_id)
}

/// Execute the final `INSERT INTO triples` for `add_triple`.
/// Extracted to keep `add_triple` within the 70-line limit.
async fn add_triple_insert_row(
    connection: &Connection,
    triple_id: &str,
    subject_id: &str,
    predicate: &str,
    object_id: &str,
    params: &TripleParams<'_>,
    validated: &ValidatedTemporals,
) -> Result<()> {
    assert!(!triple_id.is_empty(), "triple_id must not be empty");
    assert!(!object_id.is_empty(), "object_id must not be empty");

    // Persist the sanitized form (e.g. `+00:00` → `Z`) so the stored row matches
    // the canonical shape relied on by `temporal_start_key`/`temporal_end_key`.
    let valid_from_value: turso::Value = validated
        .valid_from
        .as_deref()
        .filter(|value| !value.is_empty())
        .map_or(turso::Value::Null, turso::Value::from);
    let valid_to_value: turso::Value = validated
        .valid_to
        .as_deref()
        .filter(|value| !value.is_empty())
        .map_or(turso::Value::Null, turso::Value::from);
    let source_closet_value: turso::Value = params
        .source_closet
        .map_or(turso::Value::Null, turso::Value::from);
    let source_file_value: turso::Value = params
        .source_file
        .map_or(turso::Value::Null, turso::Value::from);
    let source_drawer_id_value: turso::Value = params
        .source_drawer_id
        .map_or(turso::Value::Null, turso::Value::from);
    let adapter_name_value: turso::Value = params
        .adapter_name
        .map_or(turso::Value::Null, turso::Value::from);

    connection
        .execute(
            "INSERT INTO triples \
             (id, subject, predicate, object, valid_from, valid_to, \
              confidence, source_closet, source_file, \
              source_drawer_id, adapter_name) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            turso::params![
                triple_id,
                subject_id,
                predicate,
                object_id,
                valid_from_value,
                valid_to_value,
                params.confidence,
                source_closet_value,
                source_file_value,
                source_drawer_id_value,
                adapter_name_value
            ],
        )
        .await?;
    Ok(())
}

/// Mark a relationship as ended (set `valid_to`).
/// Invalidate (end-date) matching triples and return the date that was stored.
pub async fn invalidate(
    connection: &Connection,
    subject: &str,
    predicate: &str,
    object: &str,
    ended: Option<&str>,
) -> Result<String> {
    assert!(!subject.is_empty(), "invalidate: subject must not be empty");
    assert!(
        !predicate.is_empty(),
        "invalidate: predicate must not be empty"
    );
    assert!(!object.is_empty(), "invalidate: object must not be empty");

    let subject_id = entity_id(subject);
    let object_id = entity_id(object);
    let predicate_normalized = predicate.to_lowercase().replace(' ', "_");

    // Pair assertions: entity_id can return "" for apostrophe-only inputs.
    // An empty normalized ID would silently run an UPDATE with a blank key.
    assert!(
        !subject_id.is_empty(),
        "invalidate: subject normalizes to empty ID"
    );
    assert!(
        !object_id.is_empty(),
        "invalidate: object normalizes to empty ID"
    );
    assert!(
        !predicate_normalized.is_empty(),
        "invalidate: predicate normalizes to empty ID"
    );

    // Resolve the date once so the returned value always matches what was written.
    let raw_ended = ended.map_or_else(
        || chrono::Local::now().format("%Y-%m-%d").to_string(),
        str::to_string,
    );
    let persisted_ended = sanitize_iso_temporal(Some(&raw_ended), "ended")?
        .filter(|value| !value.is_empty())
        .unwrap_or(raw_ended);

    // Pre-flight: reject if `ended` would invert any matching active triple's
    // valid_from. Without this check, an UPDATE with `ended < valid_from`
    // creates a row that's invisible to every temporal KG query.
    invalidate_check_inverted_intervals(
        connection,
        &subject_id,
        &predicate_normalized,
        &object_id,
        &persisted_ended,
    )
    .await?;

    connection.execute(
        "UPDATE triples SET valid_to=?1 WHERE subject=?2 AND predicate=?3 AND object=?4 AND valid_to IS NULL",
        turso::params![persisted_ended.as_str(), subject_id.as_str(), predicate_normalized.as_str(), object_id.as_str()],
    )
    .await?;

    Ok(persisted_ended)
}

/// Reject `invalidate(...)` calls whose `ended` would create an inverted interval.
///
/// Scans the active triples that the UPDATE would touch and compares each
/// `valid_from` against `ended` using temporal keys, so a date-only `ended` of the
/// same day as a datetime `valid_from` is treated as end-of-day rather than midnight.
async fn invalidate_check_inverted_intervals(
    connection: &Connection,
    subject_id: &str,
    predicate: &str,
    object_id: &str,
    ended: &str,
) -> Result<()> {
    assert!(!subject_id.is_empty());
    assert!(!predicate.is_empty());
    assert!(!object_id.is_empty());
    assert!(!ended.is_empty());

    let rows = db::query_all(
        connection,
        "SELECT valid_from FROM triples \
         WHERE subject=?1 AND predicate=?2 AND object=?3 AND valid_to IS NULL",
        turso::params![subject_id, predicate, object_id],
    )
    .await?;

    let ended_key = temporal_end_key(ended);
    for row in &rows {
        let Ok(value) = row.get_value(0) else {
            continue;
        };
        let Some(valid_from) = value.as_text() else {
            continue;
        };
        if valid_from.is_empty() {
            continue;
        }
        if ended_key < temporal_start_key(valid_from) {
            return Err(crate::error::Error::Other(format!(
                "valid_to={ended:?} is before valid_from={valid_from:?}; \
                 an inverted interval would be invisible to every KG query"
            )));
        }
    }
    Ok(())
}
