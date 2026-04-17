//! Temporal knowledge graph — entities, relationship triples, and time-bounded facts.

pub mod query;

use turso::Connection;

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
        };
        let result = add_triple_validate_params(&params);
        assert!(
            result.is_err(),
            "valid_from after valid_to must be rejected"
        );
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("must not be after")),
            "error message must explain the temporal ordering constraint"
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
            },
        )
        .await
        .expect("add_triple should succeed for valid subject/predicate/object params");

        // Both entities should exist
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
#[allow(dead_code)]
pub async fn add_entity(
    connection: &Connection,
    name: &str,
    entity_type: &str,
    properties: Option<&str>,
) -> Result<String> {
    let eid = entity_id(name);
    // entity_id can return "" for inputs like "'"; reject before writing a blank key.
    if eid.is_empty() {
        return Err(crate::error::Error::Other(
            "empty normalized entity id".to_string(),
        ));
    }
    let props = properties.unwrap_or("{}");
    connection
        .execute(
            "INSERT OR REPLACE INTO entities (id, name, type, properties) VALUES (?1, ?2, ?3, ?4)",
            turso::params![eid.as_str(), name, entity_type, props],
        )
        .await?;
    Ok(eid)
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
}

/// Validate confidence and ISO date fields on a `TripleParams`.
/// Called by `add_triple` to keep that function within the 70-line limit.
fn add_triple_validate_params(params: &TripleParams<'_>) -> Result<()> {
    if params.confidence.is_nan() || params.confidence < 0.0 || params.confidence > 1.0 {
        return Err(crate::error::Error::Other(format!(
            "confidence must be between 0.0 and 1.0, got {}",
            params.confidence
        )));
    }
    let valid_from_date = params
        .valid_from
        .map(|v| {
            chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d")
                .map_err(|_| crate::error::Error::Other(format!("invalid valid_from date: {v}")))
        })
        .transpose()?;
    let valid_to_date = params
        .valid_to
        .map(|v| {
            chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d")
                .map_err(|_| crate::error::Error::Other(format!("invalid valid_to date: {v}")))
        })
        .transpose()?;
    if let (Some(from), Some(to)) = (valid_from_date, valid_to_date)
        && from > to
    {
        return Err(crate::error::Error::Other(format!(
            "valid_from ({}) must not be after valid_to ({})",
            params.valid_from.unwrap_or(""),
            params.valid_to.unwrap_or(""),
        )));
    }
    Ok(())
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

    add_triple_validate_params(params)?;

    let sub_id = entity_id(params.subject);
    let obj_id = entity_id(params.object);
    let pred = params.predicate.to_lowercase().replace(' ', "_");

    // Pair assertions: entity_id can return "" for inputs like "'".
    // An empty normalized ID would silently corrupt the graph with a blank key.
    assert!(!sub_id.is_empty(), "triple subject normalizes to empty ID");
    assert!(!obj_id.is_empty(), "triple object normalizes to empty ID");
    assert!(!pred.is_empty(), "triple predicate normalizes to empty ID");

    // Auto-create entities
    connection
        .execute(
            "INSERT OR IGNORE INTO entities (id, name) VALUES (?1, ?2)",
            turso::params![sub_id.as_str(), params.subject],
        )
        .await?;
    connection
        .execute(
            "INSERT OR IGNORE INTO entities (id, name) VALUES (?1, ?2)",
            turso::params![obj_id.as_str(), params.object],
        )
        .await?;

    // Check for existing identical active triple
    let existing = db::query_all(
        connection,
        "SELECT id FROM triples WHERE subject=?1 AND predicate=?2 AND object=?3 AND valid_to IS NULL",
        turso::params![sub_id.as_str(), pred.as_str(), obj_id.as_str()],
    )
    .await?;

    if let Some(row) = existing.first()
        && let Ok(val) = row.get_value(0)
        && let Some(id) = val.as_text()
    {
        return Ok(id.clone());
    }

    let triple_id = format!(
        "t_{sub_id}_{pred}_{obj_id}_{}",
        &uuid::Uuid::new_v4().to_string().replace('-', "")[..8]
    );

    let valid_from_value: turso::Value = match params.valid_from {
        Some(v) => turso::Value::from(v),
        None => turso::Value::Null,
    };
    let valid_to_value: turso::Value = match params.valid_to {
        Some(v) => turso::Value::from(v),
        None => turso::Value::Null,
    };
    let source_closet_value: turso::Value = match params.source_closet {
        Some(v) => turso::Value::from(v),
        None => turso::Value::Null,
    };
    let source_file_value: turso::Value = match params.source_file {
        Some(v) => turso::Value::from(v),
        None => turso::Value::Null,
    };

    // Postcondition: triple ID follows naming convention.
    assert!(triple_id.starts_with("t_"), "triple_id must start with t_");

    connection.execute(
        "INSERT INTO triples (id, subject, predicate, object, valid_from, valid_to, confidence, source_closet, source_file) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        turso::params![triple_id.as_str(), sub_id.as_str(), pred.as_str(), obj_id.as_str(), valid_from_value, valid_to_value, params.confidence, source_closet_value, source_file_value],
    )
    .await?;

    Ok(triple_id)
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

    let sub_id = entity_id(subject);
    let obj_id = entity_id(object);
    let pred = predicate.to_lowercase().replace(' ', "_");

    // Pair assertions: entity_id can return "" for apostrophe-only inputs.
    // An empty normalized ID would silently run an UPDATE with a blank key.
    assert!(
        !sub_id.is_empty(),
        "invalidate: subject normalizes to empty ID"
    );
    assert!(
        !obj_id.is_empty(),
        "invalidate: object normalizes to empty ID"
    );
    assert!(
        !pred.is_empty(),
        "invalidate: predicate normalizes to empty ID"
    );

    // Resolve the date once so the returned value always matches what was written.
    let persisted_ended = ended.map_or_else(
        || chrono::Local::now().format("%Y-%m-%d").to_string(),
        std::string::ToString::to_string,
    );

    connection.execute(
        "UPDATE triples SET valid_to=?1 WHERE subject=?2 AND predicate=?3 AND object=?4 AND valid_to IS NULL",
        turso::params![persisted_ended.as_str(), sub_id.as_str(), pred.as_str(), obj_id.as_str()],
    )
    .await?;

    Ok(persisted_ended)
}
