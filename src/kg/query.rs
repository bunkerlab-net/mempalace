//! Knowledge graph query operations — entity lookup, timeline, and statistics.

use serde::Serialize;
use turso::Connection;

use super::entity_id;
use crate::db;
use crate::error::Result;

/// A single fact (triple) returned from a knowledge graph query.
#[derive(Debug, Serialize)]
pub struct Fact {
    /// Whether this is an `"outgoing"` or `"incoming"` relationship.
    pub direction: String,
    /// The subject entity name.
    pub subject: String,
    /// The relationship type.
    pub predicate: String,
    /// The object entity name.
    pub object: String,
    /// When this fact became true.
    pub valid_from: Option<String>,
    /// When this fact stopped being true (`None` if still current).
    pub valid_to: Option<String>,
    /// Confidence score (0.0–1.0).
    pub confidence: f64,
    /// Whether this fact is currently active (no `valid_to`).
    pub current: bool,
}

/// Aggregate statistics about the knowledge graph.
#[derive(Debug, Serialize)]
pub struct KgStats {
    /// Total entity count.
    pub entities: i64,
    /// Total triple count.
    pub triples: i64,
    /// Triples with no `valid_to` (still active).
    pub current_facts: i64,
    /// Triples with a `valid_to` set.
    pub expired_facts: i64,
    /// Distinct predicate values across all triples.
    pub relationship_types: Vec<String>,
}

/// Extract a Fact from a query row (columns: subject, predicate, object,
/// `valid_from`, `valid_to`, confidence, `joined_name`).
fn query_entity_row_to_fact(
    row: &turso::Row,
    direction_label: &str,
    subject: &str,
    object: &str,
) -> Fact {
    let predicate = row
        .get_value(1)
        .ok()
        .and_then(|v| v.as_text().cloned())
        .unwrap_or_default();
    let valid_from = row.get_value(3).ok().and_then(|v| v.as_text().cloned());
    let valid_to = row.get_value(4).ok().and_then(|v| v.as_text().cloned());
    let confidence = row
        .get_value(5)
        .ok()
        .and_then(|v| v.as_real().copied())
        .unwrap_or(1.0);

    Fact {
        direction: direction_label.to_string(),
        subject: subject.to_string(),
        predicate,
        object: object.to_string(),
        current: valid_to.is_none(),
        valid_from,
        valid_to,
        confidence,
    }
}

/// Build the SQL and params for one direction of a `query_entity` call.
fn query_entity_sql(
    eid: &str,
    as_of: Option<&str>,
    join_col: &str,
    filter_col: &str,
) -> (String, Vec<turso::Value>) {
    let base = format!(
        "SELECT t.subject, t.predicate, t.object, t.valid_from, t.valid_to, t.confidence, e.name \
         FROM triples t JOIN entities e ON t.{join_col} = e.id \
         WHERE t.{filter_col} = ?1"
    );
    if let Some(date) = as_of {
        let sql = format!(
            "{base} AND (t.valid_from IS NULL OR t.valid_from <= ?2) \
             AND (t.valid_to IS NULL OR t.valid_to >= ?3)"
        );
        let params = vec![
            turso::Value::from(eid),
            turso::Value::from(date),
            turso::Value::from(date),
        ];
        (sql, params)
    } else {
        (base, vec![turso::Value::from(eid)])
    }
}

/// Query all relationships for an entity.
pub async fn query_entity(
    connection: &Connection,
    name: &str,
    as_of: Option<&str>,
    direction: &str,
) -> Result<Vec<Fact>> {
    assert!(
        direction == "outgoing" || direction == "incoming" || direction == "both",
        "direction must be outgoing, incoming, or both — got \"{direction}\""
    );
    assert!(!name.is_empty(), "entity name must not be empty");

    let eid = entity_id(name);
    let mut results = Vec::new();

    if direction == "outgoing" || direction == "both" {
        let (sql, params) = query_entity_sql(&eid, as_of, "object", "subject");
        let rows = db::query_all(connection, &sql, turso::params_from_iter(params)).await?;
        for row in &rows {
            let obj_name = row
                .get_value(6)
                .ok()
                .and_then(|v| v.as_text().cloned())
                .unwrap_or_default();
            results.push(query_entity_row_to_fact(row, "outgoing", name, &obj_name));
        }
    }

    if direction == "incoming" || direction == "both" {
        let (sql, params) = query_entity_sql(&eid, as_of, "subject", "object");
        let rows = db::query_all(connection, &sql, turso::params_from_iter(params)).await?;
        for row in &rows {
            let sub_name = row
                .get_value(6)
                .ok()
                .and_then(|v| v.as_text().cloned())
                .unwrap_or_default();
            results.push(query_entity_row_to_fact(row, "incoming", &sub_name, name));
        }
    }

    Ok(results)
}

/// Get chronological timeline of facts.
pub async fn timeline(connection: &Connection, entity: Option<&str>) -> Result<Vec<Fact>> {
    let (sql, params) = if let Some(name) = entity {
        let eid = entity_id(name);
        (
            "SELECT t.predicate, t.valid_from, t.valid_to, s.name, o.name \
             FROM triples t \
             JOIN entities s ON t.subject = s.id \
             JOIN entities o ON t.object = o.id \
             WHERE t.subject = ?1 OR t.object = ?1 \
             ORDER BY t.valid_from ASC"
                .to_string(),
            vec![turso::Value::from(eid.as_str())],
        )
    } else {
        (
            "SELECT t.predicate, t.valid_from, t.valid_to, s.name, o.name \
             FROM triples t \
             JOIN entities s ON t.subject = s.id \
             JOIN entities o ON t.object = o.id \
             ORDER BY t.valid_from ASC \
             LIMIT 100"
                .to_string(),
            vec![],
        )
    };

    let rows = db::query_all(connection, &sql, turso::params_from_iter(params)).await?;
    let mut facts = Vec::new();

    for row in &rows {
        let predicate = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let valid_from = row.get_value(1).ok().and_then(|v| v.as_text().cloned());
        let valid_to = row.get_value(2).ok().and_then(|v| v.as_text().cloned());
        let sub_name = row
            .get_value(3)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let obj_name = row
            .get_value(4)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();

        facts.push(Fact {
            direction: "outgoing".to_string(),
            subject: sub_name,
            predicate,
            object: obj_name,
            current: valid_to.is_none(),
            valid_from,
            valid_to,
            confidence: 1.0,
        });
    }

    Ok(facts)
}

/// Knowledge graph stats.
pub async fn stats(connection: &Connection) -> Result<KgStats> {
    let entity_rows = db::query_all(connection, "SELECT COUNT(*) FROM entities", ()).await?;
    let entities = entity_rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    let triple_rows = db::query_all(connection, "SELECT COUNT(*) FROM triples", ()).await?;
    let triples = triple_rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    let current_rows = db::query_all(
        connection,
        "SELECT COUNT(*) FROM triples WHERE valid_to IS NULL",
        (),
    )
    .await?;
    let current = current_rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    let pred_rows = db::query_all(
        connection,
        "SELECT DISTINCT predicate FROM triples ORDER BY predicate",
        (),
    )
    .await?;
    let relationship_types: Vec<String> = pred_rows
        .iter()
        .filter_map(|r| r.get_value(0).ok().and_then(|v| v.as_text().cloned()))
        .collect();

    let expired = triples - current;
    // Postcondition: expired + current must equal total triples.
    assert!(
        expired + current == triples,
        "expired ({expired}) + current ({current}) must equal total ({triples})"
    );

    Ok(KgStats {
        entities,
        triples,
        current_facts: current,
        expired_facts: expired,
        relationship_types,
    })
}
