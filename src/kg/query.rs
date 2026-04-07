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

/// Query all relationships for an entity.
// Outgoing and incoming branches are intentionally parallel — splitting would duplicate
// the row-extraction logic or obscure the symmetric data flow.
#[allow(clippy::too_many_lines)]
pub async fn query_entity(
    conn: &Connection,
    name: &str,
    as_of: Option<&str>,
    direction: &str,
) -> Result<Vec<Fact>> {
    let eid = entity_id(name);
    let mut results = Vec::new();

    if direction == "outgoing" || direction == "both" {
        let (sql, params) = if let Some(date) = as_of {
            (
                "SELECT t.subject, t.predicate, t.object, t.valid_from, t.valid_to, t.confidence, e.name \
                 FROM triples t JOIN entities e ON t.object = e.id \
                 WHERE t.subject = ?1 \
                 AND (t.valid_from IS NULL OR t.valid_from <= ?2) \
                 AND (t.valid_to IS NULL OR t.valid_to >= ?3)".to_string(),
                vec![turso::Value::from(eid.as_str()), turso::Value::from(date), turso::Value::from(date)],
            )
        } else {
            (
                "SELECT t.subject, t.predicate, t.object, t.valid_from, t.valid_to, t.confidence, e.name \
                 FROM triples t JOIN entities e ON t.object = e.id \
                 WHERE t.subject = ?1".to_string(),
                vec![turso::Value::from(eid.as_str())],
            )
        };

        let rows = db::query_all(conn, &sql, turso::params_from_iter(params)).await?;
        for row in &rows {
            let obj_name = row
                .get_value(6)
                .ok()
                .and_then(|v| v.as_text().cloned())
                .unwrap_or_default();
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

            results.push(Fact {
                direction: "outgoing".to_string(),
                subject: name.to_string(),
                predicate,
                object: obj_name,
                current: valid_to.is_none(),
                valid_from,
                valid_to,
                confidence,
            });
        }
    }

    if direction == "incoming" || direction == "both" {
        let (sql, params) = if let Some(date) = as_of {
            (
                "SELECT t.subject, t.predicate, t.object, t.valid_from, t.valid_to, t.confidence, e.name \
                 FROM triples t JOIN entities e ON t.subject = e.id \
                 WHERE t.object = ?1 \
                 AND (t.valid_from IS NULL OR t.valid_from <= ?2) \
                 AND (t.valid_to IS NULL OR t.valid_to >= ?3)".to_string(),
                vec![turso::Value::from(eid.as_str()), turso::Value::from(date), turso::Value::from(date)],
            )
        } else {
            (
                "SELECT t.subject, t.predicate, t.object, t.valid_from, t.valid_to, t.confidence, e.name \
                 FROM triples t JOIN entities e ON t.subject = e.id \
                 WHERE t.object = ?1".to_string(),
                vec![turso::Value::from(eid.as_str())],
            )
        };

        let rows = db::query_all(conn, &sql, turso::params_from_iter(params)).await?;
        for row in &rows {
            let sub_name = row
                .get_value(6)
                .ok()
                .and_then(|v| v.as_text().cloned())
                .unwrap_or_default();
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

            results.push(Fact {
                direction: "incoming".to_string(),
                subject: sub_name,
                predicate,
                object: name.to_string(),
                current: valid_to.is_none(),
                valid_from,
                valid_to,
                confidence,
            });
        }
    }

    Ok(results)
}

/// Get chronological timeline of facts.
pub async fn timeline(conn: &Connection, entity: Option<&str>) -> Result<Vec<Fact>> {
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

    let rows = db::query_all(conn, &sql, turso::params_from_iter(params)).await?;
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
pub async fn stats(conn: &Connection) -> Result<KgStats> {
    let entity_rows = db::query_all(conn, "SELECT COUNT(*) FROM entities", ()).await?;
    let entities = entity_rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    let triple_rows = db::query_all(conn, "SELECT COUNT(*) FROM triples", ()).await?;
    let triples = triple_rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    let current_rows = db::query_all(
        conn,
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
        conn,
        "SELECT DISTINCT predicate FROM triples ORDER BY predicate",
        (),
    )
    .await?;
    let relationship_types: Vec<String> = pred_rows
        .iter()
        .filter_map(|r| r.get_value(0).ok().and_then(|v| v.as_text().cloned()))
        .collect();

    Ok(KgStats {
        entities,
        triples,
        current_facts: current,
        expired_facts: triples - current,
        relationship_types,
    })
}
