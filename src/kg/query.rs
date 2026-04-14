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
    // Validate caller-supplied parameters: these come from external (MCP) input
    // so invalid values are operating errors, not programmer bugs.
    if direction != "outgoing" && direction != "incoming" && direction != "both" {
        return Err(crate::error::Error::Other(format!(
            "direction must be outgoing, incoming, or both — got \"{direction}\""
        )));
    }
    if name.is_empty() {
        return Err(crate::error::Error::Other(
            "entity name must not be empty".to_string(),
        ));
    }

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
    // Postcondition: expired + current must equal total (tautologically true by construction).
    debug_assert!(
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

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::kg::{TripleParams, add_entity, add_triple};

    /// Seed a single "Alice knows Bob" triple for reuse across tests.
    async fn seed_alice_knows_bob(connection: &turso::Connection) {
        add_entity(connection, "Alice", "person", None)
            .await
            .expect("seed: add_entity Alice should succeed");
        add_entity(connection, "Bob", "person", None)
            .await
            .expect("seed: add_entity Bob should succeed");
        add_triple(
            connection,
            &TripleParams {
                subject: "Alice",
                predicate: "knows",
                object: "Bob",
                valid_from: Some("2024-01-01"),
                valid_to: None,
                confidence: 1.0,
                source_closet: None,
                source_file: None,
            },
        )
        .await
        .expect("seed: add_triple Alice->knows->Bob should succeed");
    }

    #[tokio::test]
    async fn query_entity_outgoing() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_alice_knows_bob(&conn).await;

        let facts = query_entity(&conn, "Alice", None, "outgoing")
            .await
            .expect("query_entity outgoing should succeed for seeded entity");
        assert_eq!(facts.len(), 1, "Alice should have exactly 1 outgoing fact");
        assert_eq!(facts[0].predicate, "knows");
        assert_eq!(facts[0].direction, "outgoing");
    }

    #[tokio::test]
    async fn query_entity_incoming() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_alice_knows_bob(&conn).await;

        let facts = query_entity(&conn, "Bob", None, "incoming")
            .await
            .expect("query_entity incoming should succeed for seeded entity");
        assert_eq!(facts.len(), 1, "Bob should have exactly 1 incoming fact");
        assert_eq!(facts[0].predicate, "knows");
        assert_eq!(facts[0].direction, "incoming");
    }

    #[tokio::test]
    async fn query_entity_both_directions() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_alice_knows_bob(&conn).await;

        // Alice has 1 outgoing, 0 incoming — "both" should still return 1
        let facts = query_entity(&conn, "Alice", None, "both")
            .await
            .expect("query_entity both should succeed for seeded entity");
        assert!(!facts.is_empty(), "both should return at least one fact");
        assert_eq!(facts.len(), 1);
    }

    #[tokio::test]
    async fn query_entity_nonexistent() {
        let (_db, conn) = crate::test_helpers::test_db().await;

        let facts = query_entity(&conn, "NoSuchEntity", None, "both")
            .await
            .expect("query_entity should succeed even for unknown entity");
        assert!(facts.is_empty(), "unknown entity should return no facts");
        assert_eq!(facts.len(), 0);
    }

    #[tokio::test]
    async fn timeline_all_entities() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_alice_knows_bob(&conn).await;
        add_triple(
            &conn,
            &TripleParams {
                subject: "Bob",
                predicate: "works_at",
                object: "Acme",
                valid_from: Some("2024-02-01"),
                valid_to: None,
                confidence: 1.0,
                source_closet: None,
                source_file: None,
            },
        )
        .await
        .expect("seed: add_triple Bob->works_at->Acme should succeed");

        let facts = timeline(&conn, None)
            .await
            .expect("timeline(None) should succeed with seeded data");
        assert!(facts.len() >= 2, "timeline should contain at least 2 facts");
        // Timeline is ordered by valid_from ASC
        assert_eq!(facts[0].valid_from.as_deref(), Some("2024-01-01"));
    }

    #[tokio::test]
    async fn timeline_filtered_by_entity() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_alice_knows_bob(&conn).await;
        add_triple(
            &conn,
            &TripleParams {
                subject: "Carol",
                predicate: "likes",
                object: "Rust",
                valid_from: Some("2024-03-01"),
                valid_to: None,
                confidence: 1.0,
                source_closet: None,
                source_file: None,
            },
        )
        .await
        .expect("seed: add_triple Carol->likes->Rust should succeed");

        let facts = timeline(&conn, Some("Alice"))
            .await
            .expect("timeline(Some('Alice')) should succeed with seeded data");
        assert_eq!(facts.len(), 1, "only Alice's triple should appear");
        assert_eq!(facts[0].subject, "Alice");
    }

    #[tokio::test]
    async fn stats_empty() {
        let (_db, conn) = crate::test_helpers::test_db().await;

        let s = stats(&conn)
            .await
            .expect("stats should succeed on empty database");
        assert_eq!(s.entities, 0, "fresh DB should have 0 entities");
        assert_eq!(s.triples, 0, "fresh DB should have 0 triples");
    }

    #[tokio::test]
    async fn stats_with_data() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        seed_alice_knows_bob(&conn).await;

        let s = stats(&conn)
            .await
            .expect("stats should succeed after seeding data");
        assert!(s.entities > 0, "seeded DB should have entities");
        assert!(s.triples > 0, "seeded DB should have triples");
        assert_eq!(s.current_facts, s.triples, "no expired facts yet");
    }
}
