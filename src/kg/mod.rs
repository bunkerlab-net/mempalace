//! Temporal knowledge graph — entities, relationship triples, and time-bounded facts.

pub mod query;

use turso::Connection;

use crate::db;
use crate::error::Result;

/// Normalize an entity name to an ID: lowercase, spaces→underscores, strip apostrophes.
pub fn entity_id(name: &str) -> String {
    name.to_lowercase().replace(' ', "_").replace('\'', "")
}

#[cfg(test)]
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
}

#[cfg(test)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn add_entity_inserts_and_returns_id() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let id = add_entity(&conn, "Alice Smith", "person", None)
            .await
            .expect("add_entity");
        assert_eq!(id, "alice_smith");

        let rows = crate::db::query_all(
            &conn,
            "SELECT name FROM entities WHERE id = ?1",
            turso::params!["alice_smith"],
        )
        .await
        .expect("query");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn add_triple_creates_entities_automatically() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let _tid = add_triple(
            &conn,
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
        .expect("add_triple");

        // Both entities should exist
        let entities = crate::db::query_all(&conn, "SELECT id FROM entities ORDER BY id", ())
            .await
            .expect("query");
        assert_eq!(entities.len(), 2);
    }

    #[tokio::test]
    async fn add_triple_dedup_returns_existing_id() {
        let (_db, conn) = crate::test_helpers::test_db().await;
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
        let id1 = add_triple(&conn, &params).await.expect("first");
        let id2 = add_triple(&conn, &params).await.expect("second");
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn invalidate_sets_valid_to() {
        let (_db, conn) = crate::test_helpers::test_db().await;
        let _tid = add_triple(
            &conn,
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
        .expect("add");

        invalidate(&conn, "Carol", "works at", "OldCo", Some("2024-06-01"))
            .await
            .expect("invalidate");

        let rows = crate::db::query_all(
            &conn,
            "SELECT valid_to FROM triples WHERE subject = 'carol' AND predicate = 'works_at'",
            (),
        )
        .await
        .expect("query");
        assert_eq!(rows.len(), 1);
        let vt: String = rows[0].get(0).expect("get valid_to");
        assert_eq!(vt, "2024-06-01");
    }
}

/// Add or update an entity node.
#[allow(dead_code)]
pub async fn add_entity(
    conn: &Connection,
    name: &str,
    entity_type: &str,
    properties: Option<&str>,
) -> Result<String> {
    let eid = entity_id(name);
    let props = properties.unwrap_or("{}");
    conn.execute(
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

/// Add a relationship triple. Auto-creates entities if they don't exist.
/// Returns the triple ID.
pub async fn add_triple(conn: &Connection, p: &TripleParams<'_>) -> Result<String> {
    let sub_id = entity_id(p.subject);
    let obj_id = entity_id(p.object);
    let pred = p.predicate.to_lowercase().replace(' ', "_");

    // Auto-create entities
    conn.execute(
        "INSERT OR IGNORE INTO entities (id, name) VALUES (?1, ?2)",
        turso::params![sub_id.as_str(), p.subject],
    )
    .await?;
    conn.execute(
        "INSERT OR IGNORE INTO entities (id, name) VALUES (?1, ?2)",
        turso::params![obj_id.as_str(), p.object],
    )
    .await?;

    // Check for existing identical active triple
    let existing = db::query_all(
        conn,
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

    let vf: turso::Value = match p.valid_from {
        Some(v) => turso::Value::from(v),
        None => turso::Value::Null,
    };
    let vt: turso::Value = match p.valid_to {
        Some(v) => turso::Value::from(v),
        None => turso::Value::Null,
    };
    let sc: turso::Value = match p.source_closet {
        Some(v) => turso::Value::from(v),
        None => turso::Value::Null,
    };
    let sf: turso::Value = match p.source_file {
        Some(v) => turso::Value::from(v),
        None => turso::Value::Null,
    };

    conn.execute(
        "INSERT INTO triples (id, subject, predicate, object, valid_from, valid_to, confidence, source_closet, source_file) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        turso::params![triple_id.as_str(), sub_id.as_str(), pred.as_str(), obj_id.as_str(), vf, vt, p.confidence, sc, sf],
    )
    .await?;

    Ok(triple_id)
}

/// Mark a relationship as ended (set `valid_to`).
pub async fn invalidate(
    conn: &Connection,
    subject: &str,
    predicate: &str,
    object: &str,
    ended: Option<&str>,
) -> Result<()> {
    let sub_id = entity_id(subject);
    let obj_id = entity_id(object);
    let pred = predicate.to_lowercase().replace(' ', "_");
    let ended = ended.map_or_else(
        || chrono::Local::now().format("%Y-%m-%d").to_string(),
        std::string::ToString::to_string,
    );

    conn.execute(
        "UPDATE triples SET valid_to=?1 WHERE subject=?2 AND predicate=?3 AND object=?4 AND valid_to IS NULL",
        turso::params![ended.as_str(), sub_id.as_str(), pred.as_str(), obj_id.as_str()],
    )
    .await?;

    Ok(())
}
