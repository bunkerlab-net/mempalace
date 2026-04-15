// Integration test — .expect() is acceptable with a descriptive message.
#![allow(clippy::expect_used)]

use mempalace::kg::query::{query_entity, stats, timeline};
use mempalace::kg::{TripleParams, add_entity, add_triple, invalidate};
use mempalace::test_helpers::test_db;

/// Add entities, create a triple, then query from both sides to verify
/// outgoing and incoming relationships are correctly stored and returned.
#[tokio::test]
async fn entity_triple_query_lifecycle() {
    let (_db, connection) = test_db().await;

    add_entity(&connection, "Alice", "person", None)
        .await
        .expect("add_entity Alice should succeed");
    add_entity(&connection, "Bob", "person", None)
        .await
        .expect("add_entity Bob should succeed");

    add_triple(
        &connection,
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
    .expect("add_triple Alice->knows->Bob should succeed");

    // Alice's outgoing facts should include "knows".
    let outgoing = query_entity(&connection, "Alice", None, "outgoing")
        .await
        .expect("query_entity outgoing should succeed");
    assert_eq!(outgoing.len(), 1, "Alice should have 1 outgoing fact");
    assert_eq!(outgoing[0].predicate, "knows");

    // Bob's incoming facts should include "knows".
    let incoming = query_entity(&connection, "Bob", None, "incoming")
        .await
        .expect("query_entity incoming should succeed");
    assert_eq!(incoming.len(), 1, "Bob should have 1 incoming fact");
    assert_eq!(incoming[0].predicate, "knows");
}

/// Add a triple, verify it appears in the timeline as current, invalidate it,
/// then verify the timeline shows a `valid_to` date.
#[tokio::test]
async fn invalidate_updates_timeline() {
    let (_db, connection) = test_db().await;

    add_triple(
        &connection,
        &TripleParams {
            subject: "Carol",
            predicate: "works at",
            object: "OldCo",
            valid_from: Some("2024-01-15"),
            valid_to: None,
            confidence: 0.9,
            source_closet: None,
            source_file: None,
        },
    )
    .await
    .expect("add_triple Carol->works_at->OldCo should succeed");

    // Before invalidation: timeline should show the fact as current.
    let before = timeline(&connection, Some("Carol"))
        .await
        .expect("timeline should succeed before invalidation");
    assert_eq!(before.len(), 1, "should have 1 timeline entry");
    assert!(
        before[0].valid_to.is_none(),
        "fact should be current (no valid_to)"
    );

    // Invalidate with explicit end date.
    invalidate(
        &connection,
        "Carol",
        "works at",
        "OldCo",
        Some("2024-06-01"),
    )
    .await
    .expect("invalidate should succeed");

    // After invalidation: timeline should show valid_to.
    let after = timeline(&connection, Some("Carol"))
        .await
        .expect("timeline should succeed after invalidation");
    assert_eq!(after.len(), 1, "should still have 1 timeline entry");
    assert_eq!(
        after[0].valid_to.as_deref(),
        Some("2024-06-01"),
        "valid_to should match the invalidation date"
    );
}

/// Verify stats counters track entities, triples, and expired facts accurately.
#[tokio::test]
async fn stats_reflect_operations() {
    let (_db, connection) = test_db().await;

    // Empty DB.
    let stats_empty = stats(&connection)
        .await
        .expect("stats on empty DB should succeed");
    assert_eq!(stats_empty.entities, 0, "fresh DB should have 0 entities");
    assert_eq!(stats_empty.triples, 0, "fresh DB should have 0 triples");

    // Add entities and a triple.
    add_entity(&connection, "Dave", "person", None)
        .await
        .expect("add_entity Dave should succeed");
    add_entity(&connection, "Eve", "person", None)
        .await
        .expect("add_entity Eve should succeed");
    add_triple(
        &connection,
        &TripleParams {
            subject: "Dave",
            predicate: "likes",
            object: "Eve",
            valid_from: None,
            valid_to: None,
            confidence: 1.0,
            source_closet: None,
            source_file: None,
        },
    )
    .await
    .expect("add_triple Dave->likes->Eve should succeed");

    let stats_after_add = stats(&connection)
        .await
        .expect("stats after adds should succeed");
    assert_eq!(stats_after_add.entities, 2, "should have 2 entities");
    assert_eq!(stats_after_add.triples, 1, "should have 1 triple");
    assert_eq!(
        stats_after_add.current_facts, 1,
        "1 current fact before invalidation"
    );

    // Invalidate the triple.
    invalidate(&connection, "Dave", "likes", "Eve", Some("2025-01-01"))
        .await
        .expect("invalidate should succeed");

    let stats_after_invalidate = stats(&connection)
        .await
        .expect("stats after invalidate should succeed");
    assert_eq!(
        stats_after_invalidate.triples, 1,
        "total triples unchanged after invalidation"
    );
    assert_eq!(
        stats_after_invalidate.current_facts, 0,
        "no current facts after invalidation"
    );
    assert_eq!(
        stats_after_invalidate.expired_facts, 1,
        "1 expired fact after invalidation"
    );
}

/// Create multiple triples between the same pair of entities and verify
/// all are returned by `query_entity`.
#[tokio::test]
async fn multiple_triples_same_entities() {
    let (_db, connection) = test_db().await;

    add_entity(&connection, "Frank", "person", None)
        .await
        .expect("add_entity Frank should succeed");
    add_entity(&connection, "Grace", "person", None)
        .await
        .expect("add_entity Grace should succeed");

    // Add three distinct relationships between the same pair.
    for predicate in &["knows", "works with", "mentors"] {
        add_triple(
            &connection,
            &TripleParams {
                subject: "Frank",
                predicate,
                object: "Grace",
                valid_from: None,
                valid_to: None,
                confidence: 1.0,
                source_closet: None,
                source_file: None,
            },
        )
        .await
        .expect("add_triple should succeed for each predicate");
    }

    let facts = query_entity(&connection, "Frank", None, "outgoing")
        .await
        .expect("query_entity outgoing should succeed");
    assert_eq!(facts.len(), 3, "Frank should have 3 outgoing facts");

    let predicates: Vec<&str> = facts.iter().map(|f| f.predicate.as_str()).collect();
    assert!(predicates.contains(&"knows"), "should contain 'knows'");
    // Predicates are normalized on write: spaces become underscores.
    assert!(
        predicates.contains(&"works_with"),
        "should contain 'works_with' (normalized from 'works with')"
    );
    assert!(predicates.contains(&"mentors"), "should contain 'mentors'");
}
