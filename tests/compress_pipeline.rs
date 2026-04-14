// Integration test — .expect() is acceptable with a descriptive message.
#![allow(clippy::expect_used)]

use std::collections::HashMap;

use mempalace::dialect::Dialect;

/// Compress a verbose paragraph and verify the output is shorter than the input.
/// The AAAK dialect extracts topics, entities, and key sentences — the output
/// should always be substantially smaller than conversational prose.
#[test]
fn compress_reduces_content_size() {
    let dialect = Dialect::empty();

    let verbose = "We had a long meeting today where we discussed the architecture \
        of the new microservices platform. The team decided to use GraphQL instead \
        of REST because it provides better flexibility for the frontend developers. \
        We also talked about switching from PostgreSQL to CockroachDB for better \
        horizontal scaling. The deployment pipeline needs to be updated to support \
        the new container orchestration system.";

    let compressed = dialect.compress(verbose, None);

    assert!(
        compressed.len() < verbose.len(),
        "compressed ({} chars) should be shorter than original ({} chars)",
        compressed.len(),
        verbose.len()
    );
    // The compressed output should contain the AAAK format marker.
    assert!(
        compressed.contains("0:"),
        "compressed output should contain AAAK entity marker '0:'"
    );
}

/// Create a Dialect with known entity mappings, compress text mentioning
/// those entities, and verify the short codes appear in the output.
#[test]
fn dialect_with_entities_replaces_names() {
    let mut entities = HashMap::new();
    entities.insert("Alice".to_string(), "ALC".to_string());
    entities.insert("Bob".to_string(), "BOB".to_string());
    let dialect = Dialect::new(&entities, vec![]);

    let text = "Alice and Bob discussed the new database migration strategy \
        for the quarterly planning review session yesterday afternoon.";

    let compressed = dialect.compress(text, None);

    assert!(
        compressed.contains("ALC"),
        "compressed output should contain Alice's short code 'ALC'"
    );
    assert!(
        compressed.contains("BOB"),
        "compressed output should contain Bob's short code 'BOB'"
    );
}
