/// Verify inverted index search works with turso (FTS5 is not supported).
// Sequential setup → insert → query narrative; splitting into helpers would obscure the test flow.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_inverted_index_search() {
    let db = turso::Builder::new_local(":memory:")
        .build()
        .await
        .expect("failed to create db");

    let conn = db.connect().expect("failed to connect");

    conn.execute(
        "CREATE TABLE docs (id TEXT PRIMARY KEY, content TEXT NOT NULL, wing TEXT, room TEXT)",
        (),
    )
    .await
    .expect("failed to create docs table");

    conn.execute(
        "CREATE TABLE doc_words (word TEXT NOT NULL, doc_id TEXT NOT NULL, count INTEGER DEFAULT 1, PRIMARY KEY (word, doc_id))",
        (),
    )
    .await
    .expect("failed to create doc_words table");

    conn.execute("CREATE INDEX idx_w ON doc_words(word)", ())
        .await
        .expect("failed to create index");

    // Insert docs
    conn.execute(
        "INSERT INTO docs VALUES ('1', 'the quick brown fox jumps over the lazy dog', 'animals', 'general')",
        (),
    )
    .await
    .expect("failed to insert doc 1");

    conn.execute(
        "INSERT INTO docs VALUES ('2', 'rust programming language is fast and safe', 'tech', 'backend')",
        (),
    )
    .await
    .expect("failed to insert doc 2");

    // Build inverted index for doc 1
    for (word, count) in [
        ("quick", 1),
        ("brown", 1),
        ("fox", 1),
        ("jumps", 1),
        ("lazy", 1),
        ("dog", 1),
    ] {
        conn.execute(
            "INSERT INTO doc_words (word, doc_id, count) VALUES (?1, '1', ?2)",
            turso::params![word, count],
        )
        .await
        .expect("failed to insert word for doc 1");
    }

    // Build inverted index for doc 2
    for (word, count) in [
        ("rust", 1),
        ("programming", 1),
        ("language", 1),
        ("fast", 1),
        ("safe", 1),
    ] {
        conn.execute(
            "INSERT INTO doc_words (word, doc_id, count) VALUES (?1, '2', ?2)",
            turso::params![word, count],
        )
        .await
        .expect("failed to insert word for doc 2");
    }

    // Search for "fox" — should find doc 1
    let mut rows = conn
        .query(
            "SELECT d.id, d.content, SUM(dw.count) as relevance FROM docs d JOIN doc_words dw ON d.id = dw.doc_id WHERE dw.word IN ('fox') GROUP BY d.id ORDER BY relevance DESC",
            (),
        )
        .await
        .expect("failed to query for 'fox'");

    let row = rows
        .next()
        .await
        .expect("failed to advance rows")
        .expect("no results for 'fox'");
    assert_eq!(
        row.get_value(0)
            .expect("missing column 0")
            .as_text()
            .expect("column 0 not text"),
        "1"
    );

    // Search for "rust fast" — should find doc 2
    let mut rows = conn
        .query(
            "SELECT d.id, d.content, SUM(dw.count) as relevance FROM docs d JOIN doc_words dw ON d.id = dw.doc_id WHERE dw.word IN ('rust', 'fast') GROUP BY d.id ORDER BY relevance DESC",
            (),
        )
        .await
        .expect("failed to query for 'rust fast'");

    let row = rows
        .next()
        .await
        .expect("failed to advance rows")
        .expect("no results for 'rust fast'");
    assert_eq!(
        row.get_value(0)
            .expect("missing column 0")
            .as_text()
            .expect("column 0 not text"),
        "2"
    );
    let relevance = row.get_value(2).expect("missing relevance column");
    assert_eq!(*relevance.as_integer().expect("relevance not integer"), 2); // matched 2 words

    // Search for "elephant" — should return nothing
    let mut rows = conn
        .query(
            "SELECT d.id FROM docs d JOIN doc_words dw ON d.id = dw.doc_id WHERE dw.word IN ('elephant') GROUP BY d.id",
            (),
        )
        .await
        .expect("failed to query for 'elephant'");

    assert!(
        rows.next().await.expect("failed to advance rows").is_none(),
        "should find no results for 'elephant'"
    );

    println!("Inverted index search works correctly with turso");
}
