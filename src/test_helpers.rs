use turso::Connection;

/// Create an in-memory turso database with the full schema applied.
pub async fn test_db() -> Connection {
    let db = turso::Builder::new_local(":memory:")
        .build()
        .await
        .expect("failed to create in-memory db");
    let conn = db.connect().expect("failed to connect");
    crate::schema::ensure_schema(&conn)
        .await
        .expect("failed to apply schema");
    // Leak the Database so the Connection stays valid for the test lifetime.
    std::mem::forget(db);
    conn
}
