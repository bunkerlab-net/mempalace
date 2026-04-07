use turso::Connection;

/// Create an in-memory turso database with the full schema applied.
/// Returns (Database, Connection) tuple to keep the Database alive for the test lifetime.
pub async fn test_db() -> (turso::Database, Connection) {
    let db = turso::Builder::new_local(":memory:")
        .experimental_triggers(true)
        .build()
        .await
        .expect("failed to create in-memory db");
    let conn = db.connect().expect("failed to connect");
    crate::schema::ensure_schema(&conn)
        .await
        .expect("failed to apply schema");
    (db, conn)
}
