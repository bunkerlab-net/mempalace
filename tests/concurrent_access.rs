/// Verify that multiple processes can open the same database file concurrently
/// when `LIMBO_DISABLE_FILE_LOCK` is set. Regression test for
/// <https://github.com/bunkerlab-net/mempalace/issues/9>
#[allow(unsafe_code)]
#[tokio::test]
async fn two_connections_to_same_file() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_path = dir.path().join("palace.db");
    let path_str = db_path.to_str().expect("non-utf8 path");

    // Disable turso's exclusive file lock (same as open_db does at runtime).
    unsafe {
        std::env::set_var("LIMBO_DISABLE_FILE_LOCK", "1");
    }

    // First "process" opens the database and holds the connection.
    let db1 = turso::Builder::new_local(path_str)
        .build()
        .await
        .expect("first open failed");
    let conn1 = db1.connect().expect("first connect failed");
    let mut rows = conn1
        .query("PRAGMA journal_mode=WAL", ())
        .await
        .expect("WAL pragma failed");
    while rows.next().await.expect("row error").is_some() {}

    conn1
        .execute(
            "CREATE TABLE IF NOT EXISTS test_table (id TEXT PRIMARY KEY, val TEXT)",
            (),
        )
        .await
        .expect("create table failed");

    // Second "process" opens the same file — this would fail without the fix.
    let db2 = turso::Builder::new_local(path_str)
        .build()
        .await
        .expect("second open failed — file lock not disabled?");
    let conn2 = db2.connect().expect("second connect failed");

    conn2
        .execute(
            "INSERT INTO test_table (id, val) VALUES ('k1', 'hello')",
            (),
        )
        .await
        .expect("insert from second connection failed");

    // Verify the first connection can read the write.
    let mut read_rows = conn1
        .query("SELECT val FROM test_table WHERE id = 'k1'", ())
        .await
        .expect("select failed");
    let row = read_rows
        .next()
        .await
        .expect("row error")
        .expect("expected one row");
    let val: String = row.get(0).expect("get column failed");
    assert_eq!(val, "hello");
}
