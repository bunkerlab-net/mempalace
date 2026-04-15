// Integration test — .expect() is acceptable with a descriptive message.
#![allow(clippy::expect_used)]

use mempalace::mcp::tools::dispatch;
use mempalace::test_helpers::test_db;
use serde_json::json;

/// Add a drawer via dispatch, search for it, delete it, then verify search
/// returns nothing. Exercises the full CRUD path through the MCP tool layer.
#[tokio::test]
async fn full_lifecycle_add_search_delete() {
    let wal_dir = tempfile::tempdir().expect("failed to create WAL temp dir");
    // Each test gets its own temp dir so WAL writes are isolated even when tests
    // run in the same process. async_with_vars holds a mutex for the duration of
    // the future, preventing env-var races between concurrent Tokio tasks.
    temp_env::async_with_vars([("MEMPALACE_DIR", Some(wal_dir.path()))], async {
        let (_db, connection) = test_db().await;

        // Add a drawer with distinctive content.
        let add_result = dispatch(
            &connection,
            "mempalace_add_drawer",
            &json!({
                "wing": "testproject",
                "room": "backend",
                "content": "rust programming language performance benchmarks"
            }),
        )
        .await;
        assert_eq!(add_result["success"], true, "add_drawer should succeed");
        let drawer_id = add_result["drawer_id"]
            .as_str()
            .expect("drawer_id should be a string")
            .to_string();
        assert!(
            drawer_id.starts_with("drawer_"),
            "drawer_id should have drawer_ prefix"
        );

        // Search should find the drawer.
        let search_result = dispatch(
            &connection,
            "mempalace_search",
            &json!({"query": "rust programming benchmarks"}),
        )
        .await;
        let count = search_result["count"]
            .as_i64()
            .expect("count should be i64");
        assert!(count >= 1, "search should find the drawer we just added");

        // Delete the drawer.
        let delete_result = dispatch(
            &connection,
            "mempalace_delete_drawer",
            &json!({"drawer_id": drawer_id}),
        )
        .await;
        assert_eq!(delete_result["success"], true, "delete should succeed");

        // Search again — should find nothing.
        let search_after = dispatch(
            &connection,
            "mempalace_search",
            &json!({"query": "rust programming benchmarks"}),
        )
        .await;
        let after_count = search_after["count"].as_i64().expect("count should be i64");
        assert_eq!(after_count, 0, "search should return 0 after deletion");
    })
    .await;
}

/// Dispatch status after adding drawers to verify counters track operations.
#[tokio::test]
async fn status_reflects_drawer_operations() {
    let wal_dir = tempfile::tempdir().expect("failed to create WAL temp dir");
    temp_env::async_with_vars([("MEMPALACE_DIR", Some(wal_dir.path()))], async {
        let (_db, connection) = test_db().await;

        // Empty palace should report 0 drawers.
        let status0 = dispatch(&connection, "mempalace_status", &json!({})).await;
        assert_eq!(
            status0["total_drawers"], 0,
            "fresh DB should have 0 drawers"
        );

        // Add first drawer.
        let add_result_first = dispatch(
            &connection,
            "mempalace_add_drawer",
            &json!({
                "wing": "projA",
                "room": "general",
                "content": "first drawer content here with enough words"
            }),
        )
        .await;
        assert_eq!(
            add_result_first["success"], true,
            "first add should succeed"
        );

        let status1 = dispatch(&connection, "mempalace_status", &json!({})).await;
        assert_eq!(
            status1["total_drawers"], 1,
            "should have 1 drawer after first add"
        );

        // Add second drawer (different content so the deterministic ID differs).
        let add_result_second = dispatch(
            &connection,
            "mempalace_add_drawer",
            &json!({
                "wing": "projA",
                "room": "general",
                "content": "second drawer with completely different content"
            }),
        )
        .await;
        assert_eq!(
            add_result_second["success"], true,
            "second add should succeed"
        );

        let status2 = dispatch(&connection, "mempalace_status", &json!({})).await;
        assert_eq!(
            status2["total_drawers"], 2,
            "should have 2 drawers after second add"
        );
    })
    .await;
}

/// Add 5 drawers and page through them with limit/offset.
#[tokio::test]
async fn list_drawers_pagination_workflow() {
    let wal_dir = tempfile::tempdir().expect("failed to create WAL temp dir");
    temp_env::async_with_vars([("MEMPALACE_DIR", Some(wal_dir.path()))], async {
        let (_db, connection) = test_db().await;

        // Seed 5 drawers with distinct content.
        for i in 0..5 {
            let add_result = dispatch(
                &connection,
                "mempalace_add_drawer",
                &json!({
                    "wing": "paginate",
                    "room": "general",
                    "content": format!("drawer number {i} with unique pagination content seed {}", i * 7)
                }),
            )
            .await;
            assert_eq!(add_result["success"], true, "add drawer {i} should succeed");
        }

        // Page 1: limit=2, offset=0
        let page1 = dispatch(
            &connection,
            "mempalace_list_drawers",
            &json!({"wing": "paginate", "limit": 2, "offset": 0}),
        )
        .await;
        let page1_count = page1["count"].as_i64().expect("count should be i64");
        assert_eq!(page1_count, 2, "page 1 should return 2 drawers");

        // Page 2: limit=2, offset=2
        let page2 = dispatch(
            &connection,
            "mempalace_list_drawers",
            &json!({"wing": "paginate", "limit": 2, "offset": 2}),
        )
        .await;
        let page2_count = page2["count"].as_i64().expect("count should be i64");
        assert_eq!(page2_count, 2, "page 2 should return 2 drawers");

        // Pages should contain different drawers.
        let page1_ids: Vec<&str> = page1["drawers"]
            .as_array()
            .expect("drawers should be array")
            .iter()
            .filter_map(|d| d["drawer_id"].as_str())
            .collect();
        let page2_ids: Vec<&str> = page2["drawers"]
            .as_array()
            .expect("drawers should be array")
            .iter()
            .filter_map(|d| d["drawer_id"].as_str())
            .collect();
        assert!(
            page1_ids.iter().all(|id| !page2_ids.contains(id)),
            "page 1 and page 2 should have no overlapping drawer IDs"
        );
    })
    .await;
}

/// Add a drawer, update its content, and verify the `drawer_id` changes
/// because the ID is deterministic from wing+room+content.
#[tokio::test]
async fn update_drawer_changes_id() {
    let wal_dir = tempfile::tempdir().expect("failed to create WAL temp dir");
    temp_env::async_with_vars([("MEMPALACE_DIR", Some(wal_dir.path()))], async {
        let (_db, connection) = test_db().await;

        let add_result = dispatch(
            &connection,
            "mempalace_add_drawer",
            &json!({
                "wing": "upd",
                "room": "general",
                "content": "original content for update testing purposes"
            }),
        )
        .await;
        assert_eq!(add_result["success"], true, "add should succeed");
        let old_id = add_result["drawer_id"]
            .as_str()
            .expect("drawer_id should exist")
            .to_string();

        // Fetch original content.
        let get_result_before = dispatch(
            &connection,
            "mempalace_get_drawer",
            &json!({"drawer_id": old_id}),
        )
        .await;
        assert_eq!(
            get_result_before["content"],
            "original content for update testing purposes"
        );

        // Update content.
        let update_result = dispatch(
            &connection,
            "mempalace_update_drawer",
            &json!({
                "drawer_id": old_id,
                "content": "updated content that is completely different now"
            }),
        )
        .await;
        assert_eq!(update_result["success"], true, "update should succeed");
        let new_id = update_result["drawer_id"]
            .as_str()
            .expect("updated drawer_id should exist")
            .to_string();

        // Deterministic ID changes when content changes.
        assert_ne!(
            old_id, new_id,
            "drawer_id should change after content update"
        );

        // Verify new content via get.
        let get_result_after = dispatch(
            &connection,
            "mempalace_get_drawer",
            &json!({"drawer_id": new_id}),
        )
        .await;
        assert_eq!(
            get_result_after["content"],
            "updated content that is completely different now"
        );

        // Verify the old drawer was removed — update must not leave a ghost entry.
        let get_old_after = dispatch(
            &connection,
            "mempalace_get_drawer",
            &json!({"drawer_id": old_id}),
        )
        .await;
        assert!(
            get_old_after.get("error").is_some(),
            "old drawer_id should no longer be found after update"
        );
    })
    .await;
}

/// Dispatching a non-existent tool should return a structured error.
#[tokio::test]
async fn unknown_tool_returns_error() {
    let wal_dir = tempfile::tempdir().expect("failed to create WAL temp dir");
    temp_env::async_with_vars([("MEMPALACE_DIR", Some(wal_dir.path()))], async {
        let (_db, connection) = test_db().await;

        let result = dispatch(&connection, "mempalace_nonexistent_tool", &json!({})).await;
        let error = result["error"]
            .as_str()
            .expect("error field should be a string");
        assert!(
            error.to_lowercase().contains("unknown"),
            "error message should mention 'unknown': got {error}"
        );
        assert!(
            error.contains("mempalace_nonexistent_tool"),
            "error should include the tool name"
        );
    })
    .await;
}
