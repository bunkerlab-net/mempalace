// Integration test — .expect() is acceptable with a descriptive message.
#![allow(clippy::expect_used)]

use std::fs;

use mempalace::palace::miner::{MineParams, scan_project};
use mempalace::palace::search::search_memories;
use mempalace::test_helpers::test_db;

/// Create text files in a tempdir, scan to find them, mine into the palace,
/// then search for their content to verify end-to-end data flow.
#[tokio::test]
async fn scan_and_mine_creates_searchable_drawers() {
    let (_db, conn) = test_db().await;
    let dir = tempfile::tempdir().expect("tempdir should be created");

    // Write files with enough content to pass the 50-char minimum.
    fs::write(
        dir.path().join("notes.txt"),
        "Rust programming offers memory safety without garbage collection overhead",
    )
    .expect("write notes.txt should succeed");
    fs::write(
        dir.path().join("design.md"),
        "The architecture uses event sourcing with append-only storage design",
    )
    .expect("write design.md should succeed");

    // Scan should find both files.
    let files = scan_project(dir.path());
    assert!(
        files.len() >= 2,
        "scan should find at least 2 readable files"
    );

    // Write a mempalace.yaml so mine() can read config.
    fs::write(
        dir.path().join("mempalace.yaml"),
        "wing: test_mining\nrooms:\n  - name: general\n    description: default room\n    keywords: []\n",
    )
    .expect("write mempalace.yaml should succeed");

    // Mine the directory.
    let opts = MineParams {
        wing: Some("test_mining".to_string()),
        agent: "test".to_string(),
        limit: 0,
        dry_run: false,
        respect_gitignore: false,
    };
    mempalace::palace::miner::mine(&conn, dir.path(), &opts)
        .await
        .expect("mine should succeed");

    // Search for content from the mined files.
    let results = search_memories(&conn, "rust programming memory safety", None, None, 10)
        .await
        .expect("search should succeed after mining");
    assert!(!results.is_empty(), "search should find mined content");
    assert_eq!(
        results[0].wing, "test_mining",
        "wing should match mine config"
    );
}

/// Mining the same directory twice should not create duplicate drawers
/// because `file_already_mined` checks the stored mtime.
#[tokio::test]
async fn mine_skips_already_mined_files() {
    let (_db, conn) = test_db().await;
    let dir = tempfile::tempdir().expect("tempdir should be created");

    fs::write(
        dir.path().join("stable.txt"),
        "This content remains unchanged between mine runs for deduplication testing",
    )
    .expect("write stable.txt should succeed");
    fs::write(
        dir.path().join("mempalace.yaml"),
        "wing: dedup_test\nrooms:\n  - name: general\n    description: default\n    keywords: []\n",
    )
    .expect("write mempalace.yaml should succeed");

    let opts = MineParams {
        wing: Some("dedup_test".to_string()),
        agent: "test".to_string(),
        limit: 0,
        dry_run: false,
        respect_gitignore: false,
    };

    // First mine.
    mempalace::palace::miner::mine(&conn, dir.path(), &opts)
        .await
        .expect("first mine should succeed");
    let first = search_memories(&conn, "unchanged deduplication testing", None, None, 50)
        .await
        .expect("search after first mine should succeed");

    // Second mine — same files, same content.
    mempalace::palace::miner::mine(&conn, dir.path(), &opts)
        .await
        .expect("second mine should succeed");
    let second = search_memories(&conn, "unchanged deduplication testing", None, None, 50)
        .await
        .expect("search after second mine should succeed");

    assert_eq!(
        first.len(),
        second.len(),
        "second mine should not create duplicate drawers"
    );
    assert!(!first.is_empty(), "there should be at least one result");
}

/// `scan_project` respects `.gitignore` when a git repo is initialized.
#[test]
fn scan_respects_gitignore() {
    let dir = tempfile::tempdir().expect("tempdir should be created");

    // Initialize a git repo so the ignore crate picks up .gitignore.
    std::process::Command::new("git")
        .args(["init"])
        .current_dir(dir.path())
        .output()
        .expect("git init should succeed");

    // Gitignore a .rs file (must use a readable extension to be meaningful).
    fs::write(dir.path().join(".gitignore"), "secret.rs\n")
        .expect("write .gitignore should succeed");
    fs::write(dir.path().join("visible.txt"), "this file should be found")
        .expect("write visible.txt should succeed");
    fs::write(dir.path().join("secret.rs"), "this file should be ignored")
        .expect("write secret.rs should succeed");

    let files = scan_project(dir.path());
    let names: Vec<String> = files
        .iter()
        .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
        .collect();

    assert!(
        names.contains(&"visible.txt".to_string()),
        "non-ignored file should be found"
    );
    assert!(
        !names.contains(&"secret.rs".to_string()),
        "gitignored file should be excluded"
    );
}
