//! `sweep` subcommand handler.
//!
//! Delegates to `palace::sweeper` for the actual ingestion logic.  Prints
//! a human-readable summary on completion.

use std::path::Path;

use turso::Connection;

use crate::error::Result;
use crate::palace::sweeper;

/// Run the `sweep` subcommand.
///
/// Sweeps a single `.jsonl` file or every `.jsonl` in a directory tree,
/// inserting one drawer per user/assistant message that is not already
/// present.  Reports counts to stdout on completion.
pub async fn run(connection: &Connection, target: &Path, wing: &str) -> Result<()> {
    assert!(
        target.exists(),
        "sweep: target must exist: {}",
        target.display()
    );
    assert!(!wing.is_empty(), "sweep: wing must not be empty");

    if target.is_file() {
        let result = sweeper::sweep(connection, target, wing).await?;
        println!(
            "  Swept {}: +{} new, {} already present.",
            target.display(),
            result.drawers_added,
            result.drawers_already_present,
        );
    } else if target.is_dir() {
        let result = sweeper::sweep_directory(connection, target, wing).await?;
        println!(
            "  Swept {}/{} files from {}: +{} new, {} already present.",
            result.files_succeeded,
            result.files_attempted,
            target.display(),
            result.drawers_added,
            result.drawers_already_present,
        );
    } else {
        return Err(crate::error::Error::Other(format!(
            "sweep target is neither a file nor a directory: {}",
            target.display()
        )));
    }

    Ok(())
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Write a minimal valid Claude Code JSONL record to `path`.
    ///
    /// Uses the Claude Code native format: top-level `type` is `"user"` or
    /// `"assistant"`, with the message payload in a nested `message` object.
    fn write_valid_jsonl(path: &Path) {
        let record = r#"{"type":"user","sessionId":"sess1","uuid":"u001","message":{"role":"user","content":"hi"}}"#;
        std::fs::write(path, format!("{record}\n")).expect("must write test JSONL file");
    }

    /// Query the number of drawers in `wing` on `connection`.
    async fn drawer_count(connection: &turso::Connection, wing: &str) -> i64 {
        let rows = crate::db::query_all(
            connection,
            "SELECT COUNT(*) FROM drawers WHERE wing = ?1",
            turso::params![wing],
        )
        .await
        .expect("drawer count query must succeed");
        rows[0].get(0).expect("COUNT(*) must be readable as i64")
    }

    #[tokio::test]
    async fn run_file_target_returns_ok() {
        // Single-file branch: run must accept a regular .jsonl file, return Ok,
        // and insert the message as a drawer.
        let dir = tempdir().expect("must create temp dir");
        let file = dir.path().join("session.jsonl");
        write_valid_jsonl(&file);
        let (_database, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection, &file, "test_wing").await;
        assert!(result.is_ok(), "run must return Ok for a valid file target");
        // Pair assertion: the message in the fixture must have been inserted.
        assert_eq!(
            drawer_count(&connection, "test_wing").await,
            1,
            "run must insert one drawer for the single user message"
        );
    }

    #[tokio::test]
    async fn run_directory_target_returns_ok() {
        // Directory branch: run must accept a directory, return Ok, and insert
        // the message from the fixture file as a drawer.
        let dir = tempdir().expect("must create temp dir");
        let file = dir.path().join("session.jsonl");
        write_valid_jsonl(&file);
        let (_database, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection, dir.path(), "test_wing").await;
        assert!(
            result.is_ok(),
            "run must return Ok for a valid directory target"
        );
        // Pair assertion: the message from the file must have been inserted.
        assert_eq!(
            drawer_count(&connection, "test_wing").await,
            1,
            "run must insert one drawer for the single user message"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_special_file_returns_error() {
        // Else branch: /dev/null exists but is neither a regular file nor a directory.
        // run must return Err rather than silently succeeding or panicking.
        let target = Path::new("/dev/null");
        assert!(target.exists(), "/dev/null must exist on Unix");
        assert!(!target.is_file(), "/dev/null must not be a regular file");
        assert!(!target.is_dir(), "/dev/null must not be a directory");
        let (_database, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection, target, "test_wing").await;
        assert!(
            result.is_err(),
            "run must return Err for a special-file target"
        );
    }
}
