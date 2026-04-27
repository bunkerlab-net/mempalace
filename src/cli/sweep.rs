//! `sweep` subcommand handler.
//!
//! Delegates to `palace::sweeper` for the actual ingestion logic.  Prints
//! a human-readable summary on completion.

use std::path::Path;

use turso::Connection;

use crate::error::Result;
use crate::palace::entity_registry::EntityRegistry;
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
        // Read content before sweep for entity learning — best-effort, non-UTF8 files produce empty string.
        let file_content = std::fs::read_to_string(target).unwrap_or_default();
        let result = sweeper::sweep(connection, target, wing).await?;
        println!(
            "  Swept {}: +{} new, {} already present.",
            target.display(),
            result.drawers_added,
            result.drawers_already_present,
        );
        // Learn new entities from freshly swept content; skip if nothing changed.
        if result.drawers_added > 0 && !file_content.is_empty() {
            run_learn_from_file(&file_content);
        }
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

/// Scan swept file content for new entity candidates and update the local registry.
///
/// Called by [`run`] after a file sweep that added at least one drawer. Best-effort:
/// a failed registry write does not abort the sweep. Uses English language detection
/// as the default; the registry ignores duplicates so repeated calls are safe.
fn run_learn_from_file(content: &str) {
    assert!(
        !content.is_empty(),
        "run_learn_from_file: content must not be empty"
    );
    let mut registry = EntityRegistry::load();
    // Registry summary is always non-empty — confirms the load succeeded.
    assert!(
        !registry.summary().is_empty(),
        "run_learn_from_file: registry must load successfully"
    );
    // Best-effort: a failed write does not abort the parent sweep.
    let _ = registry.learn_from_text(content, 0.7, &["en"]);
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

    #[test]
    fn learn_from_file_does_not_panic_on_valid_content() {
        // run_learn_from_file must complete without panicking given any non-empty content.
        // The registry file is only written when new entities are discovered, so we
        // verify the function completed by confirming the registry still loads cleanly.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        temp_env::with_var("MEMPALACE_DIR", Some(dir.path()), || {
            run_learn_from_file("Alice called about the project meeting next week.");
            // Pair assertion: entity registry must load successfully after the call
            // (falls back to empty default if no file was written — that is correct).
            let registry = crate::palace::entity_registry::EntityRegistry::load();
            assert!(
                !registry.summary().is_empty(),
                "registry must be loadable after learn_from_file"
            );
        });
    }
}
