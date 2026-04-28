//! `mempalace diary-ingest` — ingest on-disk diary markdown files into the palace.

use std::path::Path;

use turso::Connection;

use crate::error::Result;
use crate::palace::diary_ingest;

/// Run the diary-ingest command, filing new diary sections as drawers.
pub async fn run(
    connection: &Connection,
    diary_dir: &Path,
    wing: &str,
    agent: &str,
    force: bool,
) -> Result<()> {
    assert!(!wing.is_empty(), "wing must not be empty");
    assert!(!agent.is_empty(), "agent must not be empty");
    assert!(
        !diary_dir.as_os_str().is_empty(),
        "diary_dir must not be empty"
    );

    let stats = diary_ingest::ingest_diaries(connection, diary_dir, wing, agent, force).await?;

    assert!(stats.drawers_created >= stats.days_updated);

    if stats.drawers_created == 0 {
        println!("No new diary sections found.");
    } else {
        println!(
            "Ingested {} new drawer(s) across {} diary file(s).",
            stats.drawers_created, stats.days_updated
        );
    }

    Ok(())
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_empty_diary_dir_prints_no_new_sections() {
        // A directory with no matching diary files must return Ok without creating drawers.
        // MEMPALACE_DIR is redirected because ingest_diaries always writes diary_cursors.json
        // to config_dir(), which defaults to ~/.local/share/mempalace when unset.
        let diary_dir =
            tempfile::tempdir().expect("failed to create temp dir for empty diary-ingest test");
        let cursor_dir =
            tempfile::tempdir().expect("failed to create cursor dir for empty diary-ingest test");
        let (_db, connection) = crate::test_helpers::test_db().await;
        temp_env::async_with_vars(
            [(
                "MEMPALACE_DIR",
                Some(cursor_dir.path().to_str().expect("utf-8")),
            )],
            run(
                &connection,
                diary_dir.path(),
                "journal",
                "test_agent",
                false,
            ),
        )
        .await
        .expect("run must succeed on empty diary directory");
    }

    #[tokio::test]
    async fn run_with_diary_file_creates_drawers() {
        // A valid diary file must produce at least one drawer.
        // MEMPALACE_DIR is redirected because ingest_diaries always writes diary_cursors.json
        // to config_dir(), which defaults to ~/.local/share/mempalace when unset.
        let diary_dir =
            tempfile::tempdir().expect("failed to create temp dir for diary-ingest run test");
        let cursor_dir =
            tempfile::tempdir().expect("failed to create cursor dir for diary-ingest run test");
        std::fs::write(
            diary_dir.path().join("2024-05-01.md"),
            "## Morning\n\nHad coffee and planned the sprint.\n\n## Evening\n\nCompleted the review.",
        )
        .expect("failed to write test diary file");

        let (_db, connection) = crate::test_helpers::test_db().await;
        temp_env::async_with_vars(
            [(
                "MEMPALACE_DIR",
                Some(cursor_dir.path().to_str().expect("utf-8")),
            )],
            run(
                &connection,
                diary_dir.path(),
                "journal",
                "test_agent",
                false,
            ),
        )
        .await
        .expect("run must succeed with a valid diary file");
        // Pair assertion: cursor must be written to the isolated dir, not to the real config.
        assert!(
            cursor_dir.path().join("diary_cursors.json").exists(),
            "diary_cursors.json must land in the redirected MEMPALACE_DIR"
        );
    }
}
