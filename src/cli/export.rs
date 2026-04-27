//! `mempalace export` — write palace drawers to markdown files on disk.

use std::path::Path;

use turso::Connection;

use crate::error::Result;
use crate::palace::exporter;

/// Run the export command, writing drawers to `output_dir`.
pub async fn run(
    connection: &Connection,
    output_dir: &Path,
    wing: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    assert!(
        !output_dir.as_os_str().is_empty(),
        "output_dir must not be empty"
    );

    let stats = exporter::export_palace(connection, output_dir, wing, dry_run).await?;

    assert!(
        stats.drawers != 0 || stats.rooms == 0,
        "export: rooms must be zero when no drawers were exported"
    );
    assert!(
        stats.rooms <= stats.drawers,
        "export: room count must not exceed drawer count"
    );

    if dry_run {
        if let Some(wing_filter) = wing {
            println!(
                "Dry run: {} drawer(s) across {} wing(s) / {} room(s) would be exported to {} (wing: {wing_filter})",
                stats.drawers,
                stats.wings,
                stats.rooms,
                output_dir.display()
            );
        } else {
            println!(
                "Dry run: {} drawer(s) across {} wing(s) / {} room(s) would be exported to {}",
                stats.drawers,
                stats.wings,
                stats.rooms,
                output_dir.display()
            );
        }
    } else if stats.drawers == 0 {
        if let Some(wing_filter) = wing {
            println!("No drawers found to export (wing: {wing_filter}).");
        } else {
            println!("No drawers found to export.");
        }
    } else if let Some(wing_filter) = wing {
        println!(
            "Exported {} drawer(s) across {} wing(s) / {} room(s) to {} (wing: {wing_filter})",
            stats.drawers,
            stats.wings,
            stats.rooms,
            output_dir.display()
        );
    } else {
        println!(
            "Exported {} drawer(s) across {} wing(s) / {} room(s) to {}",
            stats.drawers,
            stats.wings,
            stats.rooms,
            output_dir.display()
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
    async fn run_empty_palace_prints_no_drawers_message() {
        // An empty palace must return Ok and not crash.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let output_dir =
            tempfile::tempdir().expect("failed to create temp dir for empty export test");
        run(&connection, output_dir.path(), None, false)
            .await
            .expect("run must succeed on empty palace");
        // Pair assertion: no files must have been created.
        assert!(
            !output_dir.path().join("wings").exists(),
            "no output subdirectory must be created for empty palace"
        );
    }

    #[tokio::test]
    async fn run_dry_run_returns_ok_without_writing() {
        // dry_run must succeed and write no files.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let output_dir =
            tempfile::tempdir().expect("failed to create temp dir for dry-run cli export test");
        run(&connection, output_dir.path(), None, true)
            .await
            .expect("dry-run export must succeed");
        assert!(
            !output_dir
                .path()
                .read_dir()
                .is_ok_and(|mut entries| entries.next().is_some()),
            "dry run must not create any files"
        );
    }
}
