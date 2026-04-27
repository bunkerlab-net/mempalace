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

    #[tokio::test]
    async fn run_dry_run_with_wing_filter_prints_wing_name() {
        // dry_run with an explicit wing filter must exercise the
        // `if let Some(wing_filter) = wing` branch inside the dry_run block.
        let (_db, connection) = crate::test_helpers::test_db().await;
        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "cli-wing-dry-001",
                wing: "gamma",
                room: "notes",
                content: "dry run wing filter content",
                source_file: "g.rs",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for dry-run wing-filter test");

        let output_dir =
            tempfile::tempdir().expect("failed to create temp dir for dry-run wing-filter test");
        run(&connection, output_dir.path(), Some("gamma"), true)
            .await
            .expect("dry-run with wing filter must succeed");
        // Dry run must not write files even when a wing filter is provided.
        assert!(
            !output_dir.path().join("gamma").exists(),
            "dry run must not create wing directory"
        );
    }

    #[tokio::test]
    async fn run_empty_palace_with_wing_filter_prints_no_drawers_message() {
        // An empty palace filtered by wing must print the wing-specific no-drawers
        // message, exercising the `else { println!("No drawers … (wing: …)") }` branch.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let output_dir =
            tempfile::tempdir().expect("failed to create temp dir for empty-wing-filter test");
        run(
            &connection,
            output_dir.path(),
            Some("nonexistent_wing"),
            false,
        )
        .await
        .expect("run with wing filter on empty palace must succeed");
        // No files must be written for an empty result set.
        assert!(
            !output_dir.path().join("nonexistent_wing").exists(),
            "empty palace with wing filter must not create any directories"
        );
    }

    #[tokio::test]
    async fn run_with_wing_filter_exports_matching_drawers() {
        // run with an explicit wing filter must export only the matching drawers
        // and print the wing-qualified success message.
        let (_db, connection) = crate::test_helpers::test_db().await;

        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "cli-filt-001",
                wing: "delta",
                room: "api",
                content: "delta wing content",
                source_file: "d.rs",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for wing-filter export test");

        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "cli-filt-002",
                wing: "epsilon",
                room: "api",
                content: "epsilon wing content should not appear",
                source_file: "e.rs",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for second wing in filter test");

        let output_dir =
            tempfile::tempdir().expect("failed to create temp dir for wing-filter export test");
        run(&connection, output_dir.path(), Some("delta"), false)
            .await
            .expect("run with wing filter must succeed");

        // Only delta wing must be written.
        assert!(
            output_dir.path().join("delta").join("api.md").exists(),
            "delta/api.md must be created for the filtered wing"
        );
        assert!(
            !output_dir.path().join("epsilon").exists(),
            "epsilon wing must not be created when filtered to delta"
        );
    }
}
