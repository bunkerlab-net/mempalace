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
