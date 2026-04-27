//! `mempalace dedup` — detect and remove near-duplicate drawers.
//!
//! Uses Jaccard similarity on word bags from the inverted index. Groups
//! drawers by `source_file` and removes the shorter member of any pair
//! whose similarity exceeds the threshold.

use turso::Connection;

use crate::error::Result;
use crate::palace::dedup;

/// Run the `dedup` command.
///
/// Reports stats whether or not `dry_run` is set. In dry-run mode no
/// drawers are deleted.
pub async fn run(
    connection: &Connection,
    wing: Option<&str>,
    threshold: f64,
    dry_run: bool,
    stats_only: bool,
) -> Result<()> {
    assert!(threshold > 0.0, "run: threshold must be positive");
    assert!(threshold <= 1.0, "run: threshold must be at most 1.0");

    if dry_run {
        eprintln!("Dry run — scanning for near-duplicates (threshold = {threshold:.2})…");
    } else if stats_only {
        eprintln!("Scanning for near-duplicates (stats only)…");
    } else {
        eprintln!("Deduplicating drawers (threshold = {threshold:.2})…");
    }

    let effective_dry = dry_run || stats_only;
    let stats = dedup::dedup_drawers(connection, wing, threshold, effective_dry).await?;

    eprintln!("  Groups scanned:    {}", stats.groups_scanned);
    eprintln!("  Duplicates found:  {}", stats.duplicates_found);
    if !effective_dry {
        eprintln!("  Drawers deleted:   {}", stats.deleted);
    }

    assert!(stats.deleted <= stats.duplicates_found);
    Ok(())
}

#[cfg(test)]
// Test code — .expect() with a descriptive message is acceptable; panics are the correct failure mode.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::palace::dedup::DEFAULT_THRESHOLD;

    #[tokio::test]
    async fn run_on_empty_palace_returns_ok() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection, None, DEFAULT_THRESHOLD, true, false).await;
        assert!(result.is_ok(), "empty palace must not error");
    }

    #[tokio::test]
    async fn run_stats_only_returns_ok() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection, None, DEFAULT_THRESHOLD, false, true).await;
        assert!(result.is_ok(), "stats_only mode must not error");
    }

    #[tokio::test]
    async fn run_live_mode_on_empty_palace_returns_ok() {
        // dry_run=false, stats_only=false takes the else branch (line 31) and also
        // prints the "Drawers deleted" line (line 40). An empty palace produces
        // zero duplicates so no real deletions occur, but the code path is exercised.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection, None, DEFAULT_THRESHOLD, false, false).await;
        assert!(
            result.is_ok(),
            "live dedup mode must not error on empty palace"
        );
        // A second call confirms idempotence of the live path.
        let result2 = run(&connection, None, DEFAULT_THRESHOLD, false, false).await;
        assert!(result2.is_ok(), "live dedup mode must be idempotent");
    }
}
