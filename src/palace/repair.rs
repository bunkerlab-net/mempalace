//! Library entry point for rebuilding the inverted word index.
//!
//! Unlike `cli::repair::run`, this module does not print progress output or
//! create a file-system backup. It performs the DB operation only, making it
//! safe to call from MCP tools and library integrations.

use turso::Connection;

use crate::db::query_all;
use crate::error::Result;
use crate::palace::drawer;

/// Maximum drawers returned before the weak truncation signal fires.
///
/// `TursoDB` itself has no hidden cap, but the constant mirrors mempalace-py's
/// `CHROMADB_DEFAULT_GET_LIMIT` so the safety invariant ports cleanly. On the
/// Rust side the constant only matters when `drawer_count` returns `None`.
const EXTRACTION_DEFAULT_LIMIT: u64 = 10_000;
const _: () = assert!(EXTRACTION_DEFAULT_LIMIT > 0);

/// Count all rows in the `drawers` table without touching the word index.
///
/// Returns `None` when the query fails (schema drift, closed connection).
/// Callers treat `None` as "unknown" and fall back to the cap-detection check.
async fn drawer_count(connection: &Connection) -> Option<u64> {
    let rows = query_all(connection, "SELECT COUNT(*) FROM drawers", ())
        .await
        .ok()?;
    let value = rows.first()?.get_value(0).ok()?;
    let count = value.as_integer().copied()?;
    u64::try_from(count).ok()
}

/// Verify that `extracted` matches the palace row count before any destructive write.
///
/// Two signals trip the guard:
/// 1. **Strong** — the DB reports more drawers than `extracted`. Proceeding would
///    silently destroy the difference.
/// 2. **Weak** — `extracted` equals `EXTRACTION_DEFAULT_LIMIT` and the DB count is
///    unavailable (extraction may have been silently capped by a hidden limit).
///
/// `confirm_truncation_ok` bypasses both checks (`--confirm-truncation-ok` CLI flag).
async fn check_extraction_safety(
    connection: &Connection,
    extracted: u64,
    confirm_truncation_ok: bool,
) -> Result<()> {
    assert!(
        extracted < u64::MAX,
        "check_extraction_safety: extracted must be in range"
    );
    if confirm_truncation_ok {
        return Ok(());
    }
    let sqlite_count = drawer_count(connection).await;
    if let Some(count) = sqlite_count {
        assert!(
            count < u64::MAX,
            "check_extraction_safety: DB count must be in range"
        );
        if count > extracted {
            let loss = count - extracted;
            // Row counts are bounded well below 2^52; precision loss is negligible.
            #[allow(clippy::cast_precision_loss)]
            let percent_lost = 100.0 * loss as f64 / count as f64;
            let message = format!(
                "\n  ABORT: database reports {count} drawers but only {extracted} came back\
                 \n  from extraction. Proceeding would destroy {loss} drawers (~{percent_lost:.0}%).\
                 \n\
                 \n  Recovery options:\
                 \n    1. Restore from your most recent palace backup, then re-mine.\
                 \n    2. If the palace genuinely contains only {extracted} drawers, re-run\
                 \n       with --confirm-truncation-ok.\n"
            );
            return Err(crate::error::Error::TruncationDetected {
                sqlite_count: count,
                extracted,
                percent_lost,
                message,
            });
        }
    } else if extracted == EXTRACTION_DEFAULT_LIMIT {
        let message = format!(
            "\n  ABORT: extracted exactly {EXTRACTION_DEFAULT_LIMIT} drawers, matching the\
             \n  default extraction limit. The database count could not be verified, so we\
             \n  cannot determine whether extraction was silently capped. Refusing to overwrite.\
             \n\
             \n  If the palace genuinely contains exactly {EXTRACTION_DEFAULT_LIMIT} drawers,\
             \n  re-run with --confirm-truncation-ok.\n"
        );
        return Err(crate::error::Error::TruncationDetected {
            sqlite_count: 0,
            extracted,
            percent_lost: 0.0,
            message,
        });
    }
    Ok(())
}

/// Rebuild the full-text inverted word index from all drawer content.
///
/// Wraps the full operation in a `BEGIN IMMEDIATE` transaction. Returns the
/// number of drawers that were re-indexed. On failure, attempts a `ROLLBACK`
/// before propagating the error.
///
/// `confirm_truncation_ok` bypasses the truncation safety check (`--confirm-truncation-ok`).
pub async fn rebuild_index(connection: &Connection, confirm_truncation_ok: bool) -> Result<usize> {
    connection.execute("BEGIN IMMEDIATE", ()).await?;
    match rebuild_index_execute(connection, confirm_truncation_ok).await {
        Ok(total) => {
            connection.execute("COMMIT", ()).await?;
            Ok(total)
        }
        Err(error) => {
            if let Err(rollback_error) = connection.execute("ROLLBACK", ()).await {
                eprintln!("Rollback failed: {rollback_error}");
            }
            Err(error)
        }
    }
}

/// Collect all drawers and rebuild the index inside an already-open transaction.
///
/// Called exclusively by `rebuild_index`. Errors propagate to the caller, which
/// issues the `ROLLBACK`.
async fn rebuild_index_execute(
    connection: &Connection,
    confirm_truncation_ok: bool,
) -> Result<usize> {
    let rows = query_all(connection, "SELECT id, content FROM drawers", ()).await?;
    let total = rows.len();

    let drawers: Vec<(String, String)> = rows
        .iter()
        .map(|row| -> Result<(String, String)> {
            let id: String = row.get(0)?;
            let content: String = row.get(1)?;
            Ok((id, content))
        })
        .collect::<Result<Vec<_>>>()?;

    // Precondition: collected count must match the row count.
    assert!(
        drawers.len() == total,
        "rebuild_index_execute: drawers.len() {} must equal row count {total}",
        drawers.len()
    );

    // Safety guard: abort before any destructive write if the DB count exceeds extracted.
    // `usize as u64`: usize ≤ 64 bits on all supported platforms; the cast is widening.
    let extracted_u64 = total as u64;
    check_extraction_safety(connection, extracted_u64, confirm_truncation_ok).await?;

    connection.execute("DELETE FROM drawer_words", ()).await?;

    let mut indexed_count = 0usize;
    for (id, content) in &drawers {
        drawer::index_words(connection, id, content).await?;
        indexed_count += 1;
    }

    // Pair assertion: every collected drawer must have been indexed.
    assert!(
        indexed_count == total,
        "rebuild_index_execute: indexed {indexed_count} but expected {total} drawers"
    );

    Ok(total)
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rebuild_index_empty_palace_returns_zero() {
        // An empty palace must succeed and report zero drawers reindexed.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let total = rebuild_index(&connection, false)
            .await
            .expect("rebuild_index must succeed on an empty palace");
        assert_eq!(total, 0, "empty palace must report 0 drawers reindexed");
        // Pair assertion: drawer_words must be empty after reindexing an empty palace.
        let rows = crate::db::query_all(&connection, "SELECT COUNT(*) FROM drawer_words", ())
            .await
            .expect("drawer_words count query must succeed");
        let index_count: i64 = rows
            .first()
            .and_then(|r| r.get_value(0).ok())
            .and_then(|c| c.as_integer().copied())
            .unwrap_or(0);
        assert_eq!(
            index_count, 0,
            "drawer_words must be empty when no drawers exist"
        );
    }

    #[tokio::test]
    async fn rebuild_index_with_drawers_returns_count() {
        // A palace with drawers must return the correct reindexed count.
        let (_db, connection) = crate::test_helpers::test_db().await;

        for (id, wing) in [("repair-lib-001", "alpha"), ("repair-lib-002", "beta")] {
            crate::palace::drawer::add_drawer(
                &connection,
                &crate::palace::drawer::DrawerParams {
                    id,
                    wing,
                    room: "general",
                    content: "content for library rebuild_index test",
                    source_file: "a.rs",
                    chunk_index: 0,
                    added_by: "test",
                    ingest_mode: "projects",
                    source_mtime: None,
                },
            )
            .await
            .expect("add_drawer must succeed for rebuild_index test");
        }

        let total = rebuild_index(&connection, false)
            .await
            .expect("rebuild_index must succeed with two drawers present");
        assert_eq!(total, 2, "two drawers must report count of 2");
        // Pair assertion: drawer_words must have entries after reindexing non-empty palace.
        let rows = crate::db::query_all(&connection, "SELECT COUNT(*) FROM drawer_words", ())
            .await
            .expect("drawer_words count query must succeed after rebuild");
        let index_count: i64 = rows
            .first()
            .and_then(|r| r.get_value(0).ok())
            .and_then(|c| c.as_integer().copied())
            .unwrap_or(0);
        assert!(
            index_count > 0,
            "drawer_words must have entries after reindexing two drawers"
        );
    }

    /// Add `count` minimal drawers to the connection, bypassing the normal `add_drawer`
    /// validation so tests can control the exact row count easily.
    async fn rebuild_index_insert_drawers(connection: &Connection, count: u64) {
        for index in 0..count {
            connection
                .execute(
                    "INSERT OR IGNORE INTO drawers (id, wing, room, content) \
                     VALUES (?, 'wing', 'room', 'content')",
                    turso::params![format!("trunc-guard-{index}")],
                )
                .await
                .expect("INSERT must succeed for truncation guard test setup");
        }
    }

    #[tokio::test]
    async fn check_extraction_safety_passes_when_counts_match() {
        // Guard must succeed when the extracted count equals the DB count.
        let (_db, connection) = crate::test_helpers::test_db().await;
        rebuild_index_insert_drawers(&connection, 3).await;
        let result = check_extraction_safety(&connection, 3, false).await;
        assert!(result.is_ok(), "guard must pass when extracted == DB count");
    }

    #[tokio::test]
    async fn check_extraction_safety_fires_strong_signal() {
        // Guard must return TruncationDetected when the DB has more rows than extracted.
        let (_db, connection) = crate::test_helpers::test_db().await;
        rebuild_index_insert_drawers(&connection, 5).await;
        // Claim only 2 were extracted (DB has 5 — simulates truncated extraction).
        let result = check_extraction_safety(&connection, 2, false).await;
        assert!(result.is_err(), "guard must fire when DB count > extracted");
        let error = result.expect_err("must be an error");
        assert!(
            matches!(
                error,
                crate::error::Error::TruncationDetected {
                    sqlite_count: 5,
                    extracted: 2,
                    ..
                }
            ),
            "error must be TruncationDetected with correct counts"
        );
        assert!(
            error.to_string().contains("ABORT"),
            "error message must contain ABORT"
        );
    }

    #[tokio::test]
    async fn check_extraction_safety_bypassed_by_confirm() {
        // Guard must be skipped entirely when confirm_truncation_ok is true.
        let (_db, connection) = crate::test_helpers::test_db().await;
        rebuild_index_insert_drawers(&connection, 5).await;
        // Claim only 1 was extracted — would normally fire the strong signal.
        let result = check_extraction_safety(&connection, 1, true).await;
        assert!(
            result.is_ok(),
            "guard must be bypassed when confirm_truncation_ok is true"
        );
    }

    #[tokio::test]
    async fn drawer_count_returns_correct_value() {
        // drawer_count must return the exact row count from the drawers table.
        let (_db, connection) = crate::test_helpers::test_db().await;
        rebuild_index_insert_drawers(&connection, 4).await;
        let count = drawer_count(&connection).await;
        assert_eq!(
            count,
            Some(4),
            "drawer_count must return 4 after inserting 4 rows"
        );
        // Pair assertion: count must be non-zero for a non-empty palace.
        assert!(count.unwrap_or(0) > 0, "count must be positive");
    }
}
