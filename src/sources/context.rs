//! Adapter-to-palace interaction context and progress hook contract (RFC 002 §6-7).
//!
//! Adapters that need to interact with the palace during ingest receive a
//! [`PalaceContext`] reference. Progress events are delivered via [`ProgressHook`].

use turso::Connection;

use crate::error::Result;

// ─── Progress hook ─────────────────────────────────────────────────────────

/// Callback interface for ingest progress events (RFC 002 §6).
///
/// Implement this trait to receive per-item lifecycle events during an adapter
/// ingest run. Use [`NoOpProgressHook`] when progress reporting is not needed.
pub trait ProgressHook: Send {
    /// Called just before extraction begins for `source_file`.
    fn on_item_start(&mut self, source_file: &str);
    /// Called after extraction completes; `drawers_added` counts new drawers written.
    fn on_item_done(&mut self, source_file: &str, drawers_added: usize);
    /// Called when extraction of `source_file` fails with `message`.
    fn on_error(&mut self, source_file: &str, message: &str);
}

/// No-op [`ProgressHook`] for callers that do not need progress reporting.
pub struct NoOpProgressHook;

impl ProgressHook for NoOpProgressHook {
    fn on_item_start(&mut self, _source_file: &str) {}
    fn on_item_done(&mut self, _source_file: &str, _drawers_added: usize) {}
    fn on_error(&mut self, _source_file: &str, _message: &str) {}
}

// ─── Palace context ────────────────────────────────────────────────────────

/// Facade for adapter-to-palace interaction during an ingest run (RFC 002 §7).
///
/// Adapters receive a `PalaceContext` reference when the pipeline invokes
/// context-aware ingest. They can use it to check whether content is already
/// filed without depending on the full palace API.
pub struct PalaceContext<'a> {
    connection: &'a Connection,
}

impl<'a> PalaceContext<'a> {
    /// Wrap a live database connection for an ingest context.
    pub fn new(connection: &'a Connection) -> Self {
        PalaceContext { connection }
    }

    /// Return `true` if `source_file` chunk `chunk_index` is already in the palace.
    ///
    /// Callers use this to skip re-extraction of content that has not changed.
    /// `chunk_index` is 0-based and must match the value stored at ingest time.
    pub async fn is_filed(&self, source_file: &str, chunk_index: u32) -> Result<bool> {
        assert!(!source_file.is_empty(), "source_file must not be empty");
        // u32 fits in i64 without precision loss (u32::MAX < i64::MAX).
        let chunk_index_i64 = i64::from(chunk_index);
        assert!(
            chunk_index_i64 >= 0,
            "chunk_index_i64 must be non-negative after lossless widening cast"
        );

        let mut rows = self
            .connection
            .query(
                "SELECT 1 FROM drawers \
                 WHERE source_file = ?1 AND chunk_index = ?2 LIMIT 1",
                turso::params![source_file, chunk_index_i64],
            )
            .await?;

        let found = rows.next().await?.is_some();
        Ok(found)
    }

    /// Return the number of chunks already filed for `source_file`.
    ///
    /// Adapters use this to detect partially-ingested sources before deciding
    /// whether to re-ingest from scratch or to skip.
    pub async fn count_filed(
        &self,
        source_file: &str,
    ) -> std::result::Result<usize, crate::error::SourceAdapterError> {
        assert!(!source_file.is_empty(), "source_file must not be empty");

        let rows = crate::db::query_all(
            self.connection,
            "SELECT count(*) FROM drawers WHERE source_file = ?1",
            turso::params![source_file],
        )
        .await
        .map_err(|e| crate::error::SourceAdapterError::Other(e.to_string()))?;

        // SQLite count(*) always returns a single i64 row.
        let count: i64 = rows
            .first()
            .and_then(|row| row.get::<i64>(0).ok())
            .unwrap_or(0_i64);

        assert!(count >= 0, "drawer count must be non-negative");
        // i64::MAX exceeds any plausible palace size; usize::MAX is a safe sentinel.
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // ── NoOpProgressHook ───────────────────────────────────────────────────

    #[test]
    fn no_op_progress_hook_does_not_panic() {
        // All three methods must accept any string input without panicking.
        let mut hook = NoOpProgressHook;
        hook.on_item_start("src/main.rs");
        hook.on_item_done("src/main.rs", 3);
        hook.on_error("src/main.rs", "permission denied");
        // Pair assertion: hook state is unchanged (NoOp has no state to corrupt).
        hook.on_item_start("src/main.rs");
        assert!(
            std::mem::size_of::<NoOpProgressHook>() == 0,
            "NoOpProgressHook must be a zero-sized type"
        );
    }

    #[test]
    fn no_op_progress_hook_accepts_empty_strings() {
        // Empty strings must be accepted — some adapters omit source_file names.
        let mut hook = NoOpProgressHook;
        hook.on_item_start("");
        hook.on_item_done("", 0);
        hook.on_error("", "");
        // Pair assertion: ZST has no memory footprint.
        assert_eq!(
            std::mem::size_of_val(&hook),
            0,
            "NoOpProgressHook must be zero-sized"
        );
    }

    // ── PalaceContext ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn palace_context_is_filed_returns_false_for_missing_drawer() {
        // A freshly-initialised palace has no drawers, so is_filed must be false.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let context = PalaceContext::new(&connection);
        let filed = context
            .is_filed("nonexistent.md", 0)
            .await
            .expect("is_filed must not error on an empty palace");
        assert!(!filed, "missing source_file must return false");
        // Pair assertion: count_filed also returns 0 for a missing file.
        let count = context
            .count_filed("nonexistent.md")
            .await
            .expect("count_filed must not error on an empty palace");
        assert_eq!(count, 0, "count for missing source_file must be 0");
    }

    #[tokio::test]
    async fn palace_context_is_filed_returns_true_after_insert() {
        // After inserting a drawer, is_filed must return true for that source+chunk.
        let (_db, connection) = crate::test_helpers::test_db().await;
        connection
            .execute(
                "INSERT INTO drawers \
                 (id, wing, room, content, source_file, chunk_index) \
                 VALUES ('ctx-1', 'w', 'r', 'hello', 'notes.md', 0)",
                (),
            )
            .await
            .expect("drawer insert must succeed");

        let context = PalaceContext::new(&connection);
        let filed = context
            .is_filed("notes.md", 0)
            .await
            .expect("is_filed must succeed for an existing drawer");
        assert!(filed, "existing drawer must be reported as filed");
        // Pair assertion: chunk 1 of the same file is NOT filed.
        let chunk1_filed = context
            .is_filed("notes.md", 1)
            .await
            .expect("is_filed must not error for missing chunk");
        assert!(!chunk1_filed, "adjacent chunk must not appear filed");
    }

    #[tokio::test]
    async fn palace_context_count_filed_reflects_drawer_count() {
        // count_filed must return the exact number of chunks filed for a source.
        let (_db, connection) = crate::test_helpers::test_db().await;
        for i in 0..3_u32 {
            connection
                .execute(
                    "INSERT INTO drawers \
                     (id, wing, room, content, source_file, chunk_index) \
                     VALUES (?, 'w', 'r', 'x', 'doc.md', ?)",
                    turso::params![format!("cnt-{i}"), i64::from(i)],
                )
                .await
                .expect("drawer insert must succeed");
        }

        let context = PalaceContext::new(&connection);
        let count = context
            .count_filed("doc.md")
            .await
            .expect("count_filed must not error");
        assert_eq!(count, 3, "count must equal inserted drawer count");
        // Pair assertion: different source file is unaffected.
        let other_count = context
            .count_filed("other.md")
            .await
            .expect("count_filed for different file must not error");
        assert_eq!(other_count, 0, "other file must have 0 count");
    }
}
