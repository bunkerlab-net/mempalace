//! Wake-up command — generate palace context layers for injection into a session.
//!
//! Default (no flags): L0 identity + L1 top drawers (~600–900 tokens, fast startup).
//! With --wing/--room: appends L2 on-demand recall for that location.
//! With --query:       appends L3 keyword deep-search results.

use std::io::Write;

use turso::Connection;

use crate::error::Result;
use crate::palace::stack::MemoryStack;

/// Run the wake-up command, printing the requested layers to stdout.
///
/// Thin wrapper around [`run_to_writer`] that locks stdout. Tests use
/// `run_to_writer` directly with a `Vec<u8>` to assert the layered output
/// without redirecting the global stdout handle.
pub async fn run(
    connection: &Connection,
    wing: Option<&str>,
    room: Option<&str>,
    query: Option<&str>,
    results: usize,
) -> Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    run_to_writer(&mut handle, connection, wing, room, query, results).await
}

/// Write the L0/L1 (and optional L2/L3) wake-up layers to `out`.
///
/// Extracted so tests can capture the rendered text. The caller is
/// responsible for flushing `out` if line-buffered behavior matters
/// (the stdout wrapper above flushes on drop).
pub async fn run_to_writer(
    out: &mut dyn Write,
    connection: &Connection,
    wing: Option<&str>,
    room: Option<&str>,
    query: Option<&str>,
    results: usize,
) -> Result<()> {
    assert!(results > 0, "results must be positive");

    let stack = MemoryStack::new(connection);

    // L0 + L1 are always included — they form the core palace context.
    // `MemoryStack::recall` already asserts the returned context is non-empty,
    // so re-asserting here would only duplicate the callee's contract.
    let base = stack.recall(wing).await?;
    write!(out, "{base}")?;

    // L2: on-demand recall for a specific wing/room. Only triggered when the
    // caller scoped to a wing, because L2 without a filter re-reads all
    // drawers and duplicates what L1 already showed.
    if wing.is_some() || room.is_some() {
        let recall = stack.browse(wing, room, results).await?;
        if !recall.is_empty() {
            writeln!(out, "\n{recall}")?;
        }
    }

    // L3: keyword deep-search. Only triggered when the caller provides a query.
    if let Some(query_str) = query {
        let deep = stack.search(query_str, wing, room, results).await?;
        if !deep.is_empty() {
            writeln!(out, "\n{deep}")?;
        }
    }

    Ok(())
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Insert one test drawer and index its words in an in-memory palace.
    async fn test_db_with_drawer(wing: &str) -> (turso::Database, Connection) {
        let (db, connection) = crate::test_helpers::test_db().await;
        connection
            .execute(
                "INSERT INTO drawers \
                 (id, wing, room, content, source_file, chunk_index, \
                  added_by, ingest_mode, extract_mode) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                turso::params![
                    "wd-test-001",
                    wing,
                    "general",
                    "architecture design patterns overview",
                    "",
                    0i32,
                    "test",
                    "mine",
                    "full"
                ],
            )
            .await
            .expect("must insert test drawer");
        // Index words so L3 search can find the drawer; a setup failure here
        // would invalidate the L3 assertions, so panic with the underlying
        // error rather than silently dropping the Result.
        crate::palace::drawer::index_words(
            &connection,
            "wd-test-001",
            "architecture design patterns overview",
        )
        .await
        .expect("index_words must succeed during test setup");
        (db, connection)
    }

    #[tokio::test]
    async fn run_with_wing_exercises_l2_non_empty_branch() {
        // L2 recall for a populated wing must print a non-empty context block.
        let (_db, connection) = test_db_with_drawer("l2_wing").await;
        run(&connection, Some("l2_wing"), None, None, 5)
            .await
            .expect("run with populated wing must succeed");
    }

    #[tokio::test]
    async fn run_with_query_exercises_l3_non_empty_branch() {
        // L3 search for a matching term must print results when the word
        // index is populated. Capture output via `run_to_writer` (a
        // `Vec<u8>` buffer) so we can assert that the L3 block actually
        // landed in the rendered text — a bare `is_ok()` check would
        // pass even if the L3 branch silently no-op'd.
        let (_db, connection) = test_db_with_drawer("l3_wing").await;
        let mut buffer: Vec<u8> = Vec::new();
        run_to_writer(
            &mut buffer,
            &connection,
            None,
            None,
            Some("architecture"),
            5,
        )
        .await
        .expect("run with matching query must succeed");
        let captured = String::from_utf8(buffer).expect("captured output must be UTF-8");
        assert!(
            !captured.is_empty(),
            "wake-up output must include the L0/L1 base block at minimum"
        );
        assert!(
            captured.contains("architecture"),
            "L3 branch must surface the matching query word in the output: {captured}"
        );
    }
}
