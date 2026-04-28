//! Wake-up command — generate palace context layers for injection into a session.
//!
//! Default (no flags): L0 identity + L1 top drawers (~600–900 tokens, fast startup).
//! With --wing/--room: appends L2 on-demand recall for that location.
//! With --query:       appends L3 keyword deep-search results.

use turso::Connection;

use crate::error::Result;
use crate::palace::stack::MemoryStack;

/// Run the wake-up command, printing the requested layers to stdout.
pub async fn run(
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
    print!("{base}");

    // L2: on-demand recall for a specific wing/room. Only triggered when the
    // caller scoped to a wing, because L2 without a filter re-reads all
    // drawers and duplicates what L1 already showed.
    if wing.is_some() || room.is_some() {
        let recall = stack.browse(wing, room, results).await?;
        if !recall.is_empty() {
            println!("\n{recall}");
        }
    }

    // L3: keyword deep-search. Only triggered when the caller provides a query.
    if let Some(query_str) = query {
        let deep = stack.search(query_str, wing, room, results).await?;
        if !deep.is_empty() {
            println!("\n{deep}");
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
        // Index words so L3 search can find the drawer.
        let _ = crate::palace::drawer::index_words(
            &connection,
            "wd-test-001",
            "architecture design patterns overview",
        )
        .await;
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
        // L3 search for a matching term must print results when the word index is populated.
        let (_db, connection) = test_db_with_drawer("l3_wing").await;
        run(&connection, None, None, Some("architecture"), 5)
            .await
            .expect("run with matching query must succeed");
    }
}
