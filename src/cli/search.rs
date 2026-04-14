use turso::Connection;

use crate::error::Result;
use crate::palace::search::search_memories;

pub async fn run(
    connection: &Connection,
    query: &str,
    wing: Option<&str>,
    room: Option<&str>,
    n_results: usize,
) -> Result<()> {
    let results = search_memories(connection, query, wing, room, n_results).await?;

    if results.is_empty() {
        println!("\n  No results found for: \"{query}\"");
        return Ok(());
    }

    println!("\n============================================================");
    println!("  Results for: \"{query}\"");
    if let Some(w) = wing {
        println!("  Wing: {w}");
    }
    if let Some(r) = room {
        println!("  Room: {r}");
    }
    println!("============================================================\n");

    for (i, result) in results.iter().enumerate() {
        println!("  [{}] {} / {}", i + 1, result.wing, result.room);
        println!("      Source: {}", result.source_file);
        println!("      Match:  {} word hits", result.relevance);
        println!();
        for line in result.text.lines() {
            println!("      {line}");
        }
        println!();
        println!("  --------------------------------------------------------");
    }

    println!();
    Ok(())
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn run_no_results_succeeds() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection, "xyzzy_nonexistent_gibberish", None, None, 5).await;
        assert!(result.is_ok(), "search for gibberish must not error");
        assert_eq!(
            result.expect("run should succeed"),
            (),
            "run must return unit on success"
        );
    }

    #[tokio::test]
    async fn run_with_results_succeeds() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "search-test-1",
                wing: "docs",
                room: "guides",
                content: "quantum computing fundamentals and algorithms explained clearly",
                source_file: "quantum.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("seeding drawer for search test must succeed");

        let result = run(&connection, "quantum algorithms", None, None, 5).await;
        assert!(
            result.is_ok(),
            "search with matching content must not error"
        );
        assert_eq!(
            result.expect("run should succeed"),
            (),
            "run must return unit on success"
        );
    }
}
