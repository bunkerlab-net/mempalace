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
