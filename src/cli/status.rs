use turso::Connection;

use crate::error::Result;
use crate::palace::entity_registry::EntityRegistry;
use crate::palace::stack::MemoryStack;

/// Print palace statistics: total drawers, per-wing and per-room counts, KG summary,
/// and entity registry summary.
pub async fn run(connection: &Connection) -> Result<()> {
    let stats = MemoryStack::new(connection).status().await?;

    if stats.total_drawers == 0 {
        println!(
            "Palace is empty. Run `mempalace init <dir>` then `mempalace mine <dir>` to get started."
        );
        return Ok(());
    }

    println!("=== MemPalace Status ===\n");
    println!("Total drawers: {}\n", stats.total_drawers);

    println!("Wings:");
    for (wing, count) in &stats.wing_counts {
        println!("  {wing}: {count} drawers");
    }

    run_print_rooms(&stats.room_counts);
    run_print_kg(stats.entity_count, stats.triple_count);
    run_print_entity_registry();

    Ok(())
}

/// Print per-room drawer counts, grouping rooms under their wing header.
fn run_print_rooms(room_counts: &[(String, String, i64)]) {
    assert!(
        room_counts.iter().all(|(_, _, c)| *c >= 0),
        "run_print_rooms: all room counts must be non-negative"
    );
    println!("\nRooms:");
    let mut current_wing = String::new();
    for (wing, room, count) in room_counts {
        if wing != &current_wing {
            println!("  [{wing}]");
            current_wing.clone_from(wing);
        }
        println!("    {room}: {count}");
    }
    assert!(
        !current_wing.is_empty() || room_counts.is_empty(),
        "run_print_rooms: must print at least one wing when rooms are present"
    );
}

/// Print KG entity and triple counts when either is non-zero.
fn run_print_kg(entity_count: i64, triple_count: i64) {
    assert!(
        entity_count >= 0,
        "run_print_kg: entity_count must be non-negative"
    );
    assert!(
        triple_count >= 0,
        "run_print_kg: triple_count must be non-negative"
    );
    if entity_count > 0 || triple_count > 0 {
        println!("\nKnowledge Graph:");
        println!("  Entities: {entity_count}");
        println!("  Triples: {triple_count}");
    }
}

/// Print entity registry summary: people, projects, ambiguous names, wiki cache count.
fn run_print_entity_registry() {
    let registry = EntityRegistry::load();
    let summary = registry.summary();
    assert!(
        !summary.is_empty(),
        "run_print_entity_registry: summary must not be empty"
    );
    assert!(
        summary.contains("Mode:"),
        "run_print_entity_registry: summary must contain Mode header"
    );
    println!("\nEntity Registry:");
    for line in summary.lines() {
        println!("  {line}");
    }
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn run_empty_palace_succeeds() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection).await;
        assert!(result.is_ok(), "status on empty palace must not error");
        assert_eq!(
            result.expect("run should succeed"),
            (),
            "run must return unit on success"
        );
    }

    #[tokio::test]
    async fn run_with_data_succeeds() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "status-test-1",
                wing: "docs",
                room: "guides",
                content: "status test content with enough words for indexing",
                source_file: "readme.md",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("seeding drawer for status test must succeed");

        let result = run(&connection).await;
        assert!(result.is_ok(), "status with data must not error");
        assert_eq!(
            result.expect("run should succeed"),
            (),
            "run must return unit on success"
        );
    }
}
