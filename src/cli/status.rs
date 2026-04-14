use turso::Connection;

use crate::db;
use crate::error::Result;

async fn run_print_wings(connection: &Connection) -> Result<()> {
    let rows = db::query_all(
        connection,
        "SELECT wing, COUNT(*) as cnt FROM drawers GROUP BY wing ORDER BY cnt DESC",
        (),
    )
    .await?;

    println!("Wings:");
    for row in &rows {
        let wing = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let count = row
            .get_value(1)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .unwrap_or(0);
        println!("  {wing}: {count} drawers");
    }
    Ok(())
}

async fn run_print_rooms(connection: &Connection) -> Result<()> {
    let rows = db::query_all(
        connection,
        "SELECT wing, room, COUNT(*) as cnt FROM drawers GROUP BY wing, room ORDER BY wing, cnt DESC",
        (),
    )
    .await?;

    println!("\nRooms:");
    let mut current_wing = String::new();
    for row in &rows {
        let wing = row
            .get_value(0)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let room = row
            .get_value(1)
            .ok()
            .and_then(|v| v.as_text().cloned())
            .unwrap_or_default();
        let count = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .unwrap_or(0);

        if wing != current_wing {
            println!("  [{wing}]");
            current_wing = wing;
        }
        println!("    {room}: {count}");
    }
    Ok(())
}

async fn run_print_kg(connection: &Connection) -> Result<()> {
    let entity_rows = db::query_all(connection, "SELECT COUNT(*) FROM entities", ()).await?;
    let entity_count: i64 = entity_rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    let triple_rows = db::query_all(connection, "SELECT COUNT(*) FROM triples", ()).await?;
    let triple_count: i64 = triple_rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    if entity_count > 0 || triple_count > 0 {
        println!("\nKnowledge Graph:");
        println!("  Entities: {entity_count}");
        println!("  Triples: {triple_count}");
    }
    Ok(())
}

pub async fn run(connection: &Connection) -> Result<()> {
    let rows = db::query_all(connection, "SELECT COUNT(*) FROM drawers", ()).await?;
    let total: i64 = rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    if total == 0 {
        println!(
            "Palace is empty. Run `mempalace init <dir>` then `mempalace mine <dir>` to get started."
        );
        return Ok(());
    }

    println!("=== MemPalace Status ===\n");
    println!("Total drawers: {total}\n");

    run_print_wings(connection).await?;
    run_print_rooms(connection).await?;
    run_print_kg(connection).await?;

    Ok(())
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
