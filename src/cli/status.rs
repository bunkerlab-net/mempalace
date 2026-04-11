use turso::Connection;

use crate::db;
use crate::error::Result;

async fn run_print_wings(conn: &Connection) -> Result<()> {
    let rows = db::query_all(
        conn,
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

async fn run_print_rooms(conn: &Connection) -> Result<()> {
    let rows = db::query_all(
        conn,
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

async fn run_print_kg(conn: &Connection) -> Result<()> {
    let entity_rows = db::query_all(conn, "SELECT COUNT(*) FROM entities", ()).await?;
    let entity_count: i64 = entity_rows
        .first()
        .and_then(|r| r.get_value(0).ok())
        .and_then(|v| v.as_integer().copied())
        .unwrap_or(0);

    let triple_rows = db::query_all(conn, "SELECT COUNT(*) FROM triples", ()).await?;
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

pub async fn run(conn: &Connection) -> Result<()> {
    let rows = db::query_all(conn, "SELECT COUNT(*) FROM drawers", ()).await?;
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

    run_print_wings(conn).await?;
    run_print_rooms(conn).await?;
    run_print_kg(conn).await?;

    Ok(())
}
