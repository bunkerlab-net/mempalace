use std::collections::HashMap;

use chrono::Utc;
use serde_json::{Value, json};
use turso::Connection;

use uuid::Uuid;

use crate::db::query_all;
use crate::kg;
use crate::palace::{drawer, graph, search};

use super::protocol::{AAAK_SPEC, PALACE_PROTOCOL};

/// Dispatch a tool call by name and return the JSON result.
pub async fn dispatch(conn: &Connection, name: &str, args: &Value) -> Value {
    match name {
        "mempalace_status" => tool_status(conn).await,
        "mempalace_list_wings" => tool_list_wings(conn).await,
        "mempalace_list_rooms" => tool_list_rooms(conn, args).await,
        "mempalace_get_taxonomy" => tool_get_taxonomy(conn).await,
        "mempalace_get_aaak_spec" => json!({"aaak_spec": AAAK_SPEC}),
        "mempalace_search" => tool_search(conn, args).await,
        "mempalace_check_duplicate" => tool_check_duplicate(conn, args).await,
        "mempalace_add_drawer" => tool_add_drawer(conn, args).await,
        "mempalace_delete_drawer" => tool_delete_drawer(conn, args).await,
        "mempalace_kg_query" => tool_kg_query(conn, args).await,
        "mempalace_kg_add" => tool_kg_add(conn, args).await,
        "mempalace_kg_invalidate" => tool_kg_invalidate(conn, args).await,
        "mempalace_kg_timeline" => tool_kg_timeline(conn, args).await,
        "mempalace_kg_stats" => tool_kg_stats(conn).await,
        "mempalace_traverse" => tool_traverse(conn, args).await,
        "mempalace_find_tunnels" => tool_find_tunnels(conn, args).await,
        "mempalace_graph_stats" => tool_graph_stats(conn).await,
        "mempalace_diary_write" => tool_diary_write(conn, args).await,
        "mempalace_diary_read" => tool_diary_read(conn, args).await,
        _ => json!({"error": format!("Unknown tool: {name}")}),
    }
}

fn str_arg<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key).and_then(|v| v.as_str()).unwrap_or("")
}

/// Extract a positive integer argument, coercing floats and strings.
///
/// MCP JSON transport sometimes delivers integers as floats (`5.0`) or strings
/// (`"5"`). Trying all three representations keeps tool calls robust regardless
/// of what the client sends. Only accepts finite, whole, positive integers (>0).
fn int_arg(args: &Value, key: &str, default: i64) -> i64 {
    args.get(key)
        .and_then(|v| {
            v.as_i64()
                .and_then(|n| if n > 0 { Some(n) } else { None })
                .or_else(|| {
                    v.as_f64().and_then(|f| {
                        if f.is_finite() && f.fract() == 0.0 && f > 0.0 {
                            #[allow(clippy::cast_possible_truncation)]
                            Some(f as i64)
                        } else {
                            None
                        }
                    })
                })
                .or_else(|| {
                    v.as_str().and_then(|s| {
                        s.parse::<i64>()
                            .ok()
                            .and_then(|n| if n > 0 { Some(n) } else { None })
                            .or_else(|| {
                                s.parse::<f64>().ok().and_then(|f| {
                                    if f.is_finite() && f.fract() == 0.0 && f > 0.0 {
                                        #[allow(clippy::cast_possible_truncation)]
                                        Some(f as i64)
                                    } else {
                                        None
                                    }
                                })
                            })
                    })
                })
        })
        .unwrap_or(default)
}

async fn tool_status(conn: &Connection) -> Value {
    let rows = query_all(
        conn,
        "SELECT wing, room, COUNT(*) as cnt FROM drawers GROUP BY wing, room",
        (),
    )
    .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => return json!({"error": e.to_string()}),
    };

    let mut wings: HashMap<String, i64> = HashMap::new();
    let mut rooms: HashMap<String, i64> = HashMap::new();
    let mut total = 0i64;

    for row in &rows {
        let wing: String = row.get(0).unwrap_or_default();
        let room: String = row.get(1).unwrap_or_default();
        let count: i64 = row.get(2).unwrap_or(0);
        *wings.entry(wing).or_insert(0) += count;
        *rooms.entry(room).or_insert(0) += count;
        total += count;
    }

    json!({
        "total_drawers": total,
        "wings": wings,
        "rooms": rooms,
        "protocol": PALACE_PROTOCOL,
        "aaak_dialect": AAAK_SPEC,
    })
}

async fn tool_list_wings(conn: &Connection) -> Value {
    let rows = query_all(
        conn,
        "SELECT wing, COUNT(*) as cnt FROM drawers GROUP BY wing",
        (),
    )
    .await;

    match rows {
        Ok(rows) => {
            let mut wings: HashMap<String, i64> = HashMap::new();
            for row in &rows {
                let wing: String = row.get(0).unwrap_or_default();
                let count: i64 = row.get(1).unwrap_or(0);
                wings.insert(wing, count);
            }
            json!({"wings": wings})
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_list_rooms(conn: &Connection, args: &Value) -> Value {
    let wing = str_arg(args, "wing");

    let rows = if wing.is_empty() {
        query_all(
            conn,
            "SELECT room, COUNT(*) as cnt FROM drawers GROUP BY room",
            (),
        )
        .await
    } else {
        query_all(
            conn,
            "SELECT room, COUNT(*) as cnt FROM drawers WHERE wing = ? GROUP BY room",
            [wing.to_string()],
        )
        .await
    };

    match rows {
        Ok(rows) => {
            let mut rooms: HashMap<String, i64> = HashMap::new();
            for row in &rows {
                let room: String = row.get(0).unwrap_or_default();
                let count: i64 = row.get(1).unwrap_or(0);
                rooms.insert(room, count);
            }
            json!({"wing": if wing.is_empty() { "all" } else { wing }, "rooms": rooms})
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_get_taxonomy(conn: &Connection) -> Value {
    let rows = query_all(
        conn,
        "SELECT wing, room, COUNT(*) as cnt FROM drawers GROUP BY wing, room",
        (),
    )
    .await;

    match rows {
        Ok(rows) => {
            let mut taxonomy: HashMap<String, HashMap<String, i64>> = HashMap::new();
            for row in &rows {
                let wing: String = row.get(0).unwrap_or_default();
                let room: String = row.get(1).unwrap_or_default();
                let count: i64 = row.get(2).unwrap_or(0);
                taxonomy.entry(wing).or_default().insert(room, count);
            }
            json!({"taxonomy": taxonomy})
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_search(conn: &Connection, args: &Value) -> Value {
    let query = str_arg(args, "query");
    let limit = usize::try_from(int_arg(args, "limit", 5)).unwrap_or(5);
    let wing = {
        let w = str_arg(args, "wing");
        if w.is_empty() {
            None
        } else {
            Some(w.to_string())
        }
    };
    let room = {
        let r = str_arg(args, "room");
        if r.is_empty() {
            None
        } else {
            Some(r.to_string())
        }
    };

    match search::search_memories(conn, query, wing.as_deref(), room.as_deref(), limit).await {
        Ok(results) => {
            let items: Vec<Value> = results
                .iter()
                .map(|r| {
                    json!({
                        "wing": r.wing,
                        "room": r.room,
                        "content": r.text,
                        "source_file": r.source_file,
                        "similarity": r.relevance,
                    })
                })
                .collect();
            json!({"results": items, "count": items.len()})
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_check_duplicate(conn: &Connection, args: &Value) -> Value {
    let content = str_arg(args, "content");
    // Simple keyword overlap check since we don't have vector similarity
    match search::search_memories(conn, content, None, None, 5).await {
        Ok(results) => {
            let matches: Vec<Value> = results
                .iter()
                .filter(|r| r.relevance > 3.0) // high word overlap
                .map(|r| {
                    let preview = if r.text.len() > 200 {
                        format!("{}...", &r.text[..200])
                    } else {
                        r.text.clone()
                    };
                    json!({
                        "wing": r.wing,
                        "room": r.room,
                        "content": preview,
                    })
                })
                .collect();
            json!({
                "is_duplicate": !matches.is_empty(),
                "matches": matches,
            })
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_add_drawer(conn: &Connection, args: &Value) -> Value {
    let wing = str_arg(args, "wing");
    let room = str_arg(args, "room");
    let content = str_arg(args, "content");
    let source_file = str_arg(args, "source_file");
    let added_by = {
        let a = str_arg(args, "added_by");
        if a.is_empty() { "mcp" } else { a }
    };

    if wing.is_empty() || room.is_empty() || content.is_empty() {
        return json!({"success": false, "error": "wing, room, and content are required"});
    }

    // Reject if a highly-similar drawer already exists (mirrors Python behaviour).
    if let Ok(results) = search::search_memories(conn, content, None, None, 5).await {
        let dups: Vec<Value> = results
            .iter()
            .filter(|r| r.relevance > 3.0)
            .map(|r| {
                let preview = if r.text.len() > 200 {
                    format!("{}...", &r.text[..200])
                } else {
                    r.text.clone()
                };
                json!({"wing": r.wing, "room": r.room, "content": preview})
            })
            .collect();
        if !dups.is_empty() {
            return json!({"success": false, "reason": "duplicate", "matches": dups});
        }
    }

    let id = Uuid::new_v4().to_string();
    let params = drawer::DrawerParams {
        id: &id,
        wing,
        room,
        content,
        source_file: if source_file.is_empty() {
            ""
        } else {
            source_file
        },
        chunk_index: 0,
        added_by,
        ingest_mode: "mcp",
    };

    match drawer::add_drawer(conn, &params).await {
        Ok(_) => json!({"success": true, "drawer_id": id, "wing": wing, "room": room}),
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_delete_drawer(conn: &Connection, args: &Value) -> Value {
    let drawer_id = str_arg(args, "drawer_id");
    if drawer_id.is_empty() {
        return json!({"success": false, "error": "drawer_id is required"});
    }

    match conn
        .execute("DELETE FROM drawers WHERE id = ?", [drawer_id.to_string()])
        .await
    {
        Ok(_) => {
            // Also clean up inverted index
            let _ = conn
                .execute(
                    "DELETE FROM drawer_words WHERE drawer_id = ?",
                    [drawer_id.to_string()],
                )
                .await;
            json!({"success": true, "drawer_id": drawer_id})
        }
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_kg_query(conn: &Connection, args: &Value) -> Value {
    let entity = str_arg(args, "entity");
    let as_of = {
        let a = str_arg(args, "as_of");
        if a.is_empty() {
            None
        } else {
            Some(a.to_string())
        }
    };
    let direction = {
        let d = str_arg(args, "direction");
        if d.is_empty() { "both" } else { d }
    };

    match kg::query::query_entity(conn, entity, as_of.as_deref(), direction).await {
        Ok(facts) => {
            let count = facts.len();
            json!({"entity": entity, "as_of": as_of, "facts": facts, "count": count})
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_kg_add(conn: &Connection, args: &Value) -> Value {
    let subject = str_arg(args, "subject");
    let predicate = str_arg(args, "predicate");
    let object = str_arg(args, "object");
    let valid_from = {
        let v = str_arg(args, "valid_from");
        if v.is_empty() {
            None
        } else {
            Some(v.to_string())
        }
    };
    let source_closet = {
        let s = str_arg(args, "source_closet");
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    };

    match kg::add_triple(
        conn,
        &kg::TripleParams {
            subject,
            predicate,
            object,
            valid_from: valid_from.as_deref(),
            valid_to: None,
            confidence: 1.0,
            source_closet: source_closet.as_deref(),
            source_file: None,
        },
    )
    .await
    {
        Ok(triple_id) => json!({
            "success": true,
            "triple_id": triple_id,
            "fact": format!("{subject} → {predicate} → {object}"),
        }),
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_kg_invalidate(conn: &Connection, args: &Value) -> Value {
    let subject = str_arg(args, "subject");
    let predicate = str_arg(args, "predicate");
    let object = str_arg(args, "object");
    let ended = {
        let e = str_arg(args, "ended");
        if e.is_empty() {
            None
        } else {
            Some(e.to_string())
        }
    };

    match kg::invalidate(conn, subject, predicate, object, ended.as_deref()).await {
        Ok(()) => json!({
            "success": true,
            "fact": format!("{subject} → {predicate} → {object}"),
            "ended": ended.unwrap_or_else(|| "today".to_string()),
        }),
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_kg_timeline(conn: &Connection, args: &Value) -> Value {
    let entity = {
        let e = str_arg(args, "entity");
        if e.is_empty() {
            None
        } else {
            Some(e.to_string())
        }
    };

    match kg::query::timeline(conn, entity.as_deref()).await {
        Ok(facts) => {
            let count = facts.len();
            json!({
                "entity": entity.unwrap_or_else(|| "all".to_string()),
                "timeline": facts,
                "count": count,
            })
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_kg_stats(conn: &Connection) -> Value {
    match kg::query::stats(conn).await {
        Ok(stats) => json!(stats),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_traverse(conn: &Connection, args: &Value) -> Value {
    let start_room = str_arg(args, "start_room");
    let max_hops = usize::try_from(int_arg(args, "max_hops", 2)).unwrap_or(2);

    match graph::traverse(conn, start_room, max_hops).await {
        Ok(results) => json!(results),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_find_tunnels(conn: &Connection, args: &Value) -> Value {
    let wing_a = {
        let w = str_arg(args, "wing_a");
        if w.is_empty() {
            None
        } else {
            Some(w.to_string())
        }
    };
    let wing_b = {
        let w = str_arg(args, "wing_b");
        if w.is_empty() {
            None
        } else {
            Some(w.to_string())
        }
    };

    match graph::find_tunnels(conn, wing_a.as_deref(), wing_b.as_deref()).await {
        Ok(tunnels) => json!(tunnels),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_graph_stats(conn: &Connection) -> Value {
    match graph::graph_stats(conn).await {
        Ok(stats) => json!(stats),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_diary_write(conn: &Connection, args: &Value) -> Value {
    let agent_name = str_arg(args, "agent_name");
    let entry = str_arg(args, "entry");
    let topic = {
        let t = str_arg(args, "topic");
        if t.is_empty() { "general" } else { t }
    };

    if agent_name.is_empty() || entry.is_empty() {
        return json!({"success": false, "error": "agent_name and entry are required"});
    }

    let wing = format!("wing_{}", agent_name.to_lowercase().replace(' ', "_"));
    let now = Utc::now();
    let id = Uuid::new_v4().to_string();

    // Use direct SQL to also set extract_mode (topic) which DrawerParams doesn't support
    match conn
        .execute(
            "INSERT OR IGNORE INTO drawers (id, wing, room, content, source_file, chunk_index, added_by, ingest_mode, extract_mode) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            turso::params![id.as_str(), wing.as_str(), "diary", entry, "", 0i32, agent_name, "diary", topic],
        )
        .await
    {
        Ok(_) => {
            let _ = drawer::index_words(conn, &id, entry).await;
            json!({
                "success": true,
                "entry_id": id,
                "agent": agent_name,
                "topic": topic,
                "timestamp": now.to_rfc3339(),
            })
        }
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_diary_read(conn: &Connection, args: &Value) -> Value {
    let agent_name = str_arg(args, "agent_name");
    let last_n = int_arg(args, "last_n", 10);

    if agent_name.is_empty() {
        return json!({"error": "agent_name is required"});
    }

    let wing = format!("wing_{}", agent_name.to_lowercase().replace(' ', "_"));

    let rows = query_all(
        conn,
        "SELECT id, content, extract_mode, filed_at FROM drawers WHERE wing = ? AND room = 'diary' ORDER BY filed_at DESC LIMIT ?",
        (wing.clone(), last_n),
    )
    .await;

    match rows {
        Ok(rows) => {
            let entries: Vec<Value> = rows
                .iter()
                .map(|row| {
                    let id: String = row.get(0).unwrap_or_default();
                    let content: String = row.get(1).unwrap_or_default();
                    let topic: String = row.get(2).unwrap_or_default();
                    let filed_at: String = row.get(3).unwrap_or_default();
                    json!({
                        "id": id,
                        "content": content,
                        "topic": topic,
                        "timestamp": filed_at,
                    })
                })
                .collect();
            let total = entries.len();
            json!({
                "agent": agent_name,
                "entries": entries,
                "total": total,
                "showing": total,
            })
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}
