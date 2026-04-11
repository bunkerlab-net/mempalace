use std::collections::HashMap;
use std::io::Write as _;

use chrono::Utc;
use serde_json::{Value, json};
use sha2::Digest as _;
use turso::Connection;

use uuid::Uuid;

use crate::db::query_all;
use crate::kg;
use crate::palace::{drawer, graph, query_sanitizer, search};

use super::protocol::{AAAK_SPEC, PALACE_PROTOCOL};

/// Largest integer exactly representable as an f64 (2^53 − 1).
/// Values above this lose precision when stored in f64, so we reject them.
const MAX_EXACT_INT_F64: f64 = 9_007_199_254_740_991.0;

/// Dispatch a tool call by name and return the JSON result.
pub async fn dispatch(conn: &Connection, name: &str, args: &Value) -> Value {
    assert!(!name.is_empty(), "tool name must not be empty");
    assert!(args.is_object(), "tool args must be a JSON object");

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
        "mempalace_get_drawer" => tool_get_drawer(conn, args).await,
        "mempalace_list_drawers" => tool_list_drawers(conn, args).await,
        "mempalace_update_drawer" => tool_update_drawer(conn, args).await,
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
        _ => json!({"error": format!("Unknown tool: {name}"), "public": true}),
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
                        if f.is_finite() && f > 0.0 && f <= MAX_EXACT_INT_F64 && f.fract() == 0.0 {
                            // Safe: MAX_EXACT_INT_F64 (2^53-1) < i64::MAX, so the value fits exactly
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
                                    if f.is_finite()
                                        && f > 0.0
                                        && f <= MAX_EXACT_INT_F64
                                        && f.fract() == 0.0
                                    {
                                        // Safe: MAX_EXACT_INT_F64 (2^53-1) < i64::MAX, so the value fits exactly
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

/// Validate a wing/room/entity name.  Returns `Some(error_json)` if invalid.
///
/// Validates and trims `value`.
///
/// Returns `Ok(trimmed)` on success, or `Err(error_json)` if the value is
/// empty, too long, contains path-traversal sequences, null bytes, an invalid
/// first character, or characters outside `[a-zA-Z0-9_ .'-]`.
fn sanitize_name(value: &str, field_name: &str) -> Result<String, Value> {
    let v = value.trim();
    if v.is_empty() {
        return Err(
            json!({"success": false, "error": format!("{field_name} must be a non-empty string"), "public": true}),
        );
    }
    if v.len() > 128 {
        return Err(
            json!({"success": false, "error": format!("{field_name} exceeds maximum length of 128 characters"), "public": true}),
        );
    }
    if v.contains("..") || v.contains('/') || v.contains('\\') || v.contains('\x00') {
        return Err(
            json!({"success": false, "error": format!("{field_name} contains invalid characters"), "public": true}),
        );
    }
    if !v.chars().next().is_some_and(|c| c.is_ascii_alphanumeric()) {
        return Err(
            json!({"success": false, "error": format!("{field_name} must start with an alphanumeric character"), "public": true}),
        );
    }
    if !v
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '.' | '\'' | '-'))
    {
        return Err(
            json!({"success": false, "error": format!("{field_name} contains invalid characters"), "public": true}),
        );
    }
    let result = v.to_string();

    // Postconditions: result is non-empty, trimmed, and has no path-traversal chars.
    debug_assert!(!result.is_empty());
    debug_assert!(result == result.trim());
    debug_assert!(!result.contains(".."));
    debug_assert!(!result.contains('/'));
    debug_assert!(!result.contains('\\'));
    debug_assert!(!result.contains('\0'));

    Ok(result)
}

/// Validate an optional name filter.
///
/// Returns `Ok(None)` if the value is empty/whitespace-only, `Ok(Some(trimmed))`
/// if valid, or `Err(error_json)` if the non-empty value fails `sanitize_name`.
fn sanitize_opt_name(value: &str, field_name: &str) -> Result<Option<String>, Value> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    sanitize_name(value, field_name).map(Some)
}

/// Validate drawer/diary content.  Returns `Ok(trimmed)` if valid, or `Err(error_json)` if not.
fn sanitize_content(value: &str) -> Result<String, Value> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(
            json!({"success": false, "error": "content must be a non-empty string", "public": true}),
        );
    }
    if trimmed.chars().count() > 100_000 {
        return Err(
            json!({"success": false, "error": "content exceeds maximum length of 100,000 characters", "public": true}),
        );
    }
    if trimmed.contains('\0') {
        return Err(
            json!({"success": false, "error": "content contains null bytes", "public": true}),
        );
    }

    let result = trimmed.to_string();

    // Postconditions: result is non-empty and has no null bytes.
    debug_assert!(!result.is_empty());
    debug_assert!(!result.contains('\0'));

    Ok(result)
}

/// Append a write-operation entry to `~/.mempalace/wal/write_log.jsonl`.
///
/// Failures are non-fatal: logged to stderr so the server stays alive even if
/// the WAL directory is unwritable.  I/O is offloaded to `spawn_blocking` so
/// the async worker thread is not stalled by filesystem calls.
async fn wal_log(operation: &str, params: Value) {
    assert!(!operation.is_empty(), "WAL operation must not be empty");

    let operation = operation.to_string();
    let _ = tokio::task::spawn_blocking(move || {
        let wal_dir = crate::config::config_dir().join("wal");

        // Create directory with restrictive permissions atomically on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            if std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&wal_dir)
                .is_err()
            {
                eprintln!("WAL: could not create {}", wal_dir.display());
                return;
            }
        }
        #[cfg(not(unix))]
        if std::fs::create_dir_all(&wal_dir).is_err() {
            eprintln!("WAL: could not create {}", wal_dir.display());
            return;
        }

        let wal_file = wal_dir.join("write_log.jsonl");
        let entry = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "operation": operation,
            "params": params,
        });
        let mut line = serde_json::to_string(&entry).unwrap_or_default();
        line.push('\n');

        // Open with restrictive mode atomically on Unix.
        #[cfg(unix)]
        let open_result = {
            use std::os::unix::fs::OpenOptionsExt as _;
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(&wal_file)
        };
        #[cfg(not(unix))]
        let open_result = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_file);

        match open_result {
            Ok(mut file) => {
                if let Err(e) = file.write_all(line.as_bytes()) {
                    eprintln!("WAL write failed: {e}");
                }
            }
            Err(e) => eprintln!("WAL write failed: {e}"),
        }
    })
    .await;
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
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(v) => v,
        Err(e) => return e,
    };

    let rows = if let Some(ref w) = wing {
        query_all(
            conn,
            "SELECT room, COUNT(*) as cnt FROM drawers WHERE wing = ? GROUP BY room",
            [w.as_str()],
        )
        .await
    } else {
        query_all(
            conn,
            "SELECT room, COUNT(*) as cnt FROM drawers GROUP BY room",
            (),
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
            json!({"wing": wing.as_deref().unwrap_or("all"), "rooms": rooms})
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
    let raw_query = str_arg(args, "query").trim();
    if raw_query.is_empty() {
        return json!({"error": "query must be a non-empty string", "public": true});
    }
    let limit = usize::try_from(int_arg(args, "limit", 5).clamp(1, 100)).unwrap_or(5);
    let context_received = !str_arg(args, "context").trim().is_empty();
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let room = match sanitize_opt_name(str_arg(args, "room"), "room") {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Mitigate system prompt contamination before the search (mempalace-py issue #333).
    let sanitized = query_sanitizer::sanitize_query(raw_query);

    match search::search_memories(
        conn,
        &sanitized.clean_query,
        wing.as_deref(),
        room.as_deref(),
        limit,
    )
    .await
    {
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
            let count = items.len();
            let mut out = json!({"results": items, "count": count});
            if sanitized.was_sanitized {
                out["query_sanitized"] = json!(true);
                out["sanitizer"] = json!({
                    "method": sanitized.method,
                    "original_length": sanitized.original_length,
                    "clean_length": sanitized.clean_length,
                    "clean_query": sanitized.clean_query,
                });
            }
            if context_received {
                out["context_received"] = json!(true);
            }
            out
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
                    let preview = if r.text.chars().count() > 200 {
                        format!("{}...", r.text.chars().take(200).collect::<String>())
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

    let wing = match sanitize_name(wing, "wing") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let room = match sanitize_name(room, "room") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let content = match sanitize_content(content) {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Deterministic ID: sha256(wing+room+content) so the same content in
    // the same wing/room always produces the same ID, making the call idempotent.
    let hash = sha2::Sha256::digest(format!("{wing}\u{1f}{room}\u{1f}{content}").as_bytes());
    let hex: String = hash.iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    });
    let id = format!("drawer_{wing}_{room}_{}", &hex[..24]);
    // Postcondition: deterministic ID follows naming convention.
    assert!(
        id.starts_with("drawer_"),
        "drawer ID must start with drawer_"
    );

    tool_add_drawer_insert(conn, id, wing, room, content, source_file, added_by).await
}

/// Log the WAL event and write the drawer row. Returns the MCP response JSON.
async fn tool_add_drawer_insert(
    conn: &Connection,
    id: String,
    wing: String,
    room: String,
    content: String,
    source_file: &str,
    added_by: &str,
) -> Value {
    wal_log(
        "add_drawer",
        json!({
            "drawer_id": id,
            "wing": wing,
            "room": room,
            "added_by": added_by,
            "content_length": content.len(),
            "content_preview": format!("[REDACTED {} chars]", content.chars().count()),
        }),
    )
    .await;

    let params = drawer::DrawerParams {
        id: &id,
        wing: &wing,
        room: &room,
        content: &content,
        source_file: if source_file.is_empty() {
            ""
        } else {
            source_file
        },
        chunk_index: 0,
        added_by,
        ingest_mode: "mcp",
        source_mtime: None,
    };

    // Branch on add_drawer's bool rather than doing a separate SELECT first.
    // The INSERT OR IGNORE inside add_drawer is atomic, so this is race-free.
    match drawer::add_drawer(conn, &params).await {
        Ok(true) => json!({"success": true, "drawer_id": id, "wing": wing, "room": room}),
        Ok(false) => json!({
            "success": true,
            "reason": "already_exists",
            "drawer_id": id,
            "wing": wing,
            "room": room,
        }),
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_delete_drawer(conn: &Connection, args: &Value) -> Value {
    let drawer_id = match sanitize_name(str_arg(args, "drawer_id"), "drawer_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    if !drawer_id.starts_with("drawer_") {
        return json!({"success": false, "error": "drawer_id has invalid format", "public": true});
    }

    wal_log("delete_drawer", json!({"drawer_id": drawer_id})).await;

    match conn
        .execute("DELETE FROM drawers WHERE id = ?", [drawer_id.as_str()])
        .await
    {
        Ok(_) => {
            // Also clean up inverted index
            let _ = conn
                .execute(
                    "DELETE FROM drawer_words WHERE drawer_id = ?",
                    [drawer_id.as_str()],
                )
                .await;
            json!({"success": true, "drawer_id": drawer_id})
        }
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

/// Fetch a single drawer by ID, returning its full content and metadata.
async fn tool_get_drawer(conn: &Connection, args: &Value) -> Value {
    let drawer_id = match sanitize_name(str_arg(args, "drawer_id"), "drawer_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    if !drawer_id.starts_with("drawer_") {
        return json!({"error": "drawer_id has invalid format", "public": true});
    }

    let rows = query_all(
        conn,
        "SELECT id, content, wing, room, source_file, filed_at FROM drawers WHERE id = ?",
        [drawer_id.as_str()],
    )
    .await;

    match rows {
        Ok(rows) if rows.is_empty() => {
            json!({"error": format!("Drawer not found: {drawer_id}"), "public": true})
        }
        Ok(rows) => {
            let row = &rows[0];
            let content: String = row.get(1).unwrap_or_default();
            let wing: String = row.get(2).unwrap_or_default();
            let room: String = row.get(3).unwrap_or_default();
            let source_file: String = row.get(4).unwrap_or_default();
            let filed_at: String = row.get(5).unwrap_or_default();
            json!({
                "drawer_id": drawer_id,
                "content": content,
                "wing": wing,
                "room": room,
                "source_file": source_file,
                "filed_at": filed_at,
            })
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

/// List drawers with optional wing/room filtering and cursor-style pagination.
async fn tool_list_drawers(conn: &Connection, args: &Value) -> Value {
    const MAX_LIMIT: i64 = 100;
    let limit = int_arg(args, "limit", 20).clamp(1, MAX_LIMIT);
    let offset = int_arg(args, "offset", 0).max(0);
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let room = match sanitize_opt_name(str_arg(args, "room"), "room") {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Exclude diary entries (ingest_mode = 'diary') — they use UUID IDs, not
    // the drawer_ prefix scheme, and have their own mempalace_diary_read tool.
    // Use id DESC as a tiebreaker so pages are stable when filed_at values collide.
    let rows = match (&wing, &room) {
        (Some(w), Some(r)) => {
            query_all(
                conn,
                "SELECT id, content, wing, room FROM drawers WHERE wing = ?1 AND room = ?2 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?3 OFFSET ?4",
                (w.as_str(), r.as_str(), limit, offset),
            )
            .await
        }
        (Some(w), None) => {
            query_all(
                conn,
                "SELECT id, content, wing, room FROM drawers WHERE wing = ?1 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?2 OFFSET ?3",
                (w.as_str(), limit, offset),
            )
            .await
        }
        (None, Some(r)) => {
            query_all(
                conn,
                "SELECT id, content, wing, room FROM drawers WHERE room = ?1 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?2 OFFSET ?3",
                (r.as_str(), limit, offset),
            )
            .await
        }
        (None, None) => {
            query_all(
                conn,
                "SELECT id, content, wing, room FROM drawers WHERE (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?1 OFFSET ?2",
                (limit, offset),
            )
            .await
        }
    };

    match rows {
        Ok(rows) => {
            let drawers: Vec<Value> = rows
                .iter()
                .map(|row| {
                    let id: String = row.get(0).unwrap_or_default();
                    let content: String = row.get(1).unwrap_or_default();
                    let wing_val: String = row.get(2).unwrap_or_default();
                    let room_val: String = row.get(3).unwrap_or_default();
                    let preview = if content.chars().count() > 200 {
                        format!("{}...", content.chars().take(200).collect::<String>())
                    } else {
                        content.clone()
                    };
                    json!({
                        "drawer_id": id,
                        "wing": wing_val,
                        "room": room_val,
                        "content_preview": preview,
                    })
                })
                .collect();
            let count = drawers.len();
            json!({
                "drawers": drawers,
                "count": count,
                "offset": offset,
                "limit": limit,
            })
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

/// Update an existing drawer's content and/or location (wing/room).
///
/// Recomputes the deterministic SHA256 ID after any change to keep it
/// consistent with `tool_add_drawer`.  Rejects updates that would collide
/// with an existing drawer.
// The complexity comes from: ID recomputation, duplicate detection, conditional
// reindex, and error propagation — each a distinct correctness concern that
// cannot be collapsed without obscuring the logic.
#[allow(clippy::too_many_lines)]
async fn tool_update_drawer(conn: &Connection, args: &Value) -> Value {
    let drawer_id = match sanitize_name(str_arg(args, "drawer_id"), "drawer_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    // Diary entries use UUID IDs; they must not be mutated via this handler.
    if !drawer_id.starts_with("drawer_") {
        return json!({"success": false, "error": "drawer_id has invalid format", "public": true});
    }

    let new_content = {
        let s = str_arg(args, "content");
        if s.is_empty() {
            None
        } else {
            match sanitize_content(s) {
                Ok(v) => Some(v),
                Err(e) => return e,
            }
        }
    };
    let new_wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let new_room = match sanitize_opt_name(str_arg(args, "room"), "room") {
        Ok(v) => v,
        Err(e) => return e,
    };

    // No-op: nothing to change
    if new_content.is_none() && new_wing.is_none() && new_room.is_none() {
        return json!({"success": true, "drawer_id": drawer_id, "noop": true});
    }

    // Fetch existing drawer
    let rows = query_all(
        conn,
        "SELECT wing, room, content FROM drawers WHERE id = ?",
        [drawer_id.as_str()],
    )
    .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => return json!({"success": false, "error": e.to_string()}),
    };

    if rows.is_empty() {
        return json!({"success": false, "error": format!("Drawer not found: {drawer_id}"), "public": true});
    }

    let old_wing: String = rows[0].get(0).unwrap_or_default();
    let old_room: String = rows[0].get(1).unwrap_or_default();
    let old_content: String = rows[0].get(2).unwrap_or_default();

    let final_wing = new_wing.as_deref().unwrap_or(&old_wing);
    let final_room = new_room.as_deref().unwrap_or(&old_room);
    let final_content = new_content.as_deref().unwrap_or(&old_content);

    // Recompute the deterministic ID to keep it consistent with tool_add_drawer.
    // wing/room/content are all baked into the ID, so any change means a new ID.
    let hash = sha2::Sha256::digest(
        format!("{final_wing}\u{1f}{final_room}\u{1f}{final_content}").as_bytes(),
    );
    let hex: String = hash.iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    });
    let new_id = format!("drawer_{final_wing}_{final_room}_{}", &hex[..24]);

    wal_log(
        "update_drawer",
        json!({
            "drawer_id": drawer_id,
            "new_drawer_id": new_id,
            "old_wing": old_wing,
            "old_room": old_room,
            "new_wing": final_wing,
            "new_room": final_room,
            "content_changed": new_content.is_some(),
        }),
    )
    .await;

    // If the recomputed ID already exists (and differs), the new wing+room+content
    // is a duplicate of another drawer — reject to prevent silent duplication.
    if new_id != drawer_id {
        let existing = query_all(
            conn,
            "SELECT id FROM drawers WHERE id = ?",
            [new_id.as_str()],
        )
        .await;
        match existing {
            Ok(rows) if !rows.is_empty() => {
                return json!({
                    "success": false,
                    "error": "A drawer with this wing/room/content already exists",
                    "existing_drawer_id": new_id,
                    "public": true,
                });
            }
            Err(e) => return json!({"success": false, "error": e.to_string()}),
            Ok(_) => {}
        }
    }

    match conn
        .execute(
            "UPDATE drawers SET id = ?1, wing = ?2, room = ?3, content = ?4 WHERE id = ?5",
            turso::params![
                new_id.as_str(),
                final_wing,
                final_room,
                final_content,
                drawer_id.as_str()
            ],
        )
        .await
    {
        Ok(_) => {
            // Re-index words: always needed when the ID changes or content changes.
            if let Err(e) = conn
                .execute(
                    "DELETE FROM drawer_words WHERE drawer_id = ?",
                    [drawer_id.as_str()],
                )
                .await
            {
                return json!({"success": false, "error": e.to_string()});
            }
            if new_id != drawer_id {
                // drawer_words rows for the old ID were deleted above; if the new
                // ID already had entries (shouldn't happen — we checked above),
                // clean those too.
                let _ = conn
                    .execute(
                        "DELETE FROM drawer_words WHERE drawer_id = ?",
                        [new_id.as_str()],
                    )
                    .await;
            }
            if let Err(e) = drawer::index_words(conn, &new_id, final_content).await {
                return json!({"success": false, "error": e.to_string()});
            }
            json!({
                "success": true,
                "drawer_id": new_id,
                "wing": final_wing,
                "room": final_room,
            })
        }
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_kg_query(conn: &Connection, args: &Value) -> Value {
    let entity = match sanitize_name(str_arg(args, "entity"), "entity") {
        Ok(v) => v,
        Err(e) => return e,
    };
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
    if !matches!(direction, "outgoing" | "incoming" | "both") {
        return json!({"error": "direction must be 'outgoing', 'incoming', or 'both'", "public": true});
    }

    match kg::query::query_entity(conn, &entity, as_of.as_deref(), direction).await {
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

    let subject = match sanitize_name(subject, "subject") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let predicate = match sanitize_name(predicate, "predicate") {
        Ok(v) => v,
        Err(e) => return e,
    };
    // `sanitize_name` intentionally restricts objects to [a-zA-Z0-9_ .'-].
    // If KG identifiers ever need ':', '@', or '/' (e.g. for namespaced IRIs),
    // introduce a dedicated `sanitize_kg_object` rather than relaxing this one.
    let object = match sanitize_name(object, "object") {
        Ok(v) => v,
        Err(e) => return e,
    };

    wal_log(
        "kg_add",
        json!({
            "subject": subject,
            "predicate": predicate,
            "object": object,
            "valid_from": valid_from,
            "source_closet": source_closet,
        }),
    )
    .await;

    match kg::add_triple(
        conn,
        &kg::TripleParams {
            subject: &subject,
            predicate: &predicate,
            object: &object,
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

    let subject = match sanitize_name(subject, "subject") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let predicate = match sanitize_name(predicate, "predicate") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let object = match sanitize_name(object, "object") {
        Ok(v) => v,
        Err(e) => return e,
    };

    wal_log(
        "kg_invalidate",
        json!({"subject": subject, "predicate": predicate, "object": object, "ended": ended}),
    )
    .await;

    match kg::invalidate(conn, &subject, &predicate, &object, ended.as_deref()).await {
        Ok(()) => json!({
            "success": true,
            "fact": format!("{subject} → {predicate} → {object}"),
            "ended": ended.unwrap_or_else(|| "today".to_string()),
        }),
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_kg_timeline(conn: &Connection, args: &Value) -> Value {
    let entity = match sanitize_opt_name(str_arg(args, "entity"), "entity") {
        Ok(v) => v,
        Err(e) => return e,
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
    let start_room = match sanitize_name(str_arg(args, "start_room"), "start_room") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let max_hops = usize::try_from(int_arg(args, "max_hops", 2).clamp(1, 10)).unwrap_or(2);

    match graph::traverse(conn, &start_room, max_hops).await {
        Ok(results) => json!(results),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_find_tunnels(conn: &Connection, args: &Value) -> Value {
    let wing_a = match sanitize_opt_name(str_arg(args, "wing_a"), "wing_a") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let wing_b = match sanitize_opt_name(str_arg(args, "wing_b"), "wing_b") {
        Ok(v) => v,
        Err(e) => return e,
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
        if t.is_empty() {
            "general".to_string()
        } else {
            match sanitize_name(t, "topic") {
                Ok(v) => v,
                Err(e) => return e,
            }
        }
    };

    let agent_name = match sanitize_name(agent_name, "agent_name") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let entry = match sanitize_content(entry) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let wing = format!("wing_{}", agent_name.to_lowercase().replace(' ', "_"));
    let now = Utc::now();
    let id = Uuid::new_v4().to_string();

    wal_log(
        "diary_write",
        json!({
            "agent_name": agent_name,
            "topic": topic,
            "entry_id": id,
            "entry_preview": format!("[REDACTED {} chars]", entry.chars().count()),
        }),
    )
    .await;

    // Use direct SQL to also set extract_mode (topic) which DrawerParams doesn't support
    match conn
        .execute(
            "INSERT OR IGNORE INTO drawers (id, wing, room, content, source_file, chunk_index, added_by, ingest_mode, extract_mode) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            turso::params![id.as_str(), wing.as_str(), "diary", entry.as_str(), "", 0i32, agent_name.as_str(), "diary", topic.as_str()],
        )
        .await
    {
        Ok(_) => {
            let _ = drawer::index_words(conn, &id, &entry).await;
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
    let last_n = int_arg(args, "last_n", 10).clamp(1, 100);

    let agent_name = match sanitize_name(agent_name, "agent_name") {
        Ok(v) => v,
        Err(e) => return e,
    };

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

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    async fn test_conn() -> (turso::Database, turso::Connection) {
        crate::test_helpers::test_db().await
    }

    // --- tool_add_drawer ---

    #[tokio::test]
    async fn add_drawer_inserts_and_returns_success() {
        let (_db, conn) = test_conn().await;
        let args = json!({
            "wing": "personal",
            "room": "notes",
            "content": "the quick brown fox jumps over the lazy dog",
        });
        let result = tool_add_drawer(&conn, &args).await;
        assert_eq!(result["success"], true);
        assert!(
            result["drawer_id"]
                .as_str()
                .expect("drawer_id must be a string")
                .starts_with("drawer_personal_notes_")
        );
        assert!(
            result.get("reason").is_none(),
            "fresh insert must not carry a reason"
        );
    }

    #[tokio::test]
    async fn add_drawer_idempotent_returns_already_exists() {
        let (_db, conn) = test_conn().await;
        let args = json!({
            "wing": "personal",
            "room": "notes",
            "content": "idempotent content for testing",
        });
        let first = tool_add_drawer(&conn, &args).await;
        assert_eq!(first["success"], true);

        let second = tool_add_drawer(&conn, &args).await;
        assert_eq!(second["success"], true);
        assert_eq!(second["reason"], "already_exists");
        // The same deterministic ID must be returned both times.
        assert_eq!(first["drawer_id"], second["drawer_id"]);
    }

    #[tokio::test]
    async fn add_drawer_deterministic_id_same_content() {
        let (_db, conn) = test_conn().await;
        // Verify the ID is derived from sha256(wing+room+content)[:24].
        let content = "fn main() { println!(\"hello\"); }";
        let args = json!({
            "wing": "proj",
            "room": "code",
            "content": content,
        });
        let result = tool_add_drawer(&conn, &args).await;
        let id = result["drawer_id"]
            .as_str()
            .expect("drawer_id must be a string");

        let hash = sha2::Sha256::digest(format!("proj\u{1f}code\u{1f}{content}").as_bytes());
        let hex: String = hash.iter().fold(String::new(), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        });
        let expected = format!("drawer_proj_code_{}", &hex[..24]);
        assert_eq!(id, expected);
    }

    #[tokio::test]
    async fn add_drawer_different_content_different_id() {
        let (_db, conn) = test_conn().await;
        let ra = tool_add_drawer(
            &conn,
            &json!({"wing": "w", "room": "r", "content": "first piece of content"}),
        )
        .await;
        let rb = tool_add_drawer(
            &conn,
            &json!({"wing": "w", "room": "r", "content": "second piece of content"}),
        )
        .await;
        assert_ne!(ra["drawer_id"], rb["drawer_id"]);
    }

    #[tokio::test]
    async fn add_drawer_missing_required_fields_returns_error() {
        let (_db, conn) = test_conn().await;

        // Missing content
        let r = tool_add_drawer(&conn, &json!({"wing": "w", "room": "r"})).await;
        assert_eq!(r["success"], false);

        // Missing wing
        let r = tool_add_drawer(
            &conn,
            &json!({"room": "r", "content": "some text here for testing"}),
        )
        .await;
        assert_eq!(r["success"], false);

        // Missing room
        let r = tool_add_drawer(
            &conn,
            &json!({"wing": "w", "content": "some text here for testing"}),
        )
        .await;
        assert_eq!(r["success"], false);
    }
}
