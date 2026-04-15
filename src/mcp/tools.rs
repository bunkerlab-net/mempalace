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

/// Maximum byte length for a tunnel label.  Labels are free-form strings stored
/// in `SQLite`; without a cap an unbounded value could waste DB space or overflow
/// index rows.  255 characters is generous for a short descriptive label.
const MAX_LABEL_LEN: usize = 255;

/// Exact character length of a tunnel ID.  Tunnel IDs are the first 16 hex
/// characters of a SHA256 digest (see `canonical_tunnel_id` in graph.rs).
const TUNNEL_ID_LEN: usize = 16;

/// Dispatch a tool call by name and return the JSON result.
pub async fn dispatch(connection: &Connection, name: &str, args: &Value) -> Value {
    // Empty name and non-object args can arrive from untrusted MCP clients;
    // return a structured error rather than panicking.
    if name.is_empty() {
        return json!({"error": "tool name must not be empty", "public": true});
    }
    if !args.is_object() {
        return json!({"error": "tool arguments must be a JSON object", "public": true});
    }

    match name {
        "mempalace_status" => tool_status(connection).await,
        "mempalace_list_wings" => tool_list_wings(connection).await,
        "mempalace_list_rooms" => tool_list_rooms(connection, args).await,
        "mempalace_get_taxonomy" => tool_get_taxonomy(connection).await,
        "mempalace_get_aaak_spec" => json!({"aaak_spec": AAAK_SPEC}),
        "mempalace_search" => tool_search(connection, args).await,
        "mempalace_check_duplicate" => tool_check_duplicate(connection, args).await,
        "mempalace_add_drawer" => tool_add_drawer(connection, args).await,
        "mempalace_delete_drawer" => tool_delete_drawer(connection, args).await,
        "mempalace_get_drawer" => tool_get_drawer(connection, args).await,
        "mempalace_list_drawers" => tool_list_drawers(connection, args).await,
        "mempalace_update_drawer" => tool_update_drawer(connection, args).await,
        "mempalace_kg_query" => tool_kg_query(connection, args).await,
        "mempalace_kg_add" => tool_kg_add(connection, args).await,
        "mempalace_kg_invalidate" => tool_kg_invalidate(connection, args).await,
        "mempalace_kg_timeline" => tool_kg_timeline(connection, args).await,
        "mempalace_kg_stats" => tool_kg_stats(connection).await,
        "mempalace_traverse" => tool_traverse(connection, args).await,
        "mempalace_find_tunnels" => tool_find_tunnels(connection, args).await,
        "mempalace_graph_stats" => tool_graph_stats(connection).await,
        "mempalace_create_tunnel" => tool_create_tunnel(connection, args).await,
        "mempalace_list_tunnels" => tool_list_tunnels(connection, args).await,
        "mempalace_delete_tunnel" => tool_delete_tunnel(connection, args).await,
        "mempalace_follow_tunnels" => tool_follow_tunnels(connection, args).await,
        "mempalace_diary_write" => tool_diary_write(connection, args).await,
        "mempalace_diary_read" => tool_diary_read(connection, args).await,
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

/// Validate tunnel label: trim, non-empty, reject null bytes and length violations.
fn sanitize_label(value: &str) -> Result<String, Value> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(
            json!({"success": false, "error": "label must be a non-empty string", "public": true}),
        );
    }
    if trimmed.len() > MAX_LABEL_LEN {
        return Err(
            json!({"success": false, "error": format!("label exceeds maximum length of {MAX_LABEL_LEN} characters"), "public": true}),
        );
    }
    if trimmed.contains('\0') {
        return Err(
            json!({"success": false, "error": "label contains null bytes", "public": true}),
        );
    }
    let result = trimmed.to_string();

    // Postconditions: result is non-empty, trimmed, and safe.
    debug_assert!(!result.is_empty());
    debug_assert!(result == result.trim());
    debug_assert!(!result.contains('\0'));
    debug_assert!(result.len() <= MAX_LABEL_LEN);

    Ok(result)
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
    // wal_log is best-effort and must never crash. An empty operation string is a
    // programmer error caught in debug builds; in release builds we silently skip.
    debug_assert!(!operation.is_empty(), "WAL operation must not be empty");
    if operation.is_empty() {
        return;
    }

    // Evaluate config_dir() here, in the async context, so the path is captured
    // by the move closure. Evaluating inside spawn_blocking would race with
    // temp_env::async_with_vars restoring the env var before the task runs.
    let wal_dir = crate::config::config_dir().join("wal");
    let operation = operation.to_string();
    let _ = tokio::task::spawn_blocking(move || {
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

async fn tool_status(connection: &Connection) -> Value {
    let rows = query_all(
        connection,
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

async fn tool_list_wings(connection: &Connection) -> Value {
    let rows = query_all(
        connection,
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

async fn tool_list_rooms(connection: &Connection, args: &Value) -> Value {
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(v) => v,
        Err(e) => return e,
    };

    let rows = if let Some(ref w) = wing {
        query_all(
            connection,
            "SELECT room, COUNT(*) as cnt FROM drawers WHERE wing = ? GROUP BY room",
            [w.as_str()],
        )
        .await
    } else {
        query_all(
            connection,
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

async fn tool_get_taxonomy(connection: &Connection) -> Value {
    let rows = query_all(
        connection,
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

async fn tool_search(connection: &Connection, args: &Value) -> Value {
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
        connection,
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

async fn tool_check_duplicate(connection: &Connection, args: &Value) -> Value {
    let content = str_arg(args, "content");
    // Simple keyword overlap check since we don't have vector similarity
    match search::search_memories(connection, content, None, None, 5).await {
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

async fn tool_add_drawer(connection: &Connection, args: &Value) -> Value {
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

    tool_add_drawer_insert(connection, id, wing, room, content, source_file, added_by).await
}

/// Write the drawer row and log the WAL event on confirmed insert. Returns the MCP response JSON.
async fn tool_add_drawer_insert(
    connection: &Connection,
    id: String,
    wing: String,
    room: String,
    content: String,
    source_file: &str,
    added_by: &str,
) -> Value {
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
    // WAL is only written on confirmed insert to avoid logging deduped/failed attempts.
    match drawer::add_drawer(connection, &params).await {
        Ok(true) => {
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
            json!({"success": true, "drawer_id": id, "wing": wing, "room": room})
        }
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

async fn tool_delete_drawer(connection: &Connection, args: &Value) -> Value {
    let drawer_id = match sanitize_name(str_arg(args, "drawer_id"), "drawer_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    if !drawer_id.starts_with("drawer_") {
        return json!({"success": false, "error": "drawer_id has invalid format", "public": true});
    }

    wal_log("delete_drawer", json!({"drawer_id": drawer_id})).await;

    match connection
        .execute("DELETE FROM drawers WHERE id = ?", [drawer_id.as_str()])
        .await
    {
        Ok(_) => {
            // Also clean up inverted index
            let _ = connection
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
async fn tool_get_drawer(connection: &Connection, args: &Value) -> Value {
    let drawer_id = match sanitize_name(str_arg(args, "drawer_id"), "drawer_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    if !drawer_id.starts_with("drawer_") {
        return json!({"error": "drawer_id has invalid format", "public": true});
    }

    let rows = query_all(
        connection,
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
async fn tool_list_drawers(connection: &Connection, args: &Value) -> Value {
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

    match tool_list_drawers_query(connection, wing.as_ref(), room.as_ref(), limit, offset).await {
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

/// Run the parameterized drawer list query for the given wing/room filter combination.
///
/// Excludes diary entries — they use UUID IDs (not the `drawer_` prefix scheme) and
/// have their own `mempalace_diary_read` tool. Uses `id DESC` as a tiebreaker so
/// pages are stable when `filed_at` values collide.
async fn tool_list_drawers_query(
    connection: &Connection,
    wing: Option<&String>,
    room: Option<&String>,
    limit: i64,
    offset: i64,
) -> crate::error::Result<Vec<turso::Row>> {
    match (wing, room) {
        (Some(w), Some(r)) => {
            query_all(connection, "SELECT id, content, wing, room FROM drawers WHERE wing = ?1 AND room = ?2 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?3 OFFSET ?4", (w.as_str(), r.as_str(), limit, offset)).await
        }
        (Some(w), None) => {
            query_all(connection, "SELECT id, content, wing, room FROM drawers WHERE wing = ?1 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?2 OFFSET ?3", (w.as_str(), limit, offset)).await
        }
        (None, Some(r)) => {
            query_all(connection, "SELECT id, content, wing, room FROM drawers WHERE room = ?1 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?2 OFFSET ?3", (r.as_str(), limit, offset)).await
        }
        (None, None) => {
            query_all(connection, "SELECT id, content, wing, room FROM drawers WHERE (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?1 OFFSET ?2", (limit, offset)).await
        }
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
async fn tool_update_drawer(connection: &Connection, args: &Value) -> Value {
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
        connection,
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
            connection,
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

    // Wrap the row update and index rebuild in a transaction so drawers and
    // drawer_words cannot diverge if any step fails mid-flight.
    if let Err(e) = connection.execute("BEGIN", ()).await {
        return json!({"success": false, "error": e.to_string()});
    }

    if let Err(e) = connection
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
        let _ = connection.execute("ROLLBACK", ()).await;
        return json!({"success": false, "error": e.to_string()});
    }

    // Re-index words: always needed when the ID changes or content changes.
    if let Err(e) = connection
        .execute(
            "DELETE FROM drawer_words WHERE drawer_id = ?",
            [drawer_id.as_str()],
        )
        .await
    {
        let _ = connection.execute("ROLLBACK", ()).await;
        return json!({"success": false, "error": e.to_string()});
    }

    if new_id != drawer_id {
        // drawer_words rows for the old ID were deleted above; if the new
        // ID already had entries (shouldn't happen — we checked above),
        // clean those too.
        let _ = connection
            .execute(
                "DELETE FROM drawer_words WHERE drawer_id = ?",
                [new_id.as_str()],
            )
            .await;
    }

    if let Err(e) = drawer::index_words(connection, &new_id, final_content).await {
        let _ = connection.execute("ROLLBACK", ()).await;
        return json!({"success": false, "error": e.to_string()});
    }

    if let Err(e) = connection.execute("COMMIT", ()).await {
        let _ = connection.execute("ROLLBACK", ()).await;
        return json!({"success": false, "error": e.to_string()});
    }

    json!({
        "success": true,
        "drawer_id": new_id,
        "wing": final_wing,
        "room": final_room,
    })
}

async fn tool_kg_query(connection: &Connection, args: &Value) -> Value {
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

    match kg::query::query_entity(connection, &entity, as_of.as_deref(), direction).await {
        Ok(facts) => {
            let count = facts.len();
            json!({"entity": entity, "as_of": as_of, "facts": facts, "count": count})
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_kg_add(connection: &Connection, args: &Value) -> Value {
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
        connection,
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

async fn tool_kg_invalidate(connection: &Connection, args: &Value) -> Value {
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

    // Perform the mutation first so the WAL records persisted_ended — the value
    // actually written to the database — rather than the raw input (which may be None
    // and would be normalized to today's date by kg::invalidate).
    match kg::invalidate(connection, &subject, &predicate, &object, ended.as_deref()).await {
        Ok(persisted_ended) => {
            wal_log(
                "kg_invalidate",
                json!({"subject": subject, "predicate": predicate, "object": object, "ended": persisted_ended}),
            )
            .await;
            json!({
                "success": true,
                "fact": format!("{subject} → {predicate} → {object}"),
                "ended": persisted_ended,
            })
        }
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

async fn tool_kg_timeline(connection: &Connection, args: &Value) -> Value {
    let entity = match sanitize_opt_name(str_arg(args, "entity"), "entity") {
        Ok(v) => v,
        Err(e) => return e,
    };

    match kg::query::timeline(connection, entity.as_deref()).await {
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

async fn tool_kg_stats(connection: &Connection) -> Value {
    match kg::query::stats(connection).await {
        Ok(stats) => json!(stats),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_traverse(connection: &Connection, args: &Value) -> Value {
    let start_room = match sanitize_name(str_arg(args, "start_room"), "start_room") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let max_hops = usize::try_from(int_arg(args, "max_hops", 2).clamp(1, 10)).unwrap_or(2);

    match graph::traverse(connection, &start_room, max_hops).await {
        Ok((results, truncated)) => json!({"results": results, "truncated": truncated}),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_find_tunnels(connection: &Connection, args: &Value) -> Value {
    let wing_a = match sanitize_opt_name(str_arg(args, "wing_a"), "wing_a") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let wing_b = match sanitize_opt_name(str_arg(args, "wing_b"), "wing_b") {
        Ok(v) => v,
        Err(e) => return e,
    };

    match graph::find_tunnels(connection, wing_a.as_deref(), wing_b.as_deref()).await {
        Ok((tunnels, truncated)) => json!({"tunnels": tunnels, "truncated": truncated}),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_graph_stats(connection: &Connection) -> Value {
    match graph::graph_stats(connection).await {
        Ok(stats) => json!(stats),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_create_tunnel(connection: &Connection, args: &Value) -> Value {
    let source_wing = match sanitize_name(str_arg(args, "source_wing"), "source_wing") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let source_room = match sanitize_name(str_arg(args, "source_room"), "source_room") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let target_wing = match sanitize_name(str_arg(args, "target_wing"), "target_wing") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let target_room = match sanitize_name(str_arg(args, "target_room"), "target_room") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let label = match sanitize_label(str_arg(args, "label")) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let source_drawer_id =
        match sanitize_opt_name(str_arg(args, "source_drawer_id"), "source_drawer_id") {
            Ok(v) => v,
            Err(e) => return e,
        };
    let target_drawer_id =
        match sanitize_opt_name(str_arg(args, "target_drawer_id"), "target_drawer_id") {
            Ok(v) => v,
            Err(e) => return e,
        };

    match graph::create_tunnel(
        connection,
        &graph::CreateTunnelParams {
            source_wing: &source_wing,
            source_room: &source_room,
            target_wing: &target_wing,
            target_room: &target_room,
            label: &label,
            source_drawer_id: source_drawer_id.as_deref(),
            target_drawer_id: target_drawer_id.as_deref(),
        },
    )
    .await
    {
        Ok(tunnel) => {
            wal_log(
                "create_tunnel",
                json!({
                    "tunnel_id": tunnel.id,
                    "source_wing": tunnel.source_wing,
                    "source_room": tunnel.source_room,
                    "target_wing": tunnel.target_wing,
                    "target_room": tunnel.target_room,
                    "label": tunnel.label,
                    "source_drawer_id": tunnel.source_drawer_id,
                    "target_drawer_id": tunnel.target_drawer_id,
                }),
            )
            .await;
            json!(tunnel)
        }
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_list_tunnels(connection: &Connection, args: &Value) -> Value {
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(v) => v,
        Err(e) => return e,
    };

    match graph::list_tunnels(connection, wing.as_deref()).await {
        Ok(tunnels) => json!({"tunnels": tunnels, "count": tunnels.len()}),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_delete_tunnel(connection: &Connection, args: &Value) -> Value {
    // Trim before validation to avoid spurious failures from surrounding whitespace.
    let tunnel_id = str_arg(args, "tunnel_id").trim();
    if tunnel_id.is_empty() {
        return json!({"error": "tunnel_id is required", "public": true});
    }
    // Tunnel IDs are the first 16 hex characters of a SHA256 digest — validate
    // the exact format so arbitrary strings are never passed to the database.
    if tunnel_id.len() != TUNNEL_ID_LEN || !tunnel_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return json!({"error": "tunnel_id must be a 16-character hex string", "public": true});
    }

    wal_log("delete_tunnel", json!({"tunnel_id": tunnel_id})).await;

    match graph::delete_tunnel(connection, tunnel_id).await {
        Ok(deleted) => json!({"deleted": deleted, "tunnel_id": tunnel_id}),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_follow_tunnels(connection: &Connection, args: &Value) -> Value {
    let wing = match sanitize_name(str_arg(args, "wing"), "wing") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let room = match sanitize_name(str_arg(args, "room"), "room") {
        Ok(v) => v,
        Err(e) => return e,
    };

    match graph::follow_tunnels(connection, &wing, &room).await {
        Ok(connections) => json!({"wing": wing, "room": room, "connections": connections}),
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn tool_diary_write(connection: &Connection, args: &Value) -> Value {
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
    match connection
        .execute(
            "INSERT OR IGNORE INTO drawers (id, wing, room, content, source_file, chunk_index, added_by, ingest_mode, extract_mode) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            turso::params![id.as_str(), wing.as_str(), "diary", entry.as_str(), "", 0i32, agent_name.as_str(), "diary", topic.as_str()],
        )
        .await
    {
        Ok(_) => {
            let _ = drawer::index_words(connection, &id, &entry).await;
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

async fn tool_diary_read(connection: &Connection, args: &Value) -> Value {
    let agent_name = str_arg(args, "agent_name");
    let last_n = int_arg(args, "last_n", 10).clamp(1, 100);

    let agent_name = match sanitize_name(agent_name, "agent_name") {
        Ok(v) => v,
        Err(e) => return e,
    };

    let wing = format!("wing_{}", agent_name.to_lowercase().replace(' ', "_"));

    let rows = query_all(
        connection,
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
        // Each test gets its own temp dir so WAL writes are isolated.
        with_isolated_env(|connection| async move {
            let args = json!({
                "wing": "personal",
                "room": "notes",
                "content": "the quick brown fox jumps over the lazy dog",
            });
            let result = tool_add_drawer(&connection, &args).await;
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
        })
        .await;
    }

    #[tokio::test]
    async fn add_drawer_idempotent_returns_already_exists() {
        with_isolated_env(|connection| async move {
            let args = json!({
                "wing": "personal",
                "room": "notes",
                "content": "idempotent content for testing",
            });
            let first = tool_add_drawer(&connection, &args).await;
            assert_eq!(first["success"], true);

            let second = tool_add_drawer(&connection, &args).await;
            assert_eq!(second["success"], true);
            assert_eq!(second["reason"], "already_exists");
            // The same deterministic ID must be returned both times.
            assert_eq!(first["drawer_id"], second["drawer_id"]);
        })
        .await;
    }

    #[tokio::test]
    async fn add_drawer_deterministic_id_same_content() {
        with_isolated_env(|connection| async move {
            // Verify the ID is derived from sha256(wing+room+content)[:24].
            let content = "fn main() { println!(\"hello\"); }";
            let args = json!({
                "wing": "proj",
                "room": "code",
                "content": content,
            });
            let result = tool_add_drawer(&connection, &args).await;
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
        })
        .await;
    }

    #[tokio::test]
    async fn add_drawer_different_content_different_id() {
        with_isolated_env(|connection| async move {
            let ra = tool_add_drawer(
                &connection,
                &json!({"wing": "w", "room": "r", "content": "first piece of content"}),
            )
            .await;
            let rb = tool_add_drawer(
                &connection,
                &json!({"wing": "w", "room": "r", "content": "second piece of content"}),
            )
            .await;
            assert_ne!(ra["drawer_id"], rb["drawer_id"]);
        })
        .await;
    }

    #[tokio::test]
    async fn add_drawer_missing_required_fields_returns_error() {
        with_isolated_env(|connection| async move {
            // Missing content
            let r = tool_add_drawer(&connection, &json!({"wing": "w", "room": "r"})).await;
            assert_eq!(r["success"], false);

            // Missing wing
            let r = tool_add_drawer(
                &connection,
                &json!({"room": "r", "content": "some text here for testing"}),
            )
            .await;
            assert_eq!(r["success"], false);

            // Missing room
            let r = tool_add_drawer(
                &connection,
                &json!({"wing": "w", "content": "some text here for testing"}),
            )
            .await;
            assert_eq!(r["success"], false);
        })
        .await;
    }

    // --- Helper: seed a drawer for tests that need pre-existing data ---

    async fn seed_drawer(connection: &Connection, wing: &str, room: &str, content: &str) -> Value {
        let args = json!({"wing": wing, "room": room, "content": content});
        let result = tool_add_drawer(connection, &args).await;
        assert_eq!(result["success"], true, "seed_drawer must succeed");
        result
    }

    // --- Helper: isolated WAL dir + env override + fresh connection ---

    // Wraps the three-line test setup (tempdir, async_with_vars, test_conn) so
    // callers only express what is under test.  The connection is passed by value
    // so the closure can borrow it as `&connection` without lifetime complications.
    async fn with_isolated_env<F, Fut>(test: F)
    where
        F: FnOnce(turso::Connection) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let wal_dir = tempfile::tempdir().expect("failed to create WAL temp dir");
        temp_env::async_with_vars([("MEMPALACE_DIR", Some(wal_dir.path()))], async move {
            let (_db, connection) = test_conn().await;
            test(connection).await;
        })
        .await;
    }

    // --- tool_status ---

    #[tokio::test]
    async fn status_empty_db_returns_zero_totals() {
        with_isolated_env(|connection| async move {
            let result = tool_status(&connection).await;
            assert_eq!(result["total_drawers"], 0);
            assert!(result["protocol"].is_string(), "protocol must be present");
        })
        .await;
    }

    #[tokio::test]
    async fn status_with_drawers_counts_correctly() {
        with_isolated_env(|connection| async move {
            seed_drawer(&connection, "alpha", "notes", "first drawer content here").await;
            seed_drawer(&connection, "alpha", "code", "second drawer content here").await;
            seed_drawer(&connection, "beta", "notes", "third drawer content here").await;

            let result = tool_status(&connection).await;
            assert_eq!(result["total_drawers"], 3);
            assert_eq!(result["wings"]["alpha"], 2);
            assert_eq!(result["wings"]["beta"], 1);
        })
        .await;
    }

    // --- tool_list_wings ---

    #[tokio::test]
    async fn list_wings_empty_db() {
        with_isolated_env(|connection| async move {
            let result = tool_list_wings(&connection).await;
            let wings = result["wings"].as_object().expect("wings must be object");
            assert!(wings.is_empty(), "empty DB should have no wings");
            assert!(result.get("error").is_none(), "must not return error");
        })
        .await;
    }

    #[tokio::test]
    async fn list_wings_with_data() {
        with_isolated_env(|connection| async move {
            seed_drawer(&connection, "personal", "notes", "my personal note content").await;
            seed_drawer(&connection, "work", "tasks", "my work task content here").await;

            let result = tool_list_wings(&connection).await;
            let wings = result["wings"].as_object().expect("wings must be object");
            assert_eq!(wings.len(), 2);
            assert_eq!(result["wings"]["personal"], 1);
        })
        .await;
    }

    // --- tool_list_rooms ---

    #[tokio::test]
    async fn list_rooms_empty_db() {
        with_isolated_env(|connection| async move {
            let result = tool_list_rooms(&connection, &json!({})).await;
            let rooms = result["rooms"].as_object().expect("rooms must be object");
            assert!(rooms.is_empty(), "empty DB should have no rooms");
            assert_eq!(result["wing"], "all");
        })
        .await;
    }

    #[tokio::test]
    async fn list_rooms_with_wing_filter() {
        with_isolated_env(|connection| async move {
            seed_drawer(&connection, "proj", "code", "some code content here").await;
            seed_drawer(&connection, "proj", "docs", "some docs content here").await;
            seed_drawer(&connection, "other", "misc", "other misc content here").await;

            let result = tool_list_rooms(&connection, &json!({"wing": "proj"})).await;
            let rooms = result["rooms"].as_object().expect("rooms must be object");
            assert_eq!(rooms.len(), 2);
            assert_eq!(result["wing"], "proj");
        })
        .await;
    }

    // --- tool_get_taxonomy ---

    #[tokio::test]
    async fn get_taxonomy_empty_db() {
        with_isolated_env(|connection| async move {
            let result = tool_get_taxonomy(&connection).await;
            let taxonomy = result["taxonomy"]
                .as_object()
                .expect("taxonomy must be object");
            assert!(taxonomy.is_empty(), "empty DB should have no taxonomy");
            assert!(result.get("error").is_none(), "must not return error");
        })
        .await;
    }

    #[tokio::test]
    async fn get_taxonomy_with_data() {
        with_isolated_env(|connection| async move {
            seed_drawer(
                &connection,
                "proj",
                "code",
                "code content for taxonomy test",
            )
            .await;
            seed_drawer(
                &connection,
                "proj",
                "docs",
                "docs content for taxonomy test",
            )
            .await;

            let result = tool_get_taxonomy(&connection).await;
            let taxonomy = result["taxonomy"]
                .as_object()
                .expect("taxonomy must be object");
            assert!(taxonomy.contains_key("proj"), "must contain proj wing");
            assert_eq!(result["taxonomy"]["proj"]["code"], 1);
        })
        .await;
    }

    // --- tool_search ---

    #[tokio::test]
    async fn search_empty_query_returns_error() {
        with_isolated_env(|connection| async move {
            let result = tool_search(&connection, &json!({"query": ""})).await;
            assert!(
                result["error"].is_string(),
                "must return error for empty query"
            );
            assert!(result.get("results").is_none(), "must not return results");
        })
        .await;
    }

    #[tokio::test]
    async fn search_happy_path_returns_results() {
        with_isolated_env(|connection| async move {
            seed_drawer(
                &connection,
                "tech",
                "rust",
                "rust programming language memory safety ownership borrowing",
            )
            .await;

            let result = tool_search(&connection, &json!({"query": "rust programming"})).await;
            assert!(result.get("error").is_none(), "search must not error");
            assert!(result["count"].as_i64().expect("count must be int") >= 1);
        })
        .await;
    }

    #[tokio::test]
    async fn search_with_wing_filter() {
        with_isolated_env(|connection| async move {
            seed_drawer(
                &connection,
                "tech",
                "notes",
                "rust programming language systems",
            )
            .await;
            seed_drawer(
                &connection,
                "personal",
                "notes",
                "rust belt vacation travel plans",
            )
            .await;

            let result = tool_search(&connection, &json!({"query": "rust", "wing": "tech"})).await;
            assert!(result.get("error").is_none(), "search must not error");
            // All returned results should be from the "tech" wing
            let results = result["results"].as_array().expect("results must be array");
            for r in results {
                assert_eq!(r["wing"], "tech");
            }
        })
        .await;
    }

    // --- tool_check_duplicate ---

    #[tokio::test]
    async fn check_duplicate_no_match() {
        with_isolated_env(|connection| async move {
            let result = tool_check_duplicate(
                &connection,
                &json!({"content": "completely unique content that has no match"}),
            )
            .await;
            assert_eq!(result["is_duplicate"], false);
            assert!(
                result["matches"]
                    .as_array()
                    .expect("matches must be array")
                    .is_empty()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn check_duplicate_with_matching_content() {
        with_isolated_env(|connection| async move {
            seed_drawer(
                &connection,
                "tech",
                "notes",
                "rust programming language memory safety ownership borrowing lifetimes",
            )
            .await;

            // Check with very similar content — duplicate detection uses word overlap
            let result = tool_check_duplicate(
                &connection,
                &json!({"content": "rust programming language memory safety ownership borrowing lifetimes"}),
            )
            .await;
            assert!(result.get("error").is_none(), "must not error");
            // is_duplicate is present regardless
            assert!(
                result.get("is_duplicate").is_some(),
                "is_duplicate key must exist"
            );
        })
        .await;
    }

    // --- tool_delete_drawer ---

    #[tokio::test]
    async fn delete_drawer_success() {
        with_isolated_env(|connection| async move {
            let seeded = seed_drawer(&connection, "temp", "notes", "content to be deleted").await;
            let drawer_id = seeded["drawer_id"]
                .as_str()
                .expect("drawer_id must be string");

            let result = tool_delete_drawer(&connection, &json!({"drawer_id": drawer_id})).await;
            assert_eq!(result["success"], true);
            assert_eq!(result["drawer_id"], drawer_id);
        })
        .await;
    }

    #[tokio::test]
    async fn delete_drawer_invalid_format() {
        with_isolated_env(|connection| async move {
            let result =
                tool_delete_drawer(&connection, &json!({"drawer_id": "not_a_drawer_id"})).await;
            assert_eq!(result["success"], false);
            assert!(
                result["error"]
                    .as_str()
                    .expect("error must be string")
                    .contains("invalid format")
            );
        })
        .await;
    }

    // --- tool_get_drawer ---

    #[tokio::test]
    async fn get_drawer_success() {
        with_isolated_env(|connection| async move {
            let seeded = seed_drawer(&connection, "proj", "code", "fn main for get test").await;
            let drawer_id = seeded["drawer_id"]
                .as_str()
                .expect("drawer_id must be string");

            let result = tool_get_drawer(&connection, &json!({"drawer_id": drawer_id})).await;
            assert_eq!(result["drawer_id"], drawer_id);
            assert_eq!(result["content"], "fn main for get test");
            assert_eq!(result["wing"], "proj");
        })
        .await;
    }

    #[tokio::test]
    async fn get_drawer_not_found() {
        with_isolated_env(|connection| async move {
            let result = tool_get_drawer(
                &connection,
                &json!({"drawer_id": "drawer_x_y_aaaaaaaabbbbbbbbcccccccc"}),
            )
            .await;
            assert!(result["error"].is_string(), "must return error");
            assert!(
                result["error"]
                    .as_str()
                    .expect("error string")
                    .contains("not found"),
                "error must mention not found"
            );
        })
        .await;
    }

    // --- tool_list_drawers ---

    #[tokio::test]
    async fn list_drawers_empty_db() {
        with_isolated_env(|connection| async move {
            let result = tool_list_drawers(&connection, &json!({})).await;
            assert_eq!(result["count"], 0);
            assert!(
                result["drawers"]
                    .as_array()
                    .expect("drawers must be array")
                    .is_empty()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn list_drawers_with_pagination() {
        with_isolated_env(|connection| async move {
            seed_drawer(&connection, "w", "r", "first content for pagination test").await;
            seed_drawer(&connection, "w", "r", "second content for pagination test").await;
            seed_drawer(&connection, "w", "r", "third content for pagination test").await;

            // Limit to 2
            let result = tool_list_drawers(&connection, &json!({"limit": 2})).await;
            assert_eq!(result["count"], 2);
            assert_eq!(result["limit"], 2);

            // Offset to get the third
            let result2 = tool_list_drawers(&connection, &json!({"limit": 2, "offset": 2})).await;
            assert_eq!(result2["count"], 1);
            assert_eq!(result2["offset"], 2);
        })
        .await;
    }

    #[tokio::test]
    async fn list_drawers_wing_filter() {
        with_isolated_env(|connection| async move {
            seed_drawer(
                &connection,
                "alpha",
                "notes",
                "alpha content for filter test",
            )
            .await;
            seed_drawer(&connection, "beta", "notes", "beta content for filter test").await;

            let result = tool_list_drawers(&connection, &json!({"wing": "alpha"})).await;
            assert_eq!(result["count"], 1);
            let d = &result["drawers"][0];
            assert_eq!(d["wing"], "alpha");
        })
        .await;
    }

    // --- tool_update_drawer ---

    #[tokio::test]
    async fn update_drawer_content() {
        with_isolated_env(|connection| async move {
            let seeded =
                seed_drawer(&connection, "proj", "code", "original content for update").await;
            let old_id = seeded["drawer_id"]
                .as_str()
                .expect("drawer_id must be string");

            let result = tool_update_drawer(
                &connection,
                &json!({"drawer_id": old_id, "content": "updated content after mutation"}),
            )
            .await;
            assert_eq!(result["success"], true);
            // ID changes because content changed (deterministic ID includes content)
            assert_ne!(
                result["drawer_id"].as_str().expect("new id"),
                old_id,
                "ID must change when content changes"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn update_drawer_not_found() {
        with_isolated_env(|connection| async move {
            let result = tool_update_drawer(
                &connection,
                &json!({
                    "drawer_id": "drawer_x_y_aaaaaaaabbbbbbbbcccccccc",
                    "content": "new content for nonexistent drawer"
                }),
            )
            .await;
            assert_eq!(result["success"], false);
            assert!(
                result["error"]
                    .as_str()
                    .expect("error string")
                    .contains("not found"),
                "error must mention not found"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn update_drawer_noop_when_nothing_changes() {
        with_isolated_env(|connection| async move {
            let seeded = seed_drawer(&connection, "proj", "code", "stable content no change").await;
            let drawer_id = seeded["drawer_id"]
                .as_str()
                .expect("drawer_id must be string");

            // Send update with no actual changes
            let result = tool_update_drawer(&connection, &json!({"drawer_id": drawer_id})).await;
            assert_eq!(result["success"], true);
            assert_eq!(result["noop"], true);
        })
        .await;
    }

    // --- tool_kg_add ---

    #[tokio::test]
    async fn kg_add_triple() {
        with_isolated_env(|connection| async move {
            let result = tool_kg_add(
                &connection,
                &json!({
                    "subject": "Rust",
                    "predicate": "is",
                    "object": "fast",
                }),
            )
            .await;
            assert_eq!(result["success"], true);
            assert!(
                result["triple_id"].is_string(),
                "triple_id must be a string"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn kg_add_missing_field_returns_error() {
        with_isolated_env(|connection| async move {
            // Missing object
            let result =
                tool_kg_add(&connection, &json!({"subject": "Rust", "predicate": "is"})).await;
            assert_eq!(result["success"], false);
            assert!(result["error"].is_string(), "must return error message");
        })
        .await;
    }

    // --- tool_kg_query ---

    #[tokio::test]
    async fn kg_query_entity() {
        with_isolated_env(|connection| async move {
            // Add a triple first
            tool_kg_add(
                &connection,
                &json!({"subject": "Rust", "predicate": "compilesTo", "object": "binary"}),
            )
            .await;

            let result = tool_kg_query(&connection, &json!({"entity": "Rust"})).await;
            assert!(result.get("error").is_none(), "query must not error");
            assert_eq!(result["entity"], "Rust");
            assert!(result["count"].as_i64().expect("count") >= 1);
        })
        .await;
    }

    #[tokio::test]
    async fn kg_query_invalid_direction() {
        with_isolated_env(|connection| async move {
            let result = tool_kg_query(
                &connection,
                &json!({"entity": "Rust", "direction": "sideways"}),
            )
            .await;
            assert!(result["error"].is_string(), "must return error");
            assert!(
                result["error"]
                    .as_str()
                    .expect("error string")
                    .contains("direction")
            );
        })
        .await;
    }

    // --- tool_kg_invalidate ---

    #[tokio::test]
    async fn kg_invalidate_triple() {
        with_isolated_env(|connection| async move {
            tool_kg_add(
                &connection,
                &json!({"subject": "Alice", "predicate": "worksAt", "object": "Acme"}),
            )
            .await;

            let result = tool_kg_invalidate(
                &connection,
                &json!({"subject": "Alice", "predicate": "worksAt", "object": "Acme"}),
            )
            .await;
            assert_eq!(result["success"], true);
            assert!(
                result["ended"].is_string(),
                "ended timestamp must be present"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn kg_invalidate_with_explicit_ended_date() {
        with_isolated_env(|connection| async move {
            tool_kg_add(
                &connection,
                &json!({"subject": "Bob", "predicate": "livesIn", "object": "NYC"}),
            )
            .await;

            let result = tool_kg_invalidate(
                &connection,
                &json!({
                    "subject": "Bob",
                    "predicate": "livesIn",
                    "object": "NYC",
                    "ended": "2025-01-01"
                }),
            )
            .await;
            assert_eq!(result["success"], true);
            assert_eq!(result["ended"], "2025-01-01");
        })
        .await;
    }

    // --- tool_kg_timeline ---

    #[tokio::test]
    async fn kg_timeline_empty() {
        with_isolated_env(|connection| async move {
            let result = tool_kg_timeline(&connection, &json!({})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["count"], 0);
        })
        .await;
    }

    #[tokio::test]
    async fn kg_timeline_with_data() {
        with_isolated_env(|connection| async move {
            tool_kg_add(
                &connection,
                &json!({"subject": "Eve", "predicate": "knows", "object": "Alice"}),
            )
            .await;

            let result = tool_kg_timeline(&connection, &json!({"entity": "Eve"})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert!(result["count"].as_i64().expect("count") >= 1);
        })
        .await;
    }

    // --- tool_kg_stats ---

    #[tokio::test]
    async fn kg_stats_empty_db() {
        with_isolated_env(|connection| async move {
            let result = tool_kg_stats(&connection).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["entities"], 0);
            assert_eq!(result["triples"], 0);
        })
        .await;
    }

    #[tokio::test]
    async fn kg_stats_after_adding_triples() {
        with_isolated_env(|connection| async move {
            tool_kg_add(
                &connection,
                &json!({"subject": "X", "predicate": "rel", "object": "Y"}),
            )
            .await;

            let result = tool_kg_stats(&connection).await;
            assert!(result.get("error").is_none(), "must not error");
            assert!(result["triples"].as_i64().expect("triples") >= 1);
            assert!(result["entities"].as_i64().expect("entities") >= 1);
        })
        .await;
    }

    // --- tool_create_tunnel ---

    async fn seed_tunnel(connection: &Connection) -> Value {
        tool_create_tunnel(
            connection,
            &json!({
                "source_wing": "alpha",
                "source_room": "code",
                "target_wing": "beta",
                "target_room": "docs",
                "label": "cross-reference link",
            }),
        )
        .await
    }

    #[tokio::test]
    async fn create_tunnel_success() {
        with_isolated_env(|connection| async move {
            let result = seed_tunnel(&connection).await;
            assert!(result.get("error").is_none(), "must not error");
            assert!(result["id"].is_string(), "tunnel must have an id");
            assert_eq!(result["source_wing"], "alpha");
        })
        .await;
    }

    #[tokio::test]
    async fn create_tunnel_missing_label_returns_error() {
        with_isolated_env(|connection| async move {
            let result = tool_create_tunnel(
                &connection,
                &json!({
                    "source_wing": "a",
                    "source_room": "b",
                    "target_wing": "c",
                    "target_room": "d",
                }),
            )
            .await;
            assert_eq!(result["success"], false);
            assert!(
                result["error"].is_string(),
                "must return error for missing label"
            );
        })
        .await;
    }

    // --- tool_list_tunnels ---

    #[tokio::test]
    async fn list_tunnels_empty() {
        with_isolated_env(|connection| async move {
            let result = tool_list_tunnels(&connection, &json!({})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["count"], 0);
        })
        .await;
    }

    #[tokio::test]
    async fn list_tunnels_after_create() {
        with_isolated_env(|connection| async move {
            seed_tunnel(&connection).await;

            let result = tool_list_tunnels(&connection, &json!({})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert!(result["count"].as_i64().expect("count") >= 1);
        })
        .await;
    }

    // --- tool_delete_tunnel ---

    #[tokio::test]
    async fn delete_tunnel_success() {
        with_isolated_env(|connection| async move {
            let tunnel = seed_tunnel(&connection).await;
            let tunnel_id = tunnel["id"].as_str().expect("tunnel must have id");

            let result = tool_delete_tunnel(&connection, &json!({"tunnel_id": tunnel_id})).await;
            assert!(result.get("error").is_none(), "delete must not error");
            assert_eq!(result["deleted"], true);
        })
        .await;
    }

    #[tokio::test]
    async fn delete_tunnel_invalid_id_format() {
        with_isolated_env(|connection| async move {
            let result = tool_delete_tunnel(&connection, &json!({"tunnel_id": "not-valid"})).await;
            assert!(result["error"].is_string(), "must return error");
            assert!(
                result["error"]
                    .as_str()
                    .expect("error string")
                    .contains("16-character hex")
            );
        })
        .await;
    }

    #[tokio::test]
    async fn delete_tunnel_nonexistent_returns_false() {
        with_isolated_env(|connection| async move {
            // Valid 16-char hex that doesn't exist
            let result =
                tool_delete_tunnel(&connection, &json!({"tunnel_id": "0000000000000000"})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["deleted"], false);
        })
        .await;
    }

    // --- tool_follow_tunnels ---

    #[tokio::test]
    async fn follow_tunnels_no_connections() {
        with_isolated_env(|connection| async move {
            let result =
                tool_follow_tunnels(&connection, &json!({"wing": "alpha", "room": "code"})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert!(
                result["connections"]
                    .as_array()
                    .expect("connections must be array")
                    .is_empty()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn follow_tunnels_with_tunnel() {
        with_isolated_env(|connection| async move {
            seed_tunnel(&connection).await;

            let result =
                tool_follow_tunnels(&connection, &json!({"wing": "alpha", "room": "code"})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["wing"], "alpha");
            // Should find at least one connection from the seeded tunnel
            let conns = result["connections"]
                .as_array()
                .expect("connections must be array");
            assert!(!conns.is_empty(), "must find the seeded tunnel");
        })
        .await;
    }

    // --- tool_traverse ---

    #[tokio::test]
    async fn traverse_empty_graph() {
        with_isolated_env(|connection| async move {
            let result = tool_traverse(&connection, &json!({"start_room": "nonexistent"})).await;
            assert!(
                result.get("error").is_none(),
                "must not error on empty graph"
            );
            assert!(result.get("results").is_some(), "must have results key");
        })
        .await;
    }

    #[tokio::test]
    async fn traverse_with_shared_room() {
        with_isolated_env(|connection| async move {
            // Two wings sharing the same room name creates a graph edge
            seed_drawer(
                &connection,
                "alpha",
                "shared",
                "alpha shared drawer content",
            )
            .await;
            seed_drawer(&connection, "beta", "shared", "beta shared drawer content").await;

            let result =
                tool_traverse(&connection, &json!({"start_room": "shared", "max_hops": 1})).await;
            assert!(result.get("error").is_none(), "must not error");
            let results = result["results"].as_array().expect("results must be array");
            assert!(
                !results.is_empty(),
                "shared room should yield traversal results"
            );
        })
        .await;
    }

    // --- tool_find_tunnels ---

    #[tokio::test]
    async fn find_tunnels_empty_graph() {
        with_isolated_env(|connection| async move {
            let result = tool_find_tunnels(&connection, &json!({})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert!(
                result["tunnels"]
                    .as_array()
                    .expect("tunnels must be array")
                    .is_empty()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn find_tunnels_between_wings() {
        with_isolated_env(|connection| async move {
            // Create drawers in two wings sharing a room name
            seed_drawer(&connection, "alpha", "shared", "alpha shared content here").await;
            seed_drawer(&connection, "beta", "shared", "beta shared content here").await;

            let result =
                tool_find_tunnels(&connection, &json!({"wing_a": "alpha", "wing_b": "beta"})).await;
            assert!(result.get("error").is_none(), "must not error");
            let tunnels = result["tunnels"].as_array().expect("tunnels must be array");
            assert!(
                !tunnels.is_empty(),
                "wings sharing a room should have tunnels"
            );
        })
        .await;
    }

    // --- tool_graph_stats ---

    #[tokio::test]
    async fn graph_stats_empty_db() {
        with_isolated_env(|connection| async move {
            let result = tool_graph_stats(&connection).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["total_rooms"], 0);
            assert_eq!(result["total_edges"], 0);
        })
        .await;
    }

    #[tokio::test]
    async fn graph_stats_with_data() {
        with_isolated_env(|connection| async move {
            seed_drawer(&connection, "alpha", "notes", "alpha notes for graph stats").await;
            seed_drawer(&connection, "beta", "notes", "beta notes for graph stats").await;

            let result = tool_graph_stats(&connection).await;
            assert!(result.get("error").is_none(), "must not error");
            assert!(
                result["total_rooms"].as_i64().expect("total_rooms") >= 1,
                "must count at least one room"
            );
        })
        .await;
    }

    // --- tool_diary_write ---

    #[tokio::test]
    async fn diary_write_success() {
        with_isolated_env(|connection| async move {
            let result = tool_diary_write(
                &connection,
                &json!({
                    "agent_name": "TestAgent",
                    "entry": "Today I learned about Rust lifetimes and borrowing",
                }),
            )
            .await;
            assert_eq!(result["success"], true);
            assert!(result["entry_id"].is_string(), "must return entry_id");
            assert_eq!(result["agent"], "TestAgent");
            assert_eq!(result["topic"], "general");
        })
        .await;
    }

    #[tokio::test]
    async fn diary_write_with_topic() {
        with_isolated_env(|connection| async move {
            let result = tool_diary_write(
                &connection,
                &json!({
                    "agent_name": "TestAgent",
                    "entry": "Debugging session notes about async runtime",
                    "topic": "debugging",
                }),
            )
            .await;
            assert_eq!(result["success"], true);
            assert_eq!(result["topic"], "debugging");
        })
        .await;
    }

    #[tokio::test]
    async fn diary_write_missing_entry_returns_error() {
        with_isolated_env(|connection| async move {
            let result = tool_diary_write(&connection, &json!({"agent_name": "TestAgent"})).await;
            assert_eq!(result["success"], false);
            assert!(result["error"].is_string(), "must return error");
        })
        .await;
    }

    // --- tool_diary_read ---

    #[tokio::test]
    async fn diary_read_empty() {
        with_isolated_env(|connection| async move {
            let result = tool_diary_read(&connection, &json!({"agent_name": "TestAgent"})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["total"], 0);
            assert_eq!(result["agent"], "TestAgent");
        })
        .await;
    }

    #[tokio::test]
    async fn diary_read_after_write() {
        with_isolated_env(|connection| async move {
            tool_diary_write(
                &connection,
                &json!({
                    "agent_name": "TestAgent",
                    "entry": "diary entry content for read test",
                    "topic": "testing",
                }),
            )
            .await;

            let result = tool_diary_read(&connection, &json!({"agent_name": "TestAgent"})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["total"], 1);
            let entries = result["entries"].as_array().expect("entries must be array");
            assert_eq!(entries[0]["topic"], "testing");
        })
        .await;
    }

    // --- dispatch ---

    #[tokio::test]
    async fn dispatch_unknown_tool_returns_error() {
        with_isolated_env(|connection| async move {
            let result = dispatch(&connection, "nonexistent_tool", &json!({})).await;
            assert!(result["error"].is_string(), "must return error");
            assert!(
                result["error"]
                    .as_str()
                    .expect("error string")
                    .contains("Unknown tool")
            );
        })
        .await;
    }

    #[tokio::test]
    async fn dispatch_empty_name_returns_error() {
        with_isolated_env(|connection| async move {
            let result = dispatch(&connection, "", &json!({})).await;
            assert!(
                result["error"].is_string(),
                "must return error for empty name"
            );
            assert!(
                result["error"]
                    .as_str()
                    .expect("error string")
                    .contains("empty")
            );
        })
        .await;
    }

    #[tokio::test]
    async fn dispatch_non_object_args_returns_error() {
        with_isolated_env(|connection| async move {
            let result = dispatch(&connection, "mempalace_status", &json!("not an object")).await;
            assert!(
                result["error"].is_string(),
                "must return error for non-object args"
            );
            assert!(
                result["error"]
                    .as_str()
                    .expect("error string")
                    .contains("JSON object")
            );
        })
        .await;
    }

    #[tokio::test]
    async fn dispatch_routes_to_correct_tool() {
        with_isolated_env(|connection| async move {
            let result = dispatch(&connection, "mempalace_status", &json!({})).await;
            // tool_status returns total_drawers, proving it was routed correctly
            assert!(
                result.get("total_drawers").is_some(),
                "must route to tool_status"
            );
            assert!(
                result.get("protocol").is_some(),
                "status must include protocol"
            );
        })
        .await;
    }

    // --- get_aaak_spec (via dispatch) ---

    #[tokio::test]
    async fn get_aaak_spec_returns_non_empty() {
        with_isolated_env(|connection| async move {
            let result = dispatch(&connection, "mempalace_get_aaak_spec", &json!({})).await;
            let spec = result["aaak_spec"]
                .as_str()
                .expect("aaak_spec must be string");
            assert!(!spec.is_empty(), "spec must not be empty");
            assert!(spec.contains("AAAK"), "spec must mention AAAK");
        })
        .await;
    }
}
