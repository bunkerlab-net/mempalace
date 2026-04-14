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
        let (_db, connection) = test_conn().await;
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
    }

    #[tokio::test]
    async fn add_drawer_idempotent_returns_already_exists() {
        let (_db, connection) = test_conn().await;
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
    }

    #[tokio::test]
    async fn add_drawer_deterministic_id_same_content() {
        let (_db, connection) = test_conn().await;
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
    }

    #[tokio::test]
    async fn add_drawer_different_content_different_id() {
        let (_db, connection) = test_conn().await;
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
    }

    #[tokio::test]
    async fn add_drawer_missing_required_fields_returns_error() {
        let (_db, connection) = test_conn().await;

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
    }
}
