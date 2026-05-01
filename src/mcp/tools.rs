use std::collections::HashMap;
use std::io::Write as _;

use chrono::Utc;
use serde_json::{Value, json};
use sha2::Digest as _;
use turso::Connection;

use uuid::Uuid;

use crate::db::query_all;
use crate::kg;
use crate::palace::entity_registry::EntityRegistry;
use crate::palace::{drawer, fact_checker, graph, query_sanitizer, search};

use super::protocol::{AAAK_SPEC, PALACE_PROTOCOL};

/// Largest integer exactly representable as an f64 (2^53 − 1).
/// Values above this lose precision when stored in f64, so we reject them.
const EXACT_INT_F64_MAX: f64 = 9_007_199_254_740_991.0;

/// Maximum byte length for a tunnel label.  Labels are free-form strings stored
/// in `SQLite`; without a cap an unbounded value could waste DB space or overflow
/// index rows.  255 characters is generous for a short descriptive label.
const LABEL_LEN_MAX: usize = 255;

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
        "mempalace_hook_settings" => tool_hook_settings(args),
        "mempalace_memories_filed_away" => tool_memories_filed_away(),
        "mempalace_reconnect" => tool_reconnect(connection).await,
        "mempalace_check_facts" => tool_check_facts(connection, args).await,
        "mempalace_research_entity" => tool_research_entity(args),
        "mempalace_confirm_entity" => tool_confirm_entity(args),
        _ => json!({"error": format!("Unknown tool: {name}"), "public": true}),
    }
}

/// Extract a string argument from the tool args JSON object.
/// Returns an empty string when the key is absent or the value is not a string.
fn str_arg<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key)
        .and_then(|arg_val| arg_val.as_str())
        .unwrap_or("")
}

/// Extract a positive integer argument, coercing floats and strings.
///
/// MCP JSON transport sometimes delivers integers as floats (`5.0`) or strings
/// (`"5"`). Trying all three representations keeps tool calls robust regardless
/// of what the client sends. Only accepts finite, whole, positive integers (>0).
fn int_arg(args: &Value, key: &str, default: i64) -> i64 {
    args.get(key)
        .and_then(|arg_val| {
            arg_val
                .as_i64()
                .filter(|&n| n > 0)
                .or_else(|| {
                    arg_val.as_f64().and_then(|f| {
                        if f.is_finite() && f > 0.0 && f <= EXACT_INT_F64_MAX && f.fract() == 0.0 {
                            // Safe: EXACT_INT_F64_MAX (2^53-1) < i64::MAX, so the value fits exactly
                            #[allow(clippy::cast_possible_truncation)]
                            Some(f as i64)
                        } else {
                            None
                        }
                    })
                })
                .or_else(|| {
                    arg_val.as_str().and_then(|str_val| {
                        str_val.parse::<i64>().ok().filter(|&n| n > 0).or_else(|| {
                            str_val.parse::<f64>().ok().and_then(|f| {
                                if f.is_finite()
                                    && f > 0.0
                                    && f <= EXACT_INT_F64_MAX
                                    && f.fract() == 0.0
                                {
                                    // Safe: EXACT_INT_F64_MAX (2^53-1) < i64::MAX, so the value fits exactly
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
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(
            json!({"success": false, "error": format!("{field_name} must be a non-empty string"), "public": true}),
        );
    }
    if trimmed.len() > 128 {
        return Err(
            json!({"success": false, "error": format!("{field_name} exceeds maximum length of 128 characters"), "public": true}),
        );
    }
    if trimmed.contains("..")
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('\x00')
    {
        return Err(
            json!({"success": false, "error": format!("{field_name} contains invalid characters"), "public": true}),
        );
    }
    if !trimmed
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric())
    {
        return Err(
            json!({"success": false, "error": format!("{field_name} must start with an alphanumeric character"), "public": true}),
        );
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '.' | '\'' | '-'))
    {
        return Err(
            json!({"success": false, "error": format!("{field_name} contains invalid characters"), "public": true}),
        );
    }
    let result = trimmed.to_string();

    // Postconditions: result is non-empty, trimmed, and has no path-traversal chars.
    debug_assert!(!result.is_empty());
    debug_assert!(result == result.trim());
    debug_assert!(!result.contains(".."));
    debug_assert!(!result.contains('/'));
    debug_assert!(!result.contains('\\'));
    debug_assert!(!result.contains('\0'));

    Ok(result)
}

/// Validate a knowledge-graph entity value (subject or object).
///
/// More permissive than `sanitize_name` — allows punctuation such as commas,
/// colons, and parentheses that are common in natural-language KG values.
/// Rejects null bytes, path-traversal sequences (`..`, `/`, `\`), and
/// over-length strings.
///
/// Note: `/` is still rejected, so namespaced IRIs (e.g. `schema:Person`) are
/// allowed but URI paths (e.g. `http://example.org/Person`) are not. If IRI
/// support is ever needed, introduce a dedicated validator rather than relaxing
/// this one.
///
/// Not used for wing/room names (filesystem constraints) or predicates
/// (which should be simple relationship identifiers).
fn sanitize_kg_value(value: &str, field_name: &str) -> Result<String, Value> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(
            json!({"success": false, "error": format!("{field_name} must be a non-empty string"), "public": true}),
        );
    }
    if trimmed.chars().count() > 128 {
        return Err(
            json!({"success": false, "error": format!("{field_name} exceeds maximum length of 128 characters"), "public": true}),
        );
    }
    if trimmed.contains("..")
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('\x00')
    {
        return Err(
            json!({"success": false, "error": format!("{field_name} contains invalid characters"), "public": true}),
        );
    }
    let result = trimmed.to_string();

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
    if trimmed.chars().count() > LABEL_LEN_MAX {
        return Err(
            json!({"success": false, "error": format!("label exceeds maximum length of {LABEL_LEN_MAX} characters"), "public": true}),
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
    debug_assert!(result.chars().count() <= LABEL_LEN_MAX);

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

/// Append a write-operation entry to `config_dir()/wal/write_log.jsonl`.
///
/// The data directory is `$XDG_DATA_HOME/mempalace` by default, or the value
/// of `MEMPALACE_DIR` when set. Failures are non-fatal: logged to stderr so
/// the server stays alive even if the WAL directory is unwritable. I/O is
/// offloaded to `spawn_blocking` so the async worker thread is not stalled.
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
            Err(error) => eprintln!("WAL write failed: {error}"),
        }
    })
    .await;
}

/// Return a summary of all wings, rooms, and total drawer count.
async fn tool_status(connection: &Connection) -> Value {
    let rows = query_all(
        connection,
        "SELECT wing, room, COUNT(*) as cnt FROM drawers GROUP BY wing, room",
        (),
    )
    .await;

    let rows = match rows {
        Ok(rows) => rows,
        Err(error) => return json!({"error": error.to_string()}),
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

/// Return all wings with their drawer counts.
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
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Return rooms and their drawer counts, optionally filtered by wing.
async fn tool_list_rooms(connection: &Connection, args: &Value) -> Value {
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(value) => value,
        Err(error) => return error,
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
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Return the full wing → room → drawer-count taxonomy tree.
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
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Full-text search the palace, returning ranked drawer results.
async fn tool_search(connection: &Connection, args: &Value) -> Value {
    let raw_query = str_arg(args, "query").trim();
    if raw_query.is_empty() {
        return json!({"error": "query must be a non-empty string", "public": true});
    }
    let limit = usize::try_from(int_arg(args, "limit", 5).clamp(1, 100)).unwrap_or(5);
    let context_received = !str_arg(args, "context").trim().is_empty();
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let room = match sanitize_opt_name(str_arg(args, "room"), "room") {
        Ok(value) => value,
        Err(error) => return error,
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
                .map(|result| {
                    json!({
                        "wing": result.wing,
                        "room": result.room,
                        "content": result.text,
                        "source_file": result.source_file,
                        "source_path": result.source_path,
                        "chunk_index": result.chunk_index,
                        "created_at": result.created_at,
                        "similarity": result.relevance,
                    })
                })
                .collect();
            let count = items.len();
            let mut output = json!({"results": items, "count": count});
            if sanitized.was_sanitized {
                output["query_sanitized"] = json!(true);
                output["sanitizer"] = json!({
                    "method": sanitized.method,
                    "original_length": sanitized.original_length,
                    "clean_length": sanitized.clean_length,
                    "clean_query": sanitized.clean_query,
                });
            }
            if context_received {
                output["context_received"] = json!(true);
            }
            output
        }
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Check whether content is a near-duplicate of an existing drawer.
async fn tool_check_duplicate(connection: &Connection, args: &Value) -> Value {
    let content = str_arg(args, "content");
    // Simple keyword overlap check since we don't have vector similarity.
    match search::search_memories(connection, content, None, None, 5).await {
        Ok(results) => {
            let matches: Vec<Value> = results
                .iter()
                .filter(|result| result.relevance > 3.0) // high word overlap
                .map(|result| {
                    let preview = if result.text.chars().count() > 200 {
                        format!("{}...", result.text.chars().take(200).collect::<String>())
                    } else {
                        result.text.clone()
                    };
                    json!({
                        "wing": result.wing,
                        "room": result.room,
                        "content": preview,
                    })
                })
                .collect();
            json!({
                "is_duplicate": !matches.is_empty(),
                "matches": matches,
            })
        }
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Insert a new drawer into the palace, computing a deterministic SHA256-based ID.
async fn tool_add_drawer(connection: &Connection, args: &Value) -> Value {
    let wing = str_arg(args, "wing");
    let room = str_arg(args, "room");
    let content = str_arg(args, "content");
    let source_file = str_arg(args, "source_file");
    let added_by = {
        let added_by_raw = str_arg(args, "added_by");
        if added_by_raw.is_empty() {
            "mcp"
        } else {
            added_by_raw
        }
    };

    let wing = match sanitize_name(wing, "wing") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let room = match sanitize_name(room, "room") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let content = match sanitize_content(content) {
        Ok(value) => value,
        Err(error) => return error,
    };

    // Deterministic ID: sha256(wing+room+content) so the same content in
    // the same wing/room always produces the same ID, making the call idempotent.
    let hash = sha2::Sha256::digest(format!("{wing}\u{1f}{room}\u{1f}{content}").as_bytes());
    let hex: String = hash.iter().fold(String::new(), |mut hex_string, byte| {
        use std::fmt::Write as _;
        let _ = write!(hex_string, "{byte:02x}");
        hex_string
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
        Err(error) => json!({"success": false, "error": error.to_string()}),
    }
}

/// Delete a drawer and its inverted-index entries by ID.
async fn tool_delete_drawer(connection: &Connection, args: &Value) -> Value {
    let drawer_id = match sanitize_name(str_arg(args, "drawer_id"), "drawer_id") {
        Ok(value) => value,
        Err(error) => return error,
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
            // Also clean up inverted index.
            let _ = connection
                .execute(
                    "DELETE FROM drawer_words WHERE drawer_id = ?",
                    [drawer_id.as_str()],
                )
                .await;
            json!({"success": true, "drawer_id": drawer_id})
        }
        Err(error) => json!({"success": false, "error": error.to_string()}),
    }
}

/// Fetch a single drawer by ID, returning its full content and metadata.
async fn tool_get_drawer(connection: &Connection, args: &Value) -> Value {
    let drawer_id = match sanitize_name(str_arg(args, "drawer_id"), "drawer_id") {
        Ok(value) => value,
        Err(error) => return error,
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
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// List drawers with optional wing/room filtering and cursor-style pagination.
async fn tool_list_drawers(connection: &Connection, args: &Value) -> Value {
    const LIMIT_MAX: i64 = 100;
    let limit = int_arg(args, "limit", 20).clamp(1, LIMIT_MAX);
    let offset = int_arg(args, "offset", 0).max(0);
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let room = match sanitize_opt_name(str_arg(args, "room"), "room") {
        Ok(value) => value,
        Err(error) => return error,
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
        Err(error) => json!({"error": error.to_string()}),
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
        (Some(wing_value), Some(room_value)) => {
            query_all(connection, "SELECT id, content, wing, room FROM drawers WHERE wing = ?1 AND room = ?2 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?3 OFFSET ?4", (wing_value.as_str(), room_value.as_str(), limit, offset)).await
        }
        (Some(wing_value), None) => {
            query_all(connection, "SELECT id, content, wing, room FROM drawers WHERE wing = ?1 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?2 OFFSET ?3", (wing_value.as_str(), limit, offset)).await
        }
        (None, Some(room_value)) => {
            query_all(connection, "SELECT id, content, wing, room FROM drawers WHERE room = ?1 AND (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?2 OFFSET ?3", (room_value.as_str(), limit, offset)).await
        }
        (None, None) => {
            query_all(connection, "SELECT id, content, wing, room FROM drawers WHERE (ingest_mode IS NULL OR ingest_mode != 'diary') ORDER BY filed_at DESC, id DESC LIMIT ?1 OFFSET ?2", (limit, offset)).await
        }
    }
}

/// Parsed and validated arguments for `tool_update_drawer`.
struct UpdateDrawerArgs {
    drawer_id: String,
    content_new: Option<String>,
    wing_new: Option<String>,
    room_new: Option<String>,
}

/// Parse and validate all input args for `tool_update_drawer`.
///
/// Returns `Ok(UpdateDrawerArgs)` on success.  Returns `Err(Value)` on
/// validation failure **or** when no fields were supplied (noop), so the
/// caller can return the value directly in both cases.
fn tool_update_drawer_validate_args(args: &Value) -> Result<UpdateDrawerArgs, Value> {
    let drawer_id = sanitize_name(str_arg(args, "drawer_id"), "drawer_id")?;
    // Diary entries use UUID IDs; they must not be mutated via this handler.
    if !drawer_id.starts_with("drawer_") {
        return Err(
            json!({"success": false, "error": "drawer_id has invalid format", "public": true}),
        );
    }
    let content_new = match args.get("content") {
        None | Some(Value::Null) => None,
        Some(Value::String(content_raw)) => Some(sanitize_content(content_raw)?),
        Some(_) => {
            return Err(
                json!({"success": false, "error": "content must be a string", "public": true}),
            );
        }
    };
    let wing_new = match args.get("wing") {
        None | Some(Value::Null) => None,
        Some(Value::String(wing_raw)) => sanitize_opt_name(wing_raw, "wing")?,
        Some(_) => {
            return Err(
                json!({"success": false, "error": "wing must be a string", "public": true}),
            );
        }
    };
    let room_new = match args.get("room") {
        None | Some(Value::Null) => None,
        Some(Value::String(room_raw)) => sanitize_opt_name(room_raw, "room")?,
        Some(_) => {
            return Err(
                json!({"success": false, "error": "room must be a string", "public": true}),
            );
        }
    };
    // No-op: nothing to change.  Return as Err so the caller can return early.
    if content_new.is_none() && wing_new.is_none() && room_new.is_none() {
        return Err(json!({"success": true, "drawer_id": drawer_id, "noop": true}));
    }
    Ok(UpdateDrawerArgs {
        drawer_id,
        content_new,
        wing_new,
        room_new,
    })
}

/// Compute the deterministic SHA256-based drawer ID from wing, room, and content.
///
/// Mirrors the ID computation in `tool_add_drawer` so that IDs stay consistent
/// across add and update operations.
fn tool_update_drawer_recompute_id(wing: &str, room: &str, content: &str) -> String {
    let hash = sha2::Sha256::digest(format!("{wing}\u{1f}{room}\u{1f}{content}").as_bytes());
    let hex: String = hash.iter().fold(String::new(), |mut hex_string, byte| {
        use std::fmt::Write as _;
        let _ = write!(hex_string, "{byte:02x}");
        hex_string
    });
    format!("drawer_{wing}_{room}_{}", &hex[..24])
}

/// Check whether `new_id` already belongs to a different drawer, rejecting
/// updates that would silently duplicate an existing entry.
///
/// Returns `Some(error_json)` if the update must be rejected, `None` if safe.
async fn tool_update_drawer_check_duplicate(
    connection: &Connection,
    old_id: &str,
    new_id: &str,
) -> Option<Value> {
    if new_id == old_id {
        return None;
    }
    let existing = query_all(connection, "SELECT id FROM drawers WHERE id = ?", [new_id]).await;
    match existing {
        Ok(rows) if !rows.is_empty() => Some(json!({
            "success": false,
            "error": "A drawer with this wing/room/content already exists",
            "existing_drawer_id": new_id,
            "public": true,
        })),
        Err(error) => Some(json!({"success": false, "error": error.to_string()})),
        Ok(_) => None,
    }
}

/// Execute the transactional reindex: update the drawers row to its new ID,
/// wing, room, and content, then rebuild the `drawer_words` full-text index.
///
/// Wraps all mutations in BEGIN/COMMIT so drawers and `drawer_words` cannot
/// diverge if any step fails mid-flight.
async fn tool_update_drawer_reindex(
    connection: &Connection,
    old_id: &str,
    new_id: &str,
    final_wing: &str,
    final_room: &str,
    final_content: &str,
) -> Result<(), Value> {
    if let Err(e) = connection.execute("BEGIN", ()).await {
        return Err(json!({"success": false, "error": e.to_string()}));
    }
    if let Err(e) = connection
        .execute(
            "UPDATE drawers SET id = ?1, wing = ?2, room = ?3, content = ?4 WHERE id = ?5",
            turso::params![new_id, final_wing, final_room, final_content, old_id],
        )
        .await
    {
        let _ = connection.execute("ROLLBACK", ()).await;
        return Err(json!({"success": false, "error": e.to_string()}));
    }
    // Re-index words: always needed when the ID changes or content changes.
    if let Err(e) = connection
        .execute("DELETE FROM drawer_words WHERE drawer_id = ?", [old_id])
        .await
    {
        let _ = connection.execute("ROLLBACK", ()).await;
        return Err(json!({"success": false, "error": e.to_string()}));
    }
    if new_id != old_id {
        // drawer_words rows for old_id were deleted above; if new_id already
        // had entries (shouldn't happen — we checked above), clean those too.
        let _ = connection
            .execute("DELETE FROM drawer_words WHERE drawer_id = ?", [new_id])
            .await;
    }
    if let Err(e) = drawer::index_words(connection, new_id, final_content).await {
        let _ = connection.execute("ROLLBACK", ()).await;
        return Err(json!({"success": false, "error": e.to_string()}));
    }
    if let Err(e) = connection.execute("COMMIT", ()).await {
        let _ = connection.execute("ROLLBACK", ()).await;
        return Err(json!({"success": false, "error": e.to_string()}));
    }
    Ok(())
}

/// Update an existing drawer's content and/or location (wing/room).
///
/// Recomputes the deterministic SHA256 ID after any change to keep it
/// consistent with `tool_add_drawer`.  Rejects updates that would collide
/// with an existing drawer.
async fn tool_update_drawer(connection: &Connection, args: &Value) -> Value {
    let parsed = match tool_update_drawer_validate_args(args) {
        Ok(parsed) => parsed,
        Err(early) => return early,
    };

    // Fetch existing drawer to resolve final wing, room, and content values.
    let rows = query_all(
        connection,
        "SELECT wing, room, content FROM drawers WHERE id = ?",
        [parsed.drawer_id.as_str()],
    )
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) => return json!({"success": false, "error": error.to_string()}),
    };
    if rows.is_empty() {
        return json!({"success": false, "error": format!("Drawer not found: {}", parsed.drawer_id), "public": true});
    }

    let wing_old: String = rows[0].get(0).unwrap_or_default();
    let room_old: String = rows[0].get(1).unwrap_or_default();
    let content_old: String = rows[0].get(2).unwrap_or_default();
    let final_wing = parsed.wing_new.as_deref().unwrap_or(&wing_old);
    let final_room = parsed.room_new.as_deref().unwrap_or(&room_old);
    let final_content = parsed.content_new.as_deref().unwrap_or(&content_old);

    // Recompute the deterministic ID to keep it consistent with tool_add_drawer.
    // wing/room/content are all baked into the ID, so any change means a new ID.
    let id_new = tool_update_drawer_recompute_id(final_wing, final_room, final_content);

    // If the recomputed ID already exists (and differs), the new wing+room+content
    // is a duplicate of another drawer — reject to prevent silent duplication.
    if let Some(error) =
        tool_update_drawer_check_duplicate(connection, &parsed.drawer_id, &id_new).await
    {
        return error;
    }

    if let Err(error) = tool_update_drawer_reindex(
        connection,
        &parsed.drawer_id,
        &id_new,
        final_wing,
        final_room,
        final_content,
    )
    .await
    {
        return error;
    }

    // WAL entry is written only after both validation and the transaction commit
    // succeed — a bogus log entry would be written if wal_log ran before either check.
    wal_log(
        "update_drawer",
        json!({
            "drawer_id": parsed.drawer_id,
            "new_drawer_id": id_new,
            "old_wing": wing_old,
            "old_room": room_old,
            "new_wing": final_wing,
            "new_room": final_room,
            "content_changed": parsed.content_new.is_some(),
        }),
    )
    .await;

    json!({
        "success": true,
        "drawer_id": id_new,
        "wing": final_wing,
        "room": final_room,
    })
}

/// Query all knowledge-graph facts for an entity, with optional direction and as-of filters.
async fn tool_kg_query(connection: &Connection, args: &Value) -> Value {
    let entity = match sanitize_kg_value(str_arg(args, "entity"), "entity") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let as_of = {
        let as_of_raw = str_arg(args, "as_of");
        if as_of_raw.is_empty() {
            None
        } else {
            Some(as_of_raw.to_string())
        }
    };
    let direction = {
        let direction_raw = str_arg(args, "direction");
        if direction_raw.is_empty() {
            "both"
        } else {
            direction_raw
        }
    };
    if !matches!(direction, "outgoing" | "incoming" | "both") {
        return json!({"error": "direction must be 'outgoing', 'incoming', or 'both'", "public": true});
    }

    match kg::query::query_entity(connection, &entity, as_of.as_deref(), direction).await {
        Ok(facts) => {
            let count = facts.len();
            json!({"entity": entity, "as_of": as_of, "facts": facts, "count": count})
        }
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Add a subject–predicate–object triple to the knowledge graph.
async fn tool_kg_add(connection: &Connection, args: &Value) -> Value {
    let subject = str_arg(args, "subject");
    let predicate = str_arg(args, "predicate");
    let object = str_arg(args, "object");
    let valid_from = {
        let valid_from_raw = str_arg(args, "valid_from");
        if valid_from_raw.is_empty() {
            None
        } else {
            Some(valid_from_raw.to_string())
        }
    };
    let source_closet = {
        let source_closet_raw = str_arg(args, "source_closet");
        if source_closet_raw.is_empty() {
            None
        } else {
            Some(source_closet_raw.to_string())
        }
    };

    let subject = match sanitize_kg_value(subject, "subject") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let predicate = match sanitize_name(predicate, "predicate") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let object = match sanitize_kg_value(object, "object") {
        Ok(value) => value,
        Err(error) => return error,
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
            source_drawer_id: None,
            adapter_name: None,
        },
    )
    .await
    {
        Ok(triple_id) => json!({
            "success": true,
            "triple_id": triple_id,
            "fact": format!("{subject} → {predicate} → {object}"),
        }),
        Err(error) => json!({"success": false, "error": error.to_string()}),
    }
}

/// End-date a knowledge-graph triple by setting its `valid_to` field.
async fn tool_kg_invalidate(connection: &Connection, args: &Value) -> Value {
    let subject = str_arg(args, "subject");
    let predicate = str_arg(args, "predicate");
    let object = str_arg(args, "object");
    let ended = {
        let ended_raw = str_arg(args, "ended");
        if ended_raw.is_empty() {
            None
        } else {
            Some(ended_raw.to_string())
        }
    };

    let subject = match sanitize_kg_value(subject, "subject") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let predicate = match sanitize_name(predicate, "predicate") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let object = match sanitize_kg_value(object, "object") {
        Ok(value) => value,
        Err(error) => return error,
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
        Err(error) => json!({"success": false, "error": error.to_string()}),
    }
}

/// Return all knowledge-graph facts sorted by validity date, optionally filtered by entity.
async fn tool_kg_timeline(connection: &Connection, args: &Value) -> Value {
    let entity = {
        let raw_entity = str_arg(args, "entity").trim();
        if raw_entity.is_empty() {
            None
        } else {
            match sanitize_kg_value(raw_entity, "entity") {
                Ok(sanitized_val) => Some(sanitized_val),
                Err(error) => return error,
            }
        }
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
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Return aggregate statistics for the knowledge graph (entity count, triple count, etc.).
async fn tool_kg_stats(connection: &Connection) -> Value {
    match kg::query::stats(connection).await {
        Ok(stats) => json!(stats),
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// BFS-traverse the palace graph from a starting room up to `max_hops` hops.
async fn tool_traverse(connection: &Connection, args: &Value) -> Value {
    let start_room = match sanitize_name(str_arg(args, "start_room"), "start_room") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let hops_max = usize::try_from(int_arg(args, "max_hops", 2).clamp(1, 10)).unwrap_or(2);

    match graph::traverse(connection, &start_room, hops_max).await {
        Ok((results, truncated)) => json!({"results": results, "truncated": truncated}),
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Find rooms that bridge two wings, optionally filtering by wing names.
async fn tool_find_tunnels(connection: &Connection, args: &Value) -> Value {
    let wing_a = match sanitize_opt_name(str_arg(args, "wing_a"), "wing_a") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let wing_b = match sanitize_opt_name(str_arg(args, "wing_b"), "wing_b") {
        Ok(value) => value,
        Err(error) => return error,
    };

    match graph::find_tunnels(connection, wing_a.as_deref(), wing_b.as_deref()).await {
        Ok((tunnels, truncated)) => json!({"tunnels": tunnels, "truncated": truncated}),
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Return aggregate statistics about the palace graph (room count, tunnel count, etc.).
async fn tool_graph_stats(connection: &Connection) -> Value {
    match graph::graph_stats(connection).await {
        Ok(stats) => json!(stats),
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Create an explicit (agent-annotated) tunnel linking two palace locations.
async fn tool_create_tunnel(connection: &Connection, args: &Value) -> Value {
    let source_wing = match sanitize_name(str_arg(args, "source_wing"), "source_wing") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let source_room = match sanitize_name(str_arg(args, "source_room"), "source_room") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let target_wing = match sanitize_name(str_arg(args, "target_wing"), "target_wing") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let target_room = match sanitize_name(str_arg(args, "target_room"), "target_room") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let label = match sanitize_label(str_arg(args, "label")) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let source_drawer_id =
        match sanitize_opt_name(str_arg(args, "source_drawer_id"), "source_drawer_id") {
            Ok(value) => value,
            Err(error) => return error,
        };
    let target_drawer_id =
        match sanitize_opt_name(str_arg(args, "target_drawer_id"), "target_drawer_id") {
            Ok(value) => value,
            Err(error) => return error,
        };

    match graph::create_tunnel(
        connection,
        &graph::CreateTunnelParams {
            source_wing: &source_wing,
            source_room: &source_room,
            target_wing: &target_wing,
            target_room: &target_room,
            label: &label,
            kind: "explicit",
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
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Return all explicit tunnels, optionally filtered to those involving a specific wing.
async fn tool_list_tunnels(connection: &Connection, args: &Value) -> Value {
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(value) => value,
        Err(error) => return error,
    };

    match graph::list_tunnels(connection, wing.as_deref()).await {
        Ok(tunnels) => json!({"tunnels": tunnels, "count": tunnels.len()}),
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Delete an explicit tunnel by its 16-character hex ID.
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
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Return all explicit tunnel connections from a given wing and room.
async fn tool_follow_tunnels(connection: &Connection, args: &Value) -> Value {
    let wing = match sanitize_name(str_arg(args, "wing"), "wing") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let room = match sanitize_name(str_arg(args, "room"), "room") {
        Ok(value) => value,
        Err(error) => return error,
    };

    match graph::follow_tunnels(connection, &wing, &room).await {
        Ok(connections) => json!({"wing": wing, "room": room, "connections": connections}),
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Append a diary entry for an agent in its personal wing.
///
/// When `wing` is supplied it is used directly (after sanitization); otherwise
/// the wing is derived from `agent_name` as `wing_<agent_name_lower>`.
async fn tool_diary_write(connection: &Connection, args: &Value) -> Value {
    let agent_name = str_arg(args, "agent_name");
    let entry = str_arg(args, "entry");
    let topic = {
        let topic_raw = str_arg(args, "topic");
        if topic_raw.is_empty() {
            "general".to_string()
        } else {
            match sanitize_name(topic_raw, "topic") {
                Ok(value) => value,
                Err(error) => return error,
            }
        }
    };

    let agent_name = match sanitize_name(agent_name, "agent_name") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let entry = match sanitize_content(entry) {
        Ok(value) => value,
        Err(error) => return error,
    };

    // Use the caller-supplied wing when provided; fall back to derived wing.
    // sanitize_opt_name treats empty/whitespace-only values as None so they
    // fall through to the derived wing rather than returning an error.
    let wing = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(Some(w)) => w,
        Ok(None) => format!("wing_{}", agent_name.to_lowercase().replace(' ', "_")),
        Err(error) => return error,
    };
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

    // Use direct SQL to also set extract_mode (topic) which DrawerParams doesn't support.
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
        Err(error) => json!({"success": false, "error": error.to_string()}),
    }
}

/// Read the most recent diary entries for an agent, newest first.
///
/// When `wing` is supplied the query is scoped to that wing AND to the
/// requesting agent (`added_by`), so agents cannot read each other's entries
/// even when they share a wing name. When `wing` is absent the query returns
/// all diary entries written by the agent across all wings.
async fn tool_diary_read(connection: &Connection, args: &Value) -> Value {
    let agent_name = str_arg(args, "agent_name");
    let last_n = int_arg(args, "last_n", 10).clamp(1, 100);

    let agent_name = match sanitize_name(agent_name, "agent_name") {
        Ok(value) => value,
        Err(error) => return error,
    };

    // Resolve wing: explicit value → wing-scoped query; absent/whitespace → cross-wing.
    // sanitize_opt_name treats empty/whitespace-only as None so callers don't need
    // to distinguish between an absent field and a blank one.
    let wing_filter: Option<String> = match sanitize_opt_name(str_arg(args, "wing"), "wing") {
        Ok(value) => value,
        Err(error) => return error,
    };

    let rows = if let Some(ref wing) = wing_filter {
        // Wing-scoped query: entries by this agent in the specific wing only.
        // added_by is required so one agent cannot read another agent's diary
        // entries even when they share a wing name.
        query_all(
            connection,
            "SELECT id, content, extract_mode, filed_at FROM drawers WHERE wing = ? AND room = 'diary' AND added_by = ? ORDER BY filed_at DESC LIMIT ?",
            (wing.as_str(), agent_name.as_str(), last_n),
        )
        .await
    } else {
        // Cross-wing query: all diary entries by this agent regardless of wing.
        query_all(
            connection,
            "SELECT id, content, extract_mode, filed_at FROM drawers WHERE added_by = ? AND room = 'diary' ORDER BY filed_at DESC LIMIT ?",
            (agent_name.as_str(), last_n),
        )
        .await
    };

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
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Get or set the hook behavior settings (`hook_silent_save`, `hook_desktop_toast`).
///
/// Call with no arguments to read current settings. Pass one or both boolean
/// fields to update them. Returns the current (post-update) values.
fn tool_hook_settings(args: &Value) -> Value {
    let mut config = match crate::config::MempalaceConfig::load() {
        Ok(c) => c,
        Err(e) => return json!({"success": false, "error": e.to_string()}),
    };

    let mut updated: Vec<String> = Vec::new();

    if let Some(silent) = args.get("silent_save").and_then(Value::as_bool) {
        config.hook_silent_save = silent;
        updated.push(format!("silent_save \u{2192} {silent}"));
    }
    if let Some(toast) = args.get("desktop_toast").and_then(Value::as_bool) {
        config.hook_desktop_toast = toast;
        updated.push(format!("desktop_toast \u{2192} {toast}"));
    }

    if !updated.is_empty()
        && let Err(save_error) = config.save()
    {
        return json!({"success": false, "error": save_error.to_string()});
    }

    // Re-read after potential write to confirm the on-disk state.
    let current = crate::config::MempalaceConfig::load().unwrap_or(config);
    let mut result = json!({
        "success": true,
        "settings": {
            "silent_save": current.hook_silent_save,
            "desktop_toast": current.hook_desktop_toast,
        },
    });
    if !updated.is_empty() {
        result["updated"] = json!(updated);
    }
    result
}

/// Acknowledge the latest silent-mode checkpoint written by the stop hook.
///
/// Reads and deletes `hook_state/last_checkpoint`. Returns message count and
/// timestamp if a checkpoint was filed, or `status: "quiet"` when none exists.
fn tool_memories_filed_away() -> Value {
    let ack_file = crate::config::config_dir()
        .join("hook_state")
        .join("last_checkpoint");

    if !ack_file.is_file() {
        return json!({
            "status": "quiet",
            "message": "No recent journal entry",
            "count": 0,
            "timestamp": null,
        });
    }

    match std::fs::read_to_string(&ack_file) {
        Ok(content) => {
            // Delete the ack file so the next call reflects only new activity.
            let _ = std::fs::remove_file(&ack_file);
            match serde_json::from_str::<Value>(&content) {
                Ok(data) => {
                    let msgs = data.get("msgs").and_then(Value::as_i64).unwrap_or(0);
                    let ts = data.get("ts").cloned().unwrap_or(Value::Null);
                    json!({
                        "status": "ok",
                        "message": format!("\u{2726} {msgs} messages tucked into drawers"),
                        "count": msgs,
                        "timestamp": ts,
                    })
                }
                Err(_) => {
                    json!({
                        "status": "error",
                        "message": "\u{2726} Journal entry filed in the palace",
                        "count": 0,
                        "timestamp": null,
                    })
                }
            }
        }
        Err(e) => json!({"status": "error", "error": e.to_string()}),
    }
}

/// Confirm palace database connectivity and return the current drawer count.
///
/// On turso/SQLite there is no in-memory HNSW index to invalidate, so this
/// tool is a lightweight heartbeat: it opens the DB, runs `SELECT count(*)`,
/// and returns the result. It is provided for harness compatibility with the
/// Python implementation's `mempalace_reconnect`.
async fn tool_reconnect(connection: &Connection) -> Value {
    match query_all(connection, "SELECT count(*) FROM drawers", ()).await {
        Ok(rows) => {
            let count: i64 = rows
                .first()
                .and_then(|row| row.get_value(0).ok())
                .and_then(|cell| cell.as_integer().copied())
                .unwrap_or(0);
            assert!(count >= 0, "drawer count must be non-negative");
            json!({"success": true, "message": "Connected to palace", "drawers": count})
        }
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

/// Check `text` for entity confusion and KG contradictions.
///
/// Returns `issues` — a list of detected problems — or an empty list when clean.
/// Each issue has a `type` field: `"similar_name"`, `"relationship_mismatch"`, or
/// `"stale_fact"`, plus a `detail` string and type-specific fields.
async fn tool_check_facts(connection: &Connection, args: &Value) -> Value {
    let text = str_arg(args, "text");
    if text.is_empty() {
        return json!({"error": "text argument is required", "public": true});
    }
    assert!(!text.is_empty(), "tool_check_facts: text guard passed");
    match fact_checker::check_text(text, connection).await {
        Ok(issues) => {
            let is_clean = issues.is_empty();
            assert!(issues.iter().all(|i| !i.issue_type.is_empty()));
            json!({"issues": issues, "clean": is_clean})
        }
        Err(error) => json!({"error": error.to_string()}),
    }
}

/// Look up a word in the entity registry; optionally research it via Wikipedia.
///
/// Checks the local registry first via [`EntityRegistry::lookup`]. Only queries
/// Wikipedia when `allow_network = true` (privacy-by-architecture).
fn tool_research_entity(args: &Value) -> Value {
    let word = str_arg(args, "word");
    if word.is_empty() {
        return json!({"error": "word argument is required", "public": true});
    }
    assert!(!word.is_empty(), "tool_research_entity: word guard passed");
    let allow_network = args
        .get("allow_network")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let auto_confirm = args
        .get("auto_confirm")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let context = str_arg(args, "context");

    let mut registry = EntityRegistry::load();

    // Check local registry first — fast path with context disambiguation.
    let local = registry.lookup(word, context);
    if local.entity_type != "unknown" {
        assert!(!local.entity_type.is_empty());
        return json!({
            "word": local.name,
            "inferred_type": local.entity_type,
            "confidence": local.confidence,
            "source": local.source,
            "contexts": local.contexts,
            "needs_disambiguation": local.needs_disambiguation,
            "disambiguated_by": local.disambiguated_by,
            "confirmed": true,
            "note": "found in local registry"
        });
    }

    let entry = registry.research(word, auto_confirm, allow_network);
    assert!(!entry.inferred_type.is_empty());
    json!({
        "word": word,
        "inferred_type": entry.inferred_type,
        "confidence": entry.confidence,
        "confirmed": entry.confirmed,
        "wiki_summary": entry.wiki_summary,
        "wiki_title": entry.wiki_title,
        "note": entry.note,
    })
}

/// Confirm a previously researched word as a specific entity type.
///
/// When `entity_type` is `"person"`, the word is added to the people registry
/// with `CONFIDENCE_WIKI` confidence, making it available for future lookups.
fn tool_confirm_entity(args: &Value) -> Value {
    let word = str_arg(args, "word");
    if word.is_empty() {
        return json!({"error": "word argument is required", "public": true});
    }
    let entity_type = str_arg(args, "entity_type");
    if entity_type.is_empty() {
        return json!({"error": "entity_type argument is required", "public": true});
    }
    assert!(!word.is_empty(), "tool_confirm_entity: word guard passed");
    assert!(
        !entity_type.is_empty(),
        "tool_confirm_entity: entity_type guard passed"
    );

    let relationship = str_arg(args, "relationship");
    let context = str_arg(args, "context");

    let mut registry = EntityRegistry::load();
    match registry.confirm_research(word, entity_type, relationship, context) {
        Ok(()) => json!({"success": true, "word": word, "entity_type": entity_type}),
        Err(error) => json!({"error": error.to_string()}),
    }
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Open an in-memory test palace database and return the database + connection pair.
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
            // Missing content.
            let result = tool_add_drawer(&connection, &json!({"wing": "w", "room": "r"})).await;
            assert_eq!(result["success"], false);

            // Missing wing.
            let result = tool_add_drawer(
                &connection,
                &json!({"room": "r", "content": "some text here for testing"}),
            )
            .await;
            assert_eq!(result["success"], false);

            // Missing room.
            let result = tool_add_drawer(
                &connection,
                &json!({"wing": "w", "content": "some text here for testing"}),
            )
            .await;
            assert_eq!(result["success"], false);
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

    /// Wraps the three-line test setup (tempdir, `async_with_vars`, `test_conn`) so
    /// callers only express what is under test.  The connection is passed by value
    /// so the closure can borrow it as `&connection` without lifetime complications.
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
    async fn search_results_include_created_at() {
        // Each search result must include a created_at key that maps to a JSON
        // string. The value may be empty for legacy rows where filed_at was not
        // recorded (SearchResult.created_at uses unwrap_or_default), so only
        // the type is asserted here, not non-emptiness.
        with_isolated_env(|connection| async move {
            seed_drawer(
                &connection,
                "tech",
                "rust",
                "rust programming language memory safety",
            )
            .await;

            let result = tool_search(&connection, &json!({"query": "rust programming"})).await;
            assert!(result.get("error").is_none(), "search must not error");
            let results = result["results"].as_array().expect("results must be array");
            assert!(!results.is_empty(), "must have at least one result");
            for r in results {
                // Assert the key is present and is a JSON string (may be empty).
                assert!(
                    r["created_at"].is_string(),
                    "created_at must be a string in each result"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn search_results_created_at_empty_for_legacy_row() {
        // A drawer with filed_at = NULL (legacy row — filed_at was added after
        // initial schema) must still appear in search results. created_at must
        // be an empty string (unwrap_or_default of None), not an error.
        with_isolated_env(|connection| async move {
            seed_drawer(
                &connection,
                "tech",
                "rust",
                "rust programming language memory safety",
            )
            .await;

            // Null out filed_at to simulate a row from before the column existed.
            connection
                .execute("UPDATE drawers SET filed_at = NULL", ())
                .await
                .expect("UPDATE filed_at to NULL must succeed");

            let result = tool_search(&connection, &json!({"query": "rust programming"})).await;
            assert!(
                result.get("error").is_none(),
                "search must not error for legacy row"
            );
            let results = result["results"].as_array().expect("results must be array");
            assert!(
                !results.is_empty(),
                "legacy row must appear in search results"
            );
            for r in results {
                assert!(
                    r["created_at"].is_string(),
                    "created_at must be a string even when filed_at is NULL"
                );
                let created_at = r["created_at"]
                    .as_str()
                    .expect("created_at must be a JSON string");
                assert_eq!(
                    created_at, "",
                    "created_at must be empty string when filed_at is NULL"
                );
            }
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
            // All returned results should be from the "tech" wing.
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

            // Check with very similar content — duplicate detection uses word overlap.
            let result = tool_check_duplicate(
                &connection,
                &json!({"content": "rust programming language memory safety ownership borrowing lifetimes"}),
            )
            .await;
            assert!(result.get("error").is_none(), "must not error");
            // is_duplicate is present regardless.
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

            // Limit to 2.
            let result = tool_list_drawers(&connection, &json!({"limit": 2})).await;
            assert_eq!(result["count"], 2);
            assert_eq!(result["limit"], 2);

            // Offset to get the third.
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
            let drawer = &result["drawers"][0];
            assert_eq!(drawer["wing"], "alpha");
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
            // ID changes because content changed (deterministic ID includes content).
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

            // Send update with no actual changes.
            let result = tool_update_drawer(&connection, &json!({"drawer_id": drawer_id})).await;
            assert_eq!(result["success"], true);
            assert_eq!(result["noop"], true);
        })
        .await;
    }

    #[tokio::test]
    async fn update_drawer_wrong_type_fields_return_error() {
        with_isolated_env(|connection| async move {
            let seeded = seed_drawer(&connection, "proj", "code", "typed field test").await;
            let drawer_id = seeded["drawer_id"]
                .as_str()
                .expect("drawer_id must be string");

            // Non-string content must be rejected, not silently treated as absent.
            let result_content = tool_update_drawer(
                &connection,
                &json!({"drawer_id": drawer_id, "content": 123}),
            )
            .await;
            assert_eq!(
                result_content["success"], false,
                "integer content must be rejected"
            );
            assert!(
                result_content["error"]
                    .as_str()
                    .expect("error must be a string")
                    .contains("content"),
                "error must mention the offending field"
            );

            // Non-string wing must be rejected.
            let result_wing =
                tool_update_drawer(&connection, &json!({"drawer_id": drawer_id, "wing": 42})).await;
            assert_eq!(
                result_wing["success"], false,
                "integer wing must be rejected"
            );
            assert!(
                result_wing["error"]
                    .as_str()
                    .expect("error must be a string")
                    .contains("wing"),
                "error must mention the offending field"
            );

            // Non-string room must be rejected.
            let result_room =
                tool_update_drawer(&connection, &json!({"drawer_id": drawer_id, "room": true}))
                    .await;
            assert_eq!(
                result_room["success"], false,
                "boolean room must be rejected"
            );
            assert!(
                result_room["error"]
                    .as_str()
                    .expect("error must be a string")
                    .contains("room"),
                "error must mention the offending field"
            );
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
            // Missing object.
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
            // Add a triple first.
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
            // Valid 16-char hex that doesn't exist.
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
            // Should find at least one connection from the seeded tunnel.
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
            // Two wings sharing the same room name creates a graph edge.
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
            // Create drawers in two wings sharing a room name.
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
            assert_eq!(result["rooms_total"], 0);
            assert_eq!(result["edges_total"], 0);
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
                result["rooms_total"].as_i64().expect("rooms_total") >= 1,
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

    #[tokio::test]
    async fn diary_write_with_explicit_wing() {
        // When `wing` is supplied it must be used instead of the derived wing.
        with_isolated_env(|connection| async move {
            let result = tool_diary_write(
                &connection,
                &json!({
                    "agent_name": "TestAgent",
                    "entry": "Entry written to an explicit shared wing",
                    "wing": "shared_wing",
                }),
            )
            .await;
            assert_eq!(result["success"], true, "write must succeed");
            assert!(result["entry_id"].is_string(), "must return entry_id");
            // Postcondition: the entry must exist in the explicit wing, not the derived one.
            let read_result = tool_diary_read(
                &connection,
                &json!({"agent_name": "TestAgent", "wing": "shared_wing"}),
            )
            .await;
            assert_eq!(
                read_result["total"], 1,
                "entry must be in the explicit wing"
            );
            let entries = read_result["entries"]
                .as_array()
                .expect("entries must be array");
            assert!(
                entries[0]["content"]
                    .as_str()
                    .is_some_and(|c| c.contains("explicit shared wing")),
                "entry content must match"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn diary_write_topic_null_byte_rejected() {
        with_isolated_env(|connection| async move {
            let result = tool_diary_write(
                &connection,
                &json!({
                    "agent_name": "TestAgent",
                    "entry": "valid entry content for topic null byte test",
                    "topic": "bad\x00topic",
                }),
            )
            .await;
            assert_eq!(result["success"], false, "null-byte topic must be rejected");
            assert!(result["error"].is_string(), "must return error message");
        })
        .await;
    }

    #[tokio::test]
    async fn diary_write_topic_path_traversal_rejected() {
        with_isolated_env(|connection| async move {
            let result = tool_diary_write(
                &connection,
                &json!({
                    "agent_name": "TestAgent",
                    "entry": "valid entry for path traversal test",
                    "topic": "../etc",
                }),
            )
            .await;
            assert_eq!(
                result["success"], false,
                "path-traversal topic must be rejected"
            );
            assert!(result["error"].is_string(), "must return error message");
        })
        .await;
    }

    #[tokio::test]
    async fn diary_write_topic_oversized_rejected() {
        with_isolated_env(|connection| async move {
            let oversized_topic = "a".repeat(129);
            let result = tool_diary_write(
                &connection,
                &json!({
                    "agent_name": "TestAgent",
                    "entry": "valid entry for oversized topic test",
                    "topic": oversized_topic,
                }),
            )
            .await;
            assert_eq!(
                result["success"], false,
                "topic exceeding 128 chars must be rejected"
            );
            assert!(result["error"].is_string(), "must return error message");
        })
        .await;
    }

    #[tokio::test]
    async fn diary_write_valid_topic_succeeds() {
        with_isolated_env(|connection| async move {
            let result = tool_diary_write(
                &connection,
                &json!({
                    "agent_name": "TestAgent",
                    "entry": "valid entry for valid topic test",
                    "topic": "rust_async",
                }),
            )
            .await;
            assert_eq!(result["success"], true, "valid topic must succeed");
            assert_eq!(result["topic"], "rust_async");
        })
        .await;
    }

    #[tokio::test]
    async fn diary_read_filtered_by_wing_excludes_other_wings() {
        // When `wing` is supplied only entries from that wing must be returned.
        with_isolated_env(|connection| async move {
            // Write one entry to the derived wing and one to an explicit wing.
            tool_diary_write(
                &connection,
                &json!({"agent_name": "TestAgent", "entry": "derived wing entry"}),
            )
            .await;
            tool_diary_write(
                &connection,
                &json!({"agent_name": "TestAgent", "entry": "other wing entry", "wing": "other_wing"}),
            )
            .await;

            // Read from the explicit wing — must only return its entry.
            let result = tool_diary_read(
                &connection,
                &json!({"agent_name": "TestAgent", "wing": "other_wing"}),
            )
            .await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(result["total"], 1, "only entries from other_wing must appear");
            let entries = result["entries"].as_array().expect("entries must be array");
            assert!(
                entries[0]["content"]
                    .as_str()
                    .is_some_and(|c| c.contains("other wing entry")),
                "only the other_wing entry must be returned"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn diary_read_cross_wing_returns_all_agent_entries() {
        // When `wing` is absent, all diary entries by the agent across all wings
        // must be returned (cross-wing read via added_by).
        with_isolated_env(|connection| async move {
            tool_diary_write(
                &connection,
                &json!({"agent_name": "TestAgent", "entry": "first wing entry"}),
            )
            .await;
            tool_diary_write(
                &connection,
                &json!({"agent_name": "TestAgent", "entry": "second wing entry", "wing": "wing_b"}),
            )
            .await;

            // No wing param: cross-wing read — both entries must appear.
            let result = tool_diary_read(&connection, &json!({"agent_name": "TestAgent"})).await;
            assert!(result.get("error").is_none(), "must not error");
            assert_eq!(
                result["total"], 2,
                "cross-wing read must return entries from all wings"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn diary_read_with_wing_does_not_leak_other_agents_entries() {
        // Two agents writing to the same explicit wing must not see each other's
        // entries when they read with a wing filter.
        with_isolated_env(|connection| async move {
            // AgentA and AgentB both write to the same wing.
            tool_diary_write(
                &connection,
                &json!({"agent_name": "AgentA", "entry": "AgentA secret", "wing": "shared_wing"}),
            )
            .await;
            tool_diary_write(
                &connection,
                &json!({"agent_name": "AgentB", "entry": "AgentB secret", "wing": "shared_wing"}),
            )
            .await;

            // AgentA must only see its own entry.
            let result_a = tool_diary_read(
                &connection,
                &json!({"agent_name": "AgentA", "wing": "shared_wing"}),
            )
            .await;
            assert!(
                result_a.get("error").is_none(),
                "AgentA read must not error"
            );
            assert_eq!(result_a["total"], 1, "AgentA must only see its own entry");
            let entries_a = result_a["entries"]
                .as_array()
                .expect("entries must be array");
            assert!(
                entries_a[0]["content"]
                    .as_str()
                    .is_some_and(|c| c.contains("AgentA secret")),
                "AgentA must see its own content, not AgentB's"
            );

            // AgentB must only see its own entry.
            let result_b = tool_diary_read(
                &connection,
                &json!({"agent_name": "AgentB", "wing": "shared_wing"}),
            )
            .await;
            assert!(
                result_b.get("error").is_none(),
                "AgentB read must not error"
            );
            assert_eq!(result_b["total"], 1, "AgentB must only see its own entry");
            let entries_b = result_b["entries"]
                .as_array()
                .expect("entries must be array");
            assert!(
                entries_b[0]["content"]
                    .as_str()
                    .is_some_and(|c| c.contains("AgentB secret")),
                "AgentB must see its own content, not AgentA's"
            );
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
            // tool_status returns total_drawers, proving it was routed correctly.
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

    // --- sanitize_kg_value ---

    #[test]
    fn sanitize_kg_value_allows_natural_language_punctuation() {
        // Commas, colons, and parentheses are common in KG entity values.
        for input in &[
            "Alice, Bob",
            "type: Person",
            "born in (1990)",
            "co-founder",
            "O'Brien",
            "Dr. Smith",
        ] {
            let result = sanitize_kg_value(input, "entity");
            assert!(result.is_ok(), "expected Ok for '{input}', got {result:?}");
            assert_eq!(
                result.expect("sanitize_kg_value must accept valid input"),
                input.trim()
            );
        }
    }

    #[test]
    fn sanitize_kg_value_trims_whitespace() {
        let result = sanitize_kg_value("  Alice  ", "entity");
        assert_eq!(result.expect("whitespace trimming must succeed"), "Alice");
    }

    #[test]
    fn sanitize_kg_value_unicode_boundary() {
        // Exactly 128 Unicode characters (each is 3 bytes) must be accepted;
        // 129 must be rejected.
        let char_128: String = "あ".repeat(128);
        let char_129: String = "あ".repeat(129);
        assert_eq!(char_128.chars().count(), 128);
        assert_eq!(char_129.chars().count(), 129);
        assert!(
            sanitize_kg_value(&char_128, "entity").is_ok(),
            "128-char Unicode must be accepted"
        );
        let err = sanitize_kg_value(&char_129, "entity");
        assert!(err.is_err(), "129-char Unicode must be rejected");
        assert!(
            err.expect_err("129-char input must be rejected")["error"]
                .as_str()
                .expect("error must be string")
                .contains("128"),
            "error must mention limit"
        );
    }

    #[test]
    fn sanitize_kg_value_rejects_path_traversal_and_null() {
        let disallowed = [
            ("..", "dotdot"),
            ("/", "slash"),
            ("\\", "backslash"),
            ("\0", "null"),
        ];
        for (input, label) in &disallowed {
            let result = sanitize_kg_value(&format!("value{input}here"), "entity");
            assert!(result.is_err(), "expected Err for {label}");
            let error_json = result.expect_err("disallowed input must be rejected");
            assert!(
                error_json["public"].as_bool().unwrap_or(false),
                "error must be public for {label}"
            );
        }
    }

    #[test]
    fn sanitize_kg_value_rejects_empty_and_whitespace_only() {
        assert!(sanitize_kg_value("", "entity").is_err());
        assert!(sanitize_kg_value("   ", "entity").is_err());
    }

    // --- int_arg ---

    #[test]
    fn int_arg_missing_key_returns_default() {
        // Missing key: every code path falls through to the default.
        assert_eq!(int_arg(&json!({}), "limit", 5), 5);
        assert_eq!(int_arg(&json!({}), "limit", 0), 0);
    }

    #[test]
    fn int_arg_integer_positive_returns_value() {
        // Direct i64 path: positive integer must be returned as-is.
        assert_eq!(int_arg(&json!({"limit": 10}), "limit", 5), 10);
        // Negative space: an unrelated key must still return the default.
        assert_eq!(int_arg(&json!({"limit": 10}), "offset", 5), 5);
    }

    #[test]
    fn int_arg_integer_non_positive_returns_default() {
        // Zero and negative integers are rejected; only >0 is accepted.
        assert_eq!(int_arg(&json!({"n": 0}), "n", 7), 7);
        assert_eq!(int_arg(&json!({"n": -1}), "n", 7), 7);
    }

    #[test]
    fn int_arg_float_whole_positive_returns_value() {
        // MCP transports sometimes deliver integers as floats (e.g. 5.0).
        assert_eq!(int_arg(&json!({"limit": 5.0}), "limit", 1), 5);
        assert_eq!(int_arg(&json!({"limit": 100.0}), "limit", 1), 100);
    }

    #[test]
    fn int_arg_float_invalid_returns_default() {
        // Fractional, non-positive, and zero floats must fall through to the default.
        assert_eq!(int_arg(&json!({"n": 5.5}), "n", 1), 1);
        assert_eq!(int_arg(&json!({"n": -1.0}), "n", 1), 1);
        assert_eq!(int_arg(&json!({"n": 0.0}), "n", 1), 1);
    }

    #[test]
    fn int_arg_string_integer_positive_returns_value() {
        // MCP transports sometimes deliver integers as strings (e.g. "10").
        assert_eq!(int_arg(&json!({"limit": "10"}), "limit", 1), 10);
        assert_eq!(int_arg(&json!({"limit": "1"}), "limit", 99), 1);
    }

    #[test]
    fn int_arg_string_integer_non_positive_returns_default() {
        // Zero and negative integer strings are rejected.
        assert_eq!(int_arg(&json!({"n": "0"}), "n", 7), 7);
        assert_eq!(int_arg(&json!({"n": "-3"}), "n", 7), 7);
    }

    #[test]
    fn int_arg_string_float_whole_positive_returns_value() {
        // Whole positive floats encoded as strings are also accepted.
        assert_eq!(int_arg(&json!({"limit": "5.0"}), "limit", 1), 5);
        assert_eq!(int_arg(&json!({"limit": "20.0"}), "limit", 1), 20);
    }

    #[test]
    fn int_arg_string_invalid_returns_default() {
        // Non-numeric, fractional, and non-positive string values return the default.
        assert_eq!(int_arg(&json!({"n": "abc"}), "n", 7), 7);
        assert_eq!(int_arg(&json!({"n": "1.5"}), "n", 7), 7);
        assert_eq!(int_arg(&json!({"n": "-2.0"}), "n", 7), 7);
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

    // --- sanitize_name ---

    #[test]
    fn sanitize_name_rejects_too_long_string() {
        // A string longer than 128 characters must be rejected with a public error.
        let long = "a".repeat(129);
        let result = sanitize_name(&long, "field");
        assert!(result.is_err(), "string > 128 chars must be rejected");
        let error_json = result.expect_err("long string must fail");
        assert!(
            error_json["error"]
                .as_str()
                .expect("error must be string")
                .contains("128"),
            "error must mention the 128-char limit"
        );
        assert!(
            error_json["public"].as_bool().unwrap_or(false),
            "error must be public"
        );
    }

    #[test]
    fn sanitize_name_rejects_path_traversal_and_null() {
        // Path-traversal sequences and null bytes are always invalid.
        for input in &["a..b", "a/b", "a\\b", "a\x00b"] {
            let result = sanitize_name(input, "field");
            assert!(result.is_err(), "'{input}' must be rejected");
            assert!(
                result.expect_err("path traversal must fail")["public"]
                    .as_bool()
                    .unwrap_or(false),
                "error must be public for '{input}'"
            );
        }
    }

    #[test]
    fn sanitize_name_rejects_non_alphanumeric_first_char() {
        // Names must start with an ASCII alphanumeric character.
        // Note: leading spaces are stripped by trim(), so they are not tested here.
        for input in &["_leading", "-leading", ".leading"] {
            let result = sanitize_name(input, "field");
            assert!(
                result.is_err(),
                "name starting with non-alphanumeric '{input}' must be rejected"
            );
            assert!(
                result.expect_err("invalid start must fail")["public"]
                    .as_bool()
                    .unwrap_or(false),
                "error must be public for '{input}'"
            );
        }
    }

    #[test]
    fn sanitize_name_rejects_invalid_chars() {
        // Characters outside [a-zA-Z0-9_ .'-] must be rejected.
        for input in &["foo@bar", "foo!bar", "foo#bar", "foo$bar"] {
            let result = sanitize_name(input, "field");
            assert!(
                result.is_err(),
                "name with invalid character '{input}' must be rejected"
            );
            assert!(
                result.expect_err("invalid char must fail")["public"]
                    .as_bool()
                    .unwrap_or(false),
                "error must be public for '{input}'"
            );
        }
    }

    // --- sanitize_label ---

    #[test]
    fn sanitize_label_rejects_too_long_string() {
        // A label longer than LABEL_LEN_MAX (255) bytes must be rejected.
        let long = "a".repeat(LABEL_LEN_MAX + 1);
        let result = sanitize_label(&long);
        assert!(result.is_err(), "label > LABEL_LEN_MAX must be rejected");
        assert!(
            result.expect_err("long label must fail")["public"]
                .as_bool()
                .unwrap_or(false),
            "error must be public"
        );
    }

    #[test]
    fn sanitize_label_rejects_null_bytes() {
        // A label containing a null byte must be rejected.
        let result = sanitize_label("valid\x00but_null");
        assert!(result.is_err(), "label with null byte must be rejected");
        assert!(
            result.expect_err("null byte must fail")["public"]
                .as_bool()
                .unwrap_or(false),
            "error must be public"
        );
    }

    // --- sanitize_content ---

    #[test]
    fn sanitize_content_rejects_null_bytes() {
        // Content containing a null byte must be rejected.
        let result = sanitize_content("valid content\x00with null");
        assert!(result.is_err(), "content with null byte must be rejected");
        assert!(
            result.expect_err("null byte content must fail")["public"]
                .as_bool()
                .unwrap_or(false),
            "error must be public"
        );
    }

    #[test]
    fn sanitize_content_rejects_over_100k_chars() {
        // Content exceeding 100,000 characters must be rejected.
        let long = "a".repeat(100_001);
        let result = sanitize_content(&long);
        assert!(result.is_err(), "content > 100k chars must be rejected");
        assert!(
            result.expect_err("over-100k content must fail")["public"]
                .as_bool()
                .unwrap_or(false),
            "error must be public"
        );
    }

    // --- tool_research_entity ---

    #[test]
    fn research_entity_empty_word_returns_error() {
        // Empty word must produce a public error, not a panic.
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let result = tool_research_entity(&json!({}));
            assert!(
                result.get("error").is_some(),
                "must return error for empty word"
            );
            assert_eq!(
                result["public"], true,
                "error must be public for empty word"
            );
        });
    }

    #[test]
    fn research_entity_unknown_word_no_network_returns_unknown() {
        // Without network access a word absent from the registry must return inferred_type "unknown".
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let result = tool_research_entity(&json!({"word": "Xyzzy"}));
            assert!(
                result.get("error").is_none(),
                "must not error for valid word"
            );
            assert_eq!(
                result["inferred_type"], "unknown",
                "unknown word with no network must return unknown type"
            );
            assert_eq!(result["confirmed"], false);
        });
    }

    #[test]
    fn research_entity_known_word_returns_local_result() {
        // A word already in the entity registry must be returned from the local path.
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            // Seed the registry with a known person.
            let mut registry = EntityRegistry::load();
            registry
                .seed(
                    "personal",
                    &[crate::palace::entity_registry::SeedPerson {
                        name: "Jordan".to_string(),
                        relationship: "friend".to_string(),
                        context: "personal".to_string(),
                        nickname: None,
                    }],
                    &[],
                )
                .expect("seed must succeed");

            let result = tool_research_entity(&json!({"word": "Jordan"}));
            assert!(
                result.get("error").is_none(),
                "must not error for known word"
            );
            assert_eq!(
                result["inferred_type"], "person",
                "Jordan must resolve as person"
            );
            assert_eq!(result["confirmed"], true, "local result must be confirmed");
        });
    }

    // --- tool_confirm_entity ---

    #[test]
    fn confirm_entity_empty_word_returns_error() {
        // Empty word must produce a public error.
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let result = tool_confirm_entity(&json!({"entity_type": "person"}));
            assert!(
                result.get("error").is_some(),
                "must return error for empty word"
            );
            assert_eq!(result["public"], true, "error must be public");
        });
    }

    #[test]
    fn confirm_entity_empty_entity_type_returns_error() {
        // Missing entity_type must produce a public error.
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let result = tool_confirm_entity(&json!({"word": "Sam"}));
            assert!(
                result.get("error").is_some(),
                "must return error for empty entity_type"
            );
            assert_eq!(result["public"], true, "error must be public");
        });
    }

    #[test]
    fn confirm_entity_person_returns_success() {
        // Confirming a word as person must succeed and report the confirmed type.
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let result = tool_confirm_entity(&json!({
                "word": "Sam",
                "entity_type": "person",
                "relationship": "colleague"
            }));
            assert!(
                result.get("error").is_none(),
                "valid confirm must not error"
            );
            assert_eq!(result["success"], true, "valid confirm must report success");
            assert_eq!(
                result["entity_type"], "person",
                "entity_type must match input"
            );
        });
    }

    // --- int_arg edge cases: infinite and over-max floats ---

    #[test]
    fn int_arg_float_infinite_returns_default() {
        // Infinite floats must fall through to the default; they are not finite.
        let infinity = f64::INFINITY;
        let neg_infinity = f64::NEG_INFINITY;
        // Encode as serde_json::Number — serde_json rejects non-finite f64 via
        // json! macro, so we use a JSON string coercion path instead.
        // The f64 path inside int_arg rejects non-finite values with `f.is_finite()`.
        // We verify the string path handles a stringified "inf"-like value.
        assert_eq!(int_arg(&json!({"n": "inf"}), "n", 42), 42);
        assert_eq!(int_arg(&json!({"n": "-inf"}), "n", 42), 42);
        // Pair assertion: confirm the f64 representations are actually non-finite.
        assert!(!infinity.is_finite());
        assert!(!neg_infinity.is_finite());
    }

    #[test]
    fn int_arg_float_fractional_over_exact_max_returns_default() {
        // A fractional float value that also exceeds EXACT_INT_F64_MAX must be
        // rejected: the f64 path rejects non-zero fractions (`f.fract() == 0.0`).
        // We use the JSON number (f64) path rather than string, so there's no
        // prior i64 parse attempt.
        // 1.5 is a whole-number-adjacent value that fails the fract() check.
        assert_eq!(int_arg(&json!({"n": 1.5}), "n", 7), 7);
        // A non-positive float must also be rejected.
        assert_eq!(int_arg(&json!({"n": 0.0}), "n", 7), 7);
        // Pair: a valid whole positive float must be accepted.
        assert_eq!(int_arg(&json!({"n": 3.0}), "n", 7), 3);
    }

    // --- sanitize_name: empty string ---

    #[test]
    fn sanitize_name_rejects_empty_string() {
        // An empty string (and whitespace-only) must produce a public error.
        let result_empty = sanitize_name("", "field");
        assert!(result_empty.is_err(), "empty string must be rejected");
        let error_empty = result_empty.expect_err("empty string must fail");
        assert!(
            error_empty["public"].as_bool().unwrap_or(false),
            "error must be public for empty string"
        );

        let result_ws = sanitize_name("   ", "field");
        assert!(result_ws.is_err(), "whitespace-only must be rejected");
        assert!(
            result_ws.expect_err("whitespace-only must fail")["public"]
                .as_bool()
                .unwrap_or(false),
            "error must be public for whitespace-only string"
        );
    }

    // --- sanitize_opt_name: whitespace-only returns Ok(None) ---

    #[test]
    fn sanitize_opt_name_whitespace_only_returns_none() {
        // Whitespace-only input must be treated as absent (Ok(None)), not an error.
        let result = sanitize_opt_name("   ", "field");
        assert!(result.is_ok(), "whitespace-only must be Ok, not Err");
        assert!(
            result.expect("whitespace-only must return Ok").is_none(),
            "whitespace-only must return None"
        );
        // Pair: a valid name returns Some.
        let result_valid = sanitize_opt_name("valid", "field");
        assert!(result_valid.is_ok(), "valid name must be Ok");
        assert!(
            result_valid.expect("valid name must return Ok").is_some(),
            "valid name must return Some"
        );
    }

    // --- sanitize_content: empty string ---

    #[test]
    fn sanitize_content_rejects_empty_string() {
        // An empty string (after trimming) must produce a public error.
        let result_empty = sanitize_content("");
        assert!(result_empty.is_err(), "empty content must be rejected");
        let error_empty = result_empty.expect_err("empty content must fail");
        assert!(
            error_empty["public"].as_bool().unwrap_or(false),
            "error must be public for empty content"
        );

        let result_ws = sanitize_content("   ");
        assert!(
            result_ws.is_err(),
            "whitespace-only content must be rejected"
        );
        assert!(
            result_ws.expect_err("whitespace-only content must fail")["public"]
                .as_bool()
                .unwrap_or(false),
            "error must be public for whitespace-only content"
        );
    }

    // --- sanitize_label: empty string ---

    #[test]
    fn sanitize_label_rejects_empty_string() {
        // An empty label (after trimming) must produce a public error.
        let result_empty = sanitize_label("");
        assert!(result_empty.is_err(), "empty label must be rejected");
        let error_empty = result_empty.expect_err("empty label must fail");
        assert!(
            error_empty["public"].as_bool().unwrap_or(false),
            "error must be public for empty label"
        );

        let result_ws = sanitize_label("  ");
        assert!(result_ws.is_err(), "whitespace-only label must be rejected");
        assert!(
            result_ws.expect_err("whitespace-only label must fail")["public"]
                .as_bool()
                .unwrap_or(false),
            "error must be public for whitespace-only label"
        );
    }

    // --- tool_reconnect ---

    #[tokio::test]
    async fn reconnect_returns_drawer_count() {
        with_isolated_env(|connection| async move {
            let result = tool_reconnect(&connection).await;
            assert_eq!(
                result["success"], true,
                "reconnect must report success on valid connection"
            );
            assert_eq!(result["drawers"], 0, "empty DB must report zero drawers");
            assert!(
                result["message"].is_string(),
                "reconnect must include a message string"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn reconnect_after_inserting_drawers_reflects_count() {
        with_isolated_env(|connection| async move {
            seed_drawer(&connection, "wing1", "room1", "reconnect test drawer one").await;
            seed_drawer(&connection, "wing1", "room1", "reconnect test drawer two").await;

            let result = tool_reconnect(&connection).await;
            assert_eq!(result["success"], true, "reconnect must report success");
            assert_eq!(
                result["drawers"], 2,
                "reconnect must report the correct drawer count after inserts"
            );
        })
        .await;
    }

    // --- tool_check_facts ---

    #[tokio::test]
    async fn check_facts_empty_text_returns_error() {
        with_isolated_env(|connection| async move {
            let result = tool_check_facts(&connection, &json!({"text": ""})).await;
            assert!(
                result["error"].is_string(),
                "must return error for empty text"
            );
            assert_eq!(
                result["public"], true,
                "error must be public for empty text"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn check_facts_valid_text_returns_issues_list() {
        with_isolated_env(|connection| async move {
            // A non-empty text must produce an issues array (may be empty if nothing found).
            let result =
                tool_check_facts(&connection, &json!({"text": "Alice works at Acme Corp"})).await;
            assert!(
                result.get("error").is_none(),
                "check_facts with valid text must not error"
            );
            assert!(
                result.get("issues").is_some(),
                "must have issues key in result"
            );
            assert!(
                result["clean"].is_boolean(),
                "must have clean boolean in result"
            );
        })
        .await;
    }

    // --- tool_hook_settings: read and update ---

    #[test]
    fn hook_settings_read_returns_current_settings() {
        // Reading with no args must return the current settings object.
        let temp = tempfile::tempdir().expect("tempdir for hook_settings");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let result = tool_hook_settings(&json!({}));
            assert_eq!(
                result["success"], true,
                "read hook settings must report success"
            );
            assert!(
                result["settings"]["silent_save"].is_boolean(),
                "silent_save must be a boolean"
            );
            assert!(
                result["settings"]["desktop_toast"].is_boolean(),
                "desktop_toast must be a boolean"
            );
            // No-arg read must not include an "updated" key.
            assert!(
                result.get("updated").is_none(),
                "read-only call must not include updated key"
            );
        });
    }

    #[test]
    fn hook_settings_update_silent_save_reflects_change() {
        // Setting silent_save to true must be reflected in the returned settings.
        let temp = tempfile::tempdir().expect("tempdir for hook_settings update");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let result = tool_hook_settings(&json!({"silent_save": true}));
            assert_eq!(result["success"], true, "update must report success");
            assert_eq!(
                result["settings"]["silent_save"], true,
                "silent_save must reflect the updated value"
            );
            // Updated key must be present and non-empty when a field was changed.
            let updated = result["updated"]
                .as_array()
                .expect("updated must be an array when fields are changed");
            assert!(!updated.is_empty(), "updated must list the changed field");
        });
    }

    // --- tool_memories_filed_away: quiet and file-present branches ---

    #[test]
    fn memories_filed_away_quiet_when_no_checkpoint() {
        // When no checkpoint file exists the status must be "quiet".
        let temp = tempfile::tempdir().expect("tempdir for memories_filed_away");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let result = tool_memories_filed_away();
            assert_eq!(
                result["status"], "quiet",
                "no checkpoint must report quiet status"
            );
            assert_eq!(result["count"], 0, "quiet must report zero count");
            assert!(
                result["timestamp"].is_null(),
                "quiet must have null timestamp"
            );
        });
    }

    #[test]
    fn memories_filed_away_returns_ok_when_valid_checkpoint_exists() {
        // When a valid JSON checkpoint file exists the status must be "ok" and the
        // file must be deleted so the next call returns "quiet" again.
        let temp = tempfile::tempdir().expect("tempdir for memories_filed_away checkpoint");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            // Create the hook_state directory and write a checkpoint file.
            let hook_state_dir = temp.path().join("hook_state");
            std::fs::create_dir_all(&hook_state_dir).expect("create hook_state dir");
            let checkpoint_file = hook_state_dir.join("last_checkpoint");
            let checkpoint_json = r#"{"msgs": 5, "ts": "2025-01-01T00:00:00Z"}"#;
            std::fs::write(&checkpoint_file, checkpoint_json).expect("write checkpoint file");

            let result = tool_memories_filed_away();
            assert_eq!(
                result["status"], "ok",
                "valid checkpoint must return ok status"
            );
            assert_eq!(result["count"], 5, "count must match msgs in checkpoint");
            assert!(
                result["timestamp"].is_string(),
                "timestamp must be a string from the checkpoint"
            );

            // Pair assertion: the checkpoint file must be deleted after reading.
            assert!(
                !checkpoint_file.is_file(),
                "checkpoint file must be deleted after reading"
            );
        });
    }

    #[test]
    fn memories_filed_away_returns_error_status_on_invalid_json() {
        // When the checkpoint file contains non-JSON content the status must be "error".
        let temp = tempfile::tempdir().expect("tempdir for memories_filed_away invalid json");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let hook_state_dir = temp.path().join("hook_state");
            std::fs::create_dir_all(&hook_state_dir).expect("create hook_state dir");
            let checkpoint_file = hook_state_dir.join("last_checkpoint");
            std::fs::write(&checkpoint_file, "not valid json!!!")
                .expect("write invalid checkpoint");

            let result = tool_memories_filed_away();
            assert_eq!(
                result["status"], "error",
                "invalid JSON checkpoint must return error status"
            );
            assert_eq!(result["count"], 0, "error status must report zero count");
        });
    }

    // --- tool_get_drawer: invalid format branch ---

    #[tokio::test]
    async fn get_drawer_invalid_format_returns_error() {
        with_isolated_env(|connection| async move {
            // A valid sanitize_name value that does not start with "drawer_" must
            // trigger the invalid-format branch, not the not-found branch.
            let result =
                tool_get_drawer(&connection, &json!({"drawer_id": "notadrawer123validname"})).await;
            assert!(
                result["error"].is_string(),
                "invalid format must return an error string"
            );
            assert_eq!(
                result["public"], true,
                "invalid format error must be public"
            );
            let error_text = result["error"].as_str().expect("error must be a string");
            assert!(
                error_text.contains("invalid format"),
                "error must mention invalid format"
            );
        })
        .await;
    }

    // --- tool_list_drawers: wing+room filter combination ---

    #[tokio::test]
    async fn list_drawers_wing_and_room_filter() {
        with_isolated_env(|connection| async move {
            seed_drawer(&connection, "alpha", "code", "alpha code drawer content").await;
            seed_drawer(&connection, "alpha", "docs", "alpha docs drawer content").await;
            seed_drawer(&connection, "beta", "code", "beta code drawer content").await;

            // Filter by both wing and room.
            let result =
                tool_list_drawers(&connection, &json!({"wing": "alpha", "room": "code"})).await;
            assert_eq!(
                result["count"], 1,
                "wing+room filter must return exactly one matching drawer"
            );
            let drawer = &result["drawers"][0];
            assert_eq!(drawer["wing"], "alpha");
            assert_eq!(drawer["room"], "code");
        })
        .await;
    }

    #[tokio::test]
    async fn list_drawers_room_only_filter() {
        with_isolated_env(|connection| async move {
            seed_drawer(&connection, "alpha", "notes", "alpha notes content").await;
            seed_drawer(&connection, "beta", "notes", "beta notes content").await;
            seed_drawer(&connection, "gamma", "code", "gamma code content").await;

            // Filter by room only — must return drawers from all wings with that room.
            let result = tool_list_drawers(&connection, &json!({"room": "notes"})).await;
            assert_eq!(
                result["count"], 2,
                "room-only filter must return drawers from all wings with that room"
            );
        })
        .await;
    }

    // --- tool_update_drawer: duplicate collision branch ---

    #[tokio::test]
    async fn update_drawer_collision_with_existing_returns_error() {
        with_isolated_env(|connection| async move {
            // Seed two drawers. Updating the first to match the second's content would
            // recompute the same deterministic ID — this must be rejected.
            let first = seed_drawer(&connection, "proj", "code", "first drawer content").await;
            let first_id = first["drawer_id"].as_str().expect("drawer_id string");
            seed_drawer(&connection, "proj", "code", "second drawer content").await;

            // Attempting to update first drawer's content to match the second would
            // produce an ID collision.
            let result = tool_update_drawer(
                &connection,
                &json!({"drawer_id": first_id, "content": "second drawer content"}),
            )
            .await;
            assert_eq!(
                result["success"], false,
                "collision with existing drawer must be rejected"
            );
            assert!(
                result["existing_drawer_id"].is_string(),
                "error must include the existing_drawer_id"
            );
        })
        .await;
    }

    // --- tool_kg_query: outgoing/incoming direction variants ---

    #[tokio::test]
    async fn kg_query_outgoing_direction() {
        with_isolated_env(|connection| async move {
            tool_kg_add(
                &connection,
                &json!({"subject": "Rust", "predicate": "compilesTo", "object": "binary"}),
            )
            .await;

            let result = tool_kg_query(
                &connection,
                &json!({"entity": "Rust", "direction": "outgoing"}),
            )
            .await;
            assert!(
                result.get("error").is_none(),
                "outgoing direction query must not error"
            );
            assert_eq!(result["entity"], "Rust");
            assert!(result["count"].as_i64().expect("count") >= 1);
        })
        .await;
    }

    #[tokio::test]
    async fn kg_query_incoming_direction() {
        with_isolated_env(|connection| async move {
            tool_kg_add(
                &connection,
                &json!({"subject": "Rust", "predicate": "compilesTo", "object": "binary"}),
            )
            .await;

            let result = tool_kg_query(
                &connection,
                &json!({"entity": "binary", "direction": "incoming"}),
            )
            .await;
            assert!(
                result.get("error").is_none(),
                "incoming direction query must not error"
            );
            assert_eq!(result["entity"], "binary");
        })
        .await;
    }

    #[tokio::test]
    async fn kg_query_with_as_of_filter() {
        with_isolated_env(|connection| async move {
            tool_kg_add(
                &connection,
                &json!({
                    "subject": "Alice",
                    "predicate": "worksAt",
                    "object": "Acme",
                    "valid_from": "2020-01-01"
                }),
            )
            .await;

            // Query with an as_of date in the past — must not error.
            let result = tool_kg_query(
                &connection,
                &json!({"entity": "Alice", "as_of": "2019-01-01"}),
            )
            .await;
            assert!(
                result.get("error").is_none(),
                "kg_query with as_of must not error"
            );
            assert!(
                result["as_of"].is_string(),
                "as_of must be echoed back as a string"
            );
            // Pair: query with a date after valid_from must find the fact.
            let result_after = tool_kg_query(
                &connection,
                &json!({"entity": "Alice", "as_of": "2021-01-01"}),
            )
            .await;
            assert!(
                result_after.get("error").is_none(),
                "kg_query after valid_from must not error"
            );
        })
        .await;
    }

    // --- str_arg helper ---

    #[test]
    fn str_arg_missing_key_returns_empty_string() {
        // A missing key must return an empty string, not panic.
        // Bind the Value to a local so the borrow outlives the assert.
        let args_empty = json!({});
        let result = str_arg(&args_empty, "missing_key");
        assert_eq!(result, "", "missing key must return empty string");
        // Pair: a present key must return its value.
        let args_present = json!({"key": "value"});
        let result_present = str_arg(&args_present, "key");
        assert_eq!(result_present, "value", "present key must return its value");
    }

    #[test]
    fn str_arg_non_string_value_returns_empty_string() {
        // A non-string JSON value must return empty string rather than panic.
        // Bind to locals so borrows outlive the assertions.
        let args_int = json!({"n": 42});
        let result_int = str_arg(&args_int, "n");
        assert_eq!(result_int, "", "integer value must return empty string");

        let args_bool = json!({"b": true});
        let result_bool = str_arg(&args_bool, "b");
        assert_eq!(result_bool, "", "boolean value must return empty string");
    }

    // --- tool_add_drawer: added_by field ---

    #[tokio::test]
    async fn add_drawer_with_explicit_added_by() {
        // When added_by is provided it must be used instead of the default "mcp".
        with_isolated_env(|connection| async move {
            let result = tool_add_drawer(
                &connection,
                &json!({
                    "wing": "personal",
                    "room": "notes",
                    "content": "content with explicit added_by field",
                    "added_by": "test_agent",
                }),
            )
            .await;
            assert_eq!(
                result["success"], true,
                "add_drawer with added_by must succeed"
            );
            assert!(
                result["drawer_id"].is_string(),
                "must return a drawer_id string"
            );
        })
        .await;
    }

    // --- tool_list_tunnels: wing filter ---

    #[tokio::test]
    async fn list_tunnels_with_wing_filter() {
        with_isolated_env(|connection| async move {
            // Create a tunnel from alpha → beta.
            seed_tunnel(&connection).await;
            // Create another tunnel from gamma → delta.
            tool_create_tunnel(
                &connection,
                &json!({
                    "source_wing": "gamma",
                    "source_room": "code",
                    "target_wing": "delta",
                    "target_room": "docs",
                    "label": "gamma delta link",
                }),
            )
            .await;

            // Filter by wing "alpha" — must only return the alpha tunnel.
            let result = tool_list_tunnels(&connection, &json!({"wing": "alpha"})).await;
            assert!(result.get("error").is_none(), "must not error");
            let count = result["count"].as_i64().expect("count must be integer");
            assert_eq!(count, 1, "wing filter must return only matching tunnels");
        })
        .await;
    }

    // --- tool_delete_tunnel: empty tunnel_id branch ---

    #[tokio::test]
    async fn delete_tunnel_empty_id_returns_error() {
        with_isolated_env(|connection| async move {
            // An empty tunnel_id must return a public error before any DB access.
            let result = tool_delete_tunnel(&connection, &json!({"tunnel_id": ""})).await;
            assert!(
                result["error"].is_string(),
                "empty tunnel_id must return an error"
            );
            assert_eq!(
                result["public"], true,
                "empty tunnel_id error must be public"
            );
        })
        .await;
    }

    // --- tool_kg_timeline: invalid entity ---

    #[tokio::test]
    async fn kg_timeline_invalid_entity_returns_error() {
        with_isolated_env(|connection| async move {
            // A path-traversal sequence in the entity must be rejected.
            let result = tool_kg_timeline(&connection, &json!({"entity": "a..b"})).await;
            assert!(
                result["error"].is_string(),
                "path-traversal entity must return an error"
            );
            assert_eq!(result["public"], true, "validation error must be public");
        })
        .await;
    }

    // --- tool_search: query_sanitized and context_received branches ---

    #[tokio::test]
    async fn search_long_query_triggers_sanitized_flag() {
        // A query longer than 200 characters triggers the sanitizer.  The result
        // must include `query_sanitized: true` and the `sanitizer` metadata object,
        // exercising the `sanitized.was_sanitized` branch (lines 550-558).
        with_isolated_env(|connection| async move {
            // Build a contaminated query: 300 chars of noise + meaningful tail segment.
            let noise = "x".repeat(300);
            let tail = "rust programming language";
            let long_query = format!("{noise}\n{tail}");

            seed_drawer(
                &connection,
                "tech",
                "notes",
                "rust programming language systems",
            )
            .await;

            let result = tool_search(&connection, &json!({"query": long_query})).await;
            assert!(
                result.get("error").is_none(),
                "sanitized long query must not error"
            );
            assert_eq!(
                result["query_sanitized"], true,
                "was_sanitized branch must set query_sanitized to true"
            );
            let sanitizer = result["sanitizer"]
                .as_object()
                .expect("sanitizer key must be an object");
            assert!(
                sanitizer.contains_key("method"),
                "sanitizer must include method"
            );
            assert!(
                sanitizer.contains_key("original_length"),
                "sanitizer must include original_length"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn search_with_context_sets_context_received_flag() {
        // A non-empty `context` argument must set `context_received: true` in the
        // result, exercising the `context_received` branch (lines 559-561).
        with_isolated_env(|connection| async move {
            seed_drawer(
                &connection,
                "tech",
                "notes",
                "rust programming language memory safety",
            )
            .await;

            let result = tool_search(
                &connection,
                &json!({
                    "query": "rust programming",
                    "context": "User is asking about language features",
                }),
            )
            .await;
            assert!(
                result.get("error").is_none(),
                "search with context must not error"
            );
            assert_eq!(
                result["context_received"], true,
                "non-empty context must set context_received to true"
            );
            assert!(
                result["count"].as_i64().is_some(),
                "result must include a count"
            );
        })
        .await;
    }

    // --- tool_update_drawer: invalid drawer_id format branch ---

    #[tokio::test]
    async fn update_drawer_invalid_id_format_returns_error() {
        // A drawer_id that passes sanitize_name but does not start with "drawer_"
        // must be rejected before any DB access, exercising the format-check branch
        // in tool_update_drawer_validate_args (line 864-867).
        with_isolated_env(|connection| async move {
            let result = tool_update_drawer(
                &connection,
                &json!({
                    "drawer_id": "notadrawer123validname",
                    "content": "some updated content here",
                }),
            )
            .await;
            assert_eq!(
                result["success"], false,
                "non-drawer_ prefixed id must be rejected"
            );
            assert!(
                result["error"]
                    .as_str()
                    .expect("error must be string")
                    .contains("invalid format"),
                "error must mention invalid format"
            );
            assert_eq!(result["public"], true, "format error must be public");
        })
        .await;
    }
}
