//! Sweep ingester — message-granular miner that catches what the primary
//! file-level miners missed.
//!
//! Algorithm, per session:
//!
//!   `already_swept` = set of message UUIDs stored with `ingest_mode = "sweep"`
//!   For each user/assistant message in the .jsonl:
//!       if uuid in `already_swept`: count as already present, continue
//!       else: insert a drawer keyed by `"sweep_{session_id}_{uuid}"`
//!
//! Properties:
//!
//! - Idempotent: deterministic drawer IDs and `add_drawer`'s `INSERT OR
//!   IGNORE` make re-runs no-ops for already-stored messages.
//! - Resume-safe: a crash mid-sweep is recovered on the next run — UUID
//!   presence determines what to skip, so partial ingestion is completed
//!   automatically on rerun.
//! - Coordination with primary miners (`miner.rs`, `convo_miner.rs`) is
//!   limited: those miners chunk at a fixed char size without storing
//!   session/UUID metadata, so sweep drawers may overlap with primary-miner
//!   drawers under different IDs.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};
use turso::Connection;

use crate::error::Result;
use crate::palace::WALK_DEPTH_LIMIT;
use crate::palace::drawer::{self, DrawerParams};

/// Maximum messages parsed from a single JSONL file.
/// Guards against adversarially large inputs being loaded fully into memory.
const MESSAGES_MAX: usize = 1_000_000;

/// Maximum `.jsonl` files scanned in a single directory sweep.
const FILES_MAX: usize = 100_000;

// Compile-time invariants: limits must be positive.
const _: () = assert!(MESSAGES_MAX > 0);
const _: () = assert!(FILES_MAX > 0);

/// A parsed user or assistant message from a Claude Code `.jsonl` file.
struct SweepMessage {
    session_id: String,
    uuid: String,
    role: String,
    content: String,
}

/// Counts returned by a single-file sweep.
pub struct SweepResult {
    /// Drawers inserted that did not exist before this sweep.
    pub drawers_added: usize,
    /// Drawers already present (by UUID pre-check or INSERT OR IGNORE).
    pub drawers_already_present: usize,
}

/// Counts returned by a directory sweep.
pub struct SweepDirectoryResult {
    /// Total `.jsonl` files found.
    pub files_attempted: usize,
    /// Files that completed without error.
    pub files_succeeded: usize,
    /// Total new drawers inserted across all files.
    pub drawers_added: usize,
    /// Total drawers already present across all files.
    pub drawers_already_present: usize,
}

/// Render one assistant message content block as a plain string.
///
/// Called by `flatten_content` for each element of an array-typed message.
/// All block types are preserved verbatim — tool inputs and results must
/// not be silently discarded.
fn flatten_content_block(
    block_type: &str,
    block_map: &Map<String, Value>,
    block: &Value,
) -> String {
    assert!(
        block.is_object(),
        "flatten_content_block: block must be a JSON object"
    );
    let result = match block_type {
        "text" => block_map
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        "tool_use" => format!(
            "[tool_use: {} input={}]",
            block_map.get("name").and_then(Value::as_str).unwrap_or("?"),
            block_map
                .get("input")
                .map_or_else(|| "{}".to_string(), ToString::to_string),
        ),
        "tool_result" => format!(
            "[tool_result: {}]",
            block_map
                .get("content")
                .map_or_else(String::new, ToString::to_string),
        ),
        other => format!("[{other}: {block}]"),
    };
    // Negative space: result must not contain null bytes (would corrupt SQLite TEXT).
    debug_assert!(!result.contains('\0'), "result must not contain null bytes");
    result
}

/// Normalise Claude Code message content to a plain string.
///
/// User messages are plain strings; assistant messages are a list of content
/// blocks (`text`, `tool_use`, `tool_result`, or other).  All blocks are
/// preserved verbatim so nothing is silently discarded.
fn flatten_content(content: &Value) -> String {
    match content {
        Value::String(string_content) => string_content.clone(),
        Value::Array(blocks) => {
            // Pre-allocate one slot per block; many blocks may yield empty strings.
            let mut parts: Vec<String> = Vec::with_capacity(blocks.len());
            for block in blocks {
                let Some(block_map) = block.as_object() else {
                    continue;
                };
                let block_type = block_map.get("type").and_then(Value::as_str).unwrap_or("");
                let part = flatten_content_block(block_type, block_map, block);
                if !part.is_empty() {
                    parts.push(part);
                }
            }
            parts.join("\n")
        }
        other => other.to_string(),
    }
}

/// Parse one JSONL record into a `SweepMessage`, or `None` if not applicable.
///
/// Non-message record types (progress, file-history-snapshot, system,
/// queue-operation, last-prompt) are filtered by checking `"type"` against
/// `"user"` and `"assistant"`.  Malformed fields produce `None` — transcript
/// quality is the writer's responsibility.
fn parse_claude_jsonl_record(record: &Value) -> Option<SweepMessage> {
    assert!(record.is_object(), "record must be a JSON object");

    // Only user and assistant type records carry conversation content.
    let rtype = record.get("type")?.as_str()?;
    if rtype != "user" && rtype != "assistant" {
        return None;
    }

    let msg = record.get("message")?.as_object()?;
    let role = msg.get("role")?.as_str()?;
    if role != "user" && role != "assistant" {
        return None;
    }

    let uuid = record.get("uuid")?.as_str()?;
    let session_id = record
        .get("sessionId")
        .or_else(|| record.get("session_id"))?
        .as_str()?;

    let content = flatten_content(msg.get("content").unwrap_or(&Value::Null));
    if content.trim().is_empty() {
        return None;
    }

    // Postcondition: all required fields are non-empty strings.
    assert!(!uuid.is_empty(), "uuid must not be empty after filtering");
    assert!(
        !session_id.is_empty(),
        "session_id must not be empty after filtering"
    );

    Some(SweepMessage {
        session_id: session_id.to_string(),
        uuid: uuid.to_string(),
        role: role.to_string(),
        content,
    })
}

/// Parse a Claude Code `.jsonl` file and return all user/assistant messages.
///
/// Each JSONL line is parsed independently; malformed lines are silently
/// skipped.  Processing stops at `MESSAGES_MAX` to prevent OOM on huge files.
fn parse_claude_jsonl(path: &Path) -> Result<Vec<SweepMessage>> {
    assert!(
        path.exists(),
        "parse_claude_jsonl: path must exist: {}",
        path.display()
    );

    let file_content = std::fs::read_to_string(path)?;
    let mut messages: Vec<SweepMessage> = Vec::new();
    let mut line_count: usize = 0;

    for line in file_content.lines() {
        assert!(
            line_count < MESSAGES_MAX,
            "parse_claude_jsonl: MESSAGES_MAX exceeded"
        );
        line_count += 1;
        if line_count > MESSAGES_MAX {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(msg) = parse_claude_jsonl_record(&record) {
            messages.push(msg);
        }
    }

    // Postcondition: message count is within the configured bound.
    assert!(
        messages.len() <= MESSAGES_MAX,
        "message count must not exceed MESSAGES_MAX"
    );
    Ok(messages)
}

/// Build the deterministic drawer ID for a swept message.
///
/// Uses the full `session_id` (not a prefix) to avoid cross-session
/// collision risk when session identifiers share prefixes.
fn sweep_make_drawer_id(session_id: &str, uuid: &str) -> String {
    assert!(!session_id.is_empty(), "session_id must not be empty");
    assert!(!uuid.is_empty(), "uuid must not be empty");

    let drawer_id = format!("sweep_{session_id}_{uuid}");

    // Postcondition: ID starts with the sweep prefix and encodes both fields.
    assert!(
        drawer_id.starts_with("sweep_"),
        "drawer_id must start with sweep_"
    );
    // Negative space: no null bytes (would corrupt SQLite TEXT).
    debug_assert!(
        !drawer_id.contains('\0'),
        "drawer_id must not contain null bytes"
    );
    drawer_id
}

/// Query the set of message UUIDs already swept for each session in `session_ids`.
///
/// Drawer IDs for swept messages follow the pattern `"sweep_{session_id}_{uuid}"`.
/// Querying by that prefix and stripping it yields the stored UUIDs, letting the
/// caller skip re-processing.  A `LIMIT` is applied to avoid hitting
/// `query_all`'s `ROWS_MAX` guard on sessions with many prior sweeps.
async fn sweep_load_cursors(
    connection: &Connection,
    session_ids: &HashSet<String>,
) -> Result<HashMap<String, HashSet<String>>> {
    let mut cursors: HashMap<String, HashSet<String>> = HashMap::with_capacity(session_ids.len());

    for session_id in session_ids {
        assert!(!session_id.is_empty(), "session_id must not be empty");

        let prefix = format!("sweep_{session_id}_");
        let like_pattern = format!("{prefix}%");

        let rows = crate::db::query_all(
            connection,
            "SELECT id FROM drawers WHERE id LIKE ?1 AND ingest_mode = 'sweep' LIMIT 100000",
            turso::params![like_pattern.as_str()],
        )
        .await?;

        let mut uuids: HashSet<String> = HashSet::with_capacity(rows.len());
        for row in &rows {
            if let Ok(id) = row.get::<String>(0)
                && let Some(uuid) = id.strip_prefix(&prefix)
            {
                uuids.insert(uuid.to_string());
            }
        }

        // Pair assertion: all extracted UUIDs are non-empty strings.
        debug_assert!(
            uuids.iter().all(|u| !u.is_empty()),
            "all extracted UUIDs must be non-empty"
        );

        cursors.insert(session_id.clone(), uuids);
    }

    // Postcondition: result has one entry per session queried.
    assert!(
        cursors.len() == session_ids.len(),
        "cursor map must have one entry per session ID"
    );
    Ok(cursors)
}

/// Sweep a single Claude Code `.jsonl` file into the palace.
///
/// Parses every user/assistant message, skips those already stored (identified
/// by UUID), and inserts the rest as individual drawers.  The deterministic
/// drawer ID makes re-running safe.
pub async fn sweep(connection: &Connection, jsonl_path: &Path, wing: &str) -> Result<SweepResult> {
    assert!(
        jsonl_path.exists(),
        "sweep: path must exist: {}",
        jsonl_path.display()
    );
    assert!(!wing.is_empty(), "sweep: wing must not be empty");

    let messages = parse_claude_jsonl(jsonl_path)?;

    // Bulk-load cursors with one DB round trip per unique session.
    let session_ids: HashSet<String> = messages.iter().map(|m| m.session_id.clone()).collect();
    let cursors = sweep_load_cursors(connection, &session_ids).await?;

    let source_file = jsonl_path.to_str().unwrap_or("");
    let mut drawers_added: usize = 0;
    let mut drawers_already_present: usize = 0;

    for (file_index, message) in messages.iter().enumerate() {
        let already_swept = cursors
            .get(&message.session_id)
            .is_some_and(|uuids| uuids.contains(&message.uuid));

        if already_swept {
            drawers_already_present += 1;
            continue;
        }

        let drawer_id = sweep_make_drawer_id(&message.session_id, &message.uuid);
        let content = format!("{}: {}", message.role.to_uppercase(), message.content);

        let inserted = drawer::add_drawer(
            connection,
            &DrawerParams {
                id: &drawer_id,
                wing,
                room: "conversations",
                content: &content,
                source_file,
                chunk_index: file_index,
                added_by: "sweep",
                ingest_mode: "sweep",
                source_mtime: None,
            },
        )
        .await?;

        if inserted {
            drawers_added += 1;
        } else {
            // Drawer exists; UUID pre-check may not cover concurrent inserts.
            drawers_already_present += 1;
        }
    }

    // Postcondition: every message was either inserted or counted as present.
    assert!(
        drawers_added + drawers_already_present == messages.len(),
        "all messages must be accounted for: added={drawers_added} \
         present={drawers_already_present} total={}",
        messages.len()
    );
    Ok(SweepResult {
        drawers_added,
        drawers_already_present,
    })
}

/// Collect all `.jsonl` files under `dir_path` up to `WALK_DEPTH_LIMIT` deep.
/// Extracted from `sweep_directory` to keep that function within the 70-line limit.
fn sweep_directory_collect_files(dir_path: &Path) -> Vec<PathBuf> {
    assert!(
        dir_path.is_dir(),
        "sweep_directory_collect_files: path must be a directory: {}",
        dir_path.display()
    );

    let mut files: Vec<PathBuf> = Vec::new();
    let mut stack: Vec<(PathBuf, usize)> = vec![(dir_path.to_path_buf(), 0)];

    while let Some((current_dir, depth)) = stack.pop() {
        assert!(
            depth <= WALK_DEPTH_LIMIT,
            "depth must not exceed WALK_DEPTH_LIMIT"
        );
        if depth >= WALK_DEPTH_LIMIT {
            continue;
        }
        let Ok(read_dir) = std::fs::read_dir(&current_dir) else {
            continue;
        };
        for entry in read_dir.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push((path, depth + 1));
            } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                files.push(path);
            }
        }
    }

    files.sort();

    // Postcondition: all returned paths have the `.jsonl` extension.
    debug_assert!(
        files
            .iter()
            .all(|f| f.extension().and_then(|e| e.to_str()) == Some("jsonl")),
        "all collected files must have .jsonl extension"
    );
    files
}

/// Sweep every `.jsonl` file in a directory tree into the palace.
///
/// Individual file failures are logged to stderr and do not abort the sweep —
/// a partial failure still ingests whatever it can.  MCP servers must not
/// pollute stdout, so error output goes to stderr.
pub async fn sweep_directory(
    connection: &Connection,
    dir_path: &Path,
    wing: &str,
) -> Result<SweepDirectoryResult> {
    assert!(
        dir_path.is_dir(),
        "sweep_directory: path must be a directory"
    );
    assert!(!wing.is_empty(), "sweep_directory: wing must not be empty");

    let files = sweep_directory_collect_files(dir_path);

    assert!(
        files.len() <= FILES_MAX,
        "sweep_directory: FILES_MAX ({FILES_MAX}) exceeded"
    );

    let mut drawers_added: usize = 0;
    let mut drawers_already_present: usize = 0;
    let mut files_succeeded: usize = 0;

    for file in &files {
        match sweep(connection, file, wing).await {
            Ok(result) => {
                drawers_added += result.drawers_added;
                drawers_already_present += result.drawers_already_present;
                files_succeeded += 1;
            }
            Err(error) => {
                eprintln!("sweep: skipping {}: {error}", file.display());
            }
        }
    }

    // Postcondition: succeeded count cannot exceed attempted count.
    assert!(
        files_succeeded <= files.len(),
        "files_succeeded must not exceed files_attempted"
    );
    Ok(SweepDirectoryResult {
        files_attempted: files.len(),
        files_succeeded,
        drawers_added,
        drawers_already_present,
    })
}
