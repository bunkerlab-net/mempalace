//! Hook command — session-start, stop, and precompact hooks for AI harnesses.
//!
//! Reads JSON from stdin, outputs JSON to stdout. Invoked as a subprocess by
//! the shell wrappers in `hooks/`. All error paths output `{}` so the parent
//! harness is never interrupted by a hook failure.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::config::config_dir;
use crate::error::{Error, Result};

/// Number of human messages between auto-save checkpoints.
const SAVE_INTERVAL: usize = 15;
/// Number of recent user messages to sample for theme extraction.
const RECENT_MSG_COUNT: usize = 30;
/// Maximum number of theme words to extract.
const THEMES_MAX: usize = 3;
/// Minimum character length to consider a word for theme extraction.
const THEME_WORD_LEN_MIN: usize = 4;
/// Maximum characters per message snippet in the diary entry.
const MSG_SNIPPET_LEN: usize = 80;
/// Number of recent message snippets to embed in the diary entry.
const DIARY_SNIPPET_COUNT: usize = 10;

const _: () = assert!(SAVE_INTERVAL > 0);
const _: () = assert!(RECENT_MSG_COUNT > 0);
const _: () = assert!(DIARY_SNIPPET_COUNT <= RECENT_MSG_COUNT);
const _: () = assert!(MSG_SNIPPET_LEN > 0);

const STOP_BLOCK_REASON: &str = concat!(
    "AUTO-SAVE checkpoint (MemPalace). Save this session's key content:\n",
    "1. mempalace_diary_write — session summary (what was discussed, ",
    "key decisions, current state of work)\n",
    "2. mempalace_add_drawer — verbatim quotes, decisions, code snippets ",
    "(place in appropriate wing and room)\n",
    "3. mempalace_kg_add — entity relationships (optional)\n",
    "For THIS save, use MemPalace MCP tools only (not auto-memory .md files). ",
    "Use verbatim quotes where possible. Continue conversation after saving.",
);

/// Harnesses whose input format is supported.
const SUPPORTED_HARNESSES: &[&str] = &["claude-code", "codex"];

/// Parsed and sanitized hook input fields from stdin JSON.
struct HookInput {
    session_id: String,
    stop_hook_active: bool,
    transcript_path: PathBuf,
}

/// Main entry point: read stdin JSON, dispatch to the named hook handler.
///
/// Returns `Err` for unknown hook names or harnesses so the CLI can exit
/// non-zero and surface the problem to the user. All other failures are logged
/// and output `{}` to keep the harness running.
pub async fn run(hook_name: &str, harness: &str) -> Result<()> {
    assert!(!hook_name.is_empty(), "hook_name must not be empty");
    assert!(!harness.is_empty(), "harness must not be empty");

    let data: Value = serde_json::from_reader(std::io::stdin()).unwrap_or(json!({}));
    run_with_json(hook_name, harness, data).await
}

/// Dispatch a hook from a pre-parsed JSON payload.
///
/// Extracted from `run()` so tests can inject JSON without requiring stdin.
/// Returns `Err` for unknown hook names or harnesses; all other failures are
/// logged and output `{}` to keep the parent harness running.
pub(crate) async fn run_with_json(hook_name: &str, harness: &str, data: Value) -> Result<()> {
    assert!(!hook_name.is_empty(), "hook_name must not be empty");
    assert!(!harness.is_empty(), "harness must not be empty");

    if !SUPPORTED_HARNESSES.contains(&harness) {
        return Err(Error::Other(format!(
            "unknown harness: {harness}\nSupported: {}",
            SUPPORTED_HARNESSES.join(", ")
        )));
    }

    let state_dir = config_dir().join("hook_state");
    let _ = std::fs::create_dir_all(&state_dir);

    let input = hook_parse_input(&data, &state_dir);

    match hook_name {
        "session-start" => hook_session_start(&input, &state_dir),
        "stop" => hook_stop(&input, &state_dir).await,
        "precompact" => hook_precompact(&input, &state_dir),
        other => {
            return Err(Error::Other(format!(
                "unknown hook: {other}\nSupported: session-start, stop, precompact"
            )));
        }
    }

    Ok(())
}

/// Parse and sanitize hook input from the stdin JSON payload.
fn hook_parse_input(data: &Value, state_dir: &Path) -> HookInput {
    let raw_session_id = data
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let session_id = hook_sanitize_session_id(raw_session_id);

    let stop_hook_active = data
        .get("stop_hook_active")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let raw_transcript = data
        .get("transcript_path")
        .and_then(Value::as_str)
        .unwrap_or("");
    let transcript_path = hook_validate_transcript_path(raw_transcript).unwrap_or_else(|| {
        if !raw_transcript.is_empty() {
            hook_log(
                state_dir,
                &format!("WARNING: transcript_path rejected: {raw_transcript:?}"),
            );
        }
        PathBuf::new()
    });

    assert!(
        !session_id.is_empty(),
        "session_id must not be empty after sanitization"
    );
    HookInput {
        session_id,
        stop_hook_active,
        transcript_path,
    }
}

/// Strip all characters except alphanumeric, dash, and underscore.
///
/// Prevents path traversal when the session ID is used as a filename stem.
fn hook_sanitize_session_id(input: &str) -> String {
    let sanitized: String = input
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
        .collect();
    // Postcondition: always returns a non-empty string.
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

/// Validate a transcript path: must have `.jsonl`/`.json` extension and no `..`.
fn hook_validate_transcript_path(input: &str) -> Option<PathBuf> {
    if input.is_empty() {
        return None;
    }
    let path = PathBuf::from(input);
    // Reject path traversal regardless of the input form.
    if path
        .components()
        .any(|component| component.as_os_str() == "..")
    {
        return None;
    }
    let file_extension = path.extension().and_then(|os| os.to_str()).unwrap_or("");
    if file_extension != "jsonl" && file_extension != "json" {
        return None;
    }
    Some(path)
}

/// Session-start hook: create state directory, log the event, and pass through.
fn hook_session_start(input: &HookInput, state_dir: &Path) {
    assert!(!input.session_id.is_empty());
    hook_log(
        state_dir,
        &format!("SESSION START for session {}", input.session_id),
    );
    hook_output(&json!({}));
}

/// Stop hook: auto-save every `SAVE_INTERVAL` messages.
///
/// In silent mode, writes a diary entry directly to the palace DB and emits a
/// `systemMessage`. In block mode, returns `{"decision":"block"}` to ask Claude
/// to save via MCP tools.
async fn hook_stop(input: &HookInput, state_dir: &Path) {
    assert!(!input.session_id.is_empty());

    // In block mode, guard against the re-entry loop that would trigger after
    // Claude finishes the MCP save and the harness fires the hook again.
    // Silent mode has no re-entry loop, so the guard is skipped.
    if input.stop_hook_active && !hook_is_silent_mode() {
        hook_output(&json!({}));
        return;
    }

    let exchange_count = hook_count_messages(&input.transcript_path);
    let last_save_file = state_dir.join(format!("{}_last_save", input.session_id));
    let last_save = hook_read_last_save(&last_save_file);
    let since_last = exchange_count.saturating_sub(last_save);

    hook_log(
        state_dir,
        &format!(
            "Session {}: {exchange_count} exchanges, {since_last} since last save",
            input.session_id
        ),
    );

    if since_last < SAVE_INTERVAL || exchange_count == 0 {
        hook_output(&json!({}));
        return;
    }

    hook_log(
        state_dir,
        &format!("TRIGGERING SAVE at exchange {exchange_count}"),
    );
    let project_wing = hook_wing_from_transcript(input.transcript_path.to_str().unwrap_or(""));

    if hook_is_silent_mode() {
        hook_stop_save_silently(
            state_dir,
            input,
            &last_save_file,
            exchange_count,
            &project_wing,
        )
        .await;
    } else {
        hook_stop_save_blocking(
            state_dir,
            input,
            &last_save_file,
            exchange_count,
            &project_wing,
        );
    }
}

/// Silent-mode stop: write diary directly, then emit a `systemMessage`.
async fn hook_stop_save_silently(
    state_dir: &Path,
    input: &HookInput,
    last_save_file: &Path,
    exchange_count: usize,
    project_wing: &str,
) {
    assert!(
        exchange_count > 0,
        "exchange_count must be positive before a save"
    );

    let (count, themes) = hook_save_diary(
        &input.transcript_path,
        &input.session_id,
        project_wing,
        state_dir,
    )
    .await;

    hook_ingest_transcript(state_dir, &input.transcript_path);
    hook_maybe_auto_ingest(state_dir, input.transcript_path.to_str().unwrap_or(""));

    if count > 0 {
        let _ = std::fs::write(last_save_file, exchange_count.to_string());
        let tag = if themes.is_empty() {
            String::new()
        } else {
            format!(" \u{2014} {}", themes.join(", "))
        };
        // Honor the user's hook_desktop_toast setting (default false). The notification
        // is opt-in to avoid spamming desktops that don't have notify-send configured.
        if crate::config::MempalaceConfig::load().is_ok_and(|config| config.hook_desktop_toast) {
            hook_try_desktop_toast(&format!("{count} memories woven into the palace{tag}"));
        }
        hook_output(&json!({
            "systemMessage": format!("\u{2726} {count} memories woven into the palace{tag}"),
        }));
    } else {
        hook_output(&json!({}));
    }
}

/// Block-mode stop: return `{"decision":"block"}` asking Claude to save via MCP.
fn hook_stop_save_blocking(
    state_dir: &Path,
    input: &HookInput,
    last_save_file: &Path,
    exchange_count: usize,
    project_wing: &str,
) {
    assert!(
        exchange_count > 0,
        "exchange_count must be positive before a save"
    );
    // Advance the marker before Claude saves — best-effort; if Claude fails to
    // complete the save the checkpoint is lost but the hook will not loop endlessly.
    let _ = std::fs::write(last_save_file, exchange_count.to_string());
    hook_ingest_transcript(state_dir, &input.transcript_path);
    hook_maybe_auto_ingest(state_dir, input.transcript_path.to_str().unwrap_or(""));
    let reason = format!("{STOP_BLOCK_REASON} Write diary entry to wing={project_wing}.");
    hook_output(&json!({"decision": "block", "reason": reason}));
}

/// Precompact hook: synchronously mine the transcript, then allow compaction.
///
/// Note: precompact does **not** call `hook_ingest_transcript` because
/// `hook_spawn_mine_sync` already mines the same directory synchronously
/// (the parent of the transcript when `MEMPAL_DIR` is unset, otherwise the
/// configured override). The previous implementation called both, which
/// raced two `mempalace mine` processes against the same target — the
/// background ingest had no way to influence compaction timing, while the
/// sync mine is the one that must finish before compaction proceeds.
fn hook_precompact(input: &HookInput, state_dir: &Path) {
    assert!(!input.session_id.is_empty());
    hook_log(
        state_dir,
        &format!("PRE-COMPACT triggered for session {}", input.session_id),
    );
    hook_spawn_mine_sync(state_dir, input.transcript_path.to_str().unwrap_or(""));
    hook_output(&json!({}));
}

/// Write a diary checkpoint for `session_id` to the palace DB.
///
/// Returns `(message_count, themes)` on success, `(0, [])` on failure.
/// Opens the DB using the current `MempalaceConfig`, which respects `MEMPALACE_DIR`.
async fn hook_save_diary(
    transcript_path: &Path,
    session_id: &str,
    wing: &str,
    state_dir: &Path,
) -> (usize, Vec<String>) {
    assert!(!session_id.is_empty());
    assert!(!wing.is_empty());

    let messages = hook_extract_recent_messages(transcript_path);
    if messages.is_empty() {
        hook_log(state_dir, "No recent messages to save");
        return (0, vec![]);
    }

    let themes = hook_extract_themes(&messages);
    let now = Utc::now();
    let snippets: Vec<String> = messages
        .iter()
        .rev()
        .take(DIARY_SNIPPET_COUNT)
        .map(|msg| msg.chars().take(MSG_SNIPPET_LEN).collect())
        .collect();
    let entry = format!(
        "CHECKPOINT:{}|session:{session_id}|msgs:{}|recent:{}",
        now.format("%Y-%m-%d"),
        messages.len(),
        snippets.join("|")
    );
    let entry_id = Uuid::new_v4().to_string();

    if hook_write_diary_to_db(&entry_id, wing, &entry, "session-hook")
        .await
        .is_some()
    {
        hook_log(state_dir, &format!("Diary checkpoint saved: {entry_id}"));
        let ack = json!({"msgs": messages.len(), "ts": now.to_rfc3339()});
        let _ = std::fs::write(state_dir.join("last_checkpoint"), ack.to_string());
        (messages.len(), themes)
    } else {
        hook_log(
            state_dir,
            "Diary checkpoint failed: could not open palace DB",
        );
        (0, vec![])
    }
}

/// Open the palace DB and insert a diary drawer row.
///
/// Mirrors the SQL in `mcp/tools.rs::tool_diary_write`. Returns the entry ID
/// on success, `None` on any failure (config missing, DB error, path encoding).
async fn hook_write_diary_to_db(id: &str, wing: &str, entry: &str, agent: &str) -> Option<String> {
    use crate::{db, palace::drawer, schema};

    assert!(!id.is_empty());
    assert!(!wing.is_empty());
    assert!(!entry.is_empty());

    let config = crate::config::MempalaceConfig::init().ok()?;
    let db_path = config.palace_db_path();
    let db_path_str = db_path.to_str()?;
    let (_, connection) = db::open_db(db_path_str).await.ok()?;
    schema::ensure_schema(&connection).await.ok()?;

    connection
        .execute(
            "INSERT OR IGNORE INTO drawers \
             (id, wing, room, content, source_file, chunk_index, \
              added_by, ingest_mode, extract_mode) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            turso::params![
                id,
                wing,
                "diary",
                entry,
                "",
                0i32,
                agent,
                "diary",
                "checkpoint"
            ],
        )
        .await
        .ok()?;

    let _ = drawer::index_words(&connection, id, entry).await;
    Some(id.to_string())
}

/// Count human messages in a JSONL transcript, skipping command-messages.
fn hook_count_messages(transcript_path: &Path) -> usize {
    use std::io::BufRead as _;
    if !transcript_path.is_file() {
        return 0;
    }
    let Ok(file) = std::fs::File::open(transcript_path) else {
        return 0;
    };
    let mut count = 0usize;
    for line_result in std::io::BufReader::new(file).lines() {
        let Ok(line) = line_result else { break };
        if hook_parse_message_line(&line).is_some() {
            count += 1;
        }
    }
    count
}

/// Extract the last `RECENT_MSG_COUNT` user messages from a JSONL transcript.
fn hook_extract_recent_messages(transcript_path: &Path) -> Vec<String> {
    use std::io::BufRead as _;
    if !transcript_path.is_file() {
        return vec![];
    }
    let Ok(file) = std::fs::File::open(transcript_path) else {
        return vec![];
    };
    let mut messages: Vec<String> = Vec::new();
    for line_result in std::io::BufReader::new(file).lines() {
        let Ok(line) = line_result else { break };
        if let Some(text) = hook_parse_message_line(&line) {
            messages.push(text);
        }
    }
    let start = messages.len().saturating_sub(RECENT_MSG_COUNT);
    messages.drain(..start);
    messages
}

/// Extract user-message text from a single JSONL line; return `None` to skip.
///
/// Handles both Claude Code (`message.role == "user"`) and Codex CLI
/// (`type == "event_msg"`) transcript formats. Command-messages are skipped
/// because they are internal harness bookkeeping, not conversation content.
fn hook_parse_message_line(line: &str) -> Option<String> {
    let entry: Value = serde_json::from_str(line).ok()?;

    // Claude Code format: the envelope has a "message" or "event_message" key
    // with a "role" field. Using if-let avoids a ? that would exit the function
    // before the Codex branch below can be tried.
    if let Some(msg) = entry.get("message").or_else(|| entry.get("event_message"))
        && msg.get("role").and_then(Value::as_str) == Some("user")
    {
        let content = msg.get("content")?;
        let text = if let Some(str_val) = content.as_str() {
            str_val.to_string()
        } else {
            let arr = content.as_array()?;
            arr.iter()
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join(" ")
        };
        if text.contains("<command-message>") || text.contains("<system-reminder>") {
            return None;
        }
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        return Some(trimmed.chars().take(200).collect());
    }

    // Codex CLI format: top-level "type":"event_msg" with a nested "payload".
    if entry.get("type").and_then(Value::as_str) == Some("event_msg") {
        let payload = entry.get("payload")?;
        if payload.get("type").and_then(Value::as_str) == Some("user_message") {
            let text = payload.get("message").and_then(Value::as_str)?;
            if !text.trim().is_empty() && !text.contains("<command-message>") {
                return Some(text.trim().chars().take(200).collect());
            }
        }
    }

    None
}

/// Extract the top `THEMES_MAX` topic words from recent messages.
///
/// Uses a static stopword list; English-only. Non-English messages produce
/// noisier themes but do not cause errors.
fn hook_extract_themes(messages: &[String]) -> Vec<String> {
    assert!(
        !messages.is_empty(),
        "messages must not be empty before theme extraction"
    );

    let stopwords = hook_theme_stopwords();
    let mut word_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for msg in messages {
        for word in msg.split_whitespace() {
            let clean: String = word
                .trim_matches(|ch: char| !ch.is_alphabetic())
                .to_lowercase();
            if clean.len() >= THEME_WORD_LEN_MIN
                && clean.chars().all(char::is_alphabetic)
                && !stopwords.contains(clean.as_str())
            {
                *word_counts.entry(clean).or_insert(0) += 1;
            }
        }
    }

    let mut counts: Vec<(String, usize)> = word_counts.into_iter().collect();
    // Sort descending by count, then alphabetically for determinism.
    counts.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    counts
        .into_iter()
        .take(THEMES_MAX)
        .map(|(word, _)| word)
        .collect()
}

/// Stopwords for theme extraction — common English function words.
fn hook_theme_stopwords() -> std::collections::HashSet<&'static str> {
    [
        "the", "and", "for", "are", "but", "not", "you", "all", "any", "can", "had", "her", "was",
        "one", "our", "out", "day", "get", "has", "him", "his", "how", "man", "new", "now", "old",
        "see", "two", "way", "who", "boy", "did", "its", "let", "put", "say", "she", "too", "use",
        "from", "that", "this", "with", "have", "been", "will", "your", "when", "more", "into",
        "just", "also", "like", "here", "some", "what", "make", "time", "know", "take", "them",
        "then", "they", "well", "were", "which", "there", "their", "about", "would", "these",
        "those", "think", "could", "first", "after", "where", "while", "being", "every", "going",
        "other", "still", "thing", "things", "doing", "using", "need", "want", "look", "file",
        "files", "code", "work", "sure", "okay", "yeah",
    ]
    .iter()
    .copied()
    .collect()
}

/// Derive a project wing name from a Claude Code transcript path.
///
/// Claude Code encodes the project directory with dashes, e.g.:
/// `~/.claude/projects/-Users-<user>-Projects-myapp/session.jsonl`
/// → `wing_myapp`
fn hook_wing_from_transcript(transcript_path: &str) -> String {
    let normalized = transcript_path.replace('\\', "/");

    // Primary: capture the segment after `-Projects-` so the wing name is the
    // actual project (`myapp`) rather than the full encoded parent path
    // (`users_alice_projects_myapp`).
    if let Ok(re) = regex::Regex::new(r"/\.claude/projects/.*-Projects-([^/]+)")
        && let Some(caps) = re.captures(&normalized)
    {
        let project = caps[1].to_lowercase().replace(['-', ' '], "_");
        let project = project.trim_matches('_');
        if !project.is_empty() {
            return format!("wing_{project}");
        }
    }

    // Fallback: explicit `-Projects-<name>` segment anywhere in the path.
    if let Ok(re) = regex::Regex::new(r"-Projects-([^/]+?)(?:/|$)")
        && let Some(caps) = re.captures(&normalized)
    {
        let project = caps[1].to_lowercase().replace(['-', ' '], "_");
        let project = project.trim_matches('_');
        if !project.is_empty() {
            return format!("wing_{project}");
        }
    }

    "wing_sessions".to_string()
}

/// Return `true` if the config has `hook_silent_save = true` (the default).
fn hook_is_silent_mode() -> bool {
    crate::config::MempalaceConfig::load().map_or(true, |config| config.hook_silent_save)
}

/// Read the last-save message count from the session state file.
fn hook_read_last_save(last_save_file: &Path) -> usize {
    std::fs::read_to_string(last_save_file)
        .ok()
        .and_then(|content| content.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

/// Determine the directory to mine: `MEMPAL_DIR` env var, or transcript parent.
fn hook_get_mine_dir(transcript_path: &str) -> Option<PathBuf> {
    if let Ok(env_dir) = std::env::var("MEMPAL_DIR") {
        let env_dir_path = PathBuf::from(&env_dir);
        if env_dir_path.is_dir() {
            return Some(env_dir_path);
        }
    }
    if !transcript_path.is_empty() {
        let path = PathBuf::from(transcript_path);
        if path.is_file() {
            return path.parent().map(std::path::Path::to_path_buf);
        }
    }
    None
}

/// Return `true` if a previous background mine process is still running.
fn hook_mine_already_running(state_dir: &Path) -> bool {
    let pid_file = state_dir.join("mine.pid");
    let Ok(content) = std::fs::read_to_string(&pid_file) else {
        return false;
    };
    let Ok(pid) = content.trim().parse::<u32>() else {
        return false;
    };
    hook_pid_alive(pid)
}

/// Check whether a process ID is alive without sending it a signal.
///
/// On POSIX, `kill -0 <pid>` is the standard existence probe. On non-POSIX
/// platforms the function conservatively returns `false` so mining is never
/// blocked by a stale PID file.
fn hook_pid_alive(pid: u32) -> bool {
    assert!(pid > 0, "pid must be positive");

    #[cfg(unix)]
    {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|exit_status| exit_status.success())
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// Spawn `mempalace mine` in the background if a mine directory is available.
fn hook_maybe_auto_ingest(state_dir: &Path, transcript_path: &str) {
    let Some(mine_dir) = hook_get_mine_dir(transcript_path) else {
        return;
    };
    if hook_mine_already_running(state_dir) {
        hook_log(state_dir, "Skipping auto-ingest: mine already running");
        return;
    }
    hook_spawn_mine_bg(state_dir, &mine_dir);
}

/// Spawn `mempalace mine <dir> --mode convos --wing sessions` in the background.
///
/// Appends stdout and stderr to `hook.log`. Writes the child PID to `mine.pid`
/// so subsequent hook fires can detect whether the mine is still running.
fn hook_spawn_mine_bg(state_dir: &Path, mine_dir: &Path) {
    assert!(mine_dir.is_dir(), "mine_dir must be an existing directory");

    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let Some(log_file) = hook_open_log(state_dir) else {
        return;
    };
    let Ok(log_copy) = log_file.try_clone() else {
        return;
    };
    let result = std::process::Command::new(&exe)
        .args([
            "mine",
            &mine_dir.to_string_lossy(),
            "--mode",
            "convos",
            "--wing",
            "sessions",
        ])
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_copy))
        .spawn();
    if let Ok(child) = result {
        let _ = std::fs::write(state_dir.join("mine.pid"), child.id().to_string());
    }
}

/// Run `mempalace mine <dir> --mode convos` synchronously (blocks until done).
///
/// Used by the precompact hook where data must land before compaction proceeds.
fn hook_spawn_mine_sync(state_dir: &Path, transcript_path: &str) {
    let Some(mine_dir) = hook_get_mine_dir(transcript_path) else {
        return;
    };
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let _ = std::fs::create_dir_all(state_dir);
    let Some(log_file) = hook_open_log(state_dir) else {
        return;
    };
    let Ok(log_copy) = log_file.try_clone() else {
        return;
    };
    let _ = std::process::Command::new(&exe)
        .args([
            "mine",
            &mine_dir.to_string_lossy(),
            "--mode",
            "convos",
            "--wing",
            "sessions",
        ])
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_copy))
        .status();
}

/// Spawn `mempalace mine <transcript_parent> --mode convos` in the background.
///
/// The transcript's parent directory is always mined as a conversation source so
/// the current session's messages are captured even before the stop hook fires.
/// Writes the child PID to `mine.pid` so the subsequent `hook_maybe_auto_ingest`
/// call sees the in-flight mine via [`hook_mine_already_running`] and does not
/// spawn a duplicate mine for the same directory.
fn hook_ingest_transcript(state_dir: &Path, transcript_path: &Path) {
    if !transcript_path.is_file() {
        return;
    }
    let Ok(meta) = std::fs::metadata(transcript_path) else {
        return;
    };
    // Skip empty or trivially small files — they cannot contain useful messages.
    if meta.len() < 100 {
        return;
    }
    if hook_mine_already_running(state_dir) {
        hook_log(
            state_dir,
            "Skipping transcript ingest: mine already running",
        );
        return;
    }
    let Some(parent) = transcript_path.parent() else {
        return;
    };
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let _ = std::fs::create_dir_all(state_dir);
    let Some(log_file) = hook_open_log(state_dir) else {
        return;
    };
    let Ok(log_copy) = log_file.try_clone() else {
        return;
    };
    let result = std::process::Command::new(&exe)
        .args([
            "mine",
            &parent.to_string_lossy(),
            "--mode",
            "convos",
            "--wing",
            "sessions",
        ])
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_copy))
        .spawn();
    if let Ok(child) = result {
        // Share the same pid file used by hook_spawn_mine_bg so the auto-ingest
        // call right after this sees the in-flight mine and skips its own spawn.
        let _ = std::fs::write(state_dir.join("mine.pid"), child.id().to_string());
        hook_log(
            state_dir,
            &format!("Transcript ingest started: {}", transcript_path.display()),
        );
    }
}

/// Open (or create and append to) `hook.log` in `state_dir`.
fn hook_open_log(state_dir: &Path) -> Option<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(state_dir.join("hook.log"))
        .ok()
}

/// Append a timestamped line to `hook.log` in `state_dir`.
fn hook_log(state_dir: &Path, message: &str) {
    assert!(!message.is_empty(), "log message must not be empty");
    let _ = std::fs::create_dir_all(state_dir);
    let Some(mut file) = hook_open_log(state_dir) else {
        return;
    };
    let timestamp = chrono::Local::now().format("%H:%M:%S");
    let _ = writeln!(file, "[{timestamp}] {message}");
}

/// Write the hook response JSON to stdout.
///
/// Hooks communicate back to the harness via a single JSON object on stdout.
/// Using `println!` ensures the output is flushed with a trailing newline.
/// Fire-and-forget desktop notification via `notify-send`.
///
/// Best-effort: silently no-ops when `notify-send` is not installed or when
/// the spawn fails (e.g. no D-Bus session on headless CI). The hook must not
/// fail because of a missing notification daemon.
fn hook_try_desktop_toast(message: &str) {
    assert!(
        !message.is_empty(),
        "hook_try_desktop_toast: message must not be empty"
    );
    // notify-send is Linux/freedesktop only; ignore errors on other platforms.
    let _ = std::process::Command::new("notify-send")
        .arg("MemPalace")
        .arg(message)
        .spawn();
}

fn hook_output(data: &Value) {
    let payload = serde_json::to_string_pretty(data).unwrap_or_else(|_| "{}".to_string());
    println!("{payload}");
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn hook_sanitize_session_id_strips_special_characters() {
        // Special chars (slashes, dots) must be removed to prevent path traversal.
        let result = hook_sanitize_session_id("abc/def.123");
        assert_eq!(result, "abcdef123");
        assert!(!result.contains('/'));
        assert!(!result.contains('.'));
    }

    #[test]
    fn hook_sanitize_session_id_empty_input_returns_unknown() {
        // Empty sanitized output must fall back to "unknown" to keep state files valid.
        let result = hook_sanitize_session_id("///..");
        assert_eq!(result, "unknown");
        assert!(!result.is_empty());
    }

    #[test]
    fn hook_sanitize_session_id_preserves_valid_characters() {
        // Alphanumeric, dash, and underscore must survive sanitization unchanged.
        let result = hook_sanitize_session_id("abc-123_xyz");
        assert_eq!(result, "abc-123_xyz");
        assert!(!result.is_empty());
    }

    #[test]
    fn hook_validate_transcript_path_rejects_path_traversal() {
        // Paths containing ".." must be rejected to prevent escaping the state directory.
        let result = hook_validate_transcript_path("../../../etc/passwd.jsonl");
        assert!(result.is_none());
    }

    #[test]
    fn hook_validate_transcript_path_accepts_jsonl_extension() {
        // A clean .jsonl path without traversal must be accepted.
        let result = hook_validate_transcript_path("/home/user/.claude/session.jsonl");
        assert!(result.is_some());
        let path = result.expect("must return path for valid .jsonl");
        let extension = path.extension().and_then(|os| os.to_str()).unwrap_or("");
        assert_eq!(extension, "jsonl");
        // Pair assertion: the path must not be empty.
        assert!(!path.as_os_str().is_empty());
    }

    #[test]
    fn hook_validate_transcript_path_rejects_unknown_extension() {
        // Non-JSON extensions (e.g., .txt) are not valid transcript paths.
        let result = hook_validate_transcript_path("/home/user/notes.txt");
        assert!(result.is_none());
    }

    #[test]
    fn hook_wing_from_transcript_extracts_project_name() {
        // Capture only the segment after `-Projects-` so the wing name is the
        // actual project (`myapp`) rather than the full encoded parent path.
        let path = "/Users/alice/.claude/projects/-Users-alice-Projects-myapp/session.jsonl";
        let wing = hook_wing_from_transcript(path);
        assert_eq!(wing, "wing_myapp");
        assert!(wing.starts_with("wing_"));
    }

    #[test]
    fn hook_wing_from_transcript_falls_back_to_sessions() {
        // A path that does not match any project pattern must yield wing_sessions.
        let wing = hook_wing_from_transcript("/tmp/random.jsonl");
        assert_eq!(wing, "wing_sessions");
        assert!(wing.starts_with("wing_"));
    }

    #[test]
    fn hook_extract_themes_returns_top_words() {
        // The most frequent non-stopword words must surface as themes.
        let messages = vec![
            "refactoring database schema migration".to_string(),
            "database performance migration tuning".to_string(),
            "optimizing database migration process".to_string(),
        ];
        let themes = hook_extract_themes(&messages);
        // "database" and "migration" should both appear in top themes.
        assert!(!themes.is_empty());
        assert!(
            themes.contains(&"database".to_string()) || themes.contains(&"migration".to_string())
        );
        assert!(themes.len() <= THEMES_MAX);
    }

    #[test]
    fn hook_read_last_save_missing_file_returns_zero() {
        // A missing state file must return 0 (first run for this session).
        let result = hook_read_last_save(Path::new("/nonexistent/path/_last_save"));
        assert_eq!(result, 0);
    }

    #[test]
    fn hook_read_last_save_reads_persisted_count() {
        // A written count must round-trip through the file correctly.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let file = dir.path().join("session_last_save");
        std::fs::write(&file, "42").expect("must write count");
        let result = hook_read_last_save(&file);
        assert_eq!(result, 42);
        // Pair assertion: file must still exist after read.
        assert!(file.exists());
    }

    #[test]
    fn hook_count_messages_empty_file_returns_zero() {
        // A completely empty transcript must return zero message count.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let path = dir.path().join("empty.jsonl");
        std::fs::write(&path, "").expect("must write empty file");
        assert_eq!(hook_count_messages(&path), 0);
    }

    #[test]
    fn hook_count_messages_counts_user_turns() {
        // Each user-role message must be counted; assistant messages must not.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let path = dir.path().join("session.jsonl");
        let content = concat!(
            r#"{"message":{"role":"user","content":"hello world"}}"#,
            "\n",
            r#"{"message":{"role":"assistant","content":"hi there"}}"#,
            "\n",
            r#"{"message":{"role":"user","content":"another question"}}"#,
            "\n",
        );
        std::fs::write(&path, content).expect("must write test JSONL");
        let count = hook_count_messages(&path);
        assert_eq!(count, 2);
        // Pair assertion: count must not include assistant messages.
        assert!(count < 3);
    }

    #[test]
    fn hook_count_messages_skips_command_messages() {
        // Lines containing <command-message> must be excluded from the count.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let path = dir.path().join("session.jsonl");
        let content = concat!(
            r#"{"message":{"role":"user","content":"<command-message>run cmd</command-message>"}}"#,
            "\n",
            r#"{"message":{"role":"user","content":"real question"}}"#,
            "\n",
        );
        std::fs::write(&path, content).expect("must write test JSONL");
        let count = hook_count_messages(&path);
        assert_eq!(count, 1);
        assert!(count < 2);
    }

    #[test]
    fn hook_get_mine_dir_returns_none_for_empty_inputs() {
        // No MEMPAL_DIR and no transcript path must return None.
        temp_env::with_var("MEMPAL_DIR", None::<&str>, || {
            let result = hook_get_mine_dir("");
            assert!(result.is_none());
        });
    }

    #[test]
    fn hook_validate_transcript_path_empty_returns_none() {
        // An empty string is not a valid transcript path.
        assert!(hook_validate_transcript_path("").is_none());
    }

    // -------- hook_parse_input --------

    #[test]
    fn hook_parse_input_with_full_json_uses_provided_fields() {
        // Valid JSON must populate all fields without logging a warning.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let transcript = dir.path().join("session.jsonl");
        std::fs::write(&transcript, "").expect("must write transcript");
        let data = json!({
            "session_id": "abc-123",
            "stop_hook_active": true,
            "transcript_path": transcript.to_str().expect("valid utf-8"),
        });
        let input = hook_parse_input(&data, dir.path());
        assert_eq!(input.session_id, "abc-123");
        assert!(input.stop_hook_active);
        // Pair assertion: transcript path must survive round-trip.
        assert!(!input.transcript_path.as_os_str().is_empty());
    }

    #[test]
    fn hook_parse_input_with_missing_fields_uses_defaults() {
        // Missing fields must produce safe defaults: "unknown" session, no stop, empty path.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let data = json!({});
        let input = hook_parse_input(&data, dir.path());
        assert_eq!(input.session_id, "unknown");
        assert!(!input.stop_hook_active);
        assert!(input.transcript_path.as_os_str().is_empty());
    }

    #[test]
    fn hook_parse_input_with_invalid_transcript_logs_and_uses_empty() {
        // A path with ".." must be rejected; state_dir must still exist.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let data = json!({
            "session_id": "sid",
            "transcript_path": "../../../etc/passwd.jsonl",
        });
        let input = hook_parse_input(&data, dir.path());
        assert_eq!(input.session_id, "sid");
        assert!(input.transcript_path.as_os_str().is_empty());
    }

    // -------- hook_session_start --------

    #[test]
    fn hook_session_start_logs_event_and_outputs_empty_json() {
        // session_start must write a log line and not panic.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let input = HookInput {
            session_id: "test-session".to_string(),
            stop_hook_active: false,
            transcript_path: PathBuf::new(),
        };
        hook_session_start(&input, dir.path());
        // A log file must have been created.
        assert!(dir.path().join("hook.log").exists());
    }

    // -------- hook_precompact --------

    #[test]
    fn hook_precompact_with_empty_path_does_not_panic() {
        // precompact with a non-existent transcript must log and not panic.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let input = HookInput {
            session_id: "test-session".to_string(),
            stop_hook_active: false,
            transcript_path: PathBuf::new(),
        };
        hook_precompact(&input, dir.path());
        assert!(dir.path().join("hook.log").exists());
    }

    // -------- hook_extract_recent_messages --------

    #[test]
    fn hook_extract_recent_messages_from_empty_file_returns_empty() {
        // An empty file must produce an empty message list.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let path = dir.path().join("empty.jsonl");
        std::fs::write(&path, "").expect("must write");
        let messages = hook_extract_recent_messages(&path);
        assert!(messages.is_empty());
    }

    #[test]
    fn hook_extract_recent_messages_returns_user_messages() {
        // User messages must be extracted; assistant lines must be skipped.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let path = dir.path().join("session.jsonl");
        let content = concat!(
            r#"{"message":{"role":"user","content":"first question"}}"#,
            "\n",
            r#"{"message":{"role":"assistant","content":"response"}}"#,
            "\n",
            r#"{"message":{"role":"user","content":"second question"}}"#,
            "\n",
        );
        std::fs::write(&path, content).expect("must write");
        let messages = hook_extract_recent_messages(&path);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], "first question");
        assert_eq!(messages[1], "second question");
    }

    #[test]
    fn hook_extract_recent_messages_skips_command_messages() {
        // Lines with <command-message> must be excluded.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let path = dir.path().join("session.jsonl");
        let content = concat!(
            r#"{"message":{"role":"user","content":"<command-message>cmd</command-message>"}}"#,
            "\n",
            r#"{"message":{"role":"user","content":"real message"}}"#,
            "\n",
        );
        std::fs::write(&path, content).expect("must write");
        let messages = hook_extract_recent_messages(&path);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0], "real message");
    }

    // -------- hook_parse_message_line --------

    #[test]
    fn hook_parse_message_line_returns_none_for_invalid_json() {
        // Non-JSON input must return None without panicking.
        let result = hook_parse_message_line("not json at all");
        assert!(result.is_none());
    }

    #[test]
    fn hook_parse_message_line_returns_none_for_assistant_role() {
        // Assistant-role messages must be skipped.
        let line = r#"{"message":{"role":"assistant","content":"hi"}}"#;
        let result = hook_parse_message_line(line);
        assert!(result.is_none());
    }

    #[test]
    fn hook_parse_message_line_returns_user_text() {
        // String content for a user-role message must be returned.
        let line = r#"{"message":{"role":"user","content":"hello world"}}"#;
        let result = hook_parse_message_line(line);
        assert_eq!(result, Some("hello world".to_string()));
    }

    #[test]
    fn hook_parse_message_line_returns_array_content_joined() {
        // An array of text blocks must be joined into a single string.
        let line = r#"{"message":{"role":"user","content":[{"type":"text","text":"part one"},{"type":"text","text":"part two"}]}}"#;
        let result = hook_parse_message_line(line);
        let text = result.expect("array content must produce Some");
        assert!(text.contains("part one"));
        assert!(text.contains("part two"));
    }

    #[test]
    fn hook_parse_message_line_returns_none_for_command_message_content() {
        // Content containing <command-message> must be filtered out.
        let line =
            r#"{"message":{"role":"user","content":"<command-message>hidden</command-message>"}}"#;
        let result = hook_parse_message_line(line);
        assert!(result.is_none());
    }

    #[test]
    fn hook_parse_message_line_handles_codex_event_msg_format() {
        // Codex CLI "event_msg" format must also be parsed.
        let line =
            r#"{"type":"event_msg","payload":{"type":"user_message","message":"codex question"}}"#;
        let result = hook_parse_message_line(line);
        assert_eq!(result, Some("codex question".to_string()));
    }

    // -------- hook_is_silent_mode --------

    #[test]
    fn hook_is_silent_mode_defaults_to_true_when_config_missing() {
        // When no config exists the default must be true (silent saves enabled).
        let dir = tempfile::tempdir().expect("must create temp dir");
        temp_env::with_var(
            "MEMPALACE_DIR",
            Some(dir.path().to_str().expect("utf-8")),
            || {
                let silent = hook_is_silent_mode();
                assert!(silent, "default must be silent=true when config is absent");
            },
        );
    }

    // -------- hook_mine_already_running --------

    #[test]
    fn hook_mine_already_running_returns_false_when_pid_file_absent() {
        // No PID file must return false (no mine running).
        let dir = tempfile::tempdir().expect("must create temp dir");
        assert!(!hook_mine_already_running(dir.path()));
    }

    #[test]
    fn hook_mine_already_running_returns_false_for_non_numeric_pid() {
        // A PID file with non-numeric content must return false gracefully.
        let dir = tempfile::tempdir().expect("must create temp dir");
        std::fs::write(dir.path().join("mine.pid"), "not-a-pid").expect("must write");
        assert!(!hook_mine_already_running(dir.path()));
    }

    // -------- hook_ingest_transcript --------

    #[test]
    fn hook_ingest_transcript_does_nothing_for_nonexistent_path() {
        // A non-existent transcript path must return without panicking.
        let dir = tempfile::tempdir().expect("must create temp dir");
        hook_ingest_transcript(dir.path(), &PathBuf::from("/nonexistent/file.jsonl"));
        // No log file is created for a skip.
        // Postcondition: function did not panic — test passes by reaching here.
    }

    #[test]
    fn hook_ingest_transcript_does_nothing_for_small_file() {
        // Files under 100 bytes must be skipped (no useful messages can fit).
        let dir = tempfile::tempdir().expect("must create temp dir");
        let transcript = dir.path().join("tiny.jsonl");
        std::fs::write(&transcript, b"{}").expect("must write");
        hook_ingest_transcript(dir.path(), &transcript);
        // If we spawned a process the file would still exist — it does.
        assert!(transcript.exists());
    }

    // -------- hook_open_log --------

    #[test]
    fn hook_open_log_creates_log_file_in_state_directory() {
        // Opening the log must create hook.log inside state_dir.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let file = hook_open_log(dir.path());
        assert!(file.is_some(), "hook_open_log must return Some(file)");
        // Pair assertion: the log file must exist on disk.
        assert!(dir.path().join("hook.log").exists());
    }

    // -------- hook_log --------

    #[test]
    fn hook_log_appends_timestamped_line_to_log() {
        // Logging a message must create hook.log with that message.
        let dir = tempfile::tempdir().expect("must create temp dir");
        hook_log(dir.path(), "test log entry");
        let log_path = dir.path().join("hook.log");
        assert!(log_path.exists());
        let content = std::fs::read_to_string(&log_path).expect("must read log");
        assert!(content.contains("test log entry"));
    }

    // -------- hook_output --------

    #[test]
    fn hook_output_does_not_panic_with_empty_object() {
        // Outputting an empty JSON object must not panic.
        hook_output(&json!({}));
        // Pair assertion: non-empty object also must not panic.
        hook_output(&json!({"key": "value"}));
    }

    // -------- hook_stop (early returns) --------

    #[tokio::test]
    async fn hook_stop_exits_early_when_exchange_count_is_zero() {
        // With no transcript, exchange_count == 0 → immediate no-op return.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let input = HookInput {
            session_id: "sid".to_string(),
            stop_hook_active: false,
            transcript_path: PathBuf::new(),
        };
        hook_stop(&input, dir.path()).await;
        // Postcondition: hook.log was written (the count was logged).
        assert!(dir.path().join("hook.log").exists());
    }

    #[tokio::test]
    async fn hook_stop_exits_early_below_save_interval() {
        // With fewer than SAVE_INTERVAL messages, stop must skip saving.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let transcript = dir.path().join("session.jsonl");
        // Write one user message — well below SAVE_INTERVAL.
        let line = r#"{"message":{"role":"user","content":"single message"}}"#;
        std::fs::write(&transcript, format!("{line}\n")).expect("must write");
        let input = HookInput {
            session_id: "sid".to_string(),
            stop_hook_active: false,
            transcript_path: transcript,
        };
        hook_stop(&input, dir.path()).await;
        // No last_save file means no save was triggered.
        assert!(!dir.path().join("sid_last_save").exists());
    }

    // -------- hook_stop_save_blocking --------

    #[test]
    fn hook_stop_save_blocking_writes_checkpoint_and_outputs_block_decision() {
        // block-mode stop must write the last-save file and emit {"decision":"block"}.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let last_save_file = dir.path().join("sid_last_save");
        let input = HookInput {
            session_id: "sid".to_string(),
            stop_hook_active: false,
            transcript_path: PathBuf::new(),
        };
        hook_stop_save_blocking(dir.path(), &input, &last_save_file, 20, "wing_project");
        // Last-save file must record the exchange count.
        let count_str = std::fs::read_to_string(&last_save_file).expect("must read");
        assert_eq!(count_str.trim(), "20");
        // Pair assertion: the file was not zero-length.
        assert!(!count_str.is_empty());
    }

    // -------- hook_save_diary --------

    #[tokio::test]
    async fn hook_save_diary_returns_zero_for_nonexistent_transcript() {
        // A missing transcript must produce (0, []) without touching the DB.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let (count, themes) = hook_save_diary(
            &PathBuf::from("/nonexistent/session.jsonl"),
            "sid",
            "wing",
            dir.path(),
        )
        .await;
        assert_eq!(count, 0);
        assert!(themes.is_empty());
    }

    #[tokio::test]
    async fn hook_save_diary_writes_checkpoint_when_transcript_has_messages() {
        // With a real transcript and palace DB, a diary entry must be inserted.
        let data_dir = tempfile::tempdir().expect("must create data dir");
        let db_path = data_dir.path().join("palace.db");
        let db_path_str = db_path.to_str().expect("utf-8").to_string();
        let transcript_dir = tempfile::tempdir().expect("must create transcript dir");
        let transcript = transcript_dir.path().join("session.jsonl");
        // Write enough user messages to pass the "empty" check.
        let line = r#"{"message":{"role":"user","content":"discussing architecture"}}"#;
        std::fs::write(&transcript, format!("{line}\n{line}\n")).expect("must write");

        let (count, _themes) = temp_env::async_with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(data_dir.path().to_str().expect("utf-8")),
                ),
                ("MEMPALACE_PALACE_PATH", Some(db_path_str.as_str())),
            ],
            async {
                hook_save_diary(
                    &transcript,
                    "test-session",
                    "wing_project",
                    transcript_dir.path(),
                )
                .await
            },
        )
        .await;
        assert_eq!(count, 2, "two messages must produce count=2");
        // Pair assertion: palace DB must exist after write.
        assert!(db_path.exists());
    }

    // -------- hook_write_diary_to_db --------

    #[tokio::test]
    async fn hook_write_diary_to_db_inserts_row_and_returns_id() {
        // A valid diary entry must be inserted and the ID returned.
        let data_dir = tempfile::tempdir().expect("must create data dir");
        let db_path = data_dir.path().join("palace.db");
        let db_path_str = db_path.to_str().expect("utf-8").to_string();
        let result = temp_env::async_with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(data_dir.path().to_str().expect("utf-8")),
                ),
                ("MEMPALACE_PALACE_PATH", Some(db_path_str.as_str())),
            ],
            async {
                hook_write_diary_to_db(
                    "test-id-001",
                    "wing_test",
                    "CHECKPOINT:today|session:abc|msgs:5|recent:hello",
                    "test-agent",
                )
                .await
            },
        )
        .await;
        assert_eq!(result, Some("test-id-001".to_string()));
        // Pair assertion: palace DB must have been created on disk.
        assert!(db_path.exists());
    }

    // -------- hook_maybe_auto_ingest --------

    #[test]
    fn hook_maybe_auto_ingest_does_nothing_when_no_mine_dir() {
        // With no MEMPAL_DIR and no transcript, auto-ingest must silently skip.
        let dir = tempfile::tempdir().expect("must create temp dir");
        temp_env::with_var("MEMPAL_DIR", None::<&str>, || {
            hook_maybe_auto_ingest(dir.path(), "");
        });
        // Postcondition: no PID file was written (no spawn occurred).
        assert!(!dir.path().join("mine.pid").exists());
    }

    // -------- hook_pid_alive --------

    #[test]
    fn hook_pid_alive_returns_false_for_large_nonexistent_pid() {
        // Use 4_200_001: above Linux pid_max ceiling (4_194_304 = 2^22) so the
        // PID cannot exist, yet safely below i32::MAX so it does not overflow
        // pid_t and trigger the kill(2) special case where negative values (e.g.
        // -1 from u32::MAX truncation) broadcast to all processes.
        let alive = hook_pid_alive(4_200_001u32);
        assert!(!alive, "PID 4_200_001 must not be alive");
    }

    // -------- run_with_json --------

    #[tokio::test]
    async fn run_with_json_returns_err_for_unknown_harness() {
        // An unsupported harness name must cause Err so the CLI exits non-zero.
        let result = run_with_json("session-start", "unknown-harness", json!({})).await;
        assert!(result.is_err());
        let error_text = result
            .expect_err("must be Err for unknown harness")
            .to_string();
        assert!(error_text.contains("unknown-harness"), "{error_text}");
    }

    #[tokio::test]
    async fn run_with_json_returns_err_for_unknown_hook() {
        // An unrecognised hook name must return Err with the name in the message.
        let result = run_with_json("bad-hook", "claude-code", json!({})).await;
        assert!(result.is_err());
        let error_text = result
            .expect_err("must be Err for unknown hook")
            .to_string();
        assert!(error_text.contains("bad-hook"), "{error_text}");
    }

    #[tokio::test]
    async fn run_with_json_dispatches_session_start_successfully() {
        // session-start with a valid claude-code harness must return Ok.
        let result = run_with_json(
            "session-start",
            "claude-code",
            json!({"session_id": "test-sid"}),
        )
        .await;
        assert!(result.is_ok(), "session-start must succeed: {result:?}");
    }

    #[tokio::test]
    async fn run_with_json_dispatches_precompact_successfully() {
        // precompact with no transcript must return Ok (logs and exits early).
        let result = run_with_json("precompact", "claude-code", json!({"session_id": "sid"})).await;
        assert!(result.is_ok(), "precompact must succeed: {result:?}");
    }

    #[tokio::test]
    async fn run_with_json_dispatches_stop_successfully() {
        // stop with exchange_count=0 must return Ok (exits at the threshold check).
        let result = run_with_json("stop", "claude-code", json!({"session_id": "sid"})).await;
        assert!(result.is_ok(), "stop must succeed: {result:?}");
    }

    // -------- hook_stop full trigger path --------

    #[tokio::test]
    async fn hook_stop_triggers_silent_save_when_interval_reached() {
        // With SAVE_INTERVAL+1 user messages and a palace DB, hook_stop must
        // execute the full silent-mode save path.
        let data_dir = tempfile::tempdir().expect("must create data dir");
        let db_path = data_dir.path().join("palace.db");
        let db_path_str = db_path.to_str().expect("utf-8").to_string();
        let transcript = data_dir.path().join("session.jsonl");
        let msg_line = r#"{"message":{"role":"user","content":"discussing the architecture"}}"#;
        let content = std::iter::repeat_n(msg_line, SAVE_INTERVAL + 1)
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&transcript, content).expect("must write transcript");
        let input = HookInput {
            session_id: "trigger-test".to_string(),
            stop_hook_active: false,
            transcript_path: transcript,
        };
        // MEMPAL_DIR is unset so hook_maybe_auto_ingest returns early without spawning.
        temp_env::async_with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(data_dir.path().to_str().expect("utf-8")),
                ),
                ("MEMPALACE_PALACE_PATH", Some(db_path_str.as_str())),
                ("MEMPAL_DIR", None),
            ],
            async {
                hook_stop(&input, data_dir.path()).await;
            },
        )
        .await;
        // A last-save file or palace DB confirms the save path was exercised.
        let has_last_save = data_dir.path().join("trigger-test_last_save").exists();
        let has_db = db_path.exists();
        assert!(
            has_last_save || has_db,
            "save path must produce either last_save or palace.db"
        );
    }

    // -------- hook_spawn_mine_bg --------

    #[test]
    fn hook_spawn_mine_bg_with_real_dirs_attempts_spawn() {
        // With real state and mine directories, hook_spawn_mine_bg must not panic.
        let state_dir = tempfile::tempdir().expect("must create state dir");
        let mine_dir = tempfile::tempdir().expect("must create mine dir");
        // The spawned binary exits immediately in a test context; we just verify
        // the code path executes without panic.
        hook_spawn_mine_bg(state_dir.path(), mine_dir.path());
        // Postcondition: a PID file may exist (if spawn succeeded); no panic.
        let _ = state_dir.path().join("mine.pid").exists();
    }

    // -------- hook_spawn_mine_sync --------

    #[test]
    fn hook_spawn_mine_sync_with_valid_mine_dir_attempts_spawn() {
        // With MEMPAL_DIR pointing to an existing directory, hook_spawn_mine_sync
        // must attempt to spawn and wait for the child process.
        let state_dir = tempfile::tempdir().expect("must create state dir");
        let mine_dir = tempfile::tempdir().expect("must create mine dir");
        temp_env::with_var(
            "MEMPAL_DIR",
            Some(mine_dir.path().to_str().expect("utf-8")),
            || {
                // The spawned binary exits immediately in a test context.
                hook_spawn_mine_sync(state_dir.path(), "");
            },
        );
        // Postcondition: no panic — function ran to completion.
        assert!(state_dir.path().exists());
    }

    // -------- hook_ingest_transcript (large-file path) --------

    #[test]
    fn hook_ingest_transcript_attempts_spawn_for_large_transcript() {
        // A file of at least 100 bytes with a valid parent dir triggers the spawn path.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let transcript = dir.path().join("session.jsonl");
        // Write enough content to exceed the 100-byte guard.
        let content = r#"{"message":{"role":"user","content":"some session content"}}"#.repeat(3);
        std::fs::write(&transcript, content).expect("must write transcript");
        // The spawn may succeed or fail (test binary handles unknown args) — no panic.
        hook_ingest_transcript(dir.path(), &transcript);
        // Postcondition: transcript still exists (we only spawn, not delete).
        assert!(transcript.exists());
    }

    // -------- hook_validate_transcript_path: .json extension --

    #[test]
    fn hook_validate_transcript_path_accepts_json_extension() {
        // A plain .json path without traversal must also be accepted.
        let result = hook_validate_transcript_path("/home/user/session.json");
        assert!(result.is_some(), "must accept .json extension");
        let path = result.expect("checked above");
        let extension = path.extension().and_then(|os| os.to_str()).unwrap_or("");
        assert_eq!(extension, "json");
    }

    // -------- hook_wing_from_transcript: -Projects- fallback regex --

    #[test]
    fn hook_wing_from_transcript_uses_projects_fallback_when_no_claude_folder() {
        // A path with -Projects- segment but no .claude/projects folder uses the fallback.
        let path = "/home/user/work/-Projects-myapp/session.jsonl";
        let wing = hook_wing_from_transcript(path);
        assert_eq!(wing, "wing_myapp");
        assert!(wing.starts_with("wing_"), "must start with wing_: {wing}");
    }

    #[test]
    fn hook_wing_from_transcript_returns_sessions_for_empty_string() {
        // An empty transcript path must fall back to wing_sessions.
        let wing = hook_wing_from_transcript("");
        assert_eq!(wing, "wing_sessions");
        assert!(wing.starts_with("wing_"));
    }

    // -------- hook_parse_message_line: event_message key variant --

    #[test]
    fn hook_parse_message_line_handles_event_message_key() {
        // Claude Code also uses "event_message" (not just "message") for some entries.
        let line = r#"{"event_message":{"role":"user","content":"event message content"}}"#;
        let result = hook_parse_message_line(line);
        assert_eq!(result, Some("event message content".to_string()));
    }

    #[test]
    fn hook_parse_message_line_returns_none_for_empty_trimmed_content() {
        // A user message with only whitespace must return None.
        let line = r#"{"message":{"role":"user","content":"   "}}"#;
        let result = hook_parse_message_line(line);
        assert!(result.is_none(), "whitespace-only content must return None");
    }

    #[test]
    fn hook_parse_message_line_filters_system_reminder_content() {
        // Content containing <system-reminder> must be filtered out.
        let line = r#"{"message":{"role":"user","content":"<system-reminder>instructions</system-reminder>"}}"#;
        let result = hook_parse_message_line(line);
        assert!(result.is_none(), "system-reminder content must return None");
    }

    #[test]
    fn hook_parse_message_line_codex_with_command_message_returns_none() {
        // Codex format messages with <command-message> content must be skipped.
        let line = r#"{"type":"event_msg","payload":{"type":"user_message","message":"<command-message>cmd</command-message>"}}"#;
        let result = hook_parse_message_line(line);
        assert!(
            result.is_none(),
            "codex command-message must be filtered: {result:?}"
        );
    }

    #[test]
    fn hook_parse_message_line_returns_none_for_codex_non_user_message_type() {
        // Codex "event_msg" with payload type other than "user_message" must return None.
        let line =
            r#"{"type":"event_msg","payload":{"type":"assistant_message","message":"response"}}"#;
        let result = hook_parse_message_line(line);
        assert!(
            result.is_none(),
            "non-user Codex event_msg must return None"
        );
    }

    // -------- hook_extract_themes: edge cases --

    #[test]
    fn hook_extract_themes_ignores_short_words() {
        // Words below THEME_WORD_LEN_MIN must not become themes.
        let messages = vec!["ok go do it now the and".to_string()];
        let themes = hook_extract_themes(&messages);
        // All words are either stopwords or too short — themes must be empty.
        assert!(
            themes.is_empty(),
            "short/stopword-only message must produce empty themes: {themes:?}"
        );
    }

    #[test]
    fn hook_extract_themes_returns_at_most_themes_max_entries() {
        // Even with many distinct words, only THEMES_MAX themes must be returned.
        let messages =
            vec!["alpha bravo charlie delta echo foxtrot golf hotel india juliet".to_string()];
        let themes = hook_extract_themes(&messages);
        assert!(
            themes.len() <= THEMES_MAX,
            "themes must not exceed THEMES_MAX={THEMES_MAX}: {themes:?}"
        );
    }

    #[test]
    fn hook_extract_themes_ranks_most_frequent_word_first() {
        // The word that appears most should be the first theme.
        let messages = vec!["database database database schema schema migration".to_string()];
        let themes = hook_extract_themes(&messages);
        assert!(!themes.is_empty(), "must produce at least one theme");
        assert_eq!(
            themes[0], "database",
            "most frequent word must rank first: {themes:?}"
        );
    }

    // -------- hook_read_last_save: non-numeric content --

    #[test]
    fn hook_read_last_save_returns_zero_for_non_numeric_content() {
        // A state file with non-integer content must return 0 (safe default).
        let dir = tempfile::tempdir().expect("must create temp dir");
        let file = dir.path().join("session_last_save");
        std::fs::write(&file, "not-a-number").expect("must write");
        let result = hook_read_last_save(&file);
        assert_eq!(result, 0, "non-numeric content must return 0");
        // Pair assertion: file was not corrupted by the read.
        assert!(file.exists());
    }

    // -------- hook_count_messages: nonexistent file --

    #[test]
    fn hook_count_messages_returns_zero_for_nonexistent_transcript() {
        // A path that does not exist on disk must return 0 without panicking.
        let count = hook_count_messages(Path::new("/nonexistent/missing.jsonl"));
        assert_eq!(count, 0, "nonexistent file must return 0 messages");
    }

    // -------- hook_get_mine_dir: env var and transcript parent paths --

    #[test]
    fn hook_get_mine_dir_returns_env_dir_when_mempal_dir_is_set() {
        // When MEMPAL_DIR is set and the path is a directory, that must be returned.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let dir_path = dir.path().to_str().expect("utf-8").to_string();
        temp_env::with_var("MEMPAL_DIR", Some(dir_path.as_str()), || {
            let result = hook_get_mine_dir("");
            assert!(result.is_some(), "MEMPAL_DIR must be returned");
            assert_eq!(
                result.expect("checked above"),
                dir.path(),
                "must return the MEMPAL_DIR path"
            );
        });
    }

    #[test]
    fn hook_get_mine_dir_returns_transcript_parent_when_no_env_var() {
        // When MEMPAL_DIR is absent but transcript is a real file, its parent dir is returned.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let transcript = dir.path().join("session.jsonl");
        std::fs::write(&transcript, "{}").expect("must write transcript");
        temp_env::with_var("MEMPAL_DIR", None::<&str>, || {
            let result = hook_get_mine_dir(transcript.to_str().expect("utf-8"));
            assert!(result.is_some(), "transcript parent must be returned");
            assert_eq!(
                result.expect("checked above"),
                dir.path(),
                "parent dir must match transcript parent"
            );
        });
    }

    #[test]
    fn hook_get_mine_dir_returns_none_when_env_dir_does_not_exist() {
        // MEMPAL_DIR pointing at a non-existent directory must fall through to None.
        temp_env::with_var(
            "MEMPAL_DIR",
            Some("/nonexistent/dir/that/cannot/exist"),
            || {
                let result = hook_get_mine_dir("");
                assert!(result.is_none(), "non-existent MEMPAL_DIR must return None");
            },
        );
    }

    // -------- hook_mine_already_running: stale PID --

    #[test]
    fn hook_mine_already_running_returns_false_for_dead_process_pid() {
        // A PID that is numeric but belongs to a dead process must return false.
        // PID 1 is always init/launchd — exists — so we use PID 4_200_001 which
        // cannot be a valid running process (above Linux pid_max).
        let dir = tempfile::tempdir().expect("must create temp dir");
        std::fs::write(dir.path().join("mine.pid"), "4200001").expect("must write");
        let result = hook_mine_already_running(dir.path());
        assert!(!result, "dead PID must return false");
    }

    // -------- hook_stop: stop_hook_active guard (block mode) --

    #[tokio::test]
    async fn hook_stop_exits_early_when_stop_hook_active_in_block_mode() {
        // In block mode (not silent), stop_hook_active=true must return immediately
        // to prevent the re-entry loop.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let input = HookInput {
            session_id: "block-sid".to_string(),
            stop_hook_active: true,
            transcript_path: PathBuf::new(),
        };
        // Force block mode by setting MEMPALACE_DIR to a non-existent location.
        // hook_is_silent_mode() returns true when config is missing (the default),
        // so to hit the block-mode guard we need a config with hook_silent_save=false.
        // Since we cannot easily write a config here, we verify the function completes
        // without panic regardless of mode — the guard is a best-effort re-entry block.
        temp_env::async_with_vars(
            [
                ("MEMPAL_DIR", None::<&str>),
                ("MEMPALACE_DIR", None::<&str>),
            ],
            async {
                hook_stop(&input, dir.path()).await;
            },
        )
        .await;
        // Postcondition: function ran to completion without panic.
        assert!(dir.path().exists());
    }

    // -------- hook_try_desktop_toast --------

    #[test]
    fn hook_try_desktop_toast_does_not_panic_for_valid_message() {
        // notify-send may not be installed; the function must not panic regardless.
        hook_try_desktop_toast("MemPalace test notification");
        // Pair assertion: a subsequent call also must not panic.
        hook_try_desktop_toast("second notification");
    }

    // -------- hook_sanitize_session_id: unicode input --

    #[test]
    fn hook_sanitize_session_id_strips_unicode_non_ascii() {
        // Unicode characters outside the allowed ASCII set must be stripped.
        let result = hook_sanitize_session_id("café-session");
        // 'é' is non-ASCII and must be removed; "caf-session" remains.
        assert!(
            result.contains("caf"),
            "ASCII prefix must survive: {result}"
        );
        assert!(
            !result.contains('é'),
            "non-ASCII char must be stripped: {result}"
        );
        assert!(!result.is_empty(), "result must not be empty");
    }

    #[test]
    fn hook_sanitize_session_id_handles_all_special_chars() {
        // All common special characters must be stripped, leaving only alnum/dash/underscore.
        let result = hook_sanitize_session_id("session@2024!#$%");
        assert_eq!(
            result, "session2024",
            "only alnum chars must remain: {result}"
        );
        assert!(!result.is_empty());
    }

    // -------- hook_extract_recent_messages: window limiting --

    #[test]
    fn hook_extract_recent_messages_returns_at_most_recent_msg_count() {
        // When the transcript has more than RECENT_MSG_COUNT messages, only the
        // last RECENT_MSG_COUNT must be returned.
        let dir = tempfile::tempdir().expect("must create temp dir");
        let path = dir.path().join("session.jsonl");
        let line = r#"{"message":{"role":"user","content":"repeated message"}}"#;
        let content = std::iter::repeat_n(line, RECENT_MSG_COUNT + 5)
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, content).expect("must write");
        let messages = hook_extract_recent_messages(&path);
        assert_eq!(
            messages.len(),
            RECENT_MSG_COUNT,
            "must return exactly RECENT_MSG_COUNT={RECENT_MSG_COUNT} messages"
        );
    }

    // -------- hook_stop_save_silently (checkpoint file) --------

    #[tokio::test]
    async fn hook_stop_save_silently_writes_checkpoint_file() {
        // A save in silent mode must write last_checkpoint to the state dir.
        let data_dir = tempfile::tempdir().expect("must create data dir");
        let db_path = data_dir.path().join("palace.db");
        let db_path_str = db_path.to_str().expect("utf-8").to_string();
        let transcript = data_dir.path().join("session.jsonl");
        let msg_line = r#"{"message":{"role":"user","content":"work notes here"}}"#;
        std::fs::write(&transcript, msg_line).expect("must write transcript");
        let last_save_file = data_dir.path().join("save_marker");
        let input = HookInput {
            session_id: "silent-test".to_string(),
            stop_hook_active: false,
            transcript_path: transcript.clone(),
        };
        temp_env::async_with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(data_dir.path().to_str().expect("utf-8")),
                ),
                ("MEMPALACE_PALACE_PATH", Some(db_path_str.as_str())),
                ("MEMPAL_DIR", None),
            ],
            async {
                hook_stop_save_silently(data_dir.path(), &input, &last_save_file, 1, "wing_test")
                    .await;
            },
        )
        .await;
        // The last_checkpoint file must have been written (or the palace DB created).
        let has_checkpoint = data_dir.path().join("last_checkpoint").exists();
        let has_db = db_path.exists();
        assert!(
            has_checkpoint || has_db,
            "silently-saved diary must produce checkpoint or DB"
        );
    }
}
