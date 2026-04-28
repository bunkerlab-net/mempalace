//! Ingest on-disk markdown diary files into the palace.
//!
//! Diary files must be named `YYYY-MM-DD*.md`. Each `## ` section header
//! inside the file becomes a separate drawer filed under `room = "diary"`,
//! matching the room name `tool_diary_read` filters on.
//! A per-file cursor stored in the config dir prevents re-ingesting sections
//! that were already processed on a previous run.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use sha2::Digest as _;
use turso::Connection;

use crate::config;
use crate::error::Result;
use crate::palace::drawer::{DrawerParams, add_drawer};

// Diary files shorter than this are skipped — they are almost certainly empty
// or contain only a date header with no content worth filing.
const MIN_FILE_BYTES: u64 = 50;

const _: () = assert!(MIN_FILE_BYTES > 0);

/// Stats returned by `ingest_diaries`.
pub struct DiaryStats {
    /// Number of diary files that had new sections ingested.
    pub days_updated: usize,
    /// Total new drawers created across all files.
    pub drawers_created: usize,
}

/// Per-file cursor state persisted across runs.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FileState {
    /// Number of sections ingested from this file on the last run.
    entry_count: usize,
    /// Byte length of the file on the last run.
    size: u64,
    /// Last modification time in Unix-epoch seconds. Pairs with `size` so a
    /// same-length edit (which would not change `size`) still invalidates the
    /// cursor. `serde(default)` keeps cursor files written before this field
    /// existed loadable; their `mtime == 0` will mismatch any real file mtime
    /// and force one safe re-ingest after upgrade.
    #[serde(default)]
    mtime: u64,
}

/// Read the file's modification time as Unix-epoch seconds.
///
/// Returns `0` when the platform or filesystem does not expose mtime — this
/// pairs with the resume check, which only trusts an mtime that is both
/// non-zero and equal across runs.
fn diary_ingest_file_mtime(path: &Path) -> u64 {
    path.metadata()
        .ok()
        .and_then(|meta| meta.modified().ok())
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_secs())
}

/// Ingest all diary files from `diary_dir` into the palace.
///
/// Only files named `YYYY-MM-DD*.md` are processed. New sections discovered
/// since the last run are filed as drawers under `wing`/`"diary"`. Passing
/// `force = true` re-ingests all sections regardless of cursor state.
pub async fn ingest_diaries(
    connection: &Connection,
    diary_dir: &Path,
    wing: &str,
    agent: &str,
    force: bool,
) -> Result<DiaryStats> {
    assert!(!wing.is_empty(), "wing must not be empty");
    assert!(!agent.is_empty(), "agent must not be empty");
    assert!(
        diary_dir.is_dir(),
        "diary_dir must be an existing directory"
    );

    let cursor_path = diary_ingest_cursor_path();
    let mut cursor = diary_ingest_load_cursor(&cursor_path);

    let files = diary_ingest_scan_files(diary_dir)?;
    let mut stats = DiaryStats {
        days_updated: 0,
        drawers_created: 0,
    };

    for file_path in &files {
        let created =
            diary_ingest_file(connection, file_path, wing, agent, force, &mut cursor).await?;
        if created > 0 {
            stats.days_updated += 1;
            stats.drawers_created += created;
        }
    }

    assert!(stats.drawers_created >= stats.days_updated);
    diary_ingest_save_cursor(&cursor_path, &cursor);

    Ok(stats)
}

/// Find `<config_dir>/diary_cursors.json`.
///
/// The cursor file is global (not per-directory) so ingestion state is shared
/// across all diary roots — a file ingested from one path is not re-ingested
/// from another.
fn diary_ingest_cursor_path() -> PathBuf {
    config::config_dir().join("diary_cursors.json")
}

/// Load cursor map from disk; returns empty map on any parse or I/O error.
fn diary_ingest_load_cursor(cursor_path: &Path) -> HashMap<String, FileState> {
    assert!(!cursor_path.as_os_str().is_empty());
    if !cursor_path.exists() {
        return HashMap::new();
    }
    std::fs::read_to_string(cursor_path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
        .unwrap_or_default()
}

/// Persist cursor map to disk; silently ignores write errors.
fn diary_ingest_save_cursor(cursor_path: &Path, cursor: &HashMap<String, FileState>) {
    assert!(!cursor_path.as_os_str().is_empty());
    if let Some(parent) = cursor_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(json) = serde_json::to_string_pretty(cursor) {
        let _ = std::fs::write(cursor_path, json);
    }
}

/// Return all diary files in `diary_dir` matching `YYYY-MM-DD*.md`.
fn diary_ingest_scan_files(diary_dir: &Path) -> Result<Vec<PathBuf>> {
    assert!(diary_dir.is_dir());

    let mut files: Vec<PathBuf> = std::fs::read_dir(diary_dir)?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| diary_ingest_is_diary_file(path))
        .collect();

    // Stable sort so ingestion order is deterministic across runs.
    files.sort();
    Ok(files)
}

/// Return `true` when `path` looks like a diary file (`YYYY-MM-DD*.md`).
fn diary_ingest_is_diary_file(path: &Path) -> bool {
    if path.extension().and_then(|extension| extension.to_str()) != Some("md") {
        return false;
    }
    let stem = path
        .file_stem()
        .and_then(|stem_os| stem_os.to_str())
        .unwrap_or("");
    // Require at least "YYYY-MM-DD" at the start (10 bytes; all ASCII so byte == char length).
    // Work on raw bytes to avoid a char-boundary panic for non-ASCII filenames.
    let bytes = stem.as_bytes();
    if bytes.len() < 10 {
        return false;
    }
    // Pattern: 4 digits, '-', 2 digits, '-', 2 digits.
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

/// Ingest one diary file; return the number of new drawers created.
async fn diary_ingest_file(
    connection: &Connection,
    file_path: &Path,
    wing: &str,
    agent: &str,
    force: bool,
    cursor: &mut HashMap<String, FileState>,
) -> Result<usize> {
    assert!(file_path.exists());

    let file_size = file_path.metadata().map_or(0, |meta| meta.len());
    if file_size < MIN_FILE_BYTES {
        return Ok(0);
    }

    let file_mtime = diary_ingest_file_mtime(file_path);
    let key = file_path.to_string_lossy().to_string();
    // Pull two distinct values from the cursor:
    //   * `prev_count` — sections to skip on resume (0 when force or mismatch);
    //   * `previous_entry_count` — total sections recorded on the last run,
    //     used to purge orphaned drawers when the file has shrunk since.
    let cursor_state = cursor.get(&key);
    let previous_entry_count = cursor_state.map_or(0, |state| state.entry_count);
    let prev_count = diary_ingest_resume_count(cursor_state, file_size, file_mtime, force);

    let text = std::fs::read_to_string(file_path).unwrap_or_default();
    let sections = diary_ingest_parse_sections(&text);
    let date_prefix = diary_ingest_extract_date(file_path);

    let created = diary_ingest_file_apply_sections(
        connection,
        file_path,
        DiarySectionContext {
            wing,
            agent,
            force,
            prev_count,
            date_prefix: &date_prefix,
        },
        &sections,
    )
    .await?;

    // Purge drawers for sections that disappeared since the last run. Without
    // this, deleting a `## Header` block from a file would leave its drawer
    // floating in the palace forever, and a subsequent `--force` run would
    // only refresh the surviving indices instead of cleaning up the tail.
    diary_ingest_file_purge_orphans(
        connection,
        wing,
        &date_prefix,
        sections.len(),
        previous_entry_count,
    )
    .await?;

    cursor.insert(
        key,
        FileState {
            entry_count: sections.len(),
            size: file_size,
            mtime: file_mtime,
        },
    );
    Ok(created)
}

/// Bundle of constant per-file parameters threaded through section ingestion.
///
/// Grouped into a struct so `diary_ingest_file_apply_sections` keeps a small,
/// comprehensible parameter list rather than exploding into 7 positional args.
struct DiarySectionContext<'a> {
    wing: &'a str,
    agent: &'a str,
    force: bool,
    prev_count: usize,
    date_prefix: &'a str,
}

/// Called by `diary_ingest_file` to compute the resume index from the cursor.
///
/// Returns `0` when `force` is set or when the recorded `(size, mtime)` does
/// not match the current file. Requires `file_mtime > 0` so a platform that
/// cannot report mtime (or a pre-upgrade cursor file) re-ingests safely
/// instead of matching a degenerate zero.
fn diary_ingest_resume_count(
    cursor_state: Option<&FileState>,
    file_size: u64,
    file_mtime: u64,
    force: bool,
) -> usize {
    if force {
        return 0;
    }
    cursor_state
        .filter(|state| state.size == file_size && state.mtime == file_mtime && file_mtime > 0)
        .map_or(0, |state| state.entry_count)
}

/// Called by `diary_ingest_file` to insert/replace drawers for each section.
///
/// Skips the first `ctx.prev_count` sections (resume) and returns the number
/// of new drawers actually created.
async fn diary_ingest_file_apply_sections(
    connection: &Connection,
    file_path: &Path,
    ctx: DiarySectionContext<'_>,
    sections: &[(String, String)],
) -> Result<usize> {
    let mut created = 0usize;
    let source_path = file_path.to_string_lossy();
    for (index, (header, body)) in sections.iter().enumerate().skip(ctx.prev_count) {
        let label = if header.is_empty() {
            ctx.date_prefix
        } else {
            header.as_str()
        };
        let content = format!("{label}\n\n{body}");
        let drawer_id = diary_ingest_drawer_id(ctx.wing, ctx.date_prefix, index);

        let params = DrawerParams {
            id: &drawer_id,
            wing: ctx.wing,
            // Must match the room filter used by `tool_diary_read` in
            // src/mcp/tools.rs (`WHERE room = 'diary'`); the previous
            // value `"daily"` made ingested entries invisible to readers.
            room: "diary",
            content: &content,
            source_file: source_path.as_ref(),
            chunk_index: index,
            added_by: ctx.agent,
            ingest_mode: "diary",
            source_mtime: None,
        };
        let inserted =
            diary_ingest_file_replace(connection, &drawer_id, &params, ctx.force).await?;
        if inserted {
            created += 1;
        }
    }
    Ok(created)
}

/// Called by `diary_ingest_file` to delete drawers for sections that no longer
/// exist in the source file.
///
/// Iterates the open range `current_count..previous_count` so a shrunk file
/// does not leave stranded drawers behind. When the file grew or stayed the
/// same length this is a zero-iteration no-op.
async fn diary_ingest_file_purge_orphans(
    connection: &Connection,
    wing: &str,
    date_prefix: &str,
    current_count: usize,
    previous_count: usize,
) -> Result<()> {
    assert!(!wing.is_empty());
    assert!(!date_prefix.is_empty());
    if current_count >= previous_count {
        return Ok(());
    }
    for stale_index in current_count..previous_count {
        let drawer_id = diary_ingest_drawer_id(wing, date_prefix, stale_index);
        diary_ingest_file_purge_drawer(connection, &drawer_id).await?;
    }
    Ok(())
}

/// Called by `diary_ingest_file` when `--force` is set so `add_drawer` (INSERT OR
/// IGNORE) refreshes existing diary content instead of silently no-op'ing on the
/// duplicate id.
async fn diary_ingest_file_purge_drawer(connection: &Connection, drawer_id: &str) -> Result<()> {
    assert!(
        !drawer_id.is_empty(),
        "diary_ingest_file_purge_drawer: drawer_id must not be empty"
    );
    // Negative-space precondition: drawer ids never contain SQL wildcards or path
    // separators — those would indicate a bug in diary_ingest_drawer_id, not a
    // user-supplied value.
    assert!(
        !drawer_id.contains('%') && !drawer_id.contains('/'),
        "diary_ingest_file_purge_drawer: drawer_id must not contain wildcards or slashes"
    );
    // Mirror `dedup_delete_drawer_inner`: clean every table that can hold a
    // back-reference to this drawer id. Without these cleanups a force
    // re-ingest could leave stale `compressed`/`triples`/`explicit_tunnels`
    // rows alongside a fresh drawer row with the same id, silently
    // contaminating closet boost lookups and KG provenance.
    //
    // The caller (`diary_ingest_file_replace`) wraps this function plus the
    // follow-up `add_drawer` in a single `BEGIN`/`COMMIT`, so all five
    // statements + the insert succeed or roll back together.
    connection
        .execute("DELETE FROM drawers WHERE id = ?", (drawer_id,))
        .await?;
    connection
        .execute("DELETE FROM drawer_words WHERE drawer_id = ?", (drawer_id,))
        .await?;
    connection
        .execute("DELETE FROM compressed WHERE id = ?", (drawer_id,))
        .await?;
    connection
        .execute(
            "UPDATE triples SET source_drawer_id = NULL WHERE source_drawer_id = ?",
            (drawer_id,),
        )
        .await?;
    connection
        .execute(
            "DELETE FROM explicit_tunnels WHERE source_drawer_id = ? OR target_drawer_id = ?",
            (drawer_id, drawer_id),
        )
        .await?;
    Ok(())
}

/// Replace one diary section atomically when `force` is set; otherwise fall through
/// to a plain `add_drawer` call.
///
/// Wraps the force-purge + `add_drawer` pair in a single `BEGIN`/`COMMIT` so
/// a failed insert restores the prior row instead of leaving the palace with
/// a hole and the cursor advanced past the missing section. Called by
/// [`diary_ingest_file`] to keep that function within the 70-line limit.
async fn diary_ingest_file_replace(
    connection: &Connection,
    drawer_id: &str,
    params: &DrawerParams<'_>,
    force: bool,
) -> Result<bool> {
    if !force {
        // Plain INSERT OR IGNORE; no transaction needed because the call is
        // already a single atomic statement.
        return add_drawer(connection, params).await;
    }

    connection.execute("BEGIN", ()).await?;
    if let Err(error) = diary_ingest_file_purge_drawer(connection, drawer_id).await {
        let _ = connection.execute("ROLLBACK", ()).await;
        return Err(error);
    }
    match add_drawer(connection, params).await {
        Ok(value) => {
            connection.execute("COMMIT", ()).await?;
            Ok(value)
        }
        Err(error) => {
            let _ = connection.execute("ROLLBACK", ()).await;
            Err(error)
        }
    }
}

/// Split diary text on `## ` H2 headers.
///
/// Returns a list of `(header_line, body)` pairs. If the file has no `## ` headers
/// the entire text is returned as a single section with an empty header.
fn diary_ingest_parse_sections(text: &str) -> Vec<(String, String)> {
    let mut sections: Vec<(String, String)> = Vec::new();
    let mut current_header = String::new();
    let mut current_body = String::new();

    for line in text.lines() {
        if let Some(stripped) = line.strip_prefix("## ") {
            // Flush the previous section (if non-empty body).
            if !current_body.trim().is_empty() || !current_header.is_empty() {
                sections.push((current_header, current_body.trim_end().to_string()));
            }
            current_header = stripped.to_string();
            current_body = String::new();
        } else {
            current_body.push_str(line);
            current_body.push('\n');
        }
    }

    // Flush the last section.
    if !current_body.trim().is_empty() || !current_header.is_empty() {
        sections.push((current_header, current_body.trim_end().to_string()));
    }

    assert!(sections.len() <= text.lines().count() + 1);
    sections
}

/// Extract the `YYYY-MM-DD` date prefix from the filename.
fn diary_ingest_extract_date(file_path: &Path) -> String {
    let stem = file_path
        .file_stem()
        .and_then(|stem_os| stem_os.to_str())
        .unwrap_or("unknown");
    if stem.len() >= 10 {
        stem[..10].to_string()
    } else {
        stem.to_string()
    }
}

/// Generate a stable, unique drawer ID for a diary section.
///
/// Format: `diary-{wing_prefix}-{wing_hash}-{date}-{index}`. The 20-char
/// sanitized prefix keeps IDs human-readable; the 8-hex-char SHA-256 slug of
/// the original (unsanitized) wing string disambiguates wings that would
/// otherwise collapse to the same prefix — e.g. `"work_2024_q1"` and
/// `"work_2024_q2"` truncate identically at 20 chars, and `"work/notes"` and
/// `"work_notes"` sanitize identically.
fn diary_ingest_drawer_id(wing: &str, date: &str, index: usize) -> String {
    assert!(!wing.is_empty());
    assert!(!date.is_empty());

    let wing_prefix: String = wing
        .chars()
        .take(20)
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let hash = sha2::Sha256::digest(wing.as_bytes());
    let wing_hash: String = hash.iter().take(4).fold(String::new(), |mut acc, byte| {
        use std::fmt::Write as _;
        // 4 bytes → 8 hex chars; collisions across the full 32-bit space are
        // a non-concern for per-wing diary IDs (a user with >65k wings would
        // hit far worse problems first).
        let _ = write!(acc, "{byte:02x}");
        acc
    });
    assert!(wing_hash.len() == 8, "wing hash must be 8 hex chars");
    let id = format!("diary-{wing_prefix}-{wing_hash}-{date}-{index}");
    assert!(!id.is_empty());
    id
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn is_diary_file_accepts_valid_pattern() {
        assert!(diary_ingest_is_diary_file(Path::new("2024-01-15.md")));
        assert!(diary_ingest_is_diary_file(Path::new("2024-12-31-notes.md")));
        assert!(diary_ingest_is_diary_file(Path::new(
            "/abs/path/2024-06-01.md"
        )));
    }

    #[test]
    fn is_diary_file_rejects_invalid_patterns() {
        assert!(!diary_ingest_is_diary_file(Path::new("notes.md")));
        assert!(!diary_ingest_is_diary_file(Path::new("2024-01.md")));
        assert!(!diary_ingest_is_diary_file(Path::new("2024-01-15.txt")));
        assert!(!diary_ingest_is_diary_file(Path::new("notadate-01-15.md")));
    }

    #[test]
    fn parse_sections_single_section() {
        let text = "## My Day\n\nSome content here.\nMore content.";
        let sections = diary_ingest_parse_sections(text);
        assert_eq!(sections.len(), 1, "one section expected");
        assert_eq!(sections[0].0, "My Day");
        assert!(sections[0].1.contains("Some content here."));
    }

    #[test]
    fn parse_sections_multiple_sections() {
        let text = "## Morning\n\nGot up early.\n\n## Evening\n\nHad dinner.";
        let sections = diary_ingest_parse_sections(text);
        assert_eq!(sections.len(), 2, "two sections expected");
        assert_eq!(sections[0].0, "Morning");
        assert_eq!(sections[1].0, "Evening");
    }

    #[test]
    fn parse_sections_no_headers_is_single_section() {
        let text = "Just some plain diary text without any headers.";
        let sections = diary_ingest_parse_sections(text);
        assert_eq!(sections.len(), 1);
        assert!(
            sections[0].0.is_empty(),
            "header must be empty when no ## found"
        );
    }

    #[test]
    fn extract_date_from_standard_filename() {
        let path = Path::new("/diary/2024-03-15.md");
        assert_eq!(diary_ingest_extract_date(path), "2024-03-15");
    }

    #[test]
    fn extract_date_from_extended_filename() {
        let path = Path::new("2024-03-15-work-notes.md");
        assert_eq!(diary_ingest_extract_date(path), "2024-03-15");
    }

    #[test]
    fn drawer_id_is_stable_and_non_empty() {
        let id1 = diary_ingest_drawer_id("myproject", "2024-01-15", 0);
        let id2 = diary_ingest_drawer_id("myproject", "2024-01-15", 0);
        assert_eq!(id1, id2, "drawer ID must be deterministic");
        assert!(!id1.is_empty());
        assert!(id1.starts_with("diary-"));
    }

    #[test]
    fn drawer_id_disambiguates_wings_that_sanitize_or_truncate_alike() {
        // `work/notes` and `work_notes` both sanitize to the same wing_prefix;
        // before the hash slug they would collide. The same is true of two
        // wings that truncate identically at 20 chars. Both pairs must now
        // emit distinct IDs because the hash is computed on the original
        // unsanitized, untruncated wing string.
        let slash = diary_ingest_drawer_id("work/notes", "2024-01-15", 0);
        let underscore = diary_ingest_drawer_id("work_notes", "2024-01-15", 0);
        assert_ne!(
            slash, underscore,
            "wings that sanitize identically must still produce distinct IDs"
        );

        let q1 = diary_ingest_drawer_id("work_2024_quarter_one_long", "2024-01-15", 0);
        let q2 = diary_ingest_drawer_id("work_2024_quarter_two_long", "2024-01-15", 0);
        assert_ne!(
            q1, q2,
            "wings that truncate identically must still produce distinct IDs"
        );
    }

    #[tokio::test]
    async fn ingest_diaries_creates_drawers_from_diary_files() {
        // A diary directory with one YYYY-MM-DD.md file must produce drawers.
        let diary_dir =
            tempfile::tempdir().expect("failed to create temp diary dir for ingest test");
        let diary_file = diary_dir.path().join("2024-01-15.md");
        std::fs::write(
            &diary_file,
            "## Morning\n\nHad coffee and read the news.\n\n## Evening\n\nFinished the report.",
        )
        .expect("failed to write test diary file");

        // Redirect `config_dir()` to a fresh tempdir so this test's
        // `diary_cursors.json` cannot collide with the real user config or
        // with sibling tests running in parallel.
        let cursor_home =
            tempfile::tempdir().expect("failed to create temp cursor home for ingest test");
        let cursor_home_path = cursor_home
            .path()
            .to_str()
            .expect("temp cursor home path must be valid UTF-8")
            .to_string();
        temp_env::async_with_vars(
            [("MEMPALACE_DIR", Some(cursor_home_path.as_str()))],
            async {
                let (_db, connection) = crate::test_helpers::test_db().await;
                let stats = ingest_diaries(
                    &connection,
                    diary_dir.path(),
                    "journal",
                    "test_agent",
                    false,
                )
                .await
                .expect("ingest_diaries must succeed");

                assert_eq!(stats.days_updated, 1, "one diary file must be updated");
                assert_eq!(
                    stats.drawers_created, 2,
                    "two sections must produce two drawers"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn ingest_diaries_skips_non_diary_files() {
        // A directory with only non-matching files must produce zero drawers.
        let diary_dir = tempfile::tempdir().expect("failed to create temp diary dir for skip test");
        std::fs::write(
            diary_dir.path().join("notes.md"),
            "Some random notes that are long enough to pass the size filter and contain content.",
        )
        .expect("failed to write non-diary file");

        let cursor_home =
            tempfile::tempdir().expect("failed to create temp cursor home for skip test");
        let cursor_home_path = cursor_home
            .path()
            .to_str()
            .expect("temp cursor home path must be valid UTF-8")
            .to_string();
        temp_env::async_with_vars(
            [("MEMPALACE_DIR", Some(cursor_home_path.as_str()))],
            async {
                let (_db, connection) = crate::test_helpers::test_db().await;
                let stats = ingest_diaries(
                    &connection,
                    diary_dir.path(),
                    "journal",
                    "test_agent",
                    false,
                )
                .await
                .expect("ingest_diaries must succeed with no matching files");

                assert_eq!(stats.days_updated, 0, "non-diary file must be skipped");
                assert_eq!(stats.drawers_created, 0);
            },
        )
        .await;
    }

    #[tokio::test]
    async fn ingest_diaries_purges_drawers_for_deleted_sections() {
        // First ingest creates two drawers; a later ingest after one section
        // is removed must purge the orphaned drawer so a subsequent --force
        // run does not silently leave a stale section behind. Prior to the
        // purge fix, `diary-…-1` would persist in `drawers` forever.
        let diary_dir =
            tempfile::tempdir().expect("failed to create temp diary dir for shrink test");
        let diary_file = diary_dir.path().join("2024-04-01.md");
        std::fs::write(
            &diary_file,
            "## Morning\n\nFirst section with enough content to clear the size floor.\n\n## Evening\n\nSecond section with enough content to clear the size floor.",
        )
        .expect("failed to write initial diary file for shrink test");

        let cursor_home =
            tempfile::tempdir().expect("failed to create temp cursor home for shrink test");
        let cursor_home_path = cursor_home
            .path()
            .to_str()
            .expect("temp cursor home path must be valid UTF-8")
            .to_string();
        temp_env::async_with_vars(
            [("MEMPALACE_DIR", Some(cursor_home_path.as_str()))],
            async {
                let (_db, connection) = crate::test_helpers::test_db().await;

                let stats1 = ingest_diaries(
                    &connection,
                    diary_dir.path(),
                    "journal",
                    "test_agent",
                    false,
                )
                .await
                .expect("first ingest must succeed");
                assert_eq!(
                    stats1.drawers_created, 2,
                    "first run must create both sections"
                );

                // Sleep one second so the rewrite gets a different mtime;
                // resume requires both size and mtime to match, and on
                // some filesystems same-second writes round to the same
                // mtime which would short-circuit the shrink path.
                std::thread::sleep(std::time::Duration::from_secs(1));
                std::fs::write(
                    &diary_file,
                    "## Morning\n\nFirst section with enough content to clear the size floor.",
                )
                .expect("failed to write shrunk diary file");

                let _stats2 = ingest_diaries(
                    &connection,
                    diary_dir.path(),
                    "journal",
                    "test_agent",
                    false,
                )
                .await
                .expect("second ingest must succeed");

                // The dropped section's drawer must have been purged. We
                // query the drawers table directly so the test exercises
                // the real database state, not just the returned stats.
                let rows = crate::db::query_all(
                    &connection,
                    "SELECT COUNT(*) FROM drawers WHERE room = 'diary'",
                    (),
                )
                .await
                .expect("count query must succeed");
                let count: i64 = rows
                    .first()
                    .and_then(|row| row.get(0).ok())
                    .expect("count query must return one row with one column");
                assert_eq!(
                    count, 1,
                    "deleting a section must leave exactly one diary drawer"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn ingest_diaries_cursor_prevents_re_ingest() {
        // Running ingest twice must only create drawers on the first run.
        let diary_dir =
            tempfile::tempdir().expect("failed to create temp diary dir for cursor test");
        let diary_file = diary_dir.path().join("2024-02-01.md");
        std::fs::write(
            &diary_file,
            "## Afternoon\n\nWorked on the project and made good progress today.",
        )
        .expect("failed to write test diary file for cursor test");

        let cursor_home =
            tempfile::tempdir().expect("failed to create temp cursor home for cursor test");
        let cursor_home_path = cursor_home
            .path()
            .to_str()
            .expect("temp cursor home path must be valid UTF-8")
            .to_string();
        temp_env::async_with_vars(
            [("MEMPALACE_DIR", Some(cursor_home_path.as_str()))],
            async {
                let (_db, connection) = crate::test_helpers::test_db().await;
                let stats1 = ingest_diaries(
                    &connection,
                    diary_dir.path(),
                    "journal",
                    "test_agent",
                    false,
                )
                .await
                .expect("first ingest must succeed");
                let stats2 = ingest_diaries(
                    &connection,
                    diary_dir.path(),
                    "journal",
                    "test_agent",
                    false,
                )
                .await
                .expect("second ingest must succeed");

                assert_eq!(
                    stats1.drawers_created, 1,
                    "first run must create one drawer"
                );
                assert_eq!(
                    stats2.drawers_created, 0,
                    "second run must create no new drawers"
                );
            },
        )
        .await;
    }
}
