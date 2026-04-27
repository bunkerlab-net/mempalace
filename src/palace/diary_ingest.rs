//! Ingest on-disk markdown diary files into the palace.
//!
//! Diary files must be named `YYYY-MM-DD*.md`. Each `## ` section header
//! inside the file becomes a separate drawer filed under `room = "daily"`.
//! A per-file cursor stored in the config dir prevents re-ingesting sections
//! that were already processed on a previous run.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

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
}

/// Ingest all diary files from `diary_dir` into the palace.
///
/// Only files named `YYYY-MM-DD*.md` are processed. New sections discovered
/// since the last run are filed as drawers under `wing`/`"daily"`. Passing
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

    let key = file_path.to_string_lossy().to_string();
    let prev_count = if force {
        0
    } else {
        cursor
            .get(&key)
            .and_then(|state| {
                // Only resume from cursor if the file size has not changed.
                if state.size == file_size {
                    Some(state.entry_count)
                } else {
                    None
                }
            })
            .unwrap_or(0)
    };

    let text = std::fs::read_to_string(file_path).unwrap_or_default();
    let sections = diary_ingest_parse_sections(&text);
    let date_prefix = diary_ingest_extract_date(file_path);

    let mut created = 0usize;
    for (index, (header, body)) in sections.iter().enumerate().skip(prev_count) {
        let label = if header.is_empty() {
            &date_prefix
        } else {
            header.as_str()
        };
        let content = format!("{label}\n\n{body}");
        let drawer_id = diary_ingest_drawer_id(wing, &date_prefix, index);
        let source_path = file_path.to_string_lossy();

        let inserted = add_drawer(
            connection,
            &DrawerParams {
                id: &drawer_id,
                wing,
                room: "daily",
                content: &content,
                source_file: source_path.as_ref(),
                chunk_index: index,
                added_by: agent,
                ingest_mode: "diary",
                source_mtime: None,
            },
        )
        .await?;

        if inserted {
            created += 1;
        }
    }

    cursor.insert(
        key,
        FileState {
            entry_count: sections.len(),
            size: file_size,
        },
    );
    Ok(created)
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
/// Format: `diary-{wing_prefix}-{date}-{index}`. Wing is truncated to 20 chars
/// and sanitized to avoid characters that are invalid in drawer IDs.
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
    let id = format!("diary-{wing_prefix}-{date}-{index}");
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
    }
}
