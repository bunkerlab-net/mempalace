//! Export palace drawers to markdown files on disk.
//!
//! Output layout:
//! ```text
//! output_dir/
//!   index.md        ← navigation index listing all wings and rooms
//!   <wing>/
//!     <room>.md   ← one file per wing/room combination
//! ```
//! Each file begins with a `# Wing / Room` header followed by one `###` section
//! per drawer, in chronological order.

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::io::Write as _;
use std::path::Path;

use turso::Connection;

use crate::db;
use crate::error::{Error, Result};

/// Refuse to write into `path` when it is itself a symbolic link.
///
/// Defense-in-depth: a pre-placed symlink at an export target would redirect
/// writes to wherever the link points (e.g. into system directories). Mirrors
/// the Python exporter's `_reject_symlink`. `path` may not exist yet; only an
/// existing symlink is rejected.
fn reject_symlink(path: &Path, label: &str) -> Result<()> {
    assert!(!label.is_empty(), "reject_symlink: label must not be empty");
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(Error::Other(format!(
            "refusing to export: {label} is a symbolic link ({}). \
             Remove the symlink or choose a different output path.",
            path.display()
        ))),
        _ => Ok(()),
    }
}

/// Open a file for writing, refusing to follow a symlink at the target path.
///
/// On POSIX, `O_NOFOLLOW` makes the open itself fail with `ELOOP` when the
/// final path component is a symlink — closing the TOCTOU window between an
/// `is_symlink` check and the open. On Windows we fall back to a pre-check
/// because `O_NOFOLLOW` isn't available; narrower than no check at all.
fn safe_open_for_write_truncate(path: &Path) -> Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let result = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path);
        match result {
            Ok(file) => Ok(file),
            Err(err) if err.raw_os_error() == Some(libc::ELOOP) => Err(Error::Other(format!(
                "refusing to write: {} is a symbolic link.",
                path.display()
            ))),
            Err(err) => Err(err.into()),
        }
    }
    #[cfg(not(unix))]
    {
        if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(Error::Other(format!(
                "refusing to write: {} is a symbolic link.",
                path.display()
            )));
        }
        Ok(std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?)
    }
}

/// `std::fs::write`-style wrapper that refuses to follow a symlink at `path`.
fn safe_write(path: &Path, content: &[u8]) -> Result<()> {
    let mut file = safe_open_for_write_truncate(path)?;
    file.write_all(content)?;
    Ok(())
}

/// Stats returned by `export_palace`.
pub struct ExportStats {
    /// Number of wings written.
    pub wings: usize,
    /// Number of rooms written.
    pub rooms: usize,
    /// Total drawers written.
    pub drawers: usize,
}

/// Export palace drawers to `output_dir`, one markdown file per wing/room.
///
/// When `wing` is `Some`, only drawers in that wing are exported.
/// When `dry_run` is `true`, files are not written but stats are still computed.
pub async fn export_palace(
    connection: &Connection,
    output_dir: &Path,
    wing: Option<&str>,
    dry_run: bool,
) -> Result<ExportStats> {
    assert!(
        !output_dir.as_os_str().is_empty(),
        "output_dir must not be empty"
    );

    let rows = export_query_drawers(connection, wing).await?;
    let grouped = export_group_by_room(&rows);

    let room_count = grouped.len();
    let wing_set: std::collections::BTreeSet<String> =
        grouped.keys().map(|(w, _)| w.clone()).collect();
    let wing_count = wing_set.len();
    let drawer_count: usize = grouped.values().map(Vec::len).sum();

    assert!(room_count <= drawer_count || drawer_count == 0);

    if !dry_run {
        // Refuse to scribble into a pre-placed symlink at output_dir before any
        // directories are created or files are written.
        reject_symlink(output_dir, "output_dir")?;
        for ((wing_name, room_name), sections) in &grouped {
            export_write_room(output_dir, wing_name, room_name, sections)?;
        }
        export_write_index(output_dir, &grouped)?;
    }

    let stats = ExportStats {
        wings: wing_count,
        rooms: room_count,
        drawers: drawer_count,
    };
    assert!(stats.rooms <= drawer_count || drawer_count == 0);
    Ok(stats)
}

/// Drawer row fetched for export (columns: content, wing, room, `filed_at`).
struct ExportRow {
    content: String,
    wing: String,
    room: String,
    filed_at: String,
}

/// Fetch all drawers for export, ordered by wing / room / `filed_at`.
async fn export_query_drawers(
    connection: &Connection,
    wing: Option<&str>,
) -> Result<Vec<ExportRow>> {
    if let Some(wing_name) = wing {
        assert!(
            !wing_name.is_empty(),
            "wing filter must not be empty string"
        );
    }

    let (sql, params) = if let Some(wing_name) = wing {
        (
            "SELECT content, wing, room, filed_at \
             FROM drawers WHERE wing = ?1 ORDER BY wing, room, filed_at ASC"
                .to_string(),
            vec![turso::Value::from(wing_name)],
        )
    } else {
        (
            "SELECT content, wing, room, filed_at \
             FROM drawers ORDER BY wing, room, filed_at ASC"
                .to_string(),
            vec![],
        )
    };

    let rows = db::query_all(connection, &sql, turso::params_from_iter(params)).await?;

    Ok(rows
        .iter()
        .map(|row| ExportRow {
            content: row
                .get_value(0)
                .ok()
                .and_then(|c| c.as_text().cloned())
                .unwrap_or_default(),
            wing: row
                .get_value(1)
                .ok()
                .and_then(|c| c.as_text().cloned())
                .unwrap_or_default(),
            room: row
                .get_value(2)
                .ok()
                .and_then(|c| c.as_text().cloned())
                .unwrap_or_default(),
            filed_at: row
                .get_value(3)
                .ok()
                .and_then(|c| c.as_text().cloned())
                .unwrap_or_default(),
        })
        .filter(|row| !row.wing.is_empty() && !row.room.is_empty())
        .collect())
}

/// Group drawer rows into a `BTreeMap<(wing, room), Vec<(filed_at, content)>>`.
fn export_group_by_room(rows: &[ExportRow]) -> BTreeMap<(String, String), Vec<(String, String)>> {
    let mut grouped: BTreeMap<(String, String), Vec<(String, String)>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry((row.wing.clone(), row.room.clone()))
            .or_default()
            .push((row.filed_at.clone(), row.content.clone()));
    }
    grouped
}

/// Write a single room's markdown file to `output_dir/wing/room.md`.
fn export_write_room(
    output_dir: &Path,
    wing_name: &str,
    room_name: &str,
    sections: &[(String, String)],
) -> Result<()> {
    assert!(!wing_name.is_empty());
    assert!(!room_name.is_empty());
    assert!(!sections.is_empty());

    let wing_dir = output_dir.join(sanitize_path_component(wing_name));
    // Refuse symlinks at the wing directory before create_dir_all silently
    // succeeds on a pre-placed symlink that targets an unrelated directory.
    reject_symlink(&wing_dir, &format!("wing directory {wing_name:?}"))?;
    std::fs::create_dir_all(&wing_dir)?;

    let file_name = format!("{}.md", sanitize_path_component(room_name));
    let file_path = wing_dir.join(file_name);

    let mut content = format!("# Wing: {wing_name} / Room: {room_name}\n\n");
    for (filed_at, text) in sections {
        let label = if filed_at.is_empty() {
            "undated"
        } else {
            filed_at.as_str()
        };
        content.push_str("### ");
        content.push_str(label);
        content.push_str("\n\n");
        content.push_str(text);
        content.push_str("\n\n---\n\n");
    }

    safe_write(&file_path, content.as_bytes())?;
    // Pair assertion: file must exist after write.
    assert!(file_path.exists(), "export file must exist after write");
    Ok(())
}

/// Write a navigation `index.md` to `output_dir` listing all wings and rooms.
///
/// Called by [`export_palace`] after all room files are written.
/// The index is a flat markdown file with one `##` heading per wing and one
/// `- [Room](wing/room.md)` link per room under each wing.
fn export_write_index(
    output_dir: &Path,
    grouped: &BTreeMap<(String, String), Vec<(String, String)>>,
) -> Result<()> {
    assert!(
        !output_dir.as_os_str().is_empty(),
        "export_write_index: output_dir must not be empty"
    );
    if grouped.is_empty() {
        return Ok(());
    }

    let mut content = String::from("# Palace Export\n\n");
    let mut current_wing = String::new();
    for (wing_name, room_name) in grouped.keys() {
        if wing_name != &current_wing {
            current_wing.clone_from(wing_name);
            // write! on String is infallible; the result is intentionally discarded.
            let _ = write!(content, "\n## {wing_name}\n\n");
        }
        let wing_slug = sanitize_path_component(wing_name);
        let room_slug = sanitize_path_component(room_name);
        let _ = writeln!(content, "- [{room_name}]({wing_slug}/{room_slug}.md)");
    }

    let index_path = output_dir.join("index.md");
    safe_write(&index_path, content.as_bytes())?;
    // Pair assertion: index.md must exist after write.
    assert!(index_path.exists(), "index.md must exist after write");
    Ok(())
}

/// Sanitize a string for use as a path component.
///
/// Replaces `/`, `\`, `:`, `*`, `?`, `"`, `<`, `>`, `|`, and control
/// characters with underscores to produce a safe filename on all platforms.
fn sanitize_path_component(name: &str) -> String {
    assert!(!name.is_empty(), "path component must not be empty");
    let sanitized: String = name
        .chars()
        .map(|character| {
            if character.is_control()
                || matches!(
                    character,
                    '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|'
                )
            {
                '_'
            } else {
                character
            }
        })
        .collect();
    // "." and ".." are reserved path components on all platforms; rewrite them
    // to prevent directory traversal when joined with output_dir.
    let sanitized = if sanitized == "." || sanitized == ".." {
        "_".to_string()
    } else {
        sanitized
    };
    assert!(!sanitized.is_empty());
    sanitized
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_path_component_clean_name() {
        assert_eq!(sanitize_path_component("my_project"), "my_project");
        assert_eq!(sanitize_path_component("backend"), "backend");
    }

    #[test]
    fn sanitize_path_component_replaces_forbidden_chars() {
        let sanitized = sanitize_path_component("wing/with:slashes");
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains(':'));
        assert_eq!(sanitized, "wing_with_slashes");
    }

    #[test]
    fn export_group_by_room_groups_correctly() {
        // Two drawers in the same wing/room must be grouped together.
        let rows = vec![
            ExportRow {
                content: "content_a".into(),
                wing: "proj".into(),
                room: "backend".into(),
                filed_at: "2024-01-01".into(),
            },
            ExportRow {
                content: "content_b".into(),
                wing: "proj".into(),
                room: "backend".into(),
                filed_at: "2024-01-02".into(),
            },
            ExportRow {
                content: "content_c".into(),
                wing: "proj".into(),
                room: "frontend".into(),
                filed_at: "2024-01-03".into(),
            },
        ];
        let grouped = export_group_by_room(&rows);
        assert_eq!(grouped.len(), 2, "two rooms must produce two groups");
        assert_eq!(
            grouped[&("proj".to_string(), "backend".to_string())].len(),
            2,
            "backend must have two drawers"
        );
        assert_eq!(
            grouped[&("proj".to_string(), "frontend".to_string())].len(),
            1,
            "frontend must have one drawer"
        );
    }

    #[tokio::test]
    async fn export_palace_writes_files() {
        // export_palace must create a markdown file per room and return correct stats.
        let (_db, connection) = crate::test_helpers::test_db().await;

        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "exp-001",
                wing: "alpha",
                room: "general",
                content: "exported content alpha",
                source_file: "a.rs",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for export test");

        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "exp-002",
                wing: "alpha",
                room: "backend",
                content: "exported content backend",
                source_file: "b.rs",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for second export test drawer");

        let output_dir =
            tempfile::tempdir().expect("failed to create temp output dir for export test");
        let stats = export_palace(&connection, output_dir.path(), None, false)
            .await
            .expect("export_palace must succeed");

        assert_eq!(stats.drawers, 2, "two drawers must be exported");
        assert_eq!(stats.wings, 1, "one wing must be present");
        assert_eq!(stats.rooms, 2, "two rooms must be present");

        // Pair assertion: files must exist on disk.
        assert!(
            output_dir.path().join("alpha").join("general.md").exists(),
            "alpha/general.md must be created"
        );
        assert!(
            output_dir.path().join("alpha").join("backend.md").exists(),
            "alpha/backend.md must be created"
        );
        // Navigation index must also be written.
        let index_path = output_dir.path().join("index.md");
        assert!(
            index_path.exists(),
            "index.md must be created by export_palace"
        );
        let index_content =
            std::fs::read_to_string(&index_path).expect("index.md must be readable");
        assert!(
            index_content.contains("alpha"),
            "index.md must reference the wing name"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn export_palace_rejects_symlink_output_dir() {
        // A pre-placed symlink at output_dir must be rejected before any data is written.
        // Defense-in-depth against an attacker placing a link that redirects exports.
        let (_db, connection) = crate::test_helpers::test_db().await;

        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "sym-001",
                wing: "alpha",
                room: "general",
                content: "content under a symlink target",
                source_file: "x.rs",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for symlink rejection test");

        let temp_dir = tempfile::tempdir().expect("create temp parent dir");
        let real_target = temp_dir.path().join("real_target");
        std::fs::create_dir(&real_target).expect("create real target dir");
        let link_path = temp_dir.path().join("link_output");
        std::os::unix::fs::symlink(&real_target, &link_path).expect("create symlink");

        let result = export_palace(&connection, &link_path, None, false).await;
        assert!(
            result.is_err(),
            "export_palace must refuse a symlinked output_dir"
        );
        let message = result.err().expect("error required").to_string();
        assert!(
            message.contains("symbolic link"),
            "error must mention symbolic link; got: {message}"
        );
        // Real target must remain untouched — no rooms written via the link.
        assert!(
            std::fs::read_dir(&real_target)
                .expect("read real target")
                .next()
                .is_none(),
            "real symlink target must not receive any exported files"
        );
    }

    #[cfg(unix)]
    #[test]
    fn safe_open_for_write_rejects_symlink_file() {
        // safe_open_for_write_truncate must refuse to write through a symlink at the
        // target path. O_NOFOLLOW closes the TOCTOU window in the open syscall itself.
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let real_file = temp_dir.path().join("real_file.md");
        std::fs::write(&real_file, b"original content").expect("seed real file");
        let link_file = temp_dir.path().join("link_file.md");
        std::os::unix::fs::symlink(&real_file, &link_file).expect("create file symlink");

        let result = safe_open_for_write_truncate(&link_file);
        assert!(
            result.is_err(),
            "open must refuse to follow a symlinked file"
        );
        // Pair assertion: original content must be intact.
        let kept = std::fs::read(&real_file).expect("read real file after rejected open");
        assert_eq!(
            kept, b"original content",
            "symlink target must remain unmodified"
        );
    }

    #[tokio::test]
    async fn export_palace_dry_run_writes_no_files() {
        // dry_run=true must not write any files but must return correct stats.
        let (_db, connection) = crate::test_helpers::test_db().await;

        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "dry-001",
                wing: "beta",
                room: "general",
                content: "dry run content",
                source_file: "c.rs",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add_drawer must succeed for dry-run export test");

        let output_dir =
            tempfile::tempdir().expect("failed to create temp output dir for dry-run export test");
        let stats = export_palace(&connection, output_dir.path(), None, true)
            .await
            .expect("export_palace dry_run must succeed");

        assert_eq!(stats.drawers, 1, "dry run must count one drawer");
        // Pair assertion: no files must be written.
        assert!(
            !output_dir.path().join("beta").exists(),
            "dry run must not create any directories"
        );
        assert!(
            !output_dir.path().join("index.md").exists(),
            "dry run must not create index.md"
        );
    }
}
