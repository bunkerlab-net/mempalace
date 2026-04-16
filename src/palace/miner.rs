use std::collections::HashMap;
use std::path::{Path, PathBuf};

use turso::Connection;

use crate::config::ProjectConfig;
use crate::error::Result;
use crate::palace::chunker::chunk_text;
use crate::palace::drawer;
use crate::palace::room_detect::{detect_room, is_skip_dir};

/// Options shared by `mine` and `mine_convos`.
pub struct MineParams {
    /// Override the wing name; `None` falls back to config / directory name.
    pub wing: Option<String>,
    /// Agent name recorded on each drawer.
    pub agent: String,
    /// Maximum files to process; `0` means unlimited.
    pub limit: usize,
    /// If `true`, show what would be filed without writing to the palace.
    pub dry_run: bool,
    /// If `true`, respect `.gitignore` rules when scanning (default: true).
    pub respect_gitignore: bool,
}

/// Files larger than this are skipped — prevents OOM on huge files.
const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

use super::WALK_DEPTH_LIMIT;

const READABLE_EXTENSIONS: &[&str] = &[
    "txt", "md", "py", "js", "ts", "jsx", "tsx", "json", "yaml", "yml", "html", "css", "java",
    "go", "rs", "rb", "sh", "csv", "sql", "toml", "c", "cpp", "h", "hpp", "swift", "kt", "scala",
    "lua", "r", "php", "pl", "zig", "nim", "ex", "exs", "erl", "hs", "ml",
];

const SKIP_FILES: &[&str] = &[
    "mempalace.yaml",
    "mempalace.yml",
    "mempal.yaml",
    "mempal.yml",
    ".gitignore",
    "package-lock.json",
    "Cargo.lock",
];

/// Scan a project directory for all readable files.
pub fn scan_project_with_opts(project_dir: &Path, respect_gitignore: bool) -> Vec<PathBuf> {
    assert!(
        project_dir.is_dir(),
        "project_dir must be a directory: {}",
        project_dir.display()
    );

    let files = if respect_gitignore {
        walk_dir_gitignore(project_dir)
    } else {
        let mut files = Vec::new();
        walk_dir(project_dir, &mut files);
        files
    };

    // Postcondition: all returned paths are files, not directories.
    debug_assert!(files.iter().all(|p| p.is_file()));

    files
}

fn walk_dir_gitignore(project_dir: &Path) -> Vec<PathBuf> {
    assert!(
        project_dir.is_dir(),
        "walk_dir_gitignore: path must be a directory"
    );

    let walker = ignore::WalkBuilder::new(project_dir)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .hidden(false) // We handle skip dirs ourselves
        // Cap depth to match walk_dir() so both scan paths respect the same limit.
        .max_depth(Some(WALK_DEPTH_LIMIT))
        .build();

    let mut files = Vec::new();
    for entry in walker.flatten() {
        if !entry.file_type().is_some_and(|ft| ft.is_file()) {
            continue;
        }
        let path = entry.path();

        // Check all path components against SKIP_DIRS
        let skip = path.components().any(|c| {
            let s = c.as_os_str().to_string_lossy();
            is_skip_dir(s.as_ref())
        });
        if skip {
            continue;
        }

        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if SKIP_FILES.contains(&name.as_str()) {
            continue;
        }

        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        if READABLE_EXTENSIONS.contains(&extension.as_str()) {
            if entry.metadata().is_ok_and(|m| m.len() > MAX_FILE_SIZE) {
                continue;
            }
            files.push(path.to_path_buf());
        }
    }
    files
}

fn walk_dir(directory: &Path, files: &mut Vec<PathBuf>) {
    // Iterative DFS with explicit depth tracking — no recursion.
    let mut stack: Vec<(PathBuf, usize)> = vec![(directory.to_path_buf(), 0)];

    while let Some((current_dir, depth)) = stack.pop() {
        assert!(
            depth <= WALK_DEPTH_LIMIT,
            "walk_dir: depth {depth} exceeds WALK_DEPTH_LIMIT"
        );
        // depth > WALK_DEPTH_LIMIT is unreachable: subdirectory pushes are guarded
        // below. This continue is a defensive safety net.
        if depth > WALK_DEPTH_LIMIT {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&current_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            // Skip symlinks — prevents following links to /dev/urandom etc.
            if path.is_symlink() {
                continue;
            }
            if path.is_dir() {
                // Only descend if we haven't reached the depth limit yet.
                if !is_skip_dir(&name) && depth < WALK_DEPTH_LIMIT {
                    stack.push((path, depth + 1));
                }
            } else if let Some(extension) = path.extension() {
                let extension_lower = extension.to_string_lossy().to_lowercase();
                if READABLE_EXTENSIONS.contains(&extension_lower.as_str())
                    && !SKIP_FILES.contains(&name.as_str())
                {
                    if std::fs::metadata(&path).is_ok_and(|m| m.len() > MAX_FILE_SIZE) {
                        continue;
                    }
                    files.push(path);
                }
            }
        }
    }
}

fn mine_print_header(
    wing: &str,
    rooms: &[crate::config::RoomConfig],
    file_count: usize,
    dry_run: bool,
) {
    println!("\n=======================================================");
    if dry_run {
        println!("  MemPalace Mine [DRY RUN]");
    } else {
        println!("  MemPalace Mine");
    }
    println!("=======================================================");
    println!("  Wing:    {wing}");
    println!(
        "  Rooms:   {}",
        rooms
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("  Files:   {file_count}");
    println!("-------------------------------------------------------\n");
}

fn mine_print_summary(
    dry_run: bool,
    file_count: usize,
    files_skipped: usize,
    files_unreadable_or_too_short: usize,
    total_drawers: usize,
    room_counts: &HashMap<String, usize>,
) {
    println!("\n=======================================================");
    if dry_run {
        println!("  Dry run complete — nothing was written.");
    } else {
        println!("  Done.");
    }
    let files_processed = file_count - files_skipped - files_unreadable_or_too_short;
    println!("  Files processed: {files_processed}");
    println!("  Files skipped (already filed): {files_skipped}");
    if files_unreadable_or_too_short > 0 {
        // mine_read_file() returns None for both unreadable files and files shorter than 50
        // characters — both cases are captured under this counter.
        println!("  Files skipped (empty/unreadable/too short): {files_unreadable_or_too_short}");
    }
    println!(
        "  Drawers {}: {total_drawers}",
        if dry_run { "would be filed" } else { "filed" }
    );
    println!("\n  By room:");

    let mut sorted_rooms: Vec<_> = room_counts.iter().collect();
    sorted_rooms.sort_by(|a, b| b.1.cmp(a.1));
    for (room, count) in sorted_rooms {
        println!("    {room:20} {count} files");
    }
    if !dry_run {
        println!("\n  Next: mempalace search \"what you're looking for\"");
    }
    println!("=======================================================\n");
}

/// Write all chunks for one file into the palace.
async fn mine_write_chunks(
    connection: &Connection,
    chunks: &[crate::palace::chunker::Chunk],
    wing: &str,
    room: &str,
    source_file: &str,
    source_mtime: Option<f64>,
    opts: &MineParams,
) -> Result<()> {
    for chunk in chunks {
        let id = format!(
            "drawer_{wing}_{room}_{}",
            &uuid::Uuid::new_v4().to_string().replace('-', "")[..16]
        );
        drawer::add_drawer(
            connection,
            &drawer::DrawerParams {
                id: &id,
                wing,
                room,
                content: &chunk.content,
                source_file,
                chunk_index: chunk.chunk_index,
                added_by: &opts.agent,
                ingest_mode: "projects",
                source_mtime,
            },
        )
        .await?;
    }
    Ok(())
}

/// Read a file's content, falling back to lossy UTF-8 on binary files.
/// Returns `None` if the file is unreadable or too short to be useful.
fn mine_read_file(filepath: &Path) -> Option<String> {
    let raw = match std::fs::read_to_string(filepath) {
        Ok(c) => c,
        Err(_) => match std::fs::read(filepath) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Err(_) => return None,
        },
    };
    let content = raw.trim().to_string();
    if content.len() < 50 {
        return None;
    }
    Some(content)
}

/// Process all files in the mine loop. Returns `(total_drawers, files_skipped, files_unreadable_or_too_short, room_counts)`.
async fn mine_process_files(
    connection: &Connection,
    files: &[PathBuf],
    wing: &str,
    rooms: &[crate::config::RoomConfig],
    project_dir: &Path,
    opts: &MineParams,
) -> Result<(usize, usize, usize, HashMap<String, usize>)> {
    let mut total_drawers: usize = 0;
    let mut files_skipped: usize = 0;
    let mut files_unreadable_or_too_short: usize = 0;
    let mut room_counts: HashMap<String, usize> = HashMap::new();

    for (i, filepath) in files.iter().enumerate() {
        let source_file = filepath.to_string_lossy().to_string();

        // Always check for duplicates so dry runs report accurate skip counts.
        // Only the write path below is gated on !opts.dry_run.
        if drawer::file_already_mined(connection, &source_file).await? {
            files_skipped += 1;
            continue;
        }

        let Some(content) = mine_read_file(filepath) else {
            files_unreadable_or_too_short += 1;
            continue;
        };

        let room = detect_room(filepath, &content, rooms, project_dir);
        let chunks = chunk_text(&content);
        let drawers_added = chunks.len();

        if !opts.dry_run {
            // Capture mtime now so all chunks from the same file share the same timestamp.
            let source_mtime: Option<f64> = std::fs::metadata(filepath)
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
                .map(|d| d.as_secs_f64());
            mine_write_chunks(
                connection,
                &chunks,
                wing,
                &room,
                &source_file,
                source_mtime,
                opts,
            )
            .await?;
        }

        total_drawers += drawers_added;
        *room_counts.entry(room.clone()).or_insert(0) += 1;
        println!(
            "  [{:4}/{}] {:50} +{drawers_added}",
            i + 1,
            files.len(),
            filepath.file_name().unwrap_or_default().to_string_lossy(),
        );
    }

    Ok((
        total_drawers,
        files_skipped,
        files_unreadable_or_too_short,
        room_counts,
    ))
}

/// Mine a project directory into the palace.
pub async fn mine(connection: &Connection, project_dir: &Path, opts: &MineParams) -> Result<()> {
    if !project_dir.is_dir() {
        return Err(crate::error::Error::Other(format!(
            "mine: directory not found or not a directory: {}",
            project_dir.display()
        )));
    }

    let project_dir = project_dir.canonicalize().map_err(|e| {
        crate::error::Error::Other(format!(
            "directory not found: {}: {e}",
            project_dir.display()
        ))
    })?;

    let config_path = project_dir.join("mempalace.yaml");
    let config = ProjectConfig::load(&config_path)?;

    let wing = opts.wing.as_deref().unwrap_or(&config.wing);
    let mut rooms = config.rooms;
    if rooms.is_empty() {
        // Empty rooms list in mempalace.yaml would violate detect_room's precondition;
        // fall back to a single general room so mining can proceed.
        rooms.push(crate::config::RoomConfig {
            name: "general".to_string(),
            description: String::new(),
            keywords: vec![],
        });
    }
    let all_files = scan_project_with_opts(&project_dir, opts.respect_gitignore);
    let files: Vec<_> = if opts.limit == 0 {
        all_files
    } else {
        all_files.into_iter().take(opts.limit).collect()
    };

    mine_print_header(wing, &rooms, files.len(), opts.dry_run);

    let (total_drawers, files_skipped, files_unreadable_or_too_short, room_counts) =
        mine_process_files(connection, &files, wing, &rooms, &project_dir, opts).await?;

    mine_print_summary(
        opts.dry_run,
        files.len(),
        files_skipped,
        files_unreadable_or_too_short,
        total_drawers,
        &room_counts,
    );
    Ok(())
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn scan_project_finds_text_files() {
        let dir = tempfile::tempdir().expect("tempdir should be created");

        // Create files with readable extensions.
        std::fs::write(dir.path().join("main.rs"), "fn main() {}")
            .expect("write .rs should succeed");
        std::fs::write(dir.path().join("notes.txt"), "hello world")
            .expect("write .txt should succeed");

        let files = scan_project_with_opts(dir.path(), true);
        let names: Vec<String> = files
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        assert!(
            names.contains(&"main.rs".to_string()),
            "Should find .rs files"
        );
        assert!(
            names.contains(&"notes.txt".to_string()),
            "Should find .txt files"
        );
    }

    #[test]
    fn scan_project_respects_gitignore() {
        let dir = tempfile::tempdir().expect("tempdir should be created");

        // Initialize a git repo so the ignore crate respects .gitignore.
        let git_init_output = std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .expect("git init command should run");
        assert!(
            git_init_output.status.success(),
            "git init failed: {:?} stdout={:?} stderr={:?}",
            git_init_output.status,
            String::from_utf8_lossy(&git_init_output.stdout),
            String::from_utf8_lossy(&git_init_output.stderr),
        );

        // Gitignore a .rs file — .log is not in READABLE_EXTENSIONS so it would
        // be skipped regardless of .gitignore.
        std::fs::write(dir.path().join(".gitignore"), "ignored.rs\n")
            .expect("write .gitignore should succeed");
        std::fs::write(dir.path().join("app.rs"), "fn main() {}")
            .expect("write .rs should succeed");
        std::fs::write(dir.path().join("ignored.rs"), "// ignored")
            .expect("write ignored.rs should succeed");

        let files = scan_project_with_opts(dir.path(), true);
        let names: Vec<String> = files
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        assert!(
            names.contains(&"app.rs".to_string()),
            "Non-ignored .rs should be found"
        );
        assert!(
            !names.contains(&"ignored.rs".to_string()),
            "Gitignored file should be excluded"
        );
    }

    #[test]
    fn scan_project_with_opts_ignores_gitignore() {
        let dir = tempfile::tempdir().expect("tempdir should be created");

        // Initialize a git repo so .gitignore is meaningful.
        let git_init_output = std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .expect("git init command should run");
        assert!(
            git_init_output.status.success(),
            "git init failed: {:?} stdout={:?} stderr={:?}",
            git_init_output.status,
            String::from_utf8_lossy(&git_init_output.stdout),
            String::from_utf8_lossy(&git_init_output.stderr),
        );

        std::fs::write(dir.path().join(".gitignore"), "ignored.rs\n")
            .expect("write .gitignore should succeed");
        std::fs::write(dir.path().join("app.rs"), "fn main() {}")
            .expect("write .rs should succeed");
        std::fs::write(dir.path().join("ignored.rs"), "// should appear")
            .expect("write ignored.rs should succeed");

        let files = scan_project_with_opts(dir.path(), false);
        let names: Vec<String> = files
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        assert!(
            names.contains(&"app.rs".to_string()),
            "Non-ignored .rs should be found"
        );
        assert!(
            names.contains(&"ignored.rs".to_string()),
            "With respect_gitignore=false, gitignored file should be included"
        );
    }

    #[test]
    fn scan_project_skips_node_modules() {
        let dir = tempfile::tempdir().expect("tempdir should be created");

        let nm = dir.path().join("node_modules");
        std::fs::create_dir(&nm).expect("create node_modules should succeed");
        std::fs::write(nm.join("foo.js"), "console.log('hi')")
            .expect("write foo.js should succeed");
        std::fs::write(dir.path().join("index.js"), "console.log('main')")
            .expect("write index.js should succeed");

        let files = scan_project_with_opts(dir.path(), true);
        let names: Vec<String> = files
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        assert!(
            names.contains(&"index.js".to_string()),
            "Top-level .js should be found"
        );
        assert!(
            !names.iter().any(|n| n == "foo.js"),
            "Files inside node_modules should be skipped"
        );
    }

    #[test]
    fn scan_project_skips_binary_extensions() {
        // Files with extensions not in READABLE_EXTENSIONS should be excluded.
        let dir = tempfile::tempdir().expect("tempdir should be created");

        std::fs::write(dir.path().join("image.png"), [0x89, 0x50, 0x4E, 0x47])
            .expect("write .png should succeed");
        std::fs::write(dir.path().join("code.rs"), "fn main() {}")
            .expect("write .rs should succeed");

        let files = scan_project_with_opts(dir.path(), true);
        let names: Vec<String> = files
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        assert!(
            names.contains(&"code.rs".to_string()),
            "Readable extension should be found"
        );
        assert!(
            !names.contains(&"image.png".to_string()),
            "Non-readable extension should be excluded"
        );
    }
}
