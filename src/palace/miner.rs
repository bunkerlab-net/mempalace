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
///
/// When `respect_gitignore` is `true`, `.gitignore` rules are applied via the
/// `ignore` crate (same engine as ripgrep). `SKIP_DIRS` and `SKIP_FILES` are always
/// applied regardless.
pub fn scan_project(project_dir: &Path) -> Vec<PathBuf> {
    scan_project_with_opts(project_dir, true)
}

/// Scan with explicit gitignore control.
pub fn scan_project_with_opts(project_dir: &Path, respect_gitignore: bool) -> Vec<PathBuf> {
    if respect_gitignore {
        walk_dir_gitignore(project_dir)
    } else {
        let mut files = Vec::new();
        walk_dir(project_dir, &mut files);
        files
    }
}

fn walk_dir_gitignore(project_dir: &Path) -> Vec<PathBuf> {
    let walker = ignore::WalkBuilder::new(project_dir)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .hidden(false) // We handle skip dirs ourselves
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

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        if READABLE_EXTENSIONS.contains(&ext.as_str()) {
            files.push(path.to_path_buf());
        }
    }
    files
}

fn walk_dir(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        if path.is_dir() {
            if !is_skip_dir(&name) {
                walk_dir(&path, files);
            }
        } else if let Some(ext) = path.extension() {
            let ext_lower = ext.to_string_lossy().to_lowercase();
            if READABLE_EXTENSIONS.contains(&ext_lower.as_str())
                && !SKIP_FILES.contains(&name.as_str())
            {
                files.push(path);
            }
        }
    }
}

/// Mine a project directory into the palace.
// Single-pass file mining pipeline; dry_run and limit handling adds lines but splitting
// would fragment shared state (counts, room_counts) across functions artificially.
#[allow(clippy::too_many_lines)]
pub async fn mine(conn: &Connection, project_dir: &Path, opts: &MineParams) -> Result<()> {
    let project_dir = project_dir.canonicalize().map_err(|e| {
        crate::error::Error::Other(format!(
            "directory not found: {}: {e}",
            project_dir.display()
        ))
    })?;

    let config_path = project_dir.join("mempalace.yaml");
    let config = ProjectConfig::load(&config_path)?;

    let wing = opts.wing.as_deref().unwrap_or(&config.wing);
    let rooms = &config.rooms;
    let all_files = scan_project_with_opts(&project_dir, opts.respect_gitignore);
    let files: Vec<_> = if opts.limit == 0 {
        all_files
    } else {
        all_files.into_iter().take(opts.limit).collect()
    };

    println!("\n=======================================================");
    if opts.dry_run {
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
    println!("  Files:   {}", files.len());
    println!("-------------------------------------------------------\n");

    let mut total_drawers: usize = 0;
    let mut files_skipped: usize = 0;
    let mut room_counts: HashMap<String, usize> = HashMap::new();

    for (i, filepath) in files.iter().enumerate() {
        let source_file = filepath.to_string_lossy().to_string();

        if !opts.dry_run && drawer::file_already_mined(conn, &source_file).await? {
            files_skipped += 1;
            continue;
        }

        let content = match std::fs::read_to_string(filepath) {
            Ok(c) => c,
            Err(_) => match std::fs::read(filepath) {
                Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                Err(_) => continue,
            },
        };

        let content = content.trim().to_string();
        if content.len() < 50 {
            continue;
        }

        let room = detect_room(filepath, &content, rooms, &project_dir);
        let chunks = chunk_text(&content);
        let drawers_added = chunks.len();

        if !opts.dry_run {
            // Capture mtime now so all chunks from the same file share the
            // same recorded timestamp.
            let source_mtime: Option<f64> = std::fs::metadata(filepath)
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
                .map(|d| d.as_secs_f64());

            for chunk in &chunks {
                let id = format!(
                    "drawer_{wing}_{room}_{}",
                    &uuid::Uuid::new_v4().to_string().replace('-', "")[..16]
                );
                drawer::add_drawer(
                    conn,
                    &drawer::DrawerParams {
                        id: &id,
                        wing,
                        room: &room,
                        content: &chunk.content,
                        source_file: &source_file,
                        chunk_index: chunk.chunk_index,
                        added_by: &opts.agent,
                        ingest_mode: "projects",
                        source_mtime,
                    },
                )
                .await?;
            }
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

    println!("\n=======================================================");
    if opts.dry_run {
        println!("  Dry run complete — nothing was written.");
    } else {
        println!("  Done.");
    }
    let files_processed = files.len() - files_skipped;
    println!("  Files processed: {files_processed}");
    println!("  Files skipped (already filed): {files_skipped}");
    println!(
        "  Drawers {}: {total_drawers}",
        if opts.dry_run {
            "would be filed"
        } else {
            "filed"
        }
    );
    println!("\n  By room:");

    let mut sorted_rooms: Vec<_> = room_counts.iter().collect();
    sorted_rooms.sort_by(|a, b| b.1.cmp(a.1));
    for (room, count) in sorted_rooms {
        println!("    {room:20} {count} files");
    }
    if !opts.dry_run {
        println!("\n  Next: mempalace search \"what you're looking for\"");
    }
    println!("=======================================================\n");

    Ok(())
}
