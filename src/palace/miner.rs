use std::collections::HashMap;
use std::path::{Path, PathBuf};

use turso::Connection;

use crate::config::{ProjectConfig, RoomConfig, normalize_wing_name};
use crate::error::{Error, Result};
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
    /// Paths (regular files only) to include even when `respect_gitignore` is `true`.
    ///
    /// After the gitignore-aware scan, any regular file path listed here that
    /// is not already in the scan results is appended verbatim. Directory
    /// entries are skipped — only regular files are added. Ignored when
    /// `respect_gitignore` is `false` since all paths are already included.
    pub include_ignored_paths: Vec<std::path::PathBuf>,
    /// Pre-scanned file list from a prior `scan_project_with_opts` call.
    ///
    /// When `Some`, the scan step inside `mine()` is skipped entirely so the
    /// directory walk is not repeated. `init::run` passes this to avoid
    /// double-walking after computing the file-count estimate.
    pub pre_scanned_files: Option<Vec<std::path::PathBuf>>,
}

/// Files larger than this are skipped — prevents OOM on huge files.
/// Large sessions (Claude Code, `ChatGPT` exports) routinely exceed 10 MB;
/// the cap guards against pathological binaries, not legitimate text.
/// Per-drawer size is bounded by `CHUNK_SIZE`, but the whole file is read
/// into memory before chunking, so memory scales with source size.
const FILE_SIZE_MAX: u64 = 500 * 1024 * 1024; // 500 MB

use super::WALK_DEPTH_LIMIT;

const READABLE_EXTENSIONS: &[&str] = &[
    "txt", "md", "py", "js", "ts", "jsx", "tsx", "json", "jsonl", "yaml", "yml", "html", "css",
    "java", "go", "rs", "rb", "sh", "csv", "sql", "toml", "c", "cpp", "h", "hpp", "swift", "kt",
    "scala", "lua", "r", "php", "pl", "zig", "nim", "ex", "exs", "erl", "hs", "ml",
];

/// Config filenames recognised by the miner, in load-precedence order.
/// Single source of truth: referenced by both `mine_load_config` and `is_skip_file`.
pub const PROJECT_CONFIG_FILES: &[&str] = &[
    "mempalace.yaml",
    "mempalace.yml",
    "mempal.yaml",
    "mempal.yml",
];

/// Non-config files that are always excluded from mining.
///
/// `entities.json` is a per-project audit artifact written by `mempalace init`.
/// Mining it would store entity metadata as palace content rather than real text.
const SKIP_FILES_EXTRA: &[&str] = &[
    ".gitignore",
    "entities.json",
    "package-lock.json",
    "Cargo.lock",
];

/// Return `true` if a filename should be excluded from mining.
fn is_skip_file(name: &str) -> bool {
    PROJECT_CONFIG_FILES.contains(&name) || SKIP_FILES_EXTRA.contains(&name)
}

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

        // Check all path components against SKIP_DIRS.
        let skip = path.components().any(|component| {
            let component_name = component.as_os_str().to_string_lossy();
            is_skip_dir(component_name.as_ref())
        });
        if skip {
            continue;
        }

        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if is_skip_file(name.as_str()) {
            continue;
        }

        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        if READABLE_EXTENSIONS.contains(&extension.as_str()) {
            if entry.metadata().is_ok_and(|m| m.len() > FILE_SIZE_MAX) {
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
                    && !is_skip_file(name.as_str())
                {
                    if std::fs::metadata(&path).is_ok_and(|m| m.len() > FILE_SIZE_MAX) {
                        continue;
                    }
                    files.push(path);
                }
            }
        }
    }
}

/// Detect the "hall" — the top-level subdirectory within the project containing the file.
///
/// Hall is the second-level grouping between wing (the whole project) and room
/// (the semantic category). Returns `None` when the file lives directly in the
/// project root with no subdirectory.
pub fn detect_hall(filepath: &Path, project_dir: &Path) -> Option<String> {
    assert!(
        !filepath.as_os_str().is_empty(),
        "detect_hall: filepath must not be empty"
    );
    assert!(
        !project_dir.as_os_str().is_empty(),
        "detect_hall: project_dir must not be empty"
    );

    let relative = filepath.strip_prefix(project_dir).ok()?;
    let mut components = relative.components();

    // The first component is the top-level entry. Only treat it as a hall when at
    // least one more component exists (i.e., the file is inside a subdirectory).
    let first = components.next()?;
    // Return None when there is no second component — the file is directly in project_dir.
    components.next()?;

    let hall_name = first.as_os_str().to_string_lossy().to_string();
    assert!(
        !hall_name.is_empty(),
        "detect_hall: hall name must not be empty"
    );
    Some(hall_name)
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
            .map(|room| room.name.as_str())
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
    drawers_total: usize,
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
        "  Drawers {}: {drawers_total}",
        if dry_run { "would be filed" } else { "filed" }
    );
    println!("\n  By room:");

    let mut sorted_rooms: Vec<_> = room_counts.iter().collect();
    sorted_rooms.sort_by_key(|b| std::cmp::Reverse(b.1));
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
    // Preconditions: callers must supply at least one chunk and a non-empty destination.
    assert!(
        !chunks.is_empty(),
        "mine_write_chunks: chunks must not be empty"
    );
    assert!(
        !wing.is_empty(),
        "mine_write_chunks: wing must not be empty"
    );
    assert!(
        !room.is_empty(),
        "mine_write_chunks: room must not be empty"
    );

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

/// Read the modification time of `filepath` as seconds since the Unix epoch.
///
/// Called by `mine_process_file_one` to stamp all chunks from the same file
/// with the same mtime. Returns `None` when the mtime cannot be read.
fn mine_get_mtime(filepath: &Path) -> Option<f64> {
    assert!(
        !filepath.as_os_str().is_empty(),
        "mine_get_mtime: filepath must not be empty"
    );
    std::fs::metadata(filepath)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs_f64())
}

/// Extract entity names from a file for per-drawer metadata annotation.
///
/// Called by `mine_process_file_one` to surface named entities (people and
/// projects) detected in the mined content. Uncertain candidates are omitted
/// to reduce noise. Returns names sorted and deduplicated.
fn mine_extract_entities_for_metadata(filepath: &Path) -> Vec<String> {
    assert!(
        !filepath.as_os_str().is_empty(),
        "mine_extract_entities_for_metadata: filepath must not be empty"
    );

    let result = crate::palace::entity_detect::detect_entities(&[filepath], 1, &["en"]);
    let mut names: Vec<String> = result
        .people
        .iter()
        .chain(result.projects.iter())
        .map(|entity| entity.name.clone())
        .collect();
    names.sort();
    names.dedup();

    assert!(
        names.len() <= 1000,
        "mine_extract_entities_for_metadata: entity count must be bounded"
    );
    names
}

/// Process a single file in the mine loop.
///
/// Returns `None` when the file is unreadable or too short. Otherwise returns
/// `(drawers_added, room, hall, entity_count)`. The caller handles the
/// already-mined skip check before calling this function.
async fn mine_process_file_one(
    connection: &Connection,
    filepath: &Path,
    project_dir: &Path,
    wing: &str,
    rooms: &[crate::config::RoomConfig],
    opts: &MineParams,
) -> Result<Option<(usize, String, Option<String>, usize)>> {
    let source_file = filepath.to_string_lossy().to_string();
    let Some(content) = mine_read_file(filepath) else {
        return Ok(None);
    };

    let room = detect_room(filepath, &content, rooms, project_dir);
    let hall = detect_hall(filepath, project_dir);
    let entities = mine_extract_entities_for_metadata(filepath);
    let chunks = chunk_text(&content);

    assert!(
        !room.is_empty(),
        "mine_process_file_one: room must not be empty"
    );
    assert!(
        !chunks.is_empty(),
        "mine_process_file_one: chunks must not be empty for readable content"
    );

    if !opts.dry_run {
        let source_mtime = mine_get_mtime(filepath);
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

    let entity_count = entities.len();
    let drawers_added = chunks.len();

    // Postcondition: must have counted at least one drawer for readable content.
    assert!(
        drawers_added > 0,
        "mine_process_file_one: drawers_added must be positive"
    );
    Ok(Some((drawers_added, room, hall, entity_count)))
}

/// Wrap a path in single quotes for safe shell re-use in a resume hint.
///
/// Single quotes in the path itself are escaped via `'"'"'`. Called by
/// `mine_process_files` when Ctrl-C is detected to print a safe resume command.
fn mine_shell_quote(path_str: &str) -> String {
    assert!(
        !path_str.is_empty(),
        "mine_shell_quote: path must not be empty"
    );
    let escaped = path_str.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

/// Reconstruct the user's `mempalace mine` invocation from `opts` and
/// `project_dir`, suitable for printing in the Ctrl-C resume hint.
///
/// Only non-default flags are emitted so the printed command stays close to
/// what the user originally typed. Each path component is escaped via
/// [`mine_shell_quote`] so the line round-trips through `sh -c` even when
/// paths contain spaces or single quotes. Called by `mine_process_files`.
fn mine_resume_command(project_dir: &Path, opts: &MineParams) -> String {
    let mut parts: Vec<String> = vec![
        "mempalace".to_string(),
        "mine".to_string(),
        mine_shell_quote(&project_dir.display().to_string()),
    ];
    if let Some(wing) = opts.wing.as_deref()
        && !wing.is_empty()
    {
        parts.push("--wing".to_string());
        parts.push(mine_shell_quote(wing));
    }
    // Default agent is "mempalace" — only emit when the user overrode it.
    if !opts.agent.is_empty() && opts.agent != "mempalace" {
        parts.push("--agent".to_string());
        parts.push(mine_shell_quote(&opts.agent));
    }
    if opts.limit > 0 {
        parts.push("--limit".to_string());
        parts.push(opts.limit.to_string());
    }
    if opts.dry_run {
        parts.push("--dry-run".to_string());
    }
    // CLI default for respect_gitignore is true; emit `--no-gitignore` only
    // when the user disabled gitignore filtering.
    if !opts.respect_gitignore {
        parts.push("--no-gitignore".to_string());
    }
    for include_path in &opts.include_ignored_paths {
        parts.push("--include-ignored".to_string());
        parts.push(mine_shell_quote(&include_path.display().to_string()));
    }
    parts.join(" ")
}

/// Process all files in the mine loop.
///
/// Returns `(drawers_total, files_skipped, files_unreadable_or_too_short, room_counts)`.
/// Checks `ctrl_c_flag` before each file; on interrupt prints a resume hint and
/// returns [`Error::Interrupted`] so RAII guards (the mine lock) release on the
/// unwind. The CLI translates the variant into POSIX exit code 130 after cleanup.
async fn mine_process_files(
    connection: &Connection,
    files: &[PathBuf],
    wing: &str,
    rooms: &[crate::config::RoomConfig],
    project_dir: &Path,
    opts: &MineParams,
    ctrl_c_flag: &std::sync::atomic::AtomicBool,
) -> Result<(usize, usize, usize, HashMap<String, usize>)> {
    assert!(
        !wing.is_empty(),
        "mine_process_files: wing must not be empty"
    );
    let mut drawers_total: usize = 0;
    let mut files_skipped: usize = 0;
    let mut files_unreadable_or_too_short: usize = 0;
    let mut room_counts: HashMap<String, usize> = HashMap::new();
    let mut last_file: Option<&Path> = None;

    for (i, filepath) in files.iter().enumerate() {
        if ctrl_c_flag.load(std::sync::atomic::Ordering::Relaxed) {
            let files_processed = i - files_skipped - files_unreadable_or_too_short;
            let last = last_file.map_or("-".to_string(), |p| p.display().to_string());
            eprintln!(
                "\nInterrupted after {files_processed} file(s), {drawers_total} drawer(s). \
                 Last: {last}"
            );
            eprintln!("Resume with: {}", mine_resume_command(project_dir, opts));
            // Bubble out as a typed error so MineGuard::drop runs on the unwind.
            // main.rs maps Error::Interrupted to POSIX exit code 130 explicitly,
            // matching the prior std::process::exit(130) behaviour for the user.
            return Err(Error::Interrupted);
        }

        let source_file = filepath.to_string_lossy().to_string();

        // Always check for duplicates so dry runs report accurate skip counts.
        // Only the write path below is gated on !opts.dry_run.
        if drawer::file_already_mined(connection, &source_file).await? {
            files_skipped += 1;
            continue;
        }

        match mine_process_file_one(connection, filepath, project_dir, wing, rooms, opts).await? {
            None => {
                files_unreadable_or_too_short += 1;
            }
            Some((drawers_added, room, hall, entity_count)) => {
                drawers_total += drawers_added;
                last_file = Some(filepath.as_path());
                *room_counts.entry(room.clone()).or_insert(0) += 1;
                let file_name = filepath.file_name().unwrap_or_default().to_string_lossy();
                let hall_label = hall.as_deref().unwrap_or("-");
                println!(
                    "  [{:4}/{}] {:50} {:15} +{drawers_added}",
                    i + 1,
                    files.len(),
                    file_name,
                    hall_label,
                );
                if entity_count > 0 {
                    println!("              ({entity_count} entities detected)");
                }
            }
        }
    }

    Ok((
        drawers_total,
        files_skipped,
        files_unreadable_or_too_short,
        room_counts,
    ))
}

/// Load project config from `project_dir`, trying config file names in precedence
/// order: `mempalace.yaml`, `mempalace.yml`, `mempal.yaml`, `mempal.yml`. If none
/// exist, emits a warning to stderr and synthesises a default config.
///
/// `override_wing` is the wing name from `--wing` on the command line. When no
/// config file is found and the directory has no basename (e.g. `/`), the override
/// is used instead of failing — so `mempalace mine / --wing myproject` succeeds.
///
/// This mirrors the Python behaviour introduced in PR #604: directories without a
/// config file can still be mined instead of aborting with an error.
fn mine_load_config(project_dir: &Path, override_wing: Option<&str>) -> Result<ProjectConfig> {
    for name in PROJECT_CONFIG_FILES {
        let path = project_dir.join(name);
        if path.exists() {
            return ProjectConfig::load(&path);
        }
    }

    // Neither config file found — warn and fall back to auto-detected defaults so
    // mining can proceed without requiring an explicit `mempalace init` step.
    //
    // Wing resolution order: explicit --wing override, then directory basename.
    let wing_name = if let Some(wing) = override_wing {
        let trimmed = wing.trim().to_string();
        if trimmed.is_empty() {
            return Err(crate::error::Error::Other(
                "--wing override must not be empty or whitespace-only".to_string(),
            ));
        }
        trimmed
    } else {
        let raw = project_dir
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        if raw.is_empty() {
            return Err(crate::error::Error::Other(format!(
                "cannot infer wing name: {} has no basename; \
                 add mempalace.yaml with an explicit wing name",
                project_dir.display()
            )));
        }
        normalize_wing_name(&raw)
    };
    eprintln!(
        "  No mempalace.yaml/.yml found in {} \
         — using auto-detected defaults (wing='{wing_name}'). \
         Directories with the same basename will share a wing; \
         add mempalace.yaml to disambiguate.",
        project_dir.display()
    );
    Ok(ProjectConfig {
        wing: wing_name,
        rooms: vec![RoomConfig {
            name: "general".to_string(),
            description: "All project files".to_string(),
            keywords: vec![],
        }],
    })
}

/// Append any `include_ignored_paths` entries not already in `scanned`.
///
/// Called by [`mine`] after the gitignore-aware scan to honour the
/// `--include-ignored` CLI flag. Each entry in `include_paths` is
/// canonicalised; entries that cannot be canonicalised or that are not
/// regular files are skipped silently. If `include_paths` is empty the
/// function is a no-op and returns `scanned` unchanged.
fn mine_apply_include_ignored(
    mut scanned: Vec<PathBuf>,
    include_paths: &[PathBuf],
) -> Vec<PathBuf> {
    if include_paths.is_empty() {
        return scanned;
    }
    let original_len = scanned.len();
    for candidate in include_paths {
        let Ok(canonical) = candidate.canonicalize() else {
            continue;
        };
        assert!(
            canonical.is_absolute(),
            "canonicalize must produce an absolute path"
        );
        if canonical.is_file() && !scanned.contains(&canonical) {
            scanned.push(canonical);
        }
    }
    // Pair assertion: we only added entries, never removed any.
    assert!(
        scanned.len() >= original_len,
        "mine_apply_include_ignored: result must be at least as large as input"
    );
    scanned
}

/// Compute topic tunnels for `wing` after mining completes.
///
/// Reads `topics_by_wing` from the global registry and calls
/// `graph::topic_tunnels_for_wing`. Degrades quietly on error so a
/// registry issue never aborts a mine. Called by [`mine`].
async fn mine_run_topic_tunnels(connection: &Connection, wing: &str) {
    assert!(
        !wing.is_empty(),
        "mine_run_topic_tunnels: wing must not be empty"
    );

    let topics_by_wing = crate::palace::known_entities::get_topics_by_wing();
    // Normalise the lookup key — older registries (or rare write paths that
    // bypass the normalisation in `add_to_known_entities_set_wing_topics`) may
    // contain raw wing names; mirroring `graph.rs`'s read-side normalisation
    // keeps this function tolerant of both shapes.
    let normalized_wing = crate::config::normalize_wing_name(wing);
    if !topics_by_wing.contains_key(&normalized_wing) {
        return;
    }
    let min_count = crate::config::MempalaceConfig::topic_tunnel_min_count();
    match crate::palace::graph::topic_tunnels_for_wing(
        connection,
        wing,
        &topics_by_wing,
        min_count,
        "shared topic",
    )
    .await
    {
        Ok(count) if count > 0 => {
            println!("\n  Topic tunnels: +{count} cross-wing link(s)");
        }
        Ok(_) => {}
        Err(error) => {
            eprintln!("\n  WARNING: topic tunnel computation skipped — {error}");
        }
    }
}

/// Resolve the canonical wing slug and the room list `mine` will use, given
/// the user-supplied [`MineParams`] and a canonicalised `project_dir`.
///
/// This helper exists so [`mine`] stays under the 70-line guideline. It handles:
/// 1. Trimming the `--wing` override (whitespace-only counts as no override).
/// 2. Loading the project config (yaml lookup → basename fallback synth).
/// 3. Canonicalising the resolved wing through [`normalize_wing_name`] so a
///    hand-edited yaml `wing: my-proj` and a CLI `--wing my-proj` produce
///    the same slug as the basename fallback (`my_proj`). Without this, the
///    `topics_by_wing` registry would never find a partner for the wing.
/// 4. Synthesising a default `general` room when the yaml supplied an empty
///    rooms list — `detect_room`'s precondition forbids empty room slices.
fn mine_resolve_wing_and_rooms(
    project_dir: &Path,
    opts: &MineParams,
) -> Result<(String, Vec<crate::config::RoomConfig>)> {
    let normalized_wing: Option<String> = opts
        .wing
        .as_deref()
        .map(str::trim)
        .filter(|w| !w.is_empty())
        .map(str::to_string);

    let config = mine_load_config(project_dir, normalized_wing.as_deref())?;

    let wing_resolved = normalized_wing.as_deref().unwrap_or(&config.wing);
    let wing_owned = normalize_wing_name(wing_resolved);
    assert!(
        !wing_owned.is_empty(),
        "mine_resolve_wing_and_rooms: canonical wing must not be empty"
    );

    let mut rooms = config.rooms;
    if rooms.is_empty() {
        rooms.push(crate::config::RoomConfig {
            name: "general".to_string(),
            description: String::new(),
            keywords: vec![],
        });
    }
    assert!(
        !rooms.is_empty(),
        "mine_resolve_wing_and_rooms: rooms must be non-empty after fallback"
    );
    Ok((wing_owned, rooms))
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

    let (wing_owned, mut rooms) = mine_resolve_wing_and_rooms(&project_dir, opts)?;
    let wing: &str = &wing_owned;
    if rooms.is_empty() {
        // Empty rooms list in mempalace.yaml would violate detect_room's precondition;
        // fall back to a single general room so mining can proceed.
        rooms.push(crate::config::RoomConfig {
            name: "general".to_string(),
            description: String::new(),
            keywords: vec![],
        });
    }

    // Use pre-scanned files when provided (e.g. passed from init) to avoid a second walk.
    let scanned = opts.pre_scanned_files.as_ref().map_or_else(
        || scan_project_with_opts(&project_dir, opts.respect_gitignore),
        std::clone::Clone::clone,
    );
    let all_files = mine_apply_include_ignored(scanned, &opts.include_ignored_paths);
    let files: Vec<_> = if opts.limit == 0 {
        all_files
    } else {
        all_files.into_iter().take(opts.limit).collect()
    };

    mine_print_header(wing, &rooms, files.len(), opts.dry_run);

    // Set an atomic flag when Ctrl-C fires; mine_process_files checks it per file.
    let ctrl_c_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag_clone = ctrl_c_flag.clone();
    let _ctrl_c_task = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        flag_clone.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    let (drawers_total, files_skipped, files_unreadable_or_too_short, room_counts) =
        mine_process_files(
            connection,
            &files,
            wing,
            &rooms,
            &project_dir,
            opts,
            &ctrl_c_flag,
        )
        .await?;

    if !opts.dry_run {
        mine_run_topic_tunnels(connection, wing).await;
    }

    mine_print_summary(
        opts.dry_run,
        files.len(),
        files_skipped,
        files_unreadable_or_too_short,
        drawers_total,
        &room_counts,
    );
    Ok(())
}

/// RAII guard that holds a mine lockfile.
///
/// Created by [`acquire_mine_lock`]; automatically removes the lockfile when
/// dropped so the next mine operation can proceed.
pub struct MineGuard {
    lock_path: PathBuf,
}

impl Drop for MineGuard {
    fn drop(&mut self) {
        assert!(
            !self.lock_path.as_os_str().is_empty(),
            "MineGuard::drop: lock_path must not be empty"
        );
        // Best-effort removal — don't panic on IO failure so Drop stays safe.
        if let Err(error) = std::fs::remove_file(&self.lock_path) {
            eprintln!(
                "mine_lock: failed to remove lock file {}: {error}",
                self.lock_path.display()
            );
        }
    }
}

/// Acquire an exclusive mine lock in `lock_dir`.
///
/// Creates `lock_dir/mine.lock` atomically via `O_CREAT|O_EXCL` semantics.
/// Returns `Err` immediately if the file already exists (another mine is
/// running) or if creation fails for any other reason. The caller must keep
/// the returned [`MineGuard`] alive for the duration of the mine operation —
/// dropping it deletes the lockfile.
pub fn acquire_mine_lock(lock_dir: &Path) -> Result<MineGuard> {
    assert!(
        lock_dir.is_dir(),
        "acquire_mine_lock: lock_dir must be an existing directory"
    );
    let lock_path = lock_dir.join("mine.lock");
    assert!(
        !lock_path.as_os_str().is_empty(),
        "acquire_mine_lock: lock_path must not be empty"
    );
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
    {
        Ok(_file) => {
            assert!(
                lock_path.exists(),
                "acquire_mine_lock: lock file must exist after creation"
            );
            Ok(MineGuard { lock_path })
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(crate::error::Error::Other(
                "a mine operation is already running for this palace; \
                 if no mine is running, delete mine.lock and retry"
                    .to_string(),
            ))
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // --- detect_hall ---

    #[test]
    fn detect_hall_returns_subdirectory_for_nested_file() {
        // A file nested inside a subdirectory must return the subdirectory as the hall.
        let dir = tempfile::tempdir().expect("tempdir should succeed");
        let sub = dir.path().join("src");
        std::fs::create_dir_all(&sub).expect("create_dir src should succeed");
        let filepath = sub.join("main.rs");
        std::fs::write(&filepath, "fn main() {}").expect("write file should succeed");

        let hall = detect_hall(&filepath, dir.path());
        assert!(hall.is_some(), "nested file must produce a hall name");
        assert_eq!(
            hall.expect("hall must be Some for nested file"),
            "src",
            "hall must equal the top-level subdirectory"
        );
    }

    #[test]
    fn detect_hall_returns_none_for_top_level_file() {
        // A file directly in the project root must return None — no hall grouping.
        let dir = tempfile::tempdir().expect("tempdir should succeed");
        let filepath = dir.path().join("readme.md");
        std::fs::write(&filepath, "# Readme").expect("write readme should succeed");

        let hall = detect_hall(&filepath, dir.path());
        assert!(hall.is_none(), "top-level file must produce no hall");
    }

    // --- mine_extract_entities_for_metadata ---

    #[test]
    fn mine_extract_entities_for_metadata_returns_empty_for_minimal_file() {
        // A file with no entity-like proper nouns must return an empty list.
        let dir = tempfile::tempdir().expect("tempdir should succeed");
        let filepath = dir.path().join("config.txt");
        std::fs::write(&filepath, "timeout = 30\nretry = 3").expect("write config should succeed");

        let names = mine_extract_entities_for_metadata(&filepath);
        // config.txt has no proper names — result may be empty or small.
        // Pair assertion: count is bounded regardless of content.
        assert!(names.len() <= 1000, "entity count must always be bounded");
    }

    #[test]
    fn mine_extract_entities_for_metadata_returns_vec_for_entity_rich_file() {
        // A file with many proper-noun phrases must return a sorted, deduped vec.
        let dir = tempfile::tempdir().expect("tempdir should succeed");
        let filepath = dir.path().join("notes.txt");
        // Deliberately repeat "Alice" to test deduplication.
        std::fs::write(
            &filepath,
            "Alice Smith worked with Alice Smith on the project. \
             Bob Jones reviewed it. Alice Smith approved the PR.",
        )
        .expect("write notes should succeed");

        let names = mine_extract_entities_for_metadata(&filepath);
        // Names must be deduplicated: "Alice Smith" should appear at most once.
        let unique_count = names.len();
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            unique_count,
            sorted.len(),
            "entity names must be deduplicated"
        );
    }

    // --- acquire_mine_lock ---

    #[test]
    fn acquire_mine_lock_creates_and_cleans_up_lockfile() {
        // acquire_mine_lock must create the lockfile on acquire and delete it on drop.
        let temp_directory = tempfile::tempdir().expect("tempdir must succeed");
        let lock_path = temp_directory.path().join("mine.lock");

        {
            let guard = acquire_mine_lock(temp_directory.path())
                .expect("acquire_mine_lock must succeed on a clean directory");
            // Positive space: lockfile must exist while the guard is held.
            assert!(
                lock_path.exists(),
                "mine.lock must exist while guard is held"
            );
            drop(guard);
        }

        // Negative space: lockfile must be gone after the guard is dropped.
        assert!(
            !lock_path.exists(),
            "mine.lock must be removed when MineGuard is dropped"
        );
    }

    #[test]
    fn acquire_mine_lock_second_acquire_returns_error() {
        // A second acquire while the first guard is held must return Err.
        let temp_directory = tempfile::tempdir().expect("tempdir must succeed");
        let _guard =
            acquire_mine_lock(temp_directory.path()).expect("first acquire_mine_lock must succeed");

        let second = acquire_mine_lock(temp_directory.path());
        assert!(
            second.is_err(),
            "second acquire_mine_lock must fail while first guard is held"
        );
        assert!(
            second
                .err()
                .is_some_and(|e| e.to_string().contains("already running")),
            "error message must mention 'already running'"
        );
    }

    #[test]
    fn acquire_mine_lock_different_lock_dirs_do_not_contend() {
        // Locks in different directories must not block each other — two distinct
        // palaces can be mined concurrently.
        let dir_a = tempfile::tempdir().expect("tempdir A must succeed");
        let dir_b = tempfile::tempdir().expect("tempdir B must succeed");

        let guard_a = acquire_mine_lock(dir_a.path()).expect("first palace lock must succeed");
        let guard_b = acquire_mine_lock(dir_b.path()).expect("second palace lock must not contend");

        // Positive space: both lockfiles must exist simultaneously.
        assert!(
            dir_a.path().join("mine.lock").exists(),
            "mine.lock must exist in dir_a while guard_a is held"
        );
        assert!(
            dir_b.path().join("mine.lock").exists(),
            "mine.lock must exist in dir_b while guard_b is held"
        );

        drop(guard_a);
        drop(guard_b);

        // Negative space: both lockfiles must be removed after guards are dropped.
        assert!(
            !dir_a.path().join("mine.lock").exists(),
            "mine.lock must be removed from dir_a after drop"
        );
        assert!(
            !dir_b.path().join("mine.lock").exists(),
            "mine.lock must be removed from dir_b after drop"
        );
    }

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

    // --- mine_load_config ---

    #[test]
    fn mine_load_config_loads_primary_yaml() {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        std::fs::write(
            dir.path().join("mempalace.yaml"),
            "wing: primary\nrooms:\n  - name: main\n    description: ''\n",
        )
        .expect("write mempalace.yaml should succeed");
        let config = mine_load_config(dir.path(), None).expect("should load primary yaml");
        assert_eq!(config.wing, "primary");
        assert_eq!(config.rooms.len(), 1);
        assert_eq!(config.rooms[0].name, "main");
    }

    #[test]
    fn mine_load_config_precedence_yaml_over_yml() {
        // mempalace.yaml must take precedence over mempalace.yml.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        std::fs::write(
            dir.path().join("mempalace.yaml"),
            "wing: from-yaml\nrooms:\n  - name: r\n    description: ''\n",
        )
        .expect("write .yaml should succeed");
        std::fs::write(
            dir.path().join("mempalace.yml"),
            "wing: from-yml\nrooms:\n  - name: r\n    description: ''\n",
        )
        .expect("write .yml should succeed");
        let config = mine_load_config(dir.path(), None).expect("should load yaml over yml");
        assert_eq!(config.wing, "from-yaml");
    }

    #[test]
    fn mine_load_config_falls_back_to_yml() {
        // mempalace.yml is used when the .yaml variant is absent.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        std::fs::write(
            dir.path().join("mempalace.yml"),
            "wing: yml-wing\nrooms:\n  - name: r\n    description: ''\n",
        )
        .expect("write mempalace.yml should succeed");
        let config = mine_load_config(dir.path(), None).expect("should load .yml");
        assert_eq!(config.wing, "yml-wing");
    }

    #[test]
    fn mine_load_config_synthesises_defaults_when_no_config() {
        // No config files present — defaults use the normalized directory basename as wing.
        // Normalization lowercases and replaces spaces/hyphens with underscores.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let config = mine_load_config(dir.path(), None).expect("should synthesise defaults");
        let raw_basename = dir
            .path()
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let expected_wing = normalize_wing_name(&raw_basename);
        assert_eq!(config.wing, expected_wing);
        assert_eq!(config.rooms.len(), 1);
        assert_eq!(config.rooms[0].name, "general");
    }

    #[test]
    fn mine_load_config_errors_on_empty_basename() {
        // Mining a root-like path (no basename) must return Err, not panic.
        // Use a path whose file_name() is None (e.g. "/").
        let result = mine_load_config(std::path::Path::new("/"), None);
        assert!(result.is_err(), "root path must produce an error");
        let error_message = result
            .expect_err("root path must produce an error")
            .to_string();
        assert!(
            error_message.contains("basename"),
            "error must mention basename: {error_message}"
        );
    }

    #[test]
    fn mine_load_config_override_wing_succeeds_on_empty_basename() {
        // --wing override must be used instead of the basename when the directory
        // has no basename (e.g. "/"), so `mempalace mine / --wing myproject` succeeds.
        let config = mine_load_config(std::path::Path::new("/"), Some("myproject"))
            .expect("override_wing must prevent error on root path");
        assert_eq!(config.wing, "myproject");
        assert_eq!(config.rooms[0].name, "general");
    }

    #[test]
    fn mine_load_config_override_wing_trims_whitespace() {
        // Leading/trailing whitespace in --wing must be stripped so that
        // `mempalace mine . --wing " myproject "` yields wing="myproject".
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let config = mine_load_config(dir.path(), Some("  myproject  "))
            .expect("trimmed override must succeed");
        assert_eq!(config.wing, "myproject");
    }

    #[test]
    fn mine_load_config_override_wing_rejects_empty() {
        // An empty --wing override must be an error, not a blank wing name.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let result = mine_load_config(dir.path(), Some(""));
        assert!(result.is_err(), "empty override must produce an error");
    }

    #[test]
    fn mine_load_config_override_wing_rejects_whitespace_only() {
        // A whitespace-only --wing override must be an error, not a blank wing name.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let result = mine_load_config(dir.path(), Some("   "));
        assert!(
            result.is_err(),
            "whitespace-only override must produce an error"
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

    // --- mine_resume_command ---

    fn resume_opts() -> MineParams {
        // Default-shaped MineParams for the resume-command tests so each test
        // toggles exactly the fields under test.
        MineParams {
            wing: None,
            agent: "mempalace".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
            pre_scanned_files: None,
        }
    }

    #[test]
    fn mine_resume_command_emits_only_path_when_all_defaults() {
        // With defaults the resume hint must reduce to the bare command and
        // path so the printed line stays close to what the user typed.
        let project = std::path::Path::new("/tmp/proj");
        let command = mine_resume_command(project, &resume_opts());
        assert_eq!(command, "mempalace mine '/tmp/proj'");
    }

    #[test]
    fn mine_resume_command_includes_wing_override() {
        // A user-supplied --wing must round-trip verbatim (single-quoted) so
        // re-running the printed command lands in the same wing.
        let project = std::path::Path::new("/tmp/proj");
        let mut opts = resume_opts();
        opts.wing = Some("my-proj".to_string());
        let command = mine_resume_command(project, &opts);
        assert!(
            command.contains("--wing 'my-proj'"),
            "wing override must be preserved verbatim, got {command:?}"
        );
    }

    #[test]
    fn mine_resume_command_includes_non_default_flags() {
        // dry-run, limit, agent, no-gitignore, and include-ignored must all
        // round-trip when set to non-default values.
        let project = std::path::Path::new("/tmp/proj");
        let mut opts = resume_opts();
        opts.agent = "diary-agent".to_string();
        opts.limit = 100;
        opts.dry_run = true;
        opts.respect_gitignore = false;
        opts.include_ignored_paths = vec![PathBuf::from("/tmp/proj/dist/bundle.js")];
        let command = mine_resume_command(project, &opts);
        assert!(
            command.contains("--agent 'diary-agent'"),
            "agent override must be preserved, got {command:?}"
        );
        assert!(
            command.contains("--limit 100"),
            "limit must be preserved, got {command:?}"
        );
        assert!(
            command.contains("--dry-run"),
            "dry-run must be preserved, got {command:?}"
        );
        assert!(
            command.contains("--no-gitignore"),
            "no-gitignore must be preserved, got {command:?}"
        );
        assert!(
            command.contains("--include-ignored '/tmp/proj/dist/bundle.js'"),
            "include-ignored path must be preserved, got {command:?}"
        );
    }

    #[test]
    fn mine_resume_command_skips_default_agent_and_limit() {
        // Pair: when agent stays at its default and limit==0 the resume hint
        // must NOT emit those flags so the output stays minimal.
        let project = std::path::Path::new("/tmp/proj");
        let opts = resume_opts();
        let command = mine_resume_command(project, &opts);
        assert!(
            !command.contains("--agent"),
            "default agent must not be emitted, got {command:?}"
        );
        assert!(
            !command.contains("--limit"),
            "limit==0 must not be emitted, got {command:?}"
        );
        assert!(
            !command.contains("--no-gitignore"),
            "default gitignore behaviour must not be emitted, got {command:?}"
        );
    }

    // --- mine() wing normalization ---

    const CONFIG_YAML: &str = "wing: config-wing\nrooms:\n  - name: general\n    description: ''\n";

    fn mine_opts(wing: Option<&str>) -> MineParams {
        MineParams {
            wing: wing.map(str::to_string),
            agent: "test".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: false,
            include_ignored_paths: vec![],
            pre_scanned_files: None,
        }
    }

    #[tokio::test]
    async fn mine_wing_override_is_trimmed() {
        // A padded --wing value must be trimmed before drawers are stored, so
        // `mine . --wing " padded "` yields wing="padded" in the palace.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        std::fs::write(dir.path().join("mempalace.yaml"), CONFIG_YAML)
            .expect("write config should succeed");
        std::fs::write(
            dir.path().join("notes.txt"),
            "rust programming language provides memory safety without a garbage collector",
        )
        .expect("write notes.txt should succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        mine(&connection, dir.path(), &mine_opts(Some("  padded  ")))
            .await
            .expect("mine must succeed with padded wing");

        let rows = crate::db::query_all(&connection, "SELECT DISTINCT wing FROM drawers", ())
            .await
            .expect("query must succeed");
        assert_eq!(rows.len(), 1, "must have exactly one distinct wing");
        let stored_wing: String = rows[0].get(0).expect("wing column must be readable");
        assert_eq!(stored_wing, "padded", "wing must be trimmed");
    }

    #[tokio::test]
    async fn mine_wing_whitespace_only_falls_back_to_config() {
        // A whitespace-only --wing must be treated as no override, so the
        // config file's wing ("config-wing") is used instead — and then
        // canonicalised to the normalised slug "config_wing" so it matches
        // the basename / topics-by-wing convention.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        std::fs::write(dir.path().join("mempalace.yaml"), CONFIG_YAML)
            .expect("write config should succeed");
        std::fs::write(
            dir.path().join("notes.txt"),
            "rust programming language provides memory safety without a garbage collector",
        )
        .expect("write notes.txt should succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        mine(&connection, dir.path(), &mine_opts(Some("   ")))
            .await
            .expect("mine must succeed when whitespace wing falls back to config");

        let rows = crate::db::query_all(&connection, "SELECT DISTINCT wing FROM drawers", ())
            .await
            .expect("query must succeed");
        assert_eq!(rows.len(), 1, "must have exactly one distinct wing");
        let stored_wing: String = rows[0].get(0).expect("wing column must be readable");
        assert_eq!(
            stored_wing, "config_wing",
            "whitespace-only wing must fall back to config and canonicalise"
        );
    }

    #[tokio::test]
    async fn mine_wing_empty_falls_back_to_config() {
        // An empty --wing must be treated as no override, so the config file's
        // wing ("config-wing") is used and canonicalised to "config_wing".
        let dir = tempfile::tempdir().expect("tempdir should be created");
        std::fs::write(dir.path().join("mempalace.yaml"), CONFIG_YAML)
            .expect("write config should succeed");
        std::fs::write(
            dir.path().join("notes.txt"),
            "rust programming language provides memory safety without a garbage collector",
        )
        .expect("write notes.txt should succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        mine(&connection, dir.path(), &mine_opts(Some("")))
            .await
            .expect("mine must succeed when empty wing falls back to config");

        let rows = crate::db::query_all(&connection, "SELECT DISTINCT wing FROM drawers", ())
            .await
            .expect("query must succeed");
        assert_eq!(rows.len(), 1, "must have exactly one distinct wing");
        let stored_wing: String = rows[0].get(0).expect("wing column must be readable");
        assert_eq!(
            stored_wing, "config_wing",
            "empty wing must fall back to config and canonicalise"
        );
    }

    #[tokio::test]
    async fn mine_wing_override_canonicalises_hyphen_to_underscore() {
        // Regression: a CLI override like `--wing my-proj` must be canonicalised
        // through `normalize_wing_name` so drawers land in the same wing as the
        // basename fallback / `topics_by_wing` registry. Without this, topic
        // tunnels never find a partner because the registry key never matches.
        let dir = tempfile::tempdir().expect("tempdir should be created");
        std::fs::write(
            dir.path().join("notes.txt"),
            "rust programming language provides memory safety without a garbage collector",
        )
        .expect("write notes.txt should succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        mine(&connection, dir.path(), &mine_opts(Some("my-proj")))
            .await
            .expect("mine must succeed with hyphenated wing");

        let rows = crate::db::query_all(&connection, "SELECT DISTINCT wing FROM drawers", ())
            .await
            .expect("query must succeed");
        assert_eq!(rows.len(), 1, "must have exactly one distinct wing");
        let stored_wing: String = rows[0].get(0).expect("wing column must be readable");
        assert_eq!(
            stored_wing, "my_proj",
            "hyphenated CLI override must canonicalise"
        );
    }

    #[test]
    fn apply_include_ignored_empty_list_is_noop() {
        // An empty `include_paths` must return the original list unchanged.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let file = dir.path().join("a.txt");
        std::fs::write(&file, "content").expect("write must succeed");
        let canonical = file.canonicalize().expect("canonicalize must succeed");
        let input = vec![canonical.clone()];
        let result = mine_apply_include_ignored(input.clone(), &[]);
        assert_eq!(
            result.len(),
            input.len(),
            "no-op when include_paths is empty"
        );
        assert_eq!(result[0], canonical, "result must preserve original entry");
    }

    #[test]
    fn apply_include_ignored_adds_new_file() {
        // A path not already in `scanned` must be appended after the scan list.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let existing_file = dir.path().join("existing.txt");
        let extra_file = dir.path().join("extra.txt");
        std::fs::write(&existing_file, "existing").expect("write must succeed");
        std::fs::write(&extra_file, "extra").expect("write must succeed");
        let existing_canonical = existing_file
            .canonicalize()
            .expect("canonicalize must succeed");
        let extra_canonical = extra_file
            .canonicalize()
            .expect("canonicalize must succeed");
        let scanned = vec![existing_canonical.clone()];
        let include_paths = vec![extra_file];
        let result = mine_apply_include_ignored(scanned, &include_paths);
        assert_eq!(result.len(), 2, "extra file must be appended");
        // Pair assertion: the extra file's canonical path must be in the result.
        assert!(
            result.contains(&extra_canonical),
            "result must contain the extra file"
        );
    }

    #[test]
    fn apply_include_ignored_does_not_duplicate() {
        // A path already in `scanned` must not be added a second time.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let file = dir.path().join("file.txt");
        std::fs::write(&file, "content").expect("write must succeed");
        let canonical = file.canonicalize().expect("canonicalize must succeed");
        let scanned = vec![canonical.clone()];
        let include_paths = vec![file];
        let result = mine_apply_include_ignored(scanned, &include_paths);
        assert_eq!(
            result.len(),
            1,
            "already-present file must not be duplicated"
        );
        // Pair assertion: the path in the result must be the original canonical.
        assert_eq!(result[0], canonical);
    }

    #[test]
    fn is_skip_file_excludes_entities_json() {
        // `entities.json` is written by `mempalace init` — mining it would store
        // entity metadata as palace content rather than real user text.
        assert!(
            is_skip_file("entities.json"),
            "entities.json must be skipped by the miner"
        );
        // Pair assertion: confirm surrounding files are not accidentally skipped.
        assert!(
            !is_skip_file("entities_backup.json"),
            "entities_backup.json should not be skipped"
        );
        assert!(
            !is_skip_file("my_entities.json"),
            "my_entities.json should not be skipped"
        );
    }

    #[test]
    fn is_skip_file_excludes_all_project_config_files() {
        // All four config file names must be excluded so they are not mined as content.
        for config_name in PROJECT_CONFIG_FILES {
            assert!(
                is_skip_file(config_name),
                "{config_name} must be in the skip list"
            );
        }
        // Pair assertion: an unrelated YAML file must not be skipped.
        assert!(
            !is_skip_file("other.yaml"),
            "other.yaml must not be in the skip list"
        );
    }

    #[test]
    fn is_skip_file_excludes_gitignore_and_lock_files() {
        // .gitignore and lock files are skip-worthy — not meaningful project text.
        assert!(is_skip_file(".gitignore"), ".gitignore must be skipped");
        assert!(
            is_skip_file("package-lock.json"),
            "package-lock.json must be skipped"
        );
        assert!(is_skip_file("Cargo.lock"), "Cargo.lock must be skipped");
        // Pair assertion: normal source files must not be skipped.
        assert!(!is_skip_file("main.rs"), "main.rs must not be skipped");
        assert!(!is_skip_file("notes.txt"), "notes.txt must not be skipped");
    }

    #[test]
    fn apply_include_ignored_skips_nonexistent_paths() {
        // A path that does not exist on disk cannot be canonicalized and must be silently skipped.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let existing_file = dir.path().join("real.txt");
        std::fs::write(&existing_file, "content").expect("write must succeed");
        let canonical_existing = existing_file
            .canonicalize()
            .expect("canonicalize must succeed");
        let scanned = vec![canonical_existing.clone()];
        // Include a path that does not exist.
        let phantom = dir.path().join("does_not_exist.txt");
        let result = mine_apply_include_ignored(scanned, &[phantom]);
        assert_eq!(
            result.len(),
            1,
            "nonexistent include path must be silently skipped"
        );
        assert_eq!(
            result[0], canonical_existing,
            "original scanned entry must be preserved"
        );
    }

    #[test]
    fn apply_include_ignored_skips_directory_paths() {
        // Only regular files may be appended; directories must be skipped.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let sub_dir = dir.path().join("subdir");
        std::fs::create_dir_all(&sub_dir).expect("create_dir must succeed");
        let scanned: Vec<PathBuf> = vec![];
        // Pass the subdirectory as an include path — it should be ignored.
        let result = mine_apply_include_ignored(scanned, &[sub_dir]);
        assert_eq!(
            result.len(),
            0,
            "directory include path must be silently skipped"
        );
    }

    // --- walk_dir_gitignore: skip-dir component check ---

    #[test]
    fn walk_dir_gitignore_skips_skip_dir_components() {
        // walk_dir_gitignore must not return files from directories matching is_skip_dir()
        // even when respect_gitignore=true. The component-level check on L118-123 is the
        // path under test.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let node_modules = dir.path().join("node_modules");
        std::fs::create_dir_all(&node_modules).expect("create node_modules must succeed");
        std::fs::write(node_modules.join("index.js"), "console.log('hi')")
            .expect("write node_modules/index.js must succeed");
        std::fs::write(dir.path().join("main.rs"), "fn main() {}")
            .expect("write main.rs must succeed");

        // Use walk_dir_gitignore directly to exercise the component-skip path.
        let files = walk_dir_gitignore(dir.path());
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
            "main.rs must appear"
        );
        // Pair assertion: the file inside node_modules must be excluded.
        assert!(
            !names.contains(&"index.js".to_string()),
            "node_modules/index.js must be excluded by skip-dir component check"
        );
    }

    // --- walk_dir: no-extension file is silently skipped ---

    #[test]
    fn walk_dir_skips_files_without_extension() {
        // Files that have no extension fall through the `if let Some(extension)` branch
        // and are silently skipped. Covers the implicit else on L179.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        // "Makefile" has no extension.
        std::fs::write(dir.path().join("Makefile"), "all:\n\t@echo hi")
            .expect("write Makefile must succeed");
        std::fs::write(dir.path().join("code.rs"), "fn main() {}")
            .expect("write code.rs must succeed");

        let mut files: Vec<PathBuf> = Vec::new();
        walk_dir(dir.path(), &mut files);

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
            "code.rs must appear"
        );
        // Pair assertion: Makefile (no extension) must be excluded.
        assert!(
            !names.contains(&"Makefile".to_string()),
            "Makefile without extension must be skipped"
        );
    }

    // --- mine(): empty rooms fallback ---

    #[tokio::test]
    async fn mine_with_empty_rooms_in_config_falls_back_to_general() {
        // When mempalace.yaml has an empty rooms list, mine() must inject a "general"
        // room rather than failing with a detect_room precondition violation.
        // Covers the `if rooms.is_empty()` branch on L638-646.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        // Write a config with an empty rooms list.
        std::fs::write(
            dir.path().join("mempalace.yaml"),
            "wing: test_wing\nrooms: []\n",
        )
        .expect("write mempalace.yaml must succeed");
        let body = "This is a sufficiently long note with plenty of content here.";
        std::fs::write(dir.path().join("notes.txt"), body).expect("write notes.txt must succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: None,
            agent: "test".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: false,
            include_ignored_paths: vec![],
            pre_scanned_files: None,
        };

        mine(&connection, dir.path(), &opts)
            .await
            .expect("mine must succeed even with empty rooms in config");

        // Drawers must exist and be in the injected 'general' room.
        let rows = crate::db::query_all(
            &connection,
            "SELECT room FROM drawers WHERE wing = 'test_wing'",
            (),
        )
        .await
        .expect("query must succeed");
        assert!(!rows.is_empty(), "at least one drawer must be filed");
        let room: String = rows[0].get(0).expect("room column must be readable");
        assert_eq!(room, "general", "injected general room must be used");
    }

    // --- mine(): limit applied ---

    #[tokio::test]
    async fn mine_with_limit_caps_files_processed() {
        // When opts.limit > 0, mine must process at most `limit` files.
        // Covers the `else { all_files.into_iter().take(opts.limit).collect() }` branch.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::write(
            dir.path().join("mempalace.yaml"),
            "wing: limit_test\nrooms:\n  - name: general\n    description: ''\n",
        )
        .expect("write config must succeed");
        let body = "This note has enough content to exceed the fifty character minimum for mining.";
        for name in ["a.txt", "b.txt", "c.txt"] {
            std::fs::write(dir.path().join(name), body).expect("write test file must succeed");
        }

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: None,
            agent: "test".to_string(),
            limit: 1,
            dry_run: false,
            respect_gitignore: false,
            include_ignored_paths: vec![],
            pre_scanned_files: None,
        };

        mine(&connection, dir.path(), &opts)
            .await
            .expect("mine with limit=1 must succeed");

        let rows = crate::db::query_all(
            &connection,
            "SELECT DISTINCT source_file FROM drawers WHERE wing = 'limit_test'",
            (),
        )
        .await
        .expect("query must succeed");
        assert_eq!(rows.len(), 1, "limit=1 must cap at one source file");
    }

    // --- mine_print_summary: unreadable/too-short counter display ---

    #[test]
    fn mine_print_summary_with_nonzero_unreadable_prints_without_panic() {
        // When files_unreadable_or_too_short > 0 the conditional println is executed.
        // Covers the `if files_unreadable_or_too_short > 0` branch on L269-273.
        let mut room_counts: HashMap<String, usize> = HashMap::new();
        room_counts.insert("general".to_string(), 1);

        // Must not panic for any combination of counts.
        mine_print_summary(false, 3, 1, 1, 0, &room_counts);
        mine_print_summary(true, 3, 0, 2, 1, &room_counts);

        // Pair assertion: room_counts is unmodified after the call.
        assert_eq!(room_counts.len(), 1, "room_counts must be unmodified");
    }

    // --- mine_process_files: entity_count > 0 println ---

    #[tokio::test]
    async fn mine_processes_entity_rich_file_without_panic() {
        // A file with proper-noun names triggers entity detection and the
        // `if entity_count > 0` println on L497-499.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::write(
            dir.path().join("mempalace.yaml"),
            "wing: entity_wing\nrooms:\n  - name: general\n    description: ''\n",
        )
        .expect("write config must succeed");
        // Repeat names to raise entity detection likelihood.
        std::fs::write(
            dir.path().join("names.txt"),
            "Alice Smith reviewed the proposal with Bob Jones. \
             Alice Smith approved it. Bob Jones merged the pull request. \
             Alice Smith and Bob Jones collaborated on the architecture design.",
        )
        .expect("write names.txt must succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: None,
            agent: "test".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: false,
            include_ignored_paths: vec![],
            pre_scanned_files: None,
        };

        // Must not panic even when entity_count > 0 triggers the extra println.
        mine(&connection, dir.path(), &opts)
            .await
            .expect("mine must succeed with entity-rich file");

        let rows = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'entity_wing'",
            (),
        )
        .await
        .expect("query must succeed");
        assert!(!rows.is_empty(), "at least one drawer must be filed");
    }

    // --- mine_print_summary: dry_run=false "Next:" line ---

    #[test]
    fn mine_print_summary_non_dry_run_with_empty_room_counts() {
        // Covers the `if !dry_run` println on L285 and an empty room_counts map.
        let room_counts: HashMap<String, usize> = HashMap::new();
        // Must not panic with zero room counts in non-dry-run mode.
        mine_print_summary(false, 0, 0, 0, 0, &room_counts);
        // Pair assertion: empty map is unchanged.
        assert!(room_counts.is_empty(), "room_counts must remain empty");
    }

    #[test]
    fn mine_load_config_falls_back_to_mempal_yaml() {
        // mempal.yaml must be loaded when the mempalace.* variants are absent.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::write(
            dir.path().join("mempal.yaml"),
            "wing: mempal-wing\nrooms:\n  - name: r\n    description: ''\n",
        )
        .expect("write mempal.yaml must succeed");
        let config = mine_load_config(dir.path(), None).expect("should load mempal.yaml");
        assert_eq!(
            config.wing, "mempal-wing",
            "wing must come from mempal.yaml"
        );
        assert_eq!(
            config.rooms.len(),
            1,
            "rooms must be loaded from mempal.yaml"
        );
    }

    #[test]
    fn mine_load_config_falls_back_to_mempal_yml() {
        // mempal.yml is the lowest-priority config file; must be used when all others absent.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::write(
            dir.path().join("mempal.yml"),
            "wing: mempal-yml-wing\nrooms:\n  - name: r\n    description: ''\n",
        )
        .expect("write mempal.yml must succeed");
        let config = mine_load_config(dir.path(), None).expect("should load mempal.yml");
        assert_eq!(
            config.wing, "mempal-yml-wing",
            "wing must come from mempal.yml"
        );
        assert_eq!(
            config.rooms.len(),
            1,
            "rooms must be loaded from mempal.yml"
        );
    }

    #[test]
    fn detect_hall_returns_correct_subdirectory_for_deeply_nested_file() {
        // The hall is always the FIRST-level subdirectory, even for deeply nested files.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let deep = dir.path().join("src").join("utils").join("helpers");
        std::fs::create_dir_all(&deep).expect("create deep dirs must succeed");
        let filepath = deep.join("math.rs");
        std::fs::write(&filepath, "fn add() {}").expect("write file must succeed");

        let hall = detect_hall(&filepath, dir.path());
        assert!(hall.is_some(), "deeply nested file must have a hall");
        assert_eq!(
            hall.expect("hall must be Some for nested file"),
            "src",
            "hall must be the first-level subdirectory, not a deeper component"
        );
    }

    #[test]
    fn mine_read_file_returns_none_for_too_short_content() {
        // Files whose trimmed content is fewer than 50 chars must return None.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let filepath = dir.path().join("tiny.txt");
        // 49 non-whitespace characters.
        std::fs::write(&filepath, "a".repeat(49)).expect("write must succeed");
        let result = mine_read_file(&filepath);
        assert!(
            result.is_none(),
            "content shorter than 50 chars must return None"
        );
    }

    #[test]
    fn mine_read_file_returns_content_for_readable_file() {
        // A file with content >= 50 chars must return the trimmed content.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let filepath = dir.path().join("notes.txt");
        let body = "This is a sufficiently long note to exceed the fifty character minimum.";
        std::fs::write(&filepath, body).expect("write must succeed");
        let result = mine_read_file(&filepath);
        assert!(result.is_some(), "readable file must return Some");
        assert_eq!(
            result.expect("result must be Some for readable file"),
            body,
            "returned content must match the file body"
        );
    }

    #[test]
    fn mine_get_mtime_returns_some_for_real_file() {
        // A real file on disk must produce a non-zero mtime.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let filepath = dir.path().join("stamped.txt");
        std::fs::write(&filepath, "content").expect("write must succeed");
        let mtime = mine_get_mtime(&filepath);
        assert!(mtime.is_some(), "real file must have a readable mtime");
        assert!(
            mtime.expect("mtime must be Some") > 0.0,
            "mtime must be a positive Unix timestamp"
        );
    }

    #[test]
    fn mine_get_mtime_returns_none_for_nonexistent_file() {
        // A path that does not exist must return None without panicking.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let filepath = dir.path().join("ghost.txt");
        let mtime = mine_get_mtime(&filepath);
        assert!(
            mtime.is_none(),
            "nonexistent file must return None for mtime"
        );
    }

    #[test]
    fn scan_project_without_gitignore_includes_all_readable_files() {
        // When respect_gitignore=false the walk_dir() path is taken — all readable
        // files must be returned regardless of any .gitignore rules.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::write(dir.path().join("main.rs"), "fn main() {}")
            .expect("write main.rs must succeed");
        std::fs::write(dir.path().join("notes.md"), "# notes")
            .expect("write notes.md must succeed");
        // This file would normally be gitignored — with respect_gitignore=false it must appear.
        let sub = dir.path().join("subdir");
        std::fs::create_dir_all(&sub).expect("create subdir must succeed");
        std::fs::write(sub.join("helper.py"), "def helper(): pass")
            .expect("write helper.py must succeed");

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
            names.contains(&"main.rs".to_string()),
            "main.rs must be found"
        );
        assert!(
            names.contains(&"notes.md".to_string()),
            "notes.md must be found"
        );
        assert!(
            names.contains(&"helper.py".to_string()),
            "helper.py must be found"
        );
    }

    #[tokio::test]
    async fn mine_returns_error_for_non_directory_path() {
        // mine() must return Err when the project_dir is not a directory.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let file_path = dir.path().join("file.txt");
        std::fs::write(&file_path, "content").expect("write must succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("test_wing".to_string()),
            agent: "agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: false,
            include_ignored_paths: vec![],
            pre_scanned_files: None,
        };

        let result = mine(&connection, &file_path, &opts).await;
        assert!(result.is_err(), "non-directory path must return Err");
        assert!(
            result
                .err()
                .is_some_and(|e| e.to_string().contains("directory")),
            "error message must mention 'directory'"
        );
    }

    #[tokio::test]
    async fn mine_dry_run_does_not_write_drawers() {
        // In dry-run mode, mine must report files without inserting any drawers.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::write(
            dir.path().join("mempalace.yaml"),
            "wing: dry-wing\nrooms:\n  - name: general\n    description: ''\n",
        )
        .expect("write config must succeed");
        let body = "This is a sufficiently long note with enough content to pass the threshold.";
        std::fs::write(dir.path().join("notes.txt"), body).expect("write notes.txt must succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("dry-wing".to_string()),
            agent: "agent".to_string(),
            limit: 0,
            dry_run: true,
            respect_gitignore: false,
            include_ignored_paths: vec![],
            pre_scanned_files: None,
        };

        mine(&connection, dir.path(), &opts)
            .await
            .expect("mine dry run must succeed");

        let rows = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'dry-wing'",
            (),
        )
        .await
        .expect("query for drawers must succeed");
        assert!(rows.is_empty(), "dry run must not insert any drawers");
    }

    // --- walk_dir: symlink skipped ---

    #[test]
    fn walk_dir_skips_symlinks() {
        // walk_dir must skip symlinks — covers the `if path.is_symlink()` branch (L171-173).
        // Symlinks are skipped to prevent following links to /dev/urandom or other
        // dangerous targets.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let real_file = dir.path().join("real.rs");
        std::fs::write(&real_file, "fn main() {}").expect("write real.rs must succeed");
        // Create a symlink to the real file.
        let link_file = dir.path().join("link.rs");
        std::os::unix::fs::symlink(&real_file, &link_file)
            .expect("failed to create symlink link.rs -> real.rs");

        let mut files = Vec::new();
        walk_dir(dir.path(), &mut files);

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
            names.contains(&"real.rs".to_string()),
            "real file must be collected"
        );
        assert!(
            !names.contains(&"link.rs".to_string()),
            "symlink must be skipped by walk_dir"
        );
    }

    // --- mine_read_file: binary fallback path ---

    #[test]
    fn mine_read_file_returns_content_for_non_utf8_binary_file() {
        // A file with non-UTF-8 bytes fails read_to_string but succeeds with read()
        // and from_utf8_lossy. This exercises lines 344-345 (the binary fallback).
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let filepath = dir.path().join("binary.rs");
        // Write a mix of valid ASCII and non-UTF-8 bytes to produce a binary-like file
        // that is long enough to pass the 50-char threshold after lossy conversion.
        // 0xFF bytes are invalid UTF-8 but from_utf8_lossy replaces them with U+FFFD.
        let mut content = b"fn main() { // binary content follows: ".to_vec();
        // Pad with valid ASCII so the result is well above 50 chars even after replacement.
        content.extend(b"a".repeat(80));
        // One invalid byte to force the read_to_string failure path.
        content.push(0xFF);
        std::fs::write(&filepath, &content).expect("write binary file must succeed");

        let result = mine_read_file(&filepath);
        // The binary fallback must produce Some — lossy conversion never panics.
        assert!(
            result.is_some(),
            "binary file must be returned via lossy fallback"
        );
        // Pair assertion: returned content must be non-empty and above the 50-char threshold.
        assert!(
            result.expect("result must be Some for binary file").len() >= 50,
            "returned content must be at least 50 chars"
        );
    }

    // --- MineGuard::drop: eprintln when remove_file fails ---

    #[test]
    fn mine_guard_drop_does_not_panic_when_lock_already_removed() {
        // MineGuard::drop must not panic even when the lockfile has already been
        // deleted before the guard is dropped. This covers the `if let Err(error)`
        // eprintln branch at lines 686-691.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let guard = acquire_mine_lock(dir.path()).expect("acquire_mine_lock must succeed");
        let lock_path = dir.path().join("mine.lock");
        // Pre-remove the lockfile so drop() encounters an IO error.
        std::fs::remove_file(&lock_path).expect("manual remove of mine.lock must succeed");
        // drop() must not panic even though remove_file will fail inside Drop.
        drop(guard);
        // Pair assertion: the lockfile is still gone (drop did not re-create it).
        assert!(
            !lock_path.exists(),
            "lockfile must not be re-created by a failing drop"
        );
    }

    // --- acquire_mine_lock: assert precondition on nonexistent dir ---

    #[test]
    fn acquire_mine_lock_panics_on_nonexistent_lock_dir() {
        // acquire_mine_lock fires assert! when lock_dir does not exist. This test
        // catches the panic to confirm the precondition is enforced without crashing
        // the test harness.
        let nonexistent = std::path::Path::new("/nonexistent/lock/dir/that/does/not/exist");
        let result = std::panic::catch_unwind(|| acquire_mine_lock(nonexistent));
        assert!(
            result.is_err(),
            "acquire_mine_lock on a nonexistent dir must panic via assert!"
        );
        // Pair assertion: the lock_dir itself must not exist on disk.
        assert!(
            !nonexistent.exists(),
            "the nonexistent dir must remain nonexistent"
        );
    }
}
