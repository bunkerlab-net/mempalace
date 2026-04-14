use std::collections::HashMap;
use std::path::{Path, PathBuf};

use turso::Connection;

use crate::error::Result;
use crate::normalize;
use crate::palace::chunker::Chunk;
use crate::palace::drawer;
use crate::palace::miner::MineParams;
use crate::palace::room_detect::is_skip_dir;

const CONVO_EXTENSIONS: &[&str] = &["txt", "md", "json", "jsonl"];
const MIN_CHUNK_SIZE: usize = 30;
/// Files larger than this are skipped — prevents OOM on huge files.
const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024; // 10 MB
/// Maximum directory nesting depth for `walk_convos`. Prevents stack overflow on
/// pathological symlink graphs and enforces the no-recursion rule.
const WALK_DEPTH_LIMIT: usize = 64;

const TOPIC_KEYWORDS: &[(&str, &[&str])] = &[
    (
        "technical",
        &[
            "code", "python", "function", "bug", "error", "api", "database", "server", "deploy",
            "git", "test", "debug", "refactor",
        ],
    ),
    (
        "architecture",
        &[
            "architecture",
            "design",
            "pattern",
            "structure",
            "schema",
            "interface",
            "module",
            "component",
            "service",
            "layer",
        ],
    ),
    (
        "planning",
        &[
            "plan",
            "roadmap",
            "milestone",
            "deadline",
            "priority",
            "sprint",
            "backlog",
            "scope",
            "requirement",
            "spec",
        ],
    ),
    (
        "decisions",
        &[
            "decided",
            "chose",
            "picked",
            "switched",
            "migrated",
            "replaced",
            "trade-off",
            "alternative",
            "option",
            "approach",
        ],
    ),
    (
        "problems",
        &[
            "problem",
            "issue",
            "broken",
            "failed",
            "crash",
            "stuck",
            "workaround",
            "fix",
            "solved",
            "resolved",
        ],
    ),
];

fn detect_convo_room(content: &str) -> String {
    assert!(
        !content.is_empty(),
        "detect_convo_room: content must not be empty"
    );
    let content_lower = content
        .chars()
        .take(3000)
        .collect::<String>()
        .to_lowercase();
    let mut best = ("general", 0usize);
    for &(room, keywords) in TOPIC_KEYWORDS {
        let score: usize = keywords
            .iter()
            .filter(|kw| content_lower.contains(*kw))
            .count();
        if score > best.1 {
            best = (room, score);
        }
    }
    best.0.to_string()
}

fn chunk_exchanges(content: &str) -> Vec<Chunk> {
    assert!(
        !content.is_empty(),
        "chunk_exchanges: content must not be empty"
    );
    let lines: Vec<&str> = content.lines().collect();
    let quote_count = lines
        .iter()
        .filter(|l| l.trim_start().starts_with('>'))
        .count();

    if quote_count >= 3 {
        chunk_by_exchange(&lines)
    } else {
        chunk_by_paragraph(content)
    }
}

fn chunk_by_exchange(lines: &[&str]) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        // Upper bound: i strictly increases each iteration, bounded by lines.len().
        debug_assert!(i < lines.len());
        let line = lines[i].trim();
        if line.starts_with('>') {
            let user_turn = line;
            i += 1;

            let mut ai_lines = Vec::new();
            while i < lines.len() {
                // Upper bound: i strictly increases each inner iteration, bounded by lines.len().
                debug_assert!(i < lines.len());
                let next = lines[i].trim();
                if next.starts_with('>') || next.starts_with("---") {
                    break;
                }
                if !next.is_empty() {
                    ai_lines.push(next);
                }
                i += 1;
            }

            let ai_response = ai_lines[..ai_lines.len().min(8)].join(" ");
            let content = if ai_response.is_empty() {
                user_turn.to_string()
            } else {
                format!("{user_turn}\n{ai_response}")
            };

            if content.trim().len() > MIN_CHUNK_SIZE {
                chunks.push(Chunk {
                    content,
                    chunk_index: chunks.len(),
                });
            }
        } else {
            i += 1;
        }
    }

    chunks
}

fn chunk_by_paragraph(content: &str) -> Vec<Chunk> {
    let paragraphs: Vec<&str> = content
        .split("\n\n")
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();

    if paragraphs.len() <= 1 && content.lines().count() > 20 {
        let lines: Vec<&str> = content.lines().collect();
        return lines
            .chunks(25)
            .enumerate()
            .filter_map(|(i, group)| {
                let text = group.join("\n");
                if text.trim().len() > MIN_CHUNK_SIZE {
                    Some(Chunk {
                        content: text.trim().to_string(),
                        chunk_index: i,
                    })
                } else {
                    None
                }
            })
            .collect();
    }

    paragraphs
        .iter()
        .enumerate()
        .filter(|(_, p)| p.len() > MIN_CHUNK_SIZE)
        .map(|(i, p)| Chunk {
            content: p.to_string(),
            chunk_index: i,
        })
        .collect()
}

fn scan_convos(directory: &Path) -> Vec<PathBuf> {
    assert!(
        directory.is_dir(),
        "scan_convos: directory must be a directory"
    );
    let mut files = Vec::new();
    walk_convos(directory, &mut files);
    files
}

fn walk_convos(directory: &Path, files: &mut Vec<PathBuf>) {
    // Iterative DFS with explicit depth tracking — no recursion.
    let mut stack: Vec<(PathBuf, usize)> = vec![(directory.to_path_buf(), 0)];

    while let Some((current_dir, depth)) = stack.pop() {
        assert!(
            depth <= WALK_DEPTH_LIMIT,
            "walk_convos: depth {depth} exceeds WALK_DEPTH_LIMIT"
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
                // Skip global cache dirs plus Claude Code-specific output dirs that
                // contain tool output and agent memory — not conversation transcripts.
                // Only descend if we haven't reached the depth limit yet.
                if !is_skip_dir(&name)
                    && name != "tool-results"
                    && name != "memory"
                    && depth < WALK_DEPTH_LIMIT
                {
                    stack.push((path, depth + 1));
                }
            } else if let Some(extension) = path.extension() {
                let extension_lower = extension.to_string_lossy().to_lowercase();
                // Skip .meta.json files — these are Claude Code session metadata,
                // not conversation content.
                if CONVO_EXTENSIONS.contains(&extension_lower.as_str())
                    && !name.ends_with(".meta.json")
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

fn mine_convos_print_header(
    wing: &str,
    directory: &Path,
    file_count: usize,
    extract_mode: &str,
    dry_run: bool,
) {
    println!("\n=======================================================");
    if dry_run {
        println!("  MemPalace Mine — Conversations [DRY RUN]");
    } else {
        println!("  MemPalace Mine — Conversations");
    }
    println!("=======================================================");
    println!("  Wing:    {wing}");
    println!("  Source:  {}", directory.display());
    println!("  Files:   {file_count}");
    println!("  Mode:    {extract_mode}");
    println!("-------------------------------------------------------\n");
}

// Eight independent summary counters; a dedicated struct would be over-engineered for a single private call site.
#[allow(clippy::too_many_arguments)]
fn mine_convos_print_summary(
    dry_run: bool,
    file_count: usize,
    files_skipped: usize,
    files_unreadable: usize,
    files_too_short: usize,
    files_empty_chunks: usize,
    total_drawers: usize,
    room_counts: &HashMap<String, usize>,
) {
    let files_processed = file_count
        .saturating_sub(files_skipped)
        .saturating_sub(files_unreadable)
        .saturating_sub(files_too_short)
        .saturating_sub(files_empty_chunks);
    println!("\n=======================================================");
    if dry_run {
        println!("  Dry run complete — nothing was written.");
    } else {
        println!("  Done.");
    }
    println!("  Files processed:                  {files_processed}");
    println!("  Files skipped (already filed):    {files_skipped}");
    if files_unreadable > 0 {
        println!("  Files skipped (unreadable):       {files_unreadable}");
    }
    if files_too_short > 0 {
        println!("  Files skipped (too short):        {files_too_short}");
    }
    if files_empty_chunks > 0 {
        println!("  Files skipped (no chunks):        {files_empty_chunks}");
    }
    println!(
        "  Drawers {}: {total_drawers}",
        if dry_run { "would be filed" } else { "filed" }
    );

    let mut sorted_rooms: Vec<_> = room_counts.iter().collect();
    sorted_rooms.sort_by(|a, b| b.1.cmp(a.1));
    if !sorted_rooms.is_empty() {
        println!("\n  By room:");
        for (room, count) in sorted_rooms {
            println!("    {room:20} {count} files");
        }
    }
    if !dry_run {
        println!("\n  Next: mempalace search \"what you're looking for\"");
    }
    println!("=======================================================\n");
}

/// Write all chunks for one conversation file into the palace.
async fn mine_convos_write_chunks(
    connection: &Connection,
    chunks: &[Chunk],
    wing: &str,
    room: &str,
    source_file: &str,
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
                ingest_mode: "convos",
                source_mtime: None,
            },
        )
        .await?;
    }
    Ok(())
}

// Sequential file-scan loop with per-file counters mutated via continue/+=; no clean extraction boundary within 70 lines.
#[allow(clippy::too_many_lines)]
pub async fn mine_convos(
    connection: &Connection,
    directory: &Path,
    extract_mode: &str,
    opts: &MineParams,
) -> Result<()> {
    let directory = directory.canonicalize().map_err(|e| {
        crate::error::Error::Other(format!("directory not found: {}: {e}", directory.display()))
    })?;
    if !directory.is_dir() {
        return Err(crate::error::Error::Other(format!(
            "not a directory: {}",
            directory.display()
        )));
    }

    let dir_name = directory
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase()
        .replace([' ', '-'], "_");
    let wing = opts.wing.as_deref().unwrap_or(&dir_name);

    let all_files = scan_convos(&directory);
    let files: Vec<_> = if opts.limit == 0 {
        all_files
    } else {
        all_files.into_iter().take(opts.limit).collect()
    };

    mine_convos_print_header(wing, &directory, files.len(), extract_mode, opts.dry_run);

    let mut total_drawers: usize = 0;
    let mut files_skipped: usize = 0;
    let mut files_unreadable: usize = 0;
    let mut files_too_short: usize = 0;
    let mut files_empty_chunks: usize = 0;
    let mut room_counts: HashMap<String, usize> = HashMap::new();

    for (i, filepath) in files.iter().enumerate() {
        let source_file = filepath.to_string_lossy().to_string();

        if !opts.dry_run && drawer::file_already_mined(connection, &source_file).await? {
            files_skipped += 1;
            continue;
        }

        let Ok(content) = normalize::normalize(filepath) else {
            files_unreadable += 1;
            continue;
        };
        if content.trim().len() < MIN_CHUNK_SIZE {
            files_too_short += 1;
            continue;
        }

        let chunks = chunk_exchanges(&content);
        if chunks.is_empty() {
            files_empty_chunks += 1;
            continue;
        }

        let room = detect_convo_room(&content);
        let drawers_added = chunks.len();

        if !opts.dry_run {
            mine_convos_write_chunks(connection, &chunks, wing, &room, &source_file, opts).await?;
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

    mine_convos_print_summary(
        opts.dry_run,
        files.len(),
        files_skipped,
        files_unreadable,
        files_too_short,
        files_empty_chunks,
        total_drawers,
        &room_counts,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_convo_room_handles_utf8_without_panicking() {
        let content = "🚀 Planejamento técnico com decisão sobre API e arquitetura. ".repeat(200);
        assert_eq!(detect_convo_room(&content), "technical");
    }
}
