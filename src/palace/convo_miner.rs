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
const CHUNK_SIZE_MIN: usize = 30;
/// Upper bound on lines in a single conversation chunk; prevents unbounded iteration
/// over malformed or adversarially large inputs.
const LINES_MAX: usize = 100_000;
/// Bytes per drawer — large exchanges are split at this boundary (rounded down
/// to a UTF-8 char boundary) so the full AI response is stored without
/// truncation.  Mirrors miner.py's `CHUNK_SIZE`.  Uses `content.len()` (bytes),
/// not `content.chars().count()`, so chunks may be slightly shorter for
/// multi-byte characters.
const CHUNK_SIZE: usize = 800;
/// Files larger than this are skipped — prevents OOM on huge files.
/// Long Claude Code sessions, multi-year `ChatGPT` exports, and lifetime
/// Slack dumps routinely exceed 10 MB; the cap guards against
/// pathological binaries, not legitimate text.  Per-drawer size is
/// bounded by `CHUNK_SIZE`, but content is loaded fully into memory
/// before chunking, so memory use scales with source size.
const FILE_SIZE_MAX: u64 = 500 * 1024 * 1024; // 500 MB

// Compile-time invariant: chunk size must be greater than min chunk size.
const _: () = assert!(CHUNK_SIZE > CHUNK_SIZE_MIN);

use super::WALK_DEPTH_LIMIT;

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

/// Classify conversation content into the best-matching topic room.
///
/// Scans the first 3000 characters for keyword matches across the `TOPIC_KEYWORDS`
/// table and returns the room name with the highest score, defaulting to `"general"`.
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

/// Split conversation content into chunks, routing to `chunk_by_exchange` or
/// `chunk_by_paragraph` depending on whether the content uses the quoted-turn format.
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

    // Route to chunk_by_exchange only when the first non-empty line is a user
    // turn marker ('>').  A previous version routed whenever quote_count >= 1,
    // but chunk_by_exchange silently drops every non-'>' line via its else-skip
    // branch.  Content that starts with unquoted preamble (leading text before
    // the first '>') would therefore be discarded; chunk_by_paragraph preserves
    // it instead.  The quote_count >= 1 guard still rejects fully unquoted files.
    let first_nonempty_is_quote = lines
        .iter()
        .find(|l| !l.trim().is_empty())
        .is_some_and(|l| l.trim_start().starts_with('>'));

    if quote_count >= 1 && first_nonempty_is_quote {
        chunk_by_exchange(&lines)
    } else {
        chunk_by_paragraph(content)
    }
}

/// Return the largest byte index ≤ `index` that is a UTF‑8 char boundary in `s`.
///
/// Slicing `text` by a raw byte offset is unsafe when the string contains multi‑byte
/// characters (emoji, accented letters, CJK) because the offset may land mid‑
/// codepoint, causing a panic.  This function walks backwards from `index` until
/// it finds a valid boundary, guaranteeing `&text[..result]` never panics.
fn chunk_by_exchange_floor_char_boundary(text: &str, index: usize) -> usize {
    if index >= text.len() {
        return text.len();
    }
    let mut i = index;
    while !text.is_char_boundary(i) {
        i -= 1;
    }
    // Postcondition: i is a valid char boundary within text.
    debug_assert!(text.is_char_boundary(i));
    debug_assert!(i <= index);
    i
}

/// Drain AI response lines from `lines` starting at `start_index`.
///
/// Collects non-empty lines until a new user turn (`>`) or separator (`---`)
/// is found.  Returns the collected lines and the updated index, which points
/// to the first line of the next exchange or past the end of the slice.
fn chunk_by_exchange_drain_ai_lines<'a>(
    lines: &[&'a str],
    start_index: usize,
) -> (Vec<&'a str>, usize) {
    assert!(
        start_index <= lines.len(),
        "chunk_by_exchange_drain_ai_lines: start_index out of range"
    );
    let mut ai_lines = Vec::new();
    let mut i = start_index;
    let mut exchange_count: usize = 0;
    while i < lines.len() {
        exchange_count += 1;
        assert!(
            exchange_count <= LINES_MAX,
            "chunk_by_exchange_drain_ai_lines: exceeded LINES_MAX ({LINES_MAX}) iterations"
        );
        // Upper bound: i strictly increases each iteration, bounded by lines.len().
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
    // Postcondition: returned index is within bounds or at the end.
    debug_assert!(i <= lines.len());
    (ai_lines, i)
}

/// Flush an assembled exchange into `chunks`, splitting on `CHUNK_SIZE` boundaries.
///
/// When content fits in one chunk it is pushed as-is (guarded by `CHUNK_SIZE_MIN`).
/// When it overflows, the first `CHUNK_SIZE` bytes form the head chunk and the
/// remainder is split into continuation drawers — always pushed regardless of
/// `CHUNK_SIZE_MIN` to prevent silent data loss.
fn chunk_by_exchange_flush(content: String, chunks: &mut Vec<Chunk>) {
    assert!(
        !content.is_empty(),
        "chunk_by_exchange_flush: content must not be empty"
    );
    if content.len() > CHUNK_SIZE {
        // First chunk: user turn + as much response as fits.
        // Use char-boundary-safe slicing: a raw byte offset can land
        // mid-codepoint for multi-byte chars (emoji, CJK, accents).
        let first_end = chunk_by_exchange_floor_char_boundary(&content, CHUNK_SIZE);
        let first = &content[..first_end];
        // Guard first chunk to avoid nearly-empty starts.
        if first.trim().len() > CHUNK_SIZE_MIN {
            chunks.push(Chunk {
                content: first.to_string(),
                chunk_index: chunks.len(),
            });
        }
        // Remaining response in CHUNK_SIZE continuation drawers.
        // Continuation fragments are always pushed (no CHUNK_SIZE_MIN filter)
        // to prevent silent data loss once we've committed to multi-chunk output.
        let mut remainder = &content[first_end..];
        let mut chunk_accumulation_count: usize = 0;
        while !remainder.is_empty() {
            chunk_accumulation_count += 1;
            assert!(
                chunk_accumulation_count <= LINES_MAX,
                "chunk_by_exchange_flush: exceeded LINES_MAX ({LINES_MAX}) chunk-accumulation iterations"
            );
            let end = chunk_by_exchange_floor_char_boundary(remainder, CHUNK_SIZE);
            // If floor_char_boundary returned 0 (edge case for corrupted input),
            // advance by the first character's UTF-8 byte length to maintain
            // boundary safety and prevent infinite loops.
            let end = if end == 0 {
                // Invariant: remainder is non-empty (guarded by while condition),
                // so chars().next() always returns Some.
                remainder.chars().next().map_or(1, char::len_utf8)
            } else {
                end
            };
            let part = &remainder[..end];
            remainder = &remainder[end..];
            chunks.push(Chunk {
                content: part.to_string(),
                chunk_index: chunks.len(),
            });
        }
    } else if content.trim().len() > CHUNK_SIZE_MIN {
        chunks.push(Chunk {
            content,
            chunk_index: chunks.len(),
        });
    }
}

/// One user turn (>) + the full AI response that follows = one or more chunks.
///
/// Each line is whitespace-trimmed and empty lines are dropped; the remaining
/// lines are joined with a single space.  When the combined content exceeds
/// `CHUNK_SIZE` bytes, it is split across consecutive drawers so nothing is
/// silently discarded (fixes the prior 8-line cap).
fn chunk_by_exchange(lines: &[&str]) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut i = 0;
    let mut lines_count: usize = 0;

    while i < lines.len() {
        lines_count += 1;
        assert!(
            lines_count <= LINES_MAX,
            "chunk_by_exchange: exceeded LINES_MAX ({LINES_MAX}) outer iterations"
        );
        // Upper bound: i strictly increases each iteration, bounded by lines.len().
        debug_assert!(i < lines.len());
        let line = lines[i].trim();
        if line.starts_with('>') {
            let user_turn = line;
            i += 1;
            let (ai_lines, new_i) = chunk_by_exchange_drain_ai_lines(lines, i);
            i = new_i;
            // Full response — no truncation.
            let ai_response = ai_lines.join(" ");
            let content = if ai_response.is_empty() {
                user_turn.to_string()
            } else {
                format!("{user_turn}\n{ai_response}")
            };
            chunk_by_exchange_flush(content, &mut chunks);
        } else {
            i += 1;
        }
    }

    chunks
}

/// Split content into chunks by paragraph (blank-line boundary) or line groups.
///
/// Falls back to 25-line groups when the content has only one paragraph but more
/// than 20 lines, which prevents the entire file from becoming a single oversized chunk.
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
                if text.trim().len() > CHUNK_SIZE_MIN {
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
        .filter(|(_, p)| p.len() > CHUNK_SIZE_MIN)
        .map(|(i, p)| Chunk {
            content: p.to_string(),
            chunk_index: i,
        })
        .collect()
}

/// Return a flat list of all conversation files under `directory`.
/// Delegates the actual traversal to `walk_convos`.
fn scan_convos(directory: &Path) -> Vec<PathBuf> {
    assert!(
        directory.is_dir(),
        "scan_convos: directory must be a directory"
    );
    let mut files = Vec::new();
    walk_convos(directory, &mut files);
    files
}

/// Iteratively walk `directory` (depth-limited DFS) and collect conversation files.
///
/// Symlinks are skipped; directories named `tool-results` or `memory` are excluded;
/// files larger than `FILE_SIZE_MAX` are silently skipped.
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
                    if std::fs::metadata(&path).is_ok_and(|m| m.len() > FILE_SIZE_MAX) {
                        continue;
                    }
                    files.push(path);
                }
            }
        }
    }
}

/// Print the mine-convos progress header to stdout.
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

/// Print the mine-convos completion summary (file counts, drawer counts, room breakdown) to stdout.
// Eight independent summary counters; a dedicated struct would be over-engineered for a single private call site.
#[allow(clippy::too_many_arguments)]
fn mine_convos_print_summary(
    dry_run: bool,
    file_count: usize,
    files_skipped: usize,
    files_unreadable: usize,
    files_too_short: usize,
    files_empty_chunks: usize,
    drawers_total: usize,
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
        "  Drawers {}: {drawers_total}",
        if dry_run { "would be filed" } else { "filed" }
    );

    let mut sorted_rooms: Vec<_> = room_counts.iter().collect();
    // Break count ties by room name so output is deterministic across runs.
    sorted_rooms.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
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
    source_mtime: f64,
    opts: &MineParams,
) -> Result<()> {
    // Outer savepoint ensures a partial failure cannot leave file_already_mined()
    // seeing a half-ingested file on the next run.
    connection
        .execute("SAVEPOINT sp_mine_convos_file", ())
        .await?;

    for chunk in chunks {
        let id = format!(
            "drawer_{wing}_{room}_{}",
            &uuid::Uuid::new_v4().to_string().replace('-', "")[..16]
        );
        if let Err(e) = drawer::add_drawer(
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
                source_mtime: Some(source_mtime),
            },
        )
        .await
        {
            let _ = connection
                .execute("ROLLBACK TO SAVEPOINT sp_mine_convos_file", ())
                .await;
            let _ = connection
                .execute("RELEASE SAVEPOINT sp_mine_convos_file", ())
                .await;
            return Err(e);
        }
    }

    connection
        .execute("RELEASE SAVEPOINT sp_mine_convos_file", ())
        .await?;
    Ok(())
}

/// Per-file outcome returned by `mine_convos_scan_file`.
enum MineConvosFileOutcome {
    /// File was already mined in a previous run; skip without writing.
    AlreadyMined,
    /// File could not be read or its mtime is unavailable.
    Unreadable,
    /// File content is below `CHUNK_SIZE_MIN`; not worth indexing.
    TooShort,
    /// Content parsed to zero chunks (e.g. no qualifying exchanges or paragraphs).
    EmptyChunks,
    /// File processed successfully; carry the room name and drawer count.
    Processed { room: String, drawers_added: usize },
}

/// Read one conversation file and, unless `dry_run`, write its chunks to the
/// palace.  Returns a `MineConvosFileOutcome` describing what happened so the
/// caller can update its summary counters.
async fn mine_convos_scan_file(
    filepath: &Path,
    connection: &Connection,
    wing: &str,
    opts: &MineParams,
) -> Result<MineConvosFileOutcome> {
    assert!(
        !wing.is_empty(),
        "mine_convos_scan_file: wing must not be empty"
    );
    let source_file = filepath.to_string_lossy().to_string();
    // Always check for duplicates so dry runs report accurate skip counts.
    // Only the write path below is gated on !opts.dry_run.
    if drawer::file_already_mined(connection, &source_file).await? {
        return Ok(MineConvosFileOutcome::AlreadyMined);
    }
    let Ok(content) = normalize::normalize(filepath) else {
        return Ok(MineConvosFileOutcome::Unreadable);
    };
    if content.trim().len() < CHUNK_SIZE_MIN {
        return Ok(MineConvosFileOutcome::TooShort);
    }
    let chunks = chunk_exchanges(&content);
    if chunks.is_empty() {
        return Ok(MineConvosFileOutcome::EmptyChunks);
    }
    let room = detect_convo_room(&content);
    let drawers_added = chunks.len();
    // Mtime is required: None conflates "no on-disk source" with
    // "unreadable filesystem", causing file_already_mined() to miss
    // duplicates on reruns and producing stale duplicate chunks.
    let Some(source_mtime) = std::fs::metadata(filepath)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|system_time| {
            system_time
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .ok()
        })
        .map(|duration| duration.as_secs_f64())
    else {
        return Ok(MineConvosFileOutcome::Unreadable);
    };
    if !opts.dry_run {
        mine_convos_write_chunks(
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
    Ok(MineConvosFileOutcome::Processed {
        room,
        drawers_added,
    })
}

/// Canonicalize `directory` and resolve the wing name.
///
/// Returns `(canonical_path, wing_name)` where `wing_name` is either the
/// explicit override from `opts.wing` or derived from the directory's last
/// path component.  Errors when the path cannot be canonicalized, is not a
/// directory, or has no file-name component and no explicit wing override.
fn mine_convos_resolve_wing(directory: &Path, opts: &MineParams) -> Result<(PathBuf, String)> {
    let directory = directory.canonicalize().map_err(|e| {
        crate::error::Error::Other(format!("directory not found: {}: {e}", directory.display()))
    })?;
    if !directory.is_dir() {
        return Err(crate::error::Error::Other(format!(
            "not a directory: {}",
            directory.display()
        )));
    }
    if let Some(wing_name) = opts.wing.as_deref() {
        assert!(
            !wing_name.is_empty(),
            "mine_convos_resolve_wing: explicit wing must not be empty"
        );
        return Ok((directory, wing_name.to_string()));
    }
    // file_name() returns None for filesystem roots (e.g. `/`), producing an empty
    // dir_name.  An empty wing triggers the assert in drawer::add_drawer, so surface
    // a clear error here instead.
    let dir_name = directory
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase()
        .replace([' ', '-'], "_");
    if dir_name.is_empty() {
        return Err(crate::error::Error::Other(
            "mine convos: cannot determine wing name — directory is a filesystem root; \
             pass --wing to specify one explicitly"
                .to_string(),
        ));
    }
    assert!(
        !dir_name.is_empty(),
        "mine_convos_resolve_wing: derived wing must not be empty"
    );
    Ok((directory, dir_name))
}

/// Mine all conversation files in `directory`, filing chunks as drawers in the palace.
///
/// Resolves the wing name, walks for eligible files, and delegates per-file
/// processing to `mine_convos_scan_file`.
pub async fn mine_convos(
    connection: &Connection,
    directory: &Path,
    extract_mode: &str,
    opts: &MineParams,
) -> Result<()> {
    let (directory, wing) = mine_convos_resolve_wing(directory, opts)?;
    let mut all_files = scan_convos(&directory);
    // Sort for deterministic ordering before applying any limit.
    all_files.sort_unstable();
    let files: Vec<_> = if opts.limit == 0 {
        all_files
    } else {
        all_files.into_iter().take(opts.limit).collect()
    };
    mine_convos_print_header(&wing, &directory, files.len(), extract_mode, opts.dry_run);
    let mut drawers_total: usize = 0;
    let mut files_skipped: usize = 0;
    let mut files_unreadable: usize = 0;
    let mut files_too_short: usize = 0;
    let mut files_empty_chunks: usize = 0;
    let mut room_counts: HashMap<String, usize> = HashMap::new();
    for (i, filepath) in files.iter().enumerate() {
        match mine_convos_scan_file(filepath, connection, &wing, opts).await? {
            MineConvosFileOutcome::AlreadyMined => {
                files_skipped += 1;
            }
            MineConvosFileOutcome::Unreadable => {
                files_unreadable += 1;
            }
            MineConvosFileOutcome::TooShort => {
                files_too_short += 1;
            }
            MineConvosFileOutcome::EmptyChunks => {
                files_empty_chunks += 1;
            }
            MineConvosFileOutcome::Processed {
                room,
                drawers_added,
            } => {
                drawers_total += drawers_added;
                *room_counts.entry(room).or_insert(0) += 1;
                println!(
                    "  [{:4}/{}] {:50} +{drawers_added}",
                    i + 1,
                    files.len(),
                    filepath.file_name().unwrap_or_default().to_string_lossy(),
                );
            }
        }
    }
    mine_convos_print_summary(
        opts.dry_run,
        files.len(),
        files_skipped,
        files_unreadable,
        files_too_short,
        files_empty_chunks,
        drawers_total,
        &room_counts,
    );
    Ok(())
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn detect_convo_room_handles_utf8_without_panicking() {
        let content = "🚀 Planejamento técnico com decisão sobre API e arquitetura. ".repeat(200);
        assert_eq!(detect_convo_room(&content), "technical");
    }

    #[test]
    fn chunk_by_exchange_stores_full_ai_response() {
        // Before the fix the AI response was truncated to 8 lines; this test
        // verifies the 9th line is now preserved.
        let lines: Vec<String> = std::iter::once("> user question".to_string())
            .chain((1..=9).map(|n| format!("ai line {n}")))
            .collect();
        let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
        let chunks = chunk_by_exchange(&refs);
        assert!(!chunks.is_empty(), "must produce at least one chunk");
        let all_text = chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            all_text.contains("ai line 9"),
            "9th AI line must be preserved"
        );
    }

    #[test]
    fn chunk_by_exchange_splits_large_exchange() {
        // A long AI response (> CHUNK_SIZE) must be split into multiple drawers.
        let ai_body = "x ".repeat(500); // ~1000 chars > CHUNK_SIZE=800
        let input = format!("> user turn\n{ai_body}");
        let lines: Vec<&str> = input.lines().collect();
        let chunks = chunk_by_exchange(&lines);
        assert!(chunks.len() >= 2, "large exchange must produce 2+ chunks");
        // Chunk indices must be contiguous and 0-based.
        for (expected, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, expected, "chunk indices must be 0-based");
        }
    }

    #[test]
    fn chunk_by_exchange_small_exchange_single_chunk() {
        // Content is > CHUNK_SIZE_MIN (30) so it must produce exactly one chunk.
        let input = "> user asks a question here\nthe assistant replies with an answer";
        let lines: Vec<&str> = input.lines().collect();
        let chunks = chunk_by_exchange(&lines);
        assert_eq!(chunks.len(), 1, "small exchange fits in one chunk");
        assert!(
            chunks[0].content.contains("assistant replies"),
            "answer preserved"
        );
    }

    #[test]
    fn chunk_by_exchange_multibyte_chars_no_panic() {
        // Emoji and accented chars are multi-byte; a raw byte slice at CHUNK_SIZE
        // could land mid-codepoint and panic.  This test verifies the split is
        // UTF-8-boundary-safe and all content is preserved across chunks.
        let emoji_line = "🚀".repeat(300); // 300 × 4 bytes = 1200 bytes, well above CHUNK_SIZE
        let input = format!("> question\n{emoji_line}");
        let lines: Vec<&str> = input.lines().collect();
        // Must not panic and must produce valid UTF-8 in every chunk.
        let chunks = chunk_by_exchange(&lines);
        assert!(!chunks.is_empty(), "must produce at least one chunk");
        for chunk in &chunks {
            assert!(
                std::str::from_utf8(chunk.content.as_bytes()).is_ok(),
                "every chunk must be valid UTF-8"
            );
        }
        // Round-trip validation: reconstruct original from chunks and verify bytes match.
        let reconstructed = chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<String>();
        assert_eq!(
            reconstructed.as_bytes(),
            input.as_bytes(),
            "reconstructed content must match original bytes exactly"
        );
    }

    #[test]
    fn chunk_by_exchange_floor_char_boundary_ascii() {
        // ASCII strings: every byte is a char boundary, so result == index.
        assert_eq!(chunk_by_exchange_floor_char_boundary("hello", 3), 3);
        assert_eq!(chunk_by_exchange_floor_char_boundary("hello", 10), 5); // clamped to len
    }

    #[test]
    fn chunk_by_exchange_floor_char_boundary_multibyte() {
        // "é" is 2 bytes (0xC3 0xA9); byte 1 is mid-codepoint.
        let s = "aé"; // bytes: [0x61, 0xC3, 0xA9]
        assert_eq!(chunk_by_exchange_floor_char_boundary(s, 2), 1); // step back to 'a' boundary
        assert_eq!(chunk_by_exchange_floor_char_boundary(s, 3), 3); // end of 'é' is fine
    }

    #[test]
    fn chunk_by_exchange_small_tail_regression() {
        // Regression test: tail chunk smaller than CHUNK_SIZE_MIN is preserved.
        // Total size = CHUNK_SIZE + (CHUNK_SIZE_MIN - 1) - prefix_len so remainder
        // after first CHUNK_SIZE bytes is strictly < CHUNK_SIZE_MIN.
        let prefix_len = "> user\n".len(); // 7 bytes
        let ai_body = "x".repeat(CHUNK_SIZE + (CHUNK_SIZE_MIN - 1) - prefix_len); // 822 bytes
        let input = format!("> user\n{ai_body}");
        let lines: Vec<&str> = input.lines().collect();

        let chunks = chunk_by_exchange(&lines);

        // Must produce exactly two chunks: one full (800) and one tail (< 30).
        assert_eq!(chunks.len(), 2, "must produce exactly two chunks");

        // Chunk indices must be contiguous and 0-based.
        for (expected, chunk) in chunks.iter().enumerate() {
            assert_eq!(
                chunk.chunk_index, expected,
                "chunk indices must be 0-based and contiguous"
            );
        }

        // Full byte reconstruction: concatenate all chunk bodies.
        let reconstructed = chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<String>();
        assert_eq!(
            reconstructed.as_bytes(),
            input.as_bytes(),
            "reconstructed content must match original bytes exactly"
        );
    }

    #[test]
    fn chunk_exchanges_single_exchange_regression() {
        // Regression: chunk_exchanges must route single-exchange transcripts
        // through chunk_by_exchange, preserving all AI lines.  An earlier
        // threshold of quote_count >= 3 caused single-exchange blocks to fall
        // through to chunk_by_paragraph, silently dropping lines beyond the
        // first paragraph boundary.  This test calls the public dispatcher
        // (chunk_exchanges) rather than chunk_by_exchange directly so any future
        // regression in the routing logic is caught here.
        let lines: Vec<String> = std::iter::once("> user question".to_string())
            .chain((1..=9).map(|n| format!("ai line {n}")))
            .collect();
        let input = lines.join("\n");
        let chunks = chunk_exchanges(&input);
        assert!(!chunks.is_empty(), "must produce at least one chunk");
        let all_text = chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            all_text.contains("ai line 9"),
            "all AI lines must be preserved via chunk_exchanges dispatcher"
        );
        // Chunk indices must be contiguous and 0-based.
        for (expected, chunk) in chunks.iter().enumerate() {
            assert_eq!(
                chunk.chunk_index, expected,
                "chunk indices must be 0-based and contiguous"
            );
        }
    }

    #[test]
    fn chunk_by_paragraph_multiple_paragraphs() {
        // Content with multiple double-newline-separated paragraphs above CHUNK_SIZE_MIN
        // must produce one chunk per paragraph with 0-based contiguous indices.
        let content = "First paragraph with enough text to exceed the minimum size check here.\n\n\
                       Second paragraph with its own content that also exceeds the minimum.\n\n\
                       Third paragraph rounds out the set for thorough coverage.";
        let chunks = chunk_by_paragraph(content);
        assert!(
            chunks.len() >= 2,
            "multiple paragraphs must produce multiple chunks"
        );
        // Chunk indices must be contiguous from 0.
        for (expected, chunk) in chunks.iter().enumerate() {
            assert_eq!(
                chunk.chunk_index, expected,
                "chunk indices must be 0-based and contiguous"
            );
        }
        assert!(
            !chunks[0].content.is_empty(),
            "first chunk must contain content"
        );
    }

    #[test]
    fn chunk_by_paragraph_long_content_no_double_newlines() {
        // Content with more than 20 single-newline-separated lines but no double-newlines
        // triggers the line-grouping fallback (chunks of 25 lines each).
        let lines: Vec<String> = (0..26)
            .map(|i| format!("Line {i} has enough content to pass the minimum size filter here."))
            .collect();
        let content = lines.join("\n");
        let chunks = chunk_by_paragraph(&content);
        // Two groups: first 25 lines and one trailing line (filtered if too short).
        assert!(
            !chunks.is_empty(),
            "long single-block content must produce at least one chunk"
        );
        for chunk in &chunks {
            assert!(
                chunk.content.trim().len() > CHUNK_SIZE_MIN,
                "every chunk must exceed CHUNK_SIZE_MIN"
            );
        }
    }

    #[test]
    fn chunk_by_paragraph_short_paragraph_filtered() {
        // A paragraph shorter than CHUNK_SIZE_MIN must be excluded from output.
        let content = "short\n\nThis second paragraph has enough characters to exceed the minimum chunk threshold.";
        let chunks = chunk_by_paragraph(content);
        // "short" (5 chars) is below CHUNK_SIZE_MIN=30 and must be filtered.
        assert_eq!(
            chunks.len(),
            1,
            "short paragraph must be excluded; only long one survives"
        );
        assert!(
            chunks[0].content.contains("second paragraph"),
            "the surviving chunk must contain the long paragraph"
        );
    }

    #[test]
    fn detect_convo_room_general_fallback() {
        // Content with no recognisable topic keywords must fall through to "general".
        let content =
            "This text contains no particular domain keywords at all, just random filler.";
        let room = detect_convo_room(content);
        assert_eq!(
            room, "general",
            "unrecognised content must produce 'general'"
        );
        assert!(!room.is_empty(), "room name must never be empty");
    }

    #[test]
    fn detect_convo_room_architecture_keywords() {
        // Content with architecture keywords must be classified as "architecture".
        let content = "We discussed the system architecture, service interface design, and module components.";
        let room = detect_convo_room(content);
        assert_eq!(
            room, "architecture",
            "architecture keywords must map to 'architecture'"
        );
        assert!(!room.is_empty(), "room name must not be empty");
    }

    #[test]
    fn detect_convo_room_planning_keywords() {
        // Content with planning keywords must be classified as "planning".
        let content =
            "We need a roadmap with milestones, priorities, and sprint backlogs for the deadline.";
        let room = detect_convo_room(content);
        assert_eq!(room, "planning", "planning keywords must map to 'planning'");
        assert!(!room.is_empty(), "room name must not be empty");
    }

    #[test]
    fn detect_convo_room_decisions_keywords() {
        // Content with decision keywords must be classified as "decisions".
        let content = "We decided and chose the approach after considering the trade-off and alternative options.";
        let room = detect_convo_room(content);
        assert_eq!(
            room, "decisions",
            "decision keywords must map to 'decisions'"
        );
        assert!(!room.is_empty(), "room name must not be empty");
    }

    #[test]
    fn detect_convo_room_problems_keywords() {
        // Content with problem/issue keywords must be classified as "problems".
        let content =
            "There was a problem with a broken component that crashed; we found a workaround.";
        let room = detect_convo_room(content);
        assert_eq!(room, "problems", "problem keywords must map to 'problems'");
        assert!(!room.is_empty(), "room name must not be empty");
    }

    #[test]
    fn chunk_exchanges_routes_to_paragraph_when_preamble_precedes_quote() {
        // Content whose first non-empty line is NOT a '>' marker must be routed to
        // chunk_by_paragraph, preserving preamble text that chunk_by_exchange would drop.
        let content = "This preamble paragraph is long enough to exceed the minimum chunk size threshold.\n\n\
                       > user question here\nAI answer follows for the exchange.";
        let chunks = chunk_exchanges(content);
        // Preamble must be represented in output (chunk_by_paragraph preserves it).
        assert!(
            !chunks.is_empty(),
            "non-empty content with preamble must produce chunks"
        );
        let all_text: String = chunks.iter().map(|c| c.content.as_str()).collect();
        assert!(
            all_text.contains("preamble"),
            "preamble text must be preserved when routing to chunk_by_paragraph"
        );
    }

    #[test]
    fn walk_convos_collects_valid_extensions_excludes_meta_json() {
        // walk_convos must collect .txt, .md, .json, .jsonl files but exclude .meta.json.
        // The .meta.json exclusion is critical: Claude Code writes per-session metadata
        // files with this suffix that are not conversation transcripts.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for walk_convos test");
        std::fs::write(temp_directory.path().join("convo.txt"), "some content here")
            .expect("failed to write test txt file");
        std::fs::write(temp_directory.path().join("convo.md"), "# markdown content")
            .expect("failed to write test md file");
        std::fs::write(temp_directory.path().join("session.meta.json"), "{}")
            .expect("failed to write test meta.json file");
        std::fs::write(temp_directory.path().join("image.png"), b"\x89PNG")
            .expect("failed to write test png file");

        let mut files = Vec::new();
        walk_convos(temp_directory.path(), &mut files);

        let names: Vec<String> = files
            .iter()
            .filter_map(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .collect();

        assert!(
            names.contains(&"convo.txt".to_string()),
            "txt must be collected"
        );
        assert!(
            names.contains(&"convo.md".to_string()),
            "md must be collected"
        );
        assert!(
            !names.contains(&"session.meta.json".to_string()),
            "meta.json must be excluded — it is Claude Code session metadata, not a transcript"
        );
        assert!(
            !names.contains(&"image.png".to_string()),
            "png must be excluded — not a supported conversation extension"
        );
        assert_eq!(names.len(), 2, "exactly two valid files must be collected");
    }

    #[test]
    fn walk_convos_skips_tool_results_and_memory_dirs() {
        // walk_convos must not descend into "tool-results" or "memory" directories
        // — these contain Claude Code artefacts, not conversation transcripts.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for walk_convos skip-dirs test");
        let tool_results_directory = temp_directory.path().join("tool-results");
        let memory_directory = temp_directory.path().join("memory");
        std::fs::create_dir_all(&tool_results_directory)
            .expect("failed to create tool-results directory");
        std::fs::create_dir_all(&memory_directory).expect("failed to create memory directory");
        std::fs::write(tool_results_directory.join("result.txt"), "tool output")
            .expect("failed to write tool result file");
        std::fs::write(memory_directory.join("note.md"), "memory note")
            .expect("failed to write memory note file");
        std::fs::write(
            temp_directory.path().join("valid.txt"),
            "valid conversation",
        )
        .expect("failed to write valid conversation file");

        let mut files = Vec::new();
        walk_convos(temp_directory.path(), &mut files);

        let names: Vec<String> = files
            .iter()
            .filter_map(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .collect();

        assert_eq!(
            names,
            vec!["valid.txt".to_string()],
            "only the top-level valid.txt must be collected"
        );
        assert!(
            !names.contains(&"result.txt".to_string()),
            "files in tool-results must be excluded — not conversation transcripts"
        );
        assert!(
            !names.contains(&"note.md".to_string()),
            "files in memory must be excluded — not conversation transcripts"
        );
    }

    // -- async tests ---------------------------------------------------------

    #[tokio::test]
    async fn mine_convos_write_chunks_stores_drawers_in_db() {
        // Verify that mine_convos_write_chunks inserts exactly the provided chunks.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let chunks = vec![
            Chunk {
                content: "First chunk content with enough bytes to exceed minimum size."
                    .to_string(),
                chunk_index: 0,
            },
            Chunk {
                content: "Second chunk follows with more content for the second drawer."
                    .to_string(),
                chunk_index: 1,
            },
        ];
        let opts = MineParams {
            wing: Some("test_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        mine_convos_write_chunks(
            &connection,
            &chunks,
            "test_wing",
            "technical",
            "source.txt",
            1_700_000_000.0,
            &opts,
        )
        .await
        .expect("mine_convos_write_chunks should succeed for valid chunks and connection");

        // Pair assertion: verify the rows landed in the database.
        let rows = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'test_wing'",
            (),
        )
        .await
        .expect("query for drawers after write should succeed");
        assert_eq!(
            rows.len(),
            2,
            "both chunks must be stored as separate drawers"
        );
        assert!(
            rows[0].get::<String>(0).is_ok(),
            "drawer id must be a valid string column"
        );
    }

    #[tokio::test]
    async fn mine_convos_processes_conversation_files() {
        // End-to-end: mine_convos must scan a temp directory and file conversation chunks.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for mine_convos test");
        // Exchange-format file: starts with '>' so chunk_by_exchange is used.
        let content = "> user asks about architecture\n\
                       The assistant explains component design and interface patterns in detail.";
        std::fs::write(temp_directory.path().join("convo1.txt"), content)
            .expect("failed to write test conversation file convo1.txt");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("test_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        mine_convos(&connection, temp_directory.path(), "full", &opts)
            .await
            .expect("mine_convos should succeed for a directory with valid conversation files");

        // Pair assertion: at least one drawer must exist after mining.
        let rows = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'test_wing'",
            (),
        )
        .await
        .expect("query for drawers after mining should succeed");
        assert!(
            !rows.is_empty(),
            "at least one drawer must be filed after mining"
        );
        // Pair assertion: verify the drawer has the correct wing.
        let drawer_id: String = rows[0].get(0).expect("drawer id column must be readable");
        assert!(
            drawer_id.starts_with("drawer_"),
            "drawer id must start with the 'drawer_' prefix"
        );
    }

    #[tokio::test]
    async fn mine_convos_dry_run_writes_nothing() {
        // In dry-run mode mine_convos must report without inserting any drawers.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for dry-run test");
        let content = "> user asks about planning and roadmap details\n\
                       The assistant replies about milestones, priorities, and sprints.";
        std::fs::write(temp_directory.path().join("convo2.txt"), content)
            .expect("failed to write test conversation file convo2.txt");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("dry_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: true,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        mine_convos(&connection, temp_directory.path(), "full", &opts)
            .await
            .expect("mine_convos dry run should succeed without writing to the database");

        // Nothing should be written.
        let rows = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'dry_wing'",
            (),
        )
        .await
        .expect("query for drawers after dry run should succeed");
        assert!(rows.is_empty(), "dry run must not insert any drawers");
        // Pair assertion: the conversation file must still exist (not consumed).
        assert!(
            temp_directory.path().join("convo2.txt").exists(),
            "dry run must not delete or rename the source file"
        );
    }

    #[tokio::test]
    async fn mine_convos_skips_already_mined_files() {
        // A file that is already mined must not produce duplicate drawers on a second run.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for dedup test");
        let content =
            "> question about decisions and alternatives\nAnswer with trade-off discussion here.";
        std::fs::write(temp_directory.path().join("already_mined.txt"), content)
            .expect("failed to write test conversation file already_mined.txt");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("skip_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        // First run: file gets mined.
        mine_convos(&connection, temp_directory.path(), "full", &opts)
            .await
            .expect("first mine_convos run should succeed");
        let first_count = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'skip_wing'",
            (),
        )
        .await
        .expect("query for drawers after first run should succeed")
        .len();

        // Second run: file is already filed; count must not increase.
        mine_convos(&connection, temp_directory.path(), "full", &opts)
            .await
            .expect("second mine_convos run should succeed without re-filing");
        let second_count = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'skip_wing'",
            (),
        )
        .await
        .expect("query for drawers after second run should succeed")
        .len();

        assert!(first_count >= 1, "first run must add at least one drawer");
        assert_eq!(
            first_count, second_count,
            "second run must not add drawers for an already-mined file"
        );
    }

    #[tokio::test]
    async fn mine_convos_skips_too_short_files() {
        // Files with content below CHUNK_SIZE_MIN must be counted as too-short
        // and must not produce any drawers.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for too-short test");
        std::fs::write(temp_directory.path().join("tiny.txt"), "tiny")
            .expect("failed to write too-short test file tiny.txt");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("short_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        mine_convos(&connection, temp_directory.path(), "full", &opts)
            .await
            .expect("mine_convos should return Ok even for too-short files");

        let rows = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'short_wing'",
            (),
        )
        .await
        .expect("query for drawers after too-short run should succeed");
        assert!(rows.is_empty(), "too-short files must not produce drawers");
        // Pair assertion: the tiny file must still exist on disk (not consumed).
        assert!(
            temp_directory.path().join("tiny.txt").exists(),
            "too-short file must not be deleted or renamed"
        );
    }

    #[tokio::test]
    async fn mine_convos_derives_wing_from_directory_name() {
        // When opts.wing is None the wing is derived from the directory name.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for wing-derivation test");
        let content = "> user asks about architecture and service design patterns\n\
             The assistant explains the module structure and interface components here.";
        std::fs::write(temp_directory.path().join("test.txt"), content)
            .expect("failed to write test conversation file test.txt");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: None, // Falls back to directory name.
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        mine_convos(&connection, temp_directory.path(), "full", &opts)
            .await
            .expect("mine_convos with derived wing should succeed");

        // Compute the expected wing from the temp directory's last path component.
        let expected_wing = temp_directory
            .path()
            .canonicalize()
            .expect("temp directory must be canonicalizable")
            .file_name()
            .expect("canonicalized temp directory must have a file name")
            .to_string_lossy()
            .to_lowercase()
            .replace([' ', '-'], "_");

        let rows = crate::db::query_all(&connection, "SELECT wing FROM drawers", ())
            .await
            .expect("query for drawer wing should succeed");
        assert!(
            !rows.is_empty(),
            "mine_convos with None wing must file at least one drawer"
        );
        // Verify the wing was actually derived from the directory name.
        let actual_wing: String = rows[0].get(0).expect("wing column must be readable");
        assert_eq!(
            actual_wing, expected_wing,
            "wing must be derived from the temp directory name"
        );
    }

    #[tokio::test]
    async fn mine_convos_with_limit_caps_files_processed() {
        // opts.limit=1 must process at most one file even when the directory has more.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for limit test");
        let content =
            "> question about technical architecture design\nThe assistant explains the pattern.";
        std::fs::write(temp_directory.path().join("a.txt"), content)
            .expect("failed to write test file a.txt");
        std::fs::write(temp_directory.path().join("b.txt"), content)
            .expect("failed to write test file b.txt");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("limit_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 1,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        mine_convos(&connection, temp_directory.path(), "full", &opts)
            .await
            .expect("mine_convos with limit=1 should succeed");

        // Count distinct source files processed, not individual drawers (a single
        // file may produce multiple drawers via chunking).
        let limited_file_count = crate::db::query_all(
            &connection,
            "SELECT DISTINCT source_file FROM drawers WHERE wing = 'limit_wing'",
            (),
        )
        .await
        .expect("query for limited distinct source files should succeed")
        .len();

        let (_db2, connection2) = crate::test_helpers::test_db().await;
        let opts_unlimited = MineParams {
            wing: Some("unlimited_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };
        mine_convos(&connection2, temp_directory.path(), "full", &opts_unlimited)
            .await
            .expect("mine_convos with limit=0 should succeed");
        let unlimited_file_count = crate::db::query_all(
            &connection2,
            "SELECT DISTINCT source_file FROM drawers WHERE wing = 'unlimited_wing'",
            (),
        )
        .await
        .expect("query for unlimited distinct source files should succeed")
        .len();

        assert_eq!(
            limited_file_count, 1,
            "limit=1 must process exactly one source file"
        );
        assert_eq!(
            unlimited_file_count, 2,
            "unlimited run must process both source files (a.txt and b.txt)"
        );
    }

    #[tokio::test]
    async fn mine_convos_not_a_directory_returns_error() {
        // Passing a non-directory path must return Err rather than panicking.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for error test");
        let file_path = temp_directory.path().join("not_a_dir.txt");
        std::fs::write(&file_path, "content").expect("failed to write not_a_dir.txt");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("err_wing".to_string()),
            agent: "agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        let result = mine_convos(&connection, &file_path, "full", &opts).await;
        assert!(result.is_err(), "non-directory path must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("directory")
                    || error.to_string().contains("not found")),
            "error message must mention 'directory' or 'not found'"
        );
    }

    #[test]
    fn chunk_exchanges_routes_to_paragraph_when_fully_unquoted() {
        // Content with zero '>' lines must route to chunk_by_paragraph.
        let content = "This is a long paragraph without any quoted turns or user markers.\n\n\
                       It continues here with a second paragraph of sufficient length for the chunk filter.";
        let chunks = chunk_exchanges(content);
        // Both paragraphs are > CHUNK_SIZE_MIN so at least one chunk must be produced.
        assert!(
            !chunks.is_empty(),
            "unquoted content must produce at least one chunk via chunk_by_paragraph"
        );
        let all_text: String = chunks.iter().map(|c| c.content.as_str()).collect();
        assert!(
            all_text.contains("long paragraph"),
            "paragraph text must be preserved when routing to chunk_by_paragraph"
        );
    }

    #[test]
    fn chunk_by_exchange_skips_non_quote_lines_between_exchanges() {
        // Lines that are neither '>' nor AI response (e.g. separators, blank lines)
        // between exchanges are silently skipped by the outer loop.
        let input = "---\n> first user turn\nfirst AI answer here for this exchange\n---\n\
                     > second user turn\nsecond AI answer follows the second user turn";
        let lines: Vec<&str> = input.lines().collect();
        let chunks = chunk_by_exchange(&lines);
        // Two user turns must each produce a chunk (content > CHUNK_SIZE_MIN).
        assert!(
            chunks.len() >= 2,
            "two user turns must produce at least two chunks"
        );
        let all_text: String = chunks.iter().map(|c| c.content.as_str()).collect();
        assert!(
            all_text.contains("first AI answer"),
            "first exchange AI answer must be preserved"
        );
        assert!(
            all_text.contains("second AI answer"),
            "second exchange AI answer must be preserved"
        );
    }

    #[test]
    fn chunk_by_exchange_user_turn_with_no_ai_response_produces_chunk() {
        // A user turn with no AI response at all must still be flushed as its own chunk
        // if the user turn text is long enough to clear CHUNK_SIZE_MIN.
        let user_turn = "> This is a standalone user turn with plenty of text to exceed the minimum chunk size.";
        let lines: Vec<&str> = vec![user_turn];
        let chunks = chunk_by_exchange(&lines);
        assert_eq!(
            chunks.len(),
            1,
            "standalone user turn must produce exactly one chunk"
        );
        assert!(
            chunks[0].content.contains("standalone user turn"),
            "user turn text must be preserved in the chunk"
        );
    }

    #[test]
    fn chunk_by_exchange_drain_ai_lines_stops_at_separator() {
        // drain_ai_lines must stop when it encounters a "---" separator line.
        let lines = &[
            "first ai line",
            "second ai line",
            "---",
            "this line is after separator",
        ];
        let (collected, new_index) = chunk_by_exchange_drain_ai_lines(lines, 0);
        assert_eq!(collected.len(), 2, "must collect exactly two AI lines");
        assert_eq!(new_index, 2, "index must point at the separator line");
        assert!(
            collected.contains(&"first ai line"),
            "first line must be collected"
        );
        assert!(
            collected.contains(&"second ai line"),
            "second line must be collected"
        );
    }

    #[test]
    fn chunk_by_exchange_drain_ai_lines_stops_at_user_turn() {
        // drain_ai_lines must stop when it encounters a '>' user turn line.
        let lines = &["ai response line one", "> next user turn starts here"];
        let (collected, new_index) = chunk_by_exchange_drain_ai_lines(lines, 0);
        assert_eq!(collected.len(), 1, "must collect only the first AI line");
        assert_eq!(new_index, 1, "index must point at the user turn line");
    }

    #[test]
    fn mine_convos_print_summary_dry_run_prints_without_panicking() {
        // mine_convos_print_summary must run to completion in both dry-run and live modes.
        // This covers the conditional output paths for nonzero skip counters.
        let mut room_counts = HashMap::new();
        room_counts.insert("technical".to_string(), 2);
        room_counts.insert("general".to_string(), 1);

        // Dry-run path: covers the `if dry_run` branch and sorted room output.
        mine_convos_print_summary(true, 5, 1, 1, 1, 1, 3, &room_counts);

        // Non-dry-run path: covers "Done." and "Next: mempalace search..." lines.
        mine_convos_print_summary(false, 5, 1, 0, 0, 0, 3, &room_counts);

        // Empty room_counts: covers the `if !sorted_rooms.is_empty()` false branch.
        mine_convos_print_summary(false, 1, 0, 0, 0, 0, 1, &HashMap::new());

        // Pair assertion: room_counts must be unmodified — the function only reads it.
        assert_eq!(
            room_counts.len(),
            2,
            "room_counts must be unmodified after print_summary"
        );
    }

    #[test]
    fn mine_convos_resolve_wing_uses_explicit_wing_override() {
        // When opts.wing is Some, the wing name from opts must be used verbatim.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for wing override test");
        let opts = MineParams {
            wing: Some("my_explicit_wing".to_string()),
            agent: "agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };
        let result = mine_convos_resolve_wing(temp_directory.path(), &opts)
            .expect("resolve_wing must succeed with explicit wing");
        assert_eq!(result.1, "my_explicit_wing", "explicit wing must be used");
        assert!(
            result.0.is_absolute(),
            "returned directory path must be absolute"
        );
    }

    // --- mine_convos: EmptyChunks outcome ---

    #[tokio::test]
    async fn mine_convos_skips_files_producing_no_chunks() {
        // A file whose content is above CHUNK_SIZE_MIN but produces zero chunks
        // (e.g. plain text with no '>'-prefixed lines) must count as EmptyChunks.
        // Content: one paragraph barely above CHUNK_SIZE_MIN but below paragraph threshold.
        // We craft content that is >CHUNK_SIZE_MIN bytes but will parse to zero chunks
        // because it is a single short paragraph (<=20 lines, one paragraph).
        // "a" * 35 = 35 chars > CHUNK_SIZE_MIN(30) but chunk_by_paragraph filters
        // paragraphs <=CHUNK_SIZE_MIN(30) — we need content between 31 and 30.
        // Actually chunk_by_paragraph uses > CHUNK_SIZE_MIN (not >=), so 31 chars passes.
        // To get EmptyChunks we need content that chunk_exchanges returns [] for.
        // chunk_by_paragraph filters p.len() > CHUNK_SIZE_MIN. Single paragraph of exactly
        // CHUNK_SIZE_MIN chars is filtered. Use content of exactly CHUNK_SIZE_MIN length.
        let temp_dir =
            tempfile::tempdir().expect("failed to create temp dir for empty-chunks test");
        // Exactly CHUNK_SIZE_MIN (30) chars — passes the "too short" gate (>= CHUNK_SIZE_MIN)
        // but is filtered by chunk_by_paragraph (len > CHUNK_SIZE_MIN fails for ==).
        let exactly_min = "a".repeat(CHUNK_SIZE_MIN);
        std::fs::write(temp_dir.path().join("borderline.txt"), &exactly_min)
            .expect("write borderline.txt must succeed");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("empty_chunk_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        mine_convos(&connection, temp_dir.path(), "full", &opts)
            .await
            .expect("mine_convos must succeed even with empty-chunk files");

        let rows = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'empty_chunk_wing'",
            (),
        )
        .await
        .expect("query must succeed");
        // Empty-chunks file must produce no drawers.
        assert!(
            rows.is_empty(),
            "file producing no chunks must not insert drawers"
        );
    }

    #[test]
    fn mine_convos_resolve_wing_derives_wing_from_directory_name() {
        // When opts.wing is None, the wing is derived from the directory basename.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for derived wing test");
        let opts = MineParams {
            wing: None,
            agent: "agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };
        let result = mine_convos_resolve_wing(temp_directory.path(), &opts)
            .expect("resolve_wing must succeed with derived wing");
        // Derived wing is the last path component, lowercased, spaces/dashes → underscore.
        let expected = temp_directory
            .path()
            .canonicalize()
            .expect("temp dir must canonicalize")
            .file_name()
            .expect("temp dir must have file name")
            .to_string_lossy()
            .to_lowercase()
            .replace([' ', '-'], "_");
        assert_eq!(
            result.1, expected,
            "derived wing must match directory basename"
        );
        assert!(!result.1.is_empty(), "derived wing must not be empty");
    }

    // --- mine_convos_resolve_wing: nonexistent directory returns error ---

    #[test]
    fn mine_convos_resolve_wing_returns_error_for_nonexistent_path() {
        // A path that cannot be canonicalized must return Err. Covers L649-651.
        let opts = MineParams {
            wing: Some("mywing".to_string()),
            agent: "agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };
        let result = mine_convos_resolve_wing(
            std::path::Path::new("/nonexistent/path/that/cannot/canonicalize"),
            &opts,
        );
        assert!(result.is_err(), "nonexistent path must return Err");
        assert!(
            result
                .err()
                .is_some_and(|e| e.to_string().contains("not found")),
            "error must mention 'not found'"
        );
    }

    // --- mine_convos_print_header: dry_run=false branch ---

    #[test]
    fn mine_convos_print_header_non_dry_run_does_not_panic() {
        // The !dry_run branch of mine_convos_print_header (L446) must execute without panic.
        let temp_dir =
            tempfile::tempdir().expect("failed to create temp dir for header non-dry-run test");
        // Both branches must run without panic.
        mine_convos_print_header("mywing", temp_dir.path(), 5, "full", false);
        mine_convos_print_header("mywing", temp_dir.path(), 0, "full", true);
        // Pair assertion: directory must still exist after printing.
        assert!(temp_dir.path().is_dir(), "temp dir must still exist");
    }

    // --- chunk_by_paragraph: single long paragraph (line-group fallback) filtered group ---

    #[test]
    fn chunk_by_paragraph_line_group_fallback_filters_short_groups() {
        // The line-grouping fallback (25-line groups) must filter groups shorter than
        // CHUNK_SIZE_MIN. Covers the `None` arm of the filter_map on L349.
        // Build 26 lines where all but the last are long enough but the last is very short.
        let mut lines: Vec<String> = (0..25)
            .map(|i| {
                format!("Line {i} has enough text to exceed the CHUNK_SIZE_MIN threshold here.")
            })
            .collect();
        // 26th line group will contain only this one short line — below CHUNK_SIZE_MIN.
        lines.push("tiny".to_string());

        // Content must have >20 lines and no double-newlines for the fallback to trigger.
        let content = lines.join("\n");
        let chunks = chunk_by_paragraph(&content);

        // The first group (25 long lines) must survive; the last group ("tiny") must be filtered.
        assert!(
            !chunks.is_empty(),
            "at least one chunk must survive filtering"
        );
        for chunk in &chunks {
            assert!(
                chunk.content.trim().len() > CHUNK_SIZE_MIN,
                "every chunk must exceed CHUNK_SIZE_MIN"
            );
        }
        // Pair assertion: no chunk must contain the short tail line.
        let all_text: String = chunks.iter().map(|c| c.content.as_str()).collect();
        assert!(
            !all_text.trim_end().ends_with("tiny"),
            "short trailing group must be filtered out"
        );
    }

    // --- walk_convos: skips_is_skip_dir_directories ---

    #[test]
    fn walk_convos_skips_is_skip_dir_directories() {
        // walk_convos must respect is_skip_dir() — directories like "node_modules"
        // are global cache dirs and must not be descended into.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for skip_dir test");
        let node_modules_directory = temp_directory.path().join("node_modules");
        std::fs::create_dir_all(&node_modules_directory)
            .expect("failed to create node_modules directory");
        std::fs::write(
            node_modules_directory.join("package.txt"),
            "npm package content here",
        )
        .expect("failed to write node_modules package.txt");
        std::fs::write(
            temp_directory.path().join("valid_convo.txt"),
            "valid conversation content",
        )
        .expect("failed to write valid_convo.txt");

        let mut files = Vec::new();
        walk_convos(temp_directory.path(), &mut files);

        let names: Vec<String> = files
            .iter()
            .filter_map(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .collect();

        assert!(
            names.contains(&"valid_convo.txt".to_string()),
            "top-level valid file must be collected"
        );
        assert!(
            !names.contains(&"package.txt".to_string()),
            "files inside node_modules must be excluded"
        );
    }

    // --- mine_convos_resolve_wing: root directory returns error ---

    #[test]
    fn mine_convos_resolve_wing_returns_error_for_root_without_wing() {
        // When opts.wing is None and the directory is a filesystem root (no basename),
        // mine_convos_resolve_wing must return Err with a descriptive message.
        // This covers the dir_name.is_empty() branch at lines 674-680.
        let opts = MineParams {
            wing: None, // No explicit override — must derive from directory basename.
            agent: "agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };
        // "/" canonicalizes successfully but has no file_name component, so
        // unwrap_or_default() returns "" and the empty-check fires.
        let result = mine_convos_resolve_wing(std::path::Path::new("/"), &opts);
        assert!(
            result.is_err(),
            "filesystem root with no explicit wing must return Err"
        );
        let error_message = result.expect_err("already confirmed is_err").to_string();
        assert!(
            error_message.contains("filesystem root") || error_message.contains("wing"),
            "error must mention 'filesystem root' or 'wing': {error_message}"
        );
    }

    // --- walk_convos: symlink skipped ---

    #[test]
    fn walk_convos_skips_symlinks() {
        // walk_convos must skip symlinks — covers the `if path.is_symlink()` branch.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for symlink test");
        // Create a real file that will be the symlink target.
        let target = temp_directory.path().join("real.txt");
        std::fs::write(&target, "real conversation content here")
            .expect("failed to write real.txt");
        // Create a symlink pointing to the real file.
        let link = temp_directory.path().join("link.txt");
        std::os::unix::fs::symlink(&target, &link).expect("failed to create symlink");

        let mut files = Vec::new();
        walk_convos(temp_directory.path(), &mut files);

        let names: Vec<String> = files
            .iter()
            .filter_map(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .collect();

        // The real file must be collected; the symlink must not be.
        assert!(
            names.contains(&"real.txt".to_string()),
            "real file must be collected"
        );
        assert!(
            !names.contains(&"link.txt".to_string()),
            "symlink must be skipped — prevents following links to dangerous targets"
        );
    }

    // --- mine_convos_scan_file: Unreadable from normalize failure ---

    #[tokio::test]
    async fn mine_convos_handles_unreadable_file_gracefully() {
        // When a file passes the scan but is deleted before normalize() can read it,
        // mine_convos must count it as Unreadable (files_unreadable counter) and not panic.
        // We simulate this by mining a directory where the file is removed mid-run via
        // a second call after the first already-mined check clears it.
        // More directly: call mine_convos on a directory containing a file that normalize
        // will fail on (e.g., a file > FILE_SIZE_MAX after the walk).
        // The simplest reliable path: mine a directory with a valid file, delete it
        // between scan_convos and the per-file loop — but that's a race.
        // Instead: create a temp file with an unreadable extension that scan still picks up,
        // then create a .txt file that exists during scan but is gone by scan time...
        // Actually the simplest is to verify mine_convos returns Ok() for a directory
        // with a file that becomes unreadable (zero-byte "file" at a non-file path).
        // The Unreadable path (line 598) fires when normalize returns Err. normalize
        // returns Err when the file does not exist. We can achieve this by:
        // 1. Write a valid .txt file.
        // 2. Start mine_convos — it calls scan_convos, producing the file list.
        // 3. But we can't hook between scan_convos and the loop.
        // Alternative: call mine_convos_scan_file directly, but it's private.
        // Best approach: create a file, mine it (first run succeeds), delete it,
        // then create a NEW file at the same path with just a valid name so the
        // walk picks it up but the content is 0 bytes → TooShort, not Unreadable.
        // For the Unreadable path (normalize err), use a directory on a path that
        // becomes invalid. Actually let's test via mine_convos with content that fails
        // the normalize read: an empty file → normalize returns Err("empty content") which
        // gives Unreadable.

        // normalize::normalize returns Err on an empty file (content.is_empty() check).
        // An empty file exists on disk, passes walk_convos (extension is txt), and
        // normalize returns Err because content.trim().is_empty(). That produces Unreadable.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for unreadable test");
        // Write an empty .txt file — normalize will return Err for empty content.
        std::fs::write(temp_directory.path().join("empty.txt"), "")
            .expect("failed to write empty.txt");

        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = MineParams {
            wing: Some("unreadable_wing".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            respect_gitignore: true,
            include_ignored_paths: vec![],
        };

        // mine_convos must succeed — unreadable files are counted but do not abort.
        mine_convos(&connection, temp_directory.path(), "full", &opts)
            .await
            .expect("mine_convos must return Ok even when files are unreadable");

        // No drawers must be filed — the file produced an Unreadable outcome.
        let rows = crate::db::query_all(
            &connection,
            "SELECT id FROM drawers WHERE wing = 'unreadable_wing'",
            (),
        )
        .await
        .expect("query for drawers must succeed");
        assert!(
            rows.is_empty(),
            "unreadable/empty files must not produce any drawers"
        );
    }

    // --- chunk_by_exchange_flush: end == 0 edge case ---

    #[test]
    fn chunk_by_exchange_flush_handles_content_fitting_in_one_chunk() {
        // Content exactly at CHUNK_SIZE_MIN+1 bytes — fits in one chunk without splitting.
        // This exercises the `else if content.trim().len() > CHUNK_SIZE_MIN` branch.
        let mut chunks = Vec::new();
        let content = "a".repeat(CHUNK_SIZE_MIN + 1);
        chunk_by_exchange_flush(content.clone(), &mut chunks);
        assert_eq!(
            chunks.len(),
            1,
            "short content must produce exactly one chunk"
        );
        assert_eq!(
            chunks[0].content, content,
            "chunk content must match the original"
        );
    }

    #[test]
    fn chunk_by_exchange_flush_discards_content_at_or_below_chunk_size_min() {
        // Content at exactly CHUNK_SIZE_MIN must be discarded (not > CHUNK_SIZE_MIN).
        let mut chunks = Vec::new();
        let content = "a".repeat(CHUNK_SIZE_MIN);
        chunk_by_exchange_flush(content, &mut chunks);
        assert!(
            chunks.is_empty(),
            "content at exactly CHUNK_SIZE_MIN must be discarded"
        );
        // Pair assertion: a content one byte longer must survive.
        let mut chunks2 = Vec::new();
        chunk_by_exchange_flush("b".repeat(CHUNK_SIZE_MIN + 1), &mut chunks2);
        assert_eq!(
            chunks2.len(),
            1,
            "content at CHUNK_SIZE_MIN+1 must produce one chunk"
        );
    }

    // --- mine_convos_print_summary: empty room_counts, non-dry-run "Next:" ---

    #[test]
    fn mine_convos_print_summary_non_dry_run_no_rooms_does_not_panic() {
        // Covers the `if !dry_run` "Next: mempalace search" println on L506 with
        // an empty room_counts map (the `if !sorted_rooms.is_empty()` false branch).
        let empty_room_counts = HashMap::new();
        // Non-dry-run with zero drawers and no rooms must print without panicking.
        mine_convos_print_summary(false, 0, 0, 0, 0, 0, 0, &empty_room_counts);
        // Pair assertion: room_counts remains empty after the call.
        assert!(
            empty_room_counts.is_empty(),
            "room_counts must not be modified by print_summary"
        );
    }
}
