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
/// Bytes per drawer — large exchanges are split at this boundary (rounded down
/// to a UTF-8 char boundary) so the full AI response is stored without
/// truncation.  Mirrors miner.py's `CHUNK_SIZE`.  Uses `content.len()` (bytes),
/// not `content.chars().count()`, so chunks may be slightly shorter for
/// multi-byte characters.
const CHUNK_SIZE: usize = 800;
/// Files larger than this are skipped — prevents OOM on huge files.
const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

// Compile-time invariant: chunk size must be greater than min chunk size.
const _: () = assert!(CHUNK_SIZE > MIN_CHUNK_SIZE);

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
/// Slicing `s` by a raw byte offset is unsafe when the string contains multi‑byte
/// characters (emoji, accented letters, CJK) because the offset may land mid‑
/// codepoint, causing a panic.  This function walks backwards from `index` until
/// it finds a valid boundary, guaranteeing `&s[..result]` never panics.
fn chunk_by_exchange_floor_char_boundary(s: &str, index: usize) -> usize {
    if index >= s.len() {
        return s.len();
    }
    let mut i = index;
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    // Postcondition: i is a valid char boundary within s.
    debug_assert!(s.is_char_boundary(i));
    debug_assert!(i <= index);
    i
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

            // Full response — no truncation.
            let ai_response = ai_lines.join(" ");
            let content = if ai_response.is_empty() {
                user_turn.to_string()
            } else {
                format!("{user_turn}\n{ai_response}")
            };

            if content.len() > CHUNK_SIZE {
                // First chunk: user turn + as much response as fits.
                // Use char-boundary-safe slicing: a raw byte offset can land
                // mid-codepoint for multi-byte chars (emoji, CJK, accents).
                let first_end = chunk_by_exchange_floor_char_boundary(&content, CHUNK_SIZE);
                let first = &content[..first_end];
                // Guard first chunk to avoid nearly-empty starts.
                if first.trim().len() > MIN_CHUNK_SIZE {
                    chunks.push(Chunk {
                        content: first.to_string(),
                        chunk_index: chunks.len(),
                    });
                }
                // Remaining response in CHUNK_SIZE continuation drawers.
                // Continuation fragments are always pushed (no MIN_CHUNK_SIZE filter)
                // to prevent silent data loss once we've committed to multi-chunk output.
                let mut remainder = &content[first_end..];
                while !remainder.is_empty() {
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
            } else if content.trim().len() > MIN_CHUNK_SIZE {
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
    // file_name() returns None for filesystem roots (e.g. `/`), producing an empty
    // dir_name. An empty wing triggers the assert in drawer::add_drawer, so surface
    // a clear error here instead.
    let wing = if let Some(wing_name) = opts.wing.as_deref() {
        wing_name
    } else if dir_name.is_empty() {
        return Err(crate::error::Error::Other(
            "mine convos: cannot determine wing name — directory is a filesystem root; \
             pass --wing to specify one explicitly"
                .to_string(),
        ));
    } else {
        &dir_name
    };

    let mut all_files = scan_convos(&directory);
    // Sort for deterministic ordering before applying any limit.
    all_files.sort_unstable();
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

        // Always check for duplicates so dry runs report accurate skip counts.
        // Only the write path below is gated on !opts.dry_run.
        if drawer::file_already_mined(connection, &source_file).await? {
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

        // Mtime is required: None conflates "no on-disk source" with
        // "unreadable filesystem", causing file_already_mined() to miss
        // duplicates on reruns and producing stale duplicate chunks.
        let Some(source_mtime) = std::fs::metadata(filepath)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_secs_f64())
        else {
            files_unreadable += 1;
            continue;
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
        // Content is > MIN_CHUNK_SIZE (30) so it must produce exactly one chunk.
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
        // Regression test: tail chunk smaller than MIN_CHUNK_SIZE is preserved.
        // Total size = CHUNK_SIZE + (MIN_CHUNK_SIZE - 1) - prefix_len so remainder
        // after first CHUNK_SIZE bytes is strictly < MIN_CHUNK_SIZE.
        let prefix_len = "> user\n".len(); // 7 bytes
        let ai_body = "x".repeat(CHUNK_SIZE + (MIN_CHUNK_SIZE - 1) - prefix_len); // 822 bytes
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
}
