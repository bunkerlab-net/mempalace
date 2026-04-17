use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

use crate::error::Result;

/// Collect `.txt` file paths from the top level of `directory`.
///
/// When `respect_gitignore` is `true`, files excluded by `.gitignore` rules are
/// omitted (same engine as `mine --no-gitignore`). Depth is fixed at 1 to match
/// the original `fs::read_dir` behaviour — sub-directories are never traversed.
fn split_collect_txt_files(directory: &Path, respect_gitignore: bool) -> Result<Vec<PathBuf>> {
    // Operating condition: filesystem state can change between the caller's
    // is_dir() check and this call (TOCTOU) — return Err rather than panic.
    if !directory.is_dir() {
        return Err(crate::error::Error::Other(format!(
            "split_collect_txt_files: '{}' is not an existing directory",
            directory.display()
        )));
    }

    let mut paths: Vec<PathBuf> = Vec::new();

    if respect_gitignore {
        let walker = ignore::WalkBuilder::new(directory)
            .git_ignore(true)
            .git_global(true)
            .git_exclude(true)
            .hidden(false)
            .max_depth(Some(1))
            .build();
        for entry in walker {
            // Propagate IO/permission errors rather than silently dropping them,
            // consistent with how the respect_gitignore=false branch uses `entry?`.
            let entry = entry.map_err(|e| crate::error::Error::Other(e.to_string()))?;
            if !entry.file_type().is_some_and(|ft| ft.is_file()) {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("txt") {
                paths.push(path.to_path_buf());
            }
        }
    } else {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            // Guard matches the respect_gitignore branch: skip non-files so
            // that a directory named "foo.txt" is never collected.
            if !path.is_file() {
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("txt") {
                paths.push(path);
            }
        }
    }

    Ok(paths)
}

// Regex statics are compile-time literals; .expect() cannot fail at runtime.
#[allow(clippy::expect_used)]
// Matches the timestamp header line emitted by Claude Code session logs.
static TS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"⏺\s+(\d{1,2}):(\d{2})\s+(AM|PM)\s+\w+,\s+(\w+)\s+(\d{1,2}),\s+(\d{4})")
        .expect("timestamp regex is a compile-time literal and cannot fail to compile")
});

#[allow(clippy::expect_used)]
// Matches shell commands and tool invocations that are not useful subjects.
static SKIP_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\./|cd |ls |python|bash|git |cat |source |export |claude|./activate)")
        .expect("skip regex is a compile-time literal and cannot fail to compile")
});

#[allow(clippy::expect_used)]
// Strips characters that are not safe in filenames.
static CLEAN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[^\w\s-]")
        .expect("clean regex is a compile-time literal and cannot fail to compile")
});

#[allow(clippy::expect_used)]
// Collapses runs of whitespace to a single hyphen in filename subjects.
static SPACE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\s+").expect("space regex is a compile-time literal and cannot fail to compile")
});

#[allow(clippy::expect_used)]
// Strips characters unsafe in filenames when sanitizing the source stem.
static SANITIZE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[^\w.\-]")
        .expect("sanitize regex is a compile-time literal and cannot fail to compile")
});

#[allow(clippy::expect_used)]
// Collapses repeated underscores left by SANITIZE_RE.
static MULTI_UNDERSCORE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"_+")
        .expect("multi-underscore regex is a compile-time literal and cannot fail to compile")
});

const MAX_SPLIT_FILE_SIZE: u64 = 500 * 1024 * 1024; // 500 MB safety limit
/// POSIX filename byte limit; used to compute the subject byte budget in `split_file`.
const FILENAME_BYTE_LIMIT: usize = 255;

/// Find lines where true new sessions begin (Claude Code v header not followed by context restore).
///
/// Returns an empty Vec for empty input — callers treat zero boundaries as "nothing to split".
fn find_session_boundaries(lines: &[&str]) -> Vec<usize> {
    if lines.is_empty() {
        return vec![];
    }
    let mut boundaries = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if line.contains("Claude Code v") {
            // Check next 6 lines for context restore markers
            let nearby: String = lines[i..lines.len().min(i + 6)].join("");
            if !nearby.contains("Ctrl+E") && !nearby.contains("previous messages") {
                boundaries.push(i);
            }
        }
    }
    boundaries
}

/// Extract timestamp from session lines.
fn extract_timestamp(lines: &[&str]) -> Option<String> {
    let months = [
        ("January", "01"),
        ("February", "02"),
        ("March", "03"),
        ("April", "04"),
        ("May", "05"),
        ("June", "06"),
        ("July", "07"),
        ("August", "08"),
        ("September", "09"),
        ("October", "10"),
        ("November", "11"),
        ("December", "12"),
    ];

    for line in lines.iter().take(50) {
        if let Some(caps) = TS_RE.captures(line) {
            let hour = &caps[1];
            let min = &caps[2];
            let ampm = &caps[3];
            let month_name = &caps[4];
            let day = &caps[5];
            let year = &caps[6];

            let mon = months
                .iter()
                .find(|(n, _)| *n == month_name)
                .map_or("00", |(_, m)| *m);

            return Some(format!(
                "{year}-{mon}-{:02}_{hour}{min}{ampm}",
                day.parse::<u32>().unwrap_or(0)
            ));
        }
    }
    None
}

/// Extract a subject from the first meaningful user prompt.
fn extract_subject(lines: &[&str]) -> String {
    for line in lines {
        if let Some(prompt) = line.strip_prefix("> ") {
            let prompt = prompt.trim();
            if prompt.len() > 5 && !SKIP_RE.is_match(prompt) {
                let subject = CLEAN_RE.replace_all(prompt, "");
                let subject = SPACE_RE.replace_all(subject.trim(), "-");
                return subject.into_owned();
            }
        }
    }
    "session".to_string()
}

/// Process a single mega-file: split it into per-session files and return the number written.
// Sequential write loop with per-boundary state: file I/O, timestamp extraction, subject
// extraction, dry-run branching, and backup rename — each step distinct but not extractable
// without splitting across unrelated concerns.
#[allow(clippy::too_many_lines)]
fn split_file(path: &Path, output_dir: &Path, dry_run: bool) -> Result<usize> {
    // These are operating conditions (filesystem state can change between scan and
    // call), not programmer invariants — return Err rather than panic.
    if !path.is_file() {
        return Err(crate::error::Error::Other(format!(
            "split_file: '{}' is not an existing file",
            path.display()
        )));
    }
    if !output_dir.is_dir() {
        return Err(crate::error::Error::Other(format!(
            "split_file: '{}' is not an existing directory",
            output_dir.display()
        )));
    }
    if fs::metadata(path).is_ok_and(|m| m.len() > MAX_SPLIT_FILE_SIZE) {
        println!("  SKIP: {} exceeds 500 MB limit", path.display());
        return Ok(0);
    }
    let content = fs::read_to_string(path).map_err(|e| {
        crate::error::Error::Other(format!("failed to read {}: {e}", path.display()))
    })?;
    let lines: Vec<&str> = content.lines().collect();
    let mut boundaries = find_session_boundaries(&lines);

    if boundaries.len() < 2 {
        return Ok(0);
    }

    boundaries.push(lines.len());

    let src_stem = path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .chars()
        .take(40)
        .collect::<String>();
    let src_stem = SANITIZE_RE.replace_all(&src_stem, "_");

    println!(
        "\n  {}  ({} sessions)",
        path.file_name().unwrap_or_default().to_string_lossy(),
        boundaries.len() - 1
    );

    let mut written = 0usize;
    for i in 0..boundaries.len() - 1 {
        let start = boundaries[i];
        let end = boundaries[i + 1];
        let chunk: Vec<&str> = lines[start..end].to_vec();

        if chunk.len() < 10 {
            continue;
        }

        let ts_part = extract_timestamp(&chunk).unwrap_or_else(|| format!("part{:02}", i + 1));
        let subject = extract_subject(&chunk);

        // Truncate subject to keep the assembled filename within the POSIX byte limit.
        // src_stem and ts_part are always ASCII after sanitization, so their byte
        // lengths equal their char counts. Overhead: "__" (2) + "_" (1) + ".txt" (4).
        let subject_byte_cap = FILENAME_BYTE_LIMIT
            .saturating_sub(src_stem.len())
            .saturating_sub(ts_part.len())
            .saturating_sub(7);
        assert!(
            subject_byte_cap > 0,
            "split_file: src_stem + ts_part overhead already exceeds filesystem limit"
        );
        let subject: String = if subject.len() <= subject_byte_cap {
            subject
        } else {
            // Accumulate chars only while the byte budget allows, preserving UTF-8
            // boundaries that byte-slicing would straddle.
            let mut byte_count = 0usize;
            subject
                .chars()
                .take_while(|&character| {
                    byte_count += character.len_utf8();
                    byte_count <= subject_byte_cap
                })
                .collect()
        };
        debug_assert!(
            subject.len() <= subject_byte_cap,
            "subject byte length {len} exceeds cap {subject_byte_cap}",
            len = subject.len()
        );

        let name = format!("{src_stem}__{ts_part}_{subject}.txt");
        let name = SANITIZE_RE.replace_all(&name, "_");
        let name = MULTI_UNDERSCORE_RE.replace_all(&name, "_");

        let out_path = output_dir.join(name.as_ref());

        if dry_run {
            println!(
                "    [{}/{}] {}  ({} lines)",
                i + 1,
                boundaries.len() - 1,
                out_path.file_name().unwrap_or_default().to_string_lossy(),
                chunk.len()
            );
        } else {
            fs::write(&out_path, chunk.join("\n"))?;
            println!(
                "    + {}  ({} lines)",
                out_path.file_name().unwrap_or_default().to_string_lossy(),
                chunk.len()
            );
        }
        written += 1;
    }

    if !dry_run {
        let backup = path.with_extension("mega_backup");
        fs::rename(path, &backup)?;
        println!(
            "    -> Original renamed to {}",
            backup.file_name().unwrap_or_default().to_string_lossy()
        );
    }

    Ok(written)
}

/// Split mega-files in a directory into per-session files.
// Scan loop, output-dir validation, per-file splitting, and dry-run summary
// are each a distinct step with branching that cannot be cleanly extracted.
#[allow(clippy::too_many_lines)]
pub fn run(
    directory: &Path,
    output_dir: Option<&Path>,
    dry_run: bool,
    min_sessions: usize,
    respect_gitignore: bool,
) -> Result<()> {
    if !directory.is_dir() {
        return Err(crate::error::Error::Other(format!(
            "split: '{}' is not an existing directory",
            directory.display()
        )));
    }
    if min_sessions < 2 {
        return Err(crate::error::Error::Other(
            "split: min_sessions must be at least 2".to_string(),
        ));
    }
    let mut mega_files: Vec<(PathBuf, usize)> = Vec::new();

    for path in split_collect_txt_files(directory, respect_gitignore)? {
        if fs::metadata(&path).is_ok_and(|m| m.len() > MAX_SPLIT_FILE_SIZE) {
            println!("  SKIP: {} exceeds 500 MB limit", path.display());
            continue;
        }
        let content = fs::read_to_string(&path).map_err(|e| {
            crate::error::Error::Other(format!("failed to read {}: {e}", path.display()))
        })?;
        let lines: Vec<&str> = content.lines().collect();
        let boundaries = find_session_boundaries(&lines);
        if boundaries.len() >= min_sessions {
            mega_files.push((path, boundaries.len()));
        }
    }

    // Validate an explicit output directory up front. Without this, a caller that
    // provides --output-dir /nonexistent would silently receive Ok(()) when no
    // mega-files are found, hiding the bad path from the user.
    if let Some(out_dir) = output_dir
        && !out_dir.is_dir()
    {
        return Err(crate::error::Error::Other(format!(
            "split: output directory not found or not a directory: {}",
            out_dir.display()
        )));
    }

    if mega_files.is_empty() {
        println!(
            "No mega-files found in {} (min {min_sessions} sessions).",
            directory.display()
        );
        return Ok(());
    }

    mega_files.sort_by(|a, b| a.0.cmp(&b.0));

    println!("Found {} mega-files to split:", mega_files.len());

    let mut total_written = 0usize;

    for (path, _n_sessions) in &mega_files {
        // When output_dir was provided it is already validated above; skip the
        // redundant is_dir call.  When falling back to the file's parent we must
        // validate per-iteration because different files can have different parents.
        let output_directory = if let Some(explicit_output_directory) = output_dir {
            explicit_output_directory
        } else {
            let file_directory = path.parent().unwrap_or(directory);
            if !file_directory.is_dir() {
                return Err(crate::error::Error::Other(format!(
                    "split: output directory not found or not a directory: {}",
                    file_directory.display()
                )));
            }
            file_directory
        };
        total_written += split_file(path, output_directory, dry_run)?;
    }

    println!();
    if dry_run {
        println!(
            "Dry run: would create {total_written} files from {} mega-files",
            mega_files.len()
        );
    } else {
        println!(
            "Done: created {total_written} files from {} mega-files",
            mega_files.len()
        );
    }

    Ok(())
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // --- find_session_boundaries ---

    #[test]
    fn find_session_boundaries_empty_input() {
        let result = find_session_boundaries(&[]);
        assert!(result.is_empty(), "empty input must produce empty vec");
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn find_session_boundaries_no_sessions() {
        let lines = &["hello world", "some random text", "no markers here"];
        let result = find_session_boundaries(lines);
        assert!(
            result.is_empty(),
            "lines without markers must produce empty vec"
        );
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn find_session_boundaries_finds_markers() {
        let lines = &[
            "╭──── Claude Code v1.0.0",
            "some content here",
            "more content",
            "╭──── Claude Code v1.1.0",
            "other content",
            "╭──── Claude Code v1.2.0",
        ];
        let result = find_session_boundaries(lines);
        assert_eq!(result.len(), 3, "must find all three session markers");
        assert_eq!(
            result,
            vec![0, 3, 5],
            "boundary positions must match marker lines"
        );
    }

    #[test]
    fn find_session_boundaries_skips_context_restore() {
        // A marker followed by "Ctrl+E" within 6 lines should be excluded.
        // The first marker is far enough away from the restore text to pass.
        let lines = &[
            "╭──── Claude Code v1.0.0",
            "some content line 1",
            "some content line 2",
            "some content line 3",
            "some content line 4",
            "some content line 5",
            "some content line 6",
            "some content line 7",
            "╭──── Claude Code v1.1.0",
            "Ctrl+E to restore",
            "more content",
        ];
        let result = find_session_boundaries(lines);
        // First marker's nearby window (lines 0..6) has no restore text — kept.
        // Second marker's nearby window (lines 8..11) contains "Ctrl+E" — skipped.
        assert_eq!(result.len(), 1, "context-restore marker must be excluded");
        assert_eq!(result[0], 0);
    }

    // --- extract_timestamp ---

    #[test]
    fn extract_timestamp_parses_valid() {
        let lines = &[
            "some header",
            "⏺ 2:30 PM Monday, January 15, 2025",
            "other content",
        ];
        let result = extract_timestamp(lines);
        assert!(result.is_some(), "valid timestamp line must be parsed");
        assert_eq!(result.expect("already checked Some"), "2025-01-15_230PM");
    }

    #[test]
    fn extract_timestamp_no_match() {
        let lines = &["no timestamp here", "just plain text", "nothing to see"];
        let result = extract_timestamp(lines);
        assert!(result.is_none(), "lines without timestamp must return None");
        assert_eq!(result, None);
    }

    // --- extract_subject ---

    #[test]
    fn extract_subject_from_prompt() {
        let lines = &[
            "some header",
            "> How do I fix bugs in Rust",
            "other content",
        ];
        let result = extract_subject(lines);
        assert!(
            result.contains("How"),
            "subject must contain 'How' from the prompt"
        );
        assert_ne!(result, "session", "must not fall through to default");
    }

    #[test]
    fn extract_subject_default_for_commands() {
        // Command-like prompts that match SKIP_RE should be skipped.
        let lines = &["> git status", "> cd /tmp", "> ls -la"];
        let result = extract_subject(lines);
        assert_eq!(
            result, "session",
            "command-only prompts must return default 'session'"
        );
        assert!(!result.is_empty(), "subject must not be empty");
    }

    // --- split_collect_txt_files ---

    #[test]
    fn split_collect_txt_files_no_gitignore_returns_all_top_level() {
        // Verify that when respect_gitignore=false, all top-level .txt files are
        // returned regardless of .gitignore, non-files with a .txt name are excluded,
        // and nested .txt files are excluded.
        let directory = tempfile::tempdir().expect("create temp dir");
        fs::write(directory.path().join("a.txt"), "content a").expect("write a.txt");
        fs::write(directory.path().join("b.txt"), "content b").expect("write b.txt");
        fs::write(directory.path().join("notes.md"), "# notes").expect("write notes.md");
        // .gitignore excludes b.txt — must be ignored when respect_gitignore=false.
        fs::write(directory.path().join(".gitignore"), "b.txt\n").expect("write .gitignore");
        // A directory with a .txt extension — is_file() guard must exclude it.
        fs::create_dir(directory.path().join("fake.txt")).expect("create fake.txt dir");
        // Subdirectory with a .txt — depth=1 must exclude it in both modes.
        let subdirectory = directory.path().join("sub");
        fs::create_dir(&subdirectory).expect("create sub");
        fs::write(subdirectory.join("nested.txt"), "nested").expect("write nested.txt");

        let result =
            split_collect_txt_files(directory.path(), false).expect("collect must succeed");
        let mut names: Vec<String> = result
            .iter()
            .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .collect();
        names.sort();

        assert_eq!(names.len(), 2, "both top-level .txt files must be returned");
        assert!(names.contains(&"a.txt".to_owned()), "a.txt must be present");
        assert!(
            names.contains(&"b.txt".to_owned()),
            "b.txt must be present when gitignore not respected"
        );
        assert!(
            !names.contains(&"fake.txt".to_owned()),
            "directory named fake.txt must be excluded by is_file() guard"
        );
        assert!(
            !names.contains(&"nested.txt".to_owned()),
            "nested.txt in subdirectory must be excluded"
        );
    }

    // --- split_file ---

    /// Build a test file containing `n_sessions` Claude Code session blocks, each with
    /// at least 12 content lines so the 10-line minimum is satisfied.
    fn make_mega_file(n_sessions: usize) -> String {
        let mut lines = Vec::new();
        for i in 0..n_sessions {
            lines.push(format!("╭──── Claude Code v1.{i}.0"));
            // Twelve body lines — safely above the 10-line minimum in split_file.
            for j in 0..12 {
                lines.push(format!("Content line {j} for session {i} here."));
            }
            lines.push(format!("> User prompt for session {i} to create subject."));
            lines.push(format!("Assistant answer for session {i} is here now."));
        }
        lines.join("\n")
    }

    #[test]
    fn split_file_dry_run_returns_session_count() {
        // split_file in dry-run mode must count sessions without writing any output files.
        // The dry-run path is the same split logic but skips fs::write and fs::rename.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for dry-run test");
        let mega_file_path = temp_directory.path().join("mega.txt");
        fs::write(&mega_file_path, make_mega_file(3))
            .expect("failed to write three-session mega file");
        let output_directory =
            tempfile::tempdir().expect("failed to create temporary output directory");

        let written = split_file(&mega_file_path, output_directory.path(), true)
            .expect("split_file dry run should succeed for a valid file");

        assert_eq!(written, 3, "dry run must report three sessions written");
        // Nothing must be written — output directory stays empty.
        let output_count = fs::read_dir(output_directory.path())
            .expect("failed to read output directory after dry run")
            .count();
        assert_eq!(
            output_count, 0,
            "dry run must not write any session files to disk"
        );
    }

    #[test]
    fn split_file_writes_sessions_and_renames_original() {
        // split_file must write per-session files and rename the original to .mega_backup.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for write test");
        let mega_file_path = temp_directory.path().join("mega.txt");
        fs::write(&mega_file_path, make_mega_file(2))
            .expect("failed to write two-session mega file");
        let output_directory =
            tempfile::tempdir().expect("failed to create temporary output directory");

        let written = split_file(&mega_file_path, output_directory.path(), false)
            .expect("split_file should succeed for a valid two-session file");

        assert_eq!(written, 2, "must report two sessions written");
        // Original must be renamed to .mega_backup — a rename signals successful completion.
        assert!(
            !mega_file_path.exists(),
            "original file must be renamed away after split"
        );
        assert!(
            mega_file_path.with_extension("mega_backup").exists(),
            "backup file must exist at original path with .mega_backup extension"
        );
        // Two session files must be in the output directory.
        let output_count = fs::read_dir(output_directory.path())
            .expect("failed to read output directory after split")
            .count();
        assert_eq!(
            output_count, 2,
            "two session files must be written to the output directory"
        );
    }

    #[test]
    fn split_file_fewer_than_two_sessions_returns_zero() {
        // A file with only one session boundary must return 0 — nothing to split.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for single-session test");
        let single_session_path = temp_directory.path().join("single.txt");
        fs::write(&single_session_path, make_mega_file(1))
            .expect("failed to write single-session test file");
        let output_directory =
            tempfile::tempdir().expect("failed to create temporary output directory");

        let written = split_file(&single_session_path, output_directory.path(), false)
            .expect("split_file should return Ok even when there is only one session");

        assert_eq!(
            written, 0,
            "single session must return 0 — nothing to split"
        );
        // Original must not be renamed when no split occurred.
        assert!(
            single_session_path.exists(),
            "original file must remain untouched when split produces zero sessions"
        );
    }

    #[test]
    fn split_file_not_a_file_returns_error() {
        // Passing a directory path as the file argument must return Err.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for error test");
        let output_directory =
            tempfile::tempdir().expect("failed to create temporary output directory");
        let result = split_file(temp_directory.path(), output_directory.path(), false);
        assert!(
            result.is_err(),
            "directory path as file argument must return Err"
        );
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("not an existing file")),
            "error must mention 'not an existing file'"
        );
    }

    // --- run ---

    #[test]
    fn run_min_sessions_below_two_returns_error() {
        // min_sessions=1 must be rejected immediately — callers must require at least two.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for min_sessions test");
        let result = run(temp_directory.path(), None, false, 1, false);
        assert!(result.is_err(), "min_sessions < 2 must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("min_sessions")),
            "error message must mention min_sessions"
        );
    }

    #[test]
    fn run_nonexistent_directory_returns_error() {
        // A path that does not point to an existing directory must return Err.
        let result = run(
            std::path::Path::new("/nonexistent/path/that/does/not/exist"),
            None,
            false,
            2,
            false,
        );
        assert!(result.is_err(), "nonexistent directory must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| !error.to_string().is_empty()),
            "error message must not be empty"
        );
    }

    #[test]
    fn run_no_mega_files_returns_ok() {
        // A directory with no .txt files meeting min_sessions must return Ok with no output.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for no-mega-files test");
        // One .txt file but only one session — does not meet min_sessions=2.
        fs::write(temp_directory.path().join("single.txt"), make_mega_file(1))
            .expect("failed to write single-session test file");

        let result = run(temp_directory.path(), None, false, 2, false);
        assert!(
            result.is_ok(),
            "directory with no qualifying mega-files must return Ok"
        );
    }

    #[test]
    fn run_dry_run_with_mega_file_does_not_write() {
        // run in dry-run mode must not write any output files or rename the original.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for run dry-run test");
        let mega_file_path = temp_directory.path().join("mega.txt");
        fs::write(&mega_file_path, make_mega_file(3))
            .expect("failed to write three-session mega file");
        let output_directory =
            tempfile::tempdir().expect("failed to create temporary output directory for dry run");

        run(
            temp_directory.path(),
            Some(output_directory.path()),
            true,
            2,
            false,
        )
        .expect("run dry run should succeed without writing files");

        // Original must still exist; output directory must be empty.
        assert!(
            mega_file_path.exists(),
            "dry run must not rename original mega file"
        );
        let output_count = fs::read_dir(output_directory.path())
            .expect("failed to read output directory after dry run")
            .count();
        assert_eq!(
            output_count, 0,
            "dry run must not write any session files to the output directory"
        );
    }

    #[test]
    fn run_invalid_output_dir_returns_error() {
        // An explicit output directory path that does not exist must return Err.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for invalid output test");
        fs::write(temp_directory.path().join("mega.txt"), make_mega_file(3))
            .expect("failed to write three-session mega file");

        let result = run(
            temp_directory.path(),
            Some(std::path::Path::new("/nonexistent/output/directory")),
            false,
            2,
            false,
        );
        assert!(
            result.is_err(),
            "nonexistent output directory must return Err"
        );
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("output directory")),
            "error message must mention 'output directory'"
        );
    }

    #[test]
    fn split_collect_txt_files_respect_gitignore_filters_and_caps_depth() {
        // Verify that when respect_gitignore=true, .gitignore-excluded files are
        // omitted and nested files beyond depth=1 are excluded.
        let directory = tempfile::tempdir().expect("create temp dir");
        // The ignore crate only honours .gitignore when the directory is a git repo.
        let git_init = std::process::Command::new("git")
            .args(["init"])
            .current_dir(directory.path())
            .output()
            .expect("git init must run");
        assert!(
            git_init.status.success(),
            "git init failed: {:?}",
            String::from_utf8_lossy(&git_init.stderr)
        );
        fs::write(directory.path().join("a.txt"), "content a").expect("write a.txt");
        fs::write(directory.path().join("b.txt"), "content b").expect("write b.txt");
        // .gitignore excludes b.txt.
        fs::write(directory.path().join(".gitignore"), "b.txt\n").expect("write .gitignore");
        // Subdirectory with a .txt — depth limit must exclude it.
        let subdirectory = directory.path().join("sub");
        fs::create_dir(&subdirectory).expect("create sub");
        fs::write(subdirectory.join("nested.txt"), "nested").expect("write nested.txt");

        let result = split_collect_txt_files(directory.path(), true).expect("collect must succeed");
        let mut names: Vec<String> = result
            .iter()
            .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .collect();
        names.sort();

        // Only a.txt survives: b.txt excluded by .gitignore, nested.txt by depth.
        assert_eq!(
            names.len(),
            1,
            "only the non-ignored top-level .txt must be returned"
        );
        assert!(names.contains(&"a.txt".to_owned()), "a.txt must be present");
        assert!(
            !names.contains(&"b.txt".to_owned()),
            "b.txt must be excluded by .gitignore"
        );
        assert!(
            !names.contains(&"nested.txt".to_owned()),
            "nested.txt in subdirectory must be excluded"
        );
    }
}
