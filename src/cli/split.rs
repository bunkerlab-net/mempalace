use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

use crate::error::Result;

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
                // Collect at most 60 Unicode scalar values — byte slicing would panic on
                // multibyte characters that happen to straddle the 60-byte boundary.
                let truncated: String = subject.chars().take(60).collect();
                return truncated;
            }
        }
    }
    "session".to_string()
}

/// Process a single mega-file: split it into per-session files and return the number written.
fn split_file(path: &Path, output_dir: &Path, dry_run: bool) -> Result<usize> {
    assert!(path.is_file(), "split_file: path must be an existing file");
    assert!(
        output_dir.is_dir(),
        "split_file: output_dir must be an existing directory"
    );
    if fs::metadata(path).is_ok_and(|m| m.len() > MAX_SPLIT_FILE_SIZE) {
        println!("  SKIP: {} exceeds 500 MB limit", path.display());
        return Ok(0);
    }
    let content = fs::read_to_string(path).unwrap_or_default();
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
pub fn run(
    directory: &Path,
    output_dir: Option<&Path>,
    dry_run: bool,
    min_sessions: usize,
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

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("txt") {
            continue;
        }

        if fs::metadata(&path).is_ok_and(|m| m.len() > MAX_SPLIT_FILE_SIZE) {
            println!("  SKIP: {} exceeds 500 MB limit", path.display());
            continue;
        }
        let content = fs::read_to_string(&path).unwrap_or_default();
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
        let out_dir = output_dir.unwrap_or_else(|| path.parent().unwrap_or(directory));
        if !out_dir.is_dir() {
            return Err(crate::error::Error::Other(format!(
                "split: output directory not found or not a directory: {}",
                out_dir.display()
            )));
        }
        total_written += split_file(path, out_dir, dry_run)?;
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
