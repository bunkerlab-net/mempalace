//! Claude Code session scanner — extract project names from `~/.claude/projects/`.
//!
//! Claude Code stores sessions under `~/.claude/projects/<slug>/<id>.jsonl` where the
//! `<slug>` is the original working directory path with `/` replaced by `-`. That
//! encoding is lossy: `foo-bar` cannot be distinguished from `foo/bar`. Fortunately
//! every JSONL record carries a `cwd` field with the real path, so this scanner reads
//! the first few lines of each session file to recover the accurate project name.
//!
//! Output uses the same [`ProjectInfo`] type as [`project_scanner`], so the
//! `discover_entities` orchestrator (WU-9) can combine both sources with one dedup pass.
//!
//! Public API:
//! - [`is_claude_projects_root`] — detect whether a path is a `.claude/projects/` dir
//! - [`scan_claude_projects`] — scan that dir and return one `ProjectInfo` per project

use std::collections::HashMap;
use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};

use crate::palace::project_scanner::{ProjectInfo, SESSION_HEADER_LINES};

// Maximum number of JSONL session files read per project slug before giving up.
// Reading all sessions would be slow for heavy Claude Code users.
const MAX_SESSIONS_TO_TRY: usize = 5;

const _: () = assert!(MAX_SESSIONS_TO_TRY > 0);

// ===================== PUBLIC API =====================

/// Return `true` if `path` looks like a `.claude/projects/` directory.
///
/// Heuristic: at least one child directory whose name starts with `-` and which
/// contains at least one `.jsonl` file.
pub fn is_claude_projects_root(path: &Path) -> bool {
    assert!(!path.as_os_str().is_empty());
    if !path.is_dir() {
        return false;
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return false;
    };
    for entry in entries.flatten() {
        let child = entry.path();
        if !child.is_dir() {
            continue;
        }
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with('-') {
            continue;
        }
        // Directory starts with `-` — check for at least one .jsonl session file.
        let Ok(session_entries) = std::fs::read_dir(&child) else {
            continue;
        };
        if session_entries.flatten().any(|e| {
            e.path()
                .extension()
                .is_some_and(|extension| extension == "jsonl")
        }) {
            return true;
        }
    }
    false
}

/// Scan a `.claude/projects/` directory and return one [`ProjectInfo`] per project.
///
/// `total_commits` is repurposed as session count — a density signal that lets
/// the `discover_entities` orchestrator rank projects by how much Claude Code
/// activity they have seen. `is_mine` is always `true` because Claude Code
/// sessions are authored by the current user.
pub fn scan_claude_projects(path: &Path) -> Vec<ProjectInfo> {
    assert!(!path.as_os_str().is_empty());
    if !is_claude_projects_root(path) {
        return vec![];
    }

    let mut projects: HashMap<String, ProjectInfo> = HashMap::new();

    let Ok(entries) = std::fs::read_dir(path) else {
        return vec![];
    };
    let mut slug_dirs: Vec<PathBuf> = entries
        .flatten()
        .filter(|e| e.path().is_dir() && e.file_name().to_string_lossy().starts_with('-'))
        .map(|e| e.path())
        .collect();
    slug_dirs.sort();

    for slug_dir in slug_dirs {
        scan_claude_projects_process_slug(slug_dir, &mut projects);
    }

    let mut result: Vec<ProjectInfo> = projects.into_values().collect();
    result.sort_by_key(|p| (std::cmp::Reverse(p.user_commits), p.name.clone()));

    // Postcondition: all returned projects have is_mine = true.
    debug_assert!(result.iter().all(|p| p.is_mine));
    result
}

/// Process one slug directory and insert a `ProjectInfo` into `projects` if it has sessions.
///
/// Called by [`scan_claude_projects`] to keep that function within the 70-line limit.
fn scan_claude_projects_process_slug(
    slug_dir: PathBuf,
    projects: &mut HashMap<String, ProjectInfo>,
) {
    assert!(slug_dir.is_dir(), "slug_dir must be a directory");

    let Ok(session_entries) = std::fs::read_dir(&slug_dir) else {
        return;
    };
    let mut sessions: Vec<PathBuf> = session_entries
        .flatten()
        .filter(|e| {
            e.path().is_file()
                && e.path()
                    .extension()
                    .is_some_and(|extension| extension == "jsonl")
        })
        .map(|e| e.path())
        .collect();
    if sessions.is_empty() {
        return;
    }

    // Sort newest-first (by mtime) so the most-recent session is tried first.
    sessions.sort_by(|a, b| {
        let mtime_a = a.metadata().and_then(|m| m.modified()).ok();
        let mtime_b = b.metadata().and_then(|m| m.modified()).ok();
        mtime_b.cmp(&mtime_a)
    });

    let name = resolve_project_name(&slug_dir, &sessions);
    let session_count = sessions.len();

    assert!(
        !name.is_empty(),
        "resolve_project_name must return a non-empty name"
    );

    let proj = ProjectInfo {
        name: name.clone(),
        repo_root: slug_dir,
        manifest: None,
        has_git: false,
        // Repurpose total_commits / user_commits as session count for ranking.
        total_commits: session_count,
        user_commits: session_count,
        is_mine: true,
    };

    let entry = projects.entry(name);
    match entry {
        std::collections::hash_map::Entry::Vacant(vacant) => {
            vacant.insert(proj);
        }
        std::collections::hash_map::Entry::Occupied(mut occupied) => {
            if session_count > occupied.get().user_commits {
                occupied.insert(proj);
            }
        }
    }
}

// ===================== PRIVATE HELPERS =====================

/// Read one JSONL session file and return the `cwd` field from the first record that has one.
///
/// Returns `None` if the file cannot be opened, has no parsable JSON, or no record has `cwd`.
/// Reads at most `SESSION_HEADER_LINES` lines to bound memory and I/O.
fn extract_cwd_from_session(session_file: &Path) -> Option<String> {
    assert!(!session_file.as_os_str().is_empty());
    let file = std::fs::File::open(session_file).ok()?;
    let reader = BufReader::new(file);
    for (i, line) in reader.lines().enumerate() {
        if i >= SESSION_HEADER_LINES {
            break;
        }
        let Ok(line) = line else { continue };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(obj) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        if let Some(cwd) = obj.get("cwd").and_then(|field| field.as_str())
            && !cwd.is_empty()
        {
            return Some(cwd.to_string());
        }
    }
    None
}

/// Best-effort project name from a slug when no session has a readable `cwd`.
///
/// The slug is lossy (`/` and `-` both map to `-`). The last non-empty segment is
/// the closest approximation to the original directory name.
fn decode_slug_fallback(slug: &str) -> String {
    assert!(
        !slug.is_empty(),
        "decode_slug_fallback: slug must not be empty"
    );
    let stripped = slug.trim_start_matches('-');
    let parts: Vec<&str> = stripped.split('-').filter(|p| !p.is_empty()).collect();
    let result = parts.last().copied().unwrap_or(slug);

    // Postcondition: result is non-empty (we fall back to the original slug).
    debug_assert!(!result.is_empty());
    result.to_string()
}

/// Resolve the project name for a slug directory by reading session JSONL files.
///
/// Tries up to `MAX_SESSIONS_TO_TRY` session files (newest first). Falls back to
/// slug-decoding when no session carries a `cwd`.
fn resolve_project_name(slug_dir: &Path, sessions: &[PathBuf]) -> String {
    assert!(
        slug_dir.is_dir(),
        "resolve_project_name: slug_dir must exist"
    );
    assert!(
        !sessions.is_empty(),
        "resolve_project_name: sessions must not be empty"
    );

    for session in sessions.iter().take(MAX_SESSIONS_TO_TRY) {
        if let Some(cwd) = extract_cwd_from_session(session) {
            let name = Path::new(&cwd)
                .file_name()
                .and_then(|n| n.to_str())
                .filter(|n| !n.is_empty())
                .unwrap_or(&cwd)
                .to_string();
            if !name.is_empty() {
                return name;
            }
        }
    }

    // Fall back to slug-based decoding.
    let slug = slug_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    if slug.is_empty() {
        return "unknown".to_string();
    }
    decode_slug_fallback(slug)
}

// ===================== TESTS =====================

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // -- is_claude_projects_root --

    #[test]
    fn is_claude_projects_root_returns_true_for_valid_layout() {
        // A directory with a `-`-prefixed subdirectory containing .jsonl files is
        // the expected Claude Code projects layout.
        let temp = tempfile::tempdir().expect("tempdir");
        let slug_dir = temp.path().join("-Users-robbie-my-proj");
        std::fs::create_dir(&slug_dir).expect("create slug dir");
        std::fs::write(
            slug_dir.join("abc123.jsonl"),
            r#"{"cwd":"/Users/robbie/my-proj"}"#,
        )
        .expect("write session");

        assert!(
            is_claude_projects_root(temp.path()),
            "directory with slug + .jsonl must be recognised"
        );
    }

    #[test]
    fn is_claude_projects_root_returns_false_for_plain_directory() {
        // A directory without any `-`-prefixed children is not a Claude projects root.
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("notes.txt"), "hello").expect("write file");
        assert!(
            !is_claude_projects_root(temp.path()),
            "plain directory must not be recognised as projects root"
        );
    }

    // -- decode_slug_fallback --

    #[test]
    fn decode_slug_fallback_returns_last_segment() {
        // The last `-`-separated segment is the closest approximation to the dir name.
        let result = decode_slug_fallback("-Users-robbie-my-project");
        assert_eq!(
            result,
            "my-project".split('-').next_back().unwrap_or("my-project")
        );
        // The actual last segment after splitting on `-` and filtering empty:
        assert_eq!(decode_slug_fallback("-Users-robbie-my-project"), "project");
    }

    #[test]
    fn decode_slug_fallback_handles_single_segment_slugs() {
        // A slug with only one non-empty segment returns that segment.
        let result = decode_slug_fallback("-myproject");
        assert_eq!(result, "myproject");
        assert!(!result.is_empty());
    }

    // -- extract_cwd_from_session --

    #[test]
    fn extract_cwd_from_session_reads_first_cwd_record() {
        // The first JSONL line with a `cwd` field should be returned.
        let temp = tempfile::tempdir().expect("tempdir");
        let session = temp.path().join("session.jsonl");
        std::fs::write(
            &session,
            "{\"type\":\"message\"}\n{\"cwd\":\"/Users/robbie/proj\",\"type\":\"human\"}\n",
        )
        .expect("write session");

        let result = extract_cwd_from_session(&session);
        assert!(
            result.is_some(),
            "must extract cwd from well-formed session"
        );
        assert_eq!(result.expect("checked"), "/Users/robbie/proj");
    }

    #[test]
    fn extract_cwd_from_session_returns_none_for_empty_file() {
        // An empty session file should return None gracefully.
        let temp = tempfile::tempdir().expect("tempdir");
        let session = temp.path().join("empty.jsonl");
        std::fs::write(&session, "").expect("write empty file");
        assert!(
            extract_cwd_from_session(&session).is_none(),
            "empty session must return None"
        );
    }

    // -- scan_claude_projects --

    #[test]
    fn scan_claude_projects_returns_empty_for_non_projects_root() {
        // A directory that does not match the Claude projects heuristic returns empty.
        let temp = tempfile::tempdir().expect("tempdir");
        let result = scan_claude_projects(temp.path());
        assert!(result.is_empty(), "non-projects-root must return empty");
    }

    #[test]
    fn scan_claude_projects_resolves_name_from_cwd() {
        // When the session JSONL has a cwd, the project name is the last path segment.
        let temp = tempfile::tempdir().expect("tempdir");
        let slug_dir = temp.path().join("-Users-robbie-Documents-projects-my-app");
        std::fs::create_dir(&slug_dir).expect("create slug dir");
        std::fs::write(
            slug_dir.join("session1.jsonl"),
            "{\"cwd\":\"/Users/robbie/Documents/projects/my-app\",\"type\":\"human\"}\n",
        )
        .expect("write session");

        let projects = scan_claude_projects(temp.path());
        assert!(!projects.is_empty(), "must return at least one project");
        assert_eq!(projects[0].name, "my-app", "name must come from cwd");
        assert!(projects[0].is_mine, "Claude sessions are always mine");
        assert_eq!(projects[0].total_commits, 1, "one session file = count 1");
    }

    #[test]
    fn scan_claude_projects_deduplicates_same_project_name() {
        // Two slug directories that resolve to the same project name must merge into one.
        let temp = tempfile::tempdir().expect("tempdir");
        for slug in &["-Users-robbie-proj", "-root-proj"] {
            let slug_dir = temp.path().join(slug);
            std::fs::create_dir(&slug_dir).expect("create slug dir");
            std::fs::write(
                slug_dir.join("s.jsonl"),
                "{\"cwd\":\"/Users/robbie/proj\",\"type\":\"human\"}\n",
            )
            .expect("write session");
        }

        let projects = scan_claude_projects(temp.path());
        // Both slugs resolve to "proj" — only one entry should survive.
        let names: Vec<&str> = projects.iter().map(|p| p.name.as_str()).collect();
        let proj_count = names.iter().filter(|&&n| n == "proj").count();
        assert_eq!(
            proj_count, 1,
            "duplicate project names must be deduped; found: {names:?}"
        );
    }
}
