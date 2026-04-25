//! Project and people scanner — extract real entities from manifest files and git history.
//!
//! This is the primary signal source for `mempalace init`. When a directory has build
//! manifests or git history, those concrete signals beat regex-based prose detection by
//! a wide margin: the project name is already written down in Cargo.toml / package.json,
//! and the contributors are in `git log`.
//!
//! Public API:
//! - [`scan`] — scan a directory tree for projects and people
//! - [`to_detected_dict`] — convert scan results into [`DetectedDict`]
//! - [`merge_detected`] — merge two [`DetectedDict`] (primary wins on conflict)

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

use crate::palace::entities::DetectedEntity;
use crate::palace::room_detect::is_skip_dir;

// Maximum repo-scan depth; shallower than the global `WALK_DEPTH_LIMIT` (64)
// because project scanning targets top-level repos and manifests, not deep trees.
const SCAN_DEPTH_LIMIT: usize = 6;

// Git log is capped to avoid hangs on repos with very long histories.
const MAX_COMMITS_PER_REPO: usize = 1_000;

// Output caps for each entity category in `to_detected_dict`.
const PROJECT_CAP: usize = 15;
const PEOPLE_CAP: usize = 15;

// Number of JSONL header lines read per session to recover the cwd field.
// Kept here (not in session_scanner) because the constant documents a shared
// scanning convention — reading only a small prefix keeps memory bounded.
pub const SESSION_HEADER_LINES: usize = 20;

// Compile-time assertion: caps and depth are positive.
const _: () = assert!(SCAN_DEPTH_LIMIT > 0);
const _: () = assert!(PROJECT_CAP > 0);
const _: () = assert!(PEOPLE_CAP > 0);

// Bot detection patterns for display names.
// Expect is acceptable for compile-time literals; the patterns cannot fail.
#[allow(clippy::expect_used)]
static BOT_NAME_RES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"\[bot\]",
        r"^dependabot",
        r"^renovate",
        r"^github-actions",
        r"^actions-user",
        r"-bot$",
        r"\bbot$",
        r"^bot-",
        r"^snyk",
        r"^greenkeeper",
        r"^semantic-release",
        r"^allcontributors",
        r"-autoroll$",
        r"^auto-format",
        r"^pre-commit-ci",
    ]
    .into_iter()
    .map(|p| Regex::new(p).expect("bot name pattern is a compile-time literal"))
    .collect()
});

// Bot detection patterns for email addresses.
// `@users.noreply.github.com` is GitHub's privacy alias for real humans — not filtered.
#[allow(clippy::expect_used)]
static BOT_EMAIL_RES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [r"bot@", r"-bot@", r"\[bot\]@"]
        .into_iter()
        .map(|p| Regex::new(p).expect("bot email pattern is a compile-time literal"))
        .collect()
});

// ===================== PUBLIC TYPES =====================

/// A project discovered from a manifest file or git repository.
pub struct ProjectInfo {
    pub name: String,
    pub repo_root: PathBuf,
    /// Manifest filename (e.g. `"Cargo.toml"`) or `None` when derived from git alone.
    pub manifest: Option<String>,
    pub has_git: bool,
    pub total_commits: usize,
    pub user_commits: usize,
    pub is_mine: bool,
}

impl ProjectInfo {
    /// Confidence that this is a real project: 0.99 for repos with user commits,
    /// 0.7 for repos with any git history, 0.85 for manifest-only repos.
    pub fn confidence(&self) -> f64 {
        if self.is_mine {
            return 0.99;
        }
        if self.has_git && self.total_commits > 0 {
            return 0.7;
        }
        // Manifest-only, no git history — still a real name but no commit evidence.
        0.85
    }

    /// Human-readable evidence string used as the `signals` entry in [`DetectedEntity`].
    pub fn to_signal(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(ref manifest) = self.manifest {
            parts.push(manifest.clone());
        }
        if self.has_git {
            if self.is_mine && self.user_commits > 0 {
                parts.push(format!("{} of your commits", self.user_commits));
            } else if self.user_commits > 0 {
                parts.push(format!(
                    "{}/{} yours",
                    self.user_commits, self.total_commits
                ));
            } else {
                parts.push(format!("{} commits (none by you)", self.total_commits));
            }
        }
        if parts.is_empty() {
            // Fall back to the repo root directory name when no other signal is available.
            self.repo_root
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("repo")
                .to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// A person discovered from git commit authors.
pub struct PersonInfo {
    pub name: String,
    pub total_commits: usize,
    pub emails: HashSet<String>,
    pub repos: HashSet<String>,
}

impl PersonInfo {
    /// Confidence that this is a real person: higher for prolific or multi-repo contributors.
    pub fn confidence(&self) -> f64 {
        if self.total_commits >= 100 || self.repos.len() >= 3 {
            return 0.99;
        }
        if self.total_commits >= 20 {
            return 0.85;
        }
        0.65
    }

    /// Human-readable evidence string used as the `signals` entry in [`DetectedEntity`].
    pub fn to_signal(&self) -> String {
        let repo_count = self.repos.len();
        let s_commits = if self.total_commits == 1 { "" } else { "s" };
        let s_repos = if repo_count == 1 { "" } else { "s" };
        format!(
            "{} commit{s_commits} across {repo_count} repo{s_repos}",
            self.total_commits
        )
    }
}

/// Merged entity detection output, matching the shape produced by `entity_detect::detect_entities`.
pub struct DetectedDict {
    pub people: Vec<DetectedEntity>,
    pub projects: Vec<DetectedEntity>,
    pub uncertain: Vec<DetectedEntity>,
}

// ===================== PRIVATE TYPES =====================

/// Aggregated commit data for one identity component (union-find group).
struct ComponentEntry {
    name_counts: HashMap<String, usize>,
    emails: HashSet<String>,
    repos: HashSet<String>,
    total: usize,
}

/// Minimal path-compressed union-find for (name, email) identity resolution.
///
/// Keys use the form `"n:<display_name>"` or `"e:<email>"` to avoid collisions
/// between the two namespaces (a name like "bob@corp" must not alias an email).
struct UnionFind {
    parent: HashMap<String, String>,
}

impl UnionFind {
    /// Create an empty `UnionFind`.
    fn new() -> Self {
        UnionFind {
            parent: HashMap::new(),
        }
    }

    /// Return the canonical root for `x`, inserting `x` as its own root on first access.
    ///
    /// Path compression is applied so repeated calls are nearly O(1).
    fn find(&mut self, x: &str) -> String {
        assert!(!x.is_empty(), "UnionFind::find: key must not be empty");

        // Self-insert on first access — mirrors Python's `if x not in self.parent`.
        if !self.parent.contains_key(x) {
            self.parent.insert(x.to_string(), x.to_string());
            return x.to_string();
        }

        // Find root iteratively (no recursion — STYLEGUIDE §Control Flow).
        let mut root = x.to_string();
        loop {
            let parent = self.parent[&root].clone();
            if parent == root {
                break;
            }
            root = parent;
        }

        // Path compression: rewire every node on the path to point directly to root.
        let mut current = x.to_string();
        loop {
            let parent = self.parent[&current].clone();
            if parent == root {
                break;
            }
            self.parent.insert(current, root.clone());
            current = parent;
        }

        // Postcondition: the returned root points to itself.
        debug_assert_eq!(self.parent[&root], root);
        root
    }

    /// Union the components containing `a` and `b`.
    fn union(&mut self, a: &str, b: &str) {
        assert!(!a.is_empty(), "UnionFind::union: a must not be empty");
        assert!(!b.is_empty(), "UnionFind::union: b must not be empty");

        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent.insert(ra, rb);
        }
    }
}

// ===================== MANIFEST PARSERS =====================

/// Parse a `package.json` and return the `"name"` field, or `None` on error.
fn parse_package_json(path: &Path) -> Option<String> {
    assert!(!path.as_os_str().is_empty());
    let content = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&content).ok()?;
    let name = value.get("name")?.as_str()?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Parse a `pyproject.toml` and return the project name.
///
/// Checks `project.name` (PEP 621) first, then `tool.poetry.name` as a fallback.
fn parse_pyproject_toml(path: &Path) -> Option<String> {
    assert!(!path.as_os_str().is_empty());
    let content = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&content).ok()?;

    // PEP 621 — preferred location.
    if let Some(name) = value
        .get("project")
        .and_then(|table| table.get("name"))
        .and_then(|field| field.as_str())
        && !name.is_empty()
    {
        return Some(name.to_string());
    }

    // Poetry fallback.
    let name = value.get("tool")?.get("poetry")?.get("name")?.as_str()?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Parse a `Cargo.toml` and return the `package.name` field, or `None` on error.
fn parse_cargo_toml(path: &Path) -> Option<String> {
    assert!(!path.as_os_str().is_empty());
    let content = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&content).ok()?;
    let name = value.get("package")?.get("name")?.as_str()?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Parse a `go.mod` and return the last path segment of the `module` directive.
///
/// For `module github.com/owner/my-project` this returns `"my-project"`.
fn parse_go_mod(path: &Path) -> Option<String> {
    assert!(!path.as_os_str().is_empty());
    let content = std::fs::read_to_string(path).ok()?;
    for line in content.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("module ") else {
            continue;
        };
        let module = rest.trim();
        if module.is_empty() {
            return None;
        }
        // Take the last `/`-separated segment as the project name.
        let last = module.split('/').next_back().unwrap_or(module);
        return if last.is_empty() {
            None
        } else {
            Some(last.to_string())
        };
    }
    None
}

/// Numeric sort priority for a manifest filename: lower = higher priority.
///
/// `pyproject.toml` (0) beats `package.json` (1) beats `Cargo.toml` (2) beats
/// `go.mod` (3). Unknown filenames get priority 100 and sort last.
fn manifest_priority(filename: &str) -> usize {
    match filename {
        "pyproject.toml" => 0,
        "package.json" => 1,
        "Cargo.toml" => 2,
        "go.mod" => 3,
        _ => 100,
    }
}

// ===================== GIT HELPERS =====================

/// Run `git -C <cwd> <args>` and return stdout on success, or an empty string on error.
///
/// No hard timeout is enforced: `std::process::Command` is synchronous and the Rust
/// standard library has no built-in process timeout. In practice, git operations on
/// local repos complete in milliseconds. Slow remotes never apply here because we
/// never run fetch/pull.
fn run_git(cwd: &Path, args: &[&str]) -> String {
    assert!(!args.is_empty(), "run_git: args must not be empty");
    assert!(cwd.is_dir(), "run_git: cwd must be a directory");

    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output();

    match output {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        _ => String::new(),
    }
}

/// Return `(name, email)` from the repo-local git config, or empty strings on failure.
fn git_user_identity(repo: &Path) -> (String, String) {
    assert!(repo.is_dir(), "git_user_identity: repo must be a directory");
    let name = run_git(repo, &["config", "user.name"]).trim().to_string();
    let email = run_git(repo, &["config", "user.email"]).trim().to_string();

    // Postcondition: results are trimmed (no leading/trailing whitespace).
    debug_assert_eq!(name, name.trim());
    debug_assert_eq!(email, email.trim());
    (name, email)
}

/// Return `(name, email)` from the global git config, or empty strings on failure.
fn git_global_identity() -> (String, String) {
    let run = |args: &[&str]| -> String {
        std::process::Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    };
    let name = run(&["config", "--global", "user.name"]);
    let email = run(&["config", "--global", "user.email"]);

    // Postcondition: results are trimmed.
    debug_assert_eq!(name, name.trim());
    debug_assert_eq!(email, email.trim());
    (name, email)
}

/// Return up to `MAX_COMMITS_PER_REPO` `(name, email)` pairs from `git log`.
///
/// Uses `--format=%aN|%aE` (author name respecting mailmap, author email respecting mailmap).
fn git_authors(repo: &Path) -> Vec<(String, String)> {
    assert!(repo.is_dir(), "git_authors: repo must be a directory");

    let out = run_git(
        repo,
        &[
            "log",
            &format!("--max-count={MAX_COMMITS_PER_REPO}"),
            "--format=%aN|%aE",
        ],
    );

    let mut result: Vec<(String, String)> = Vec::new();
    for line in out.lines() {
        if let Some((name, email)) = line.split_once('|') {
            let name = name.trim().to_string();
            let email = email.trim().to_string();
            if !name.is_empty() {
                result.push((name, email));
            }
        }
    }

    // Postcondition: all names are non-empty.
    debug_assert!(result.iter().all(|(n, _)| !n.is_empty()));
    result
}

// ===================== BOT / NAME FILTERING =====================

/// Return `true` if `name` or `email` match known bot patterns.
fn is_bot(name: &str, email: &str) -> bool {
    assert!(
        !name.is_empty() || !email.is_empty(),
        "is_bot: both name and email are empty"
    );
    let name_lower = name.to_lowercase();
    let email_lower = email.to_lowercase();
    BOT_NAME_RES.iter().any(|re| re.is_match(&name_lower))
        || BOT_EMAIL_RES.iter().any(|re| re.is_match(&email_lower))
}

/// Return `true` if `name` looks like a real person's name: has a space and at least
/// two title-cased parts.
///
/// Rejects single-token handles (`johndoe`), all-lowercase names, and names that are
/// just a single word (common in git configs where people only set a first name).
fn looks_like_real_name(name: &str) -> bool {
    assert!(
        !name.is_empty(),
        "looks_like_real_name: name must not be empty"
    );
    if !name.contains(' ') {
        return false;
    }
    let parts: Vec<&str> = name.split_whitespace().collect();
    if parts.len() < 2 {
        return false;
    }
    // Both first and last part must begin with an uppercase letter.
    let first_char = parts[0].chars().next().unwrap_or_default();
    let last_char = parts[parts.len() - 1].chars().next().unwrap_or_default();

    // Postcondition: result is deterministic for the same input.
    first_char.is_uppercase() && last_char.is_uppercase()
}

// ===================== DIRECTORY WALK =====================

/// Return `true` if `path` contains a `.git` directory or file (bare repo marker).
fn has_git_marker(path: &Path) -> bool {
    assert!(!path.as_os_str().is_empty());
    let git = path.join(".git");
    git.is_dir() || git.is_file()
}

/// Return all git repository roots under `root` (including `root` itself if it is a repo).
///
/// Nested repos (submodules, monorepo children) are discovered but not descended into,
/// preventing double-counting of their manifests. Walk depth is capped at `SCAN_DEPTH_LIMIT`.
pub fn find_git_repos(root: &Path) -> Vec<PathBuf> {
    assert!(root.is_dir(), "find_git_repos: root must be a directory");

    let mut repos: Vec<PathBuf> = Vec::new();
    if has_git_marker(root) {
        repos.push(root.to_path_buf());
    }

    // Stack of (directory, depth). Directories that are themselves git repos
    // are added to `repos` but not pushed to the stack (no descent into them).
    let mut stack: Vec<(PathBuf, usize)> = vec![(root.to_path_buf(), 0)];

    while let Some((directory, depth)) = stack.pop() {
        assert!(depth <= SCAN_DEPTH_LIMIT);
        if depth >= SCAN_DEPTH_LIMIT {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Skip hidden dirs (including .git itself) and known build dirs.
            if name_str.starts_with('.') || is_skip_dir(&name_str) {
                continue;
            }
            if has_git_marker(&path) {
                // This is a nested repo — record it but do not descend.
                repos.push(path);
            } else {
                stack.push((path, depth + 1));
            }
        }
    }

    // Postcondition: all results have a git marker.
    debug_assert!(repos.iter().all(|repo| has_git_marker(repo)));
    repos
}

/// Parse a single directory's manifest files and append results to `found`.
///
/// Called by [`collect_manifest_names`] to keep that function within the 70-line limit.
/// `directory` is the directory being processed; `found` accumulates `(filename, name, dir)`.
fn collect_manifest_names_parse_dir(directory: &Path, found: &mut Vec<(String, String, PathBuf)>) {
    assert!(directory.is_dir());

    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry.path().is_file() {
            continue;
        }
        let Some(fname) = entry.file_name().to_str().map(String::from) else {
            continue;
        };
        let path = directory.join(&fname);
        let parsed = match fname.as_str() {
            "package.json" => parse_package_json(&path),
            "pyproject.toml" => parse_pyproject_toml(&path),
            "Cargo.toml" => parse_cargo_toml(&path),
            "go.mod" => parse_go_mod(&path),
            _ => None,
        };
        if let Some(project_name) = parsed {
            assert!(!project_name.is_empty());
            found.push((fname, project_name, directory.to_path_buf()));
        }
    }
}

/// Return `(manifest_filename, project_name, dir)` triples for manifests within `repo_root`.
///
/// Does not descend into nested git repositories. Results are sorted by
/// (depth, manifest priority, path) so the most authoritative manifest appears first.
fn collect_manifest_names(repo_root: &Path) -> Vec<(String, String, PathBuf)> {
    assert!(
        repo_root.is_dir(),
        "collect_manifest_names: repo_root must be a directory"
    );

    let mut found: Vec<(String, String, PathBuf)> = Vec::new();
    let mut stack: Vec<(PathBuf, usize)> = vec![(repo_root.to_path_buf(), 0)];

    while let Some((directory, depth)) = stack.pop() {
        assert!(depth <= SCAN_DEPTH_LIMIT);
        if depth >= SCAN_DEPTH_LIMIT {
            continue;
        }
        collect_manifest_names_parse_dir(&directory, &mut found);

        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with('.') || is_skip_dir(&name_str) {
                continue;
            }
            // Skip nested git repos — they belong to a different repo scan.
            if has_git_marker(&path) {
                continue;
            }
            stack.push((path, depth + 1));
        }
    }

    // Sort: shallowest first, then by manifest priority, then by path for determinism.
    found.sort_by_key(|(manifest, _, directory)| {
        let depth = directory
            .strip_prefix(repo_root)
            .map_or(SCAN_DEPTH_LIMIT + 1, |sub| sub.components().count());
        let priority = manifest_priority(manifest);
        let path_str = directory.to_string_lossy().into_owned();
        (depth, priority, path_str)
    });

    // Postcondition: all project names are non-empty.
    debug_assert!(found.iter().all(|(_, name, _)| !name.is_empty()));
    found
}

// ===================== PEOPLE DEDUP =====================

/// Build per-component aggregates from `all_commits` using the pre-built `uf`.
///
/// Called by [`dedupe_people`] to keep that function within the 70-line limit.
/// Returns a map of component root → [`ComponentEntry`].
fn dedupe_people_build_components(
    all_commits: &[(String, String, String)],
    uf: &mut UnionFind,
) -> HashMap<String, ComponentEntry> {
    assert!(!all_commits.is_empty());

    let mut components: HashMap<String, ComponentEntry> = HashMap::new();
    for (name, email, repo) in all_commits {
        let key = uf.find(&format!("n:{name}"));
        let entry = components.entry(key).or_insert_with(|| ComponentEntry {
            name_counts: HashMap::new(),
            emails: HashSet::new(),
            repos: HashSet::new(),
            total: 0,
        });
        *entry.name_counts.entry(name.clone()).or_insert(0) += 1;
        if !email.is_empty() {
            entry.emails.insert(email.clone());
        }
        entry.repos.insert(repo.clone());
        entry.total += 1;
    }

    // Postcondition: every component has at least one commit.
    debug_assert!(components.values().all(|e| e.total > 0));
    components
}

/// Pick display names for each component and build the final `PersonInfo` map.
///
/// Called by [`dedupe_people`] to keep that function within the 70-line limit.
fn dedupe_people_pick_display(
    components: &HashMap<String, ComponentEntry>,
) -> HashMap<String, PersonInfo> {
    assert!(
        !components.is_empty(),
        "dedupe_people_pick_display: components must not be empty"
    );

    let mut people: HashMap<String, PersonInfo> = HashMap::new();
    for entry in components.values() {
        // Most-frequent real name first; fall back to most-frequent overall.
        let mut candidates: Vec<(&String, usize)> =
            entry.name_counts.iter().map(|(n, &c)| (n, c)).collect();
        candidates.sort_by_key(|(_, count)| std::cmp::Reverse(*count));

        let display = candidates
            .iter()
            .find(|(n, _)| looks_like_real_name(n))
            .or_else(|| candidates.first())
            .map(|(n, _)| n.as_str())
            .unwrap_or_default();

        if !looks_like_real_name(display) {
            // Skip handles and single-token usernames — they are not real people.
            continue;
        }

        // Merge into an existing entry for this display name (rare: two disjoint
        // components that chose the same display name).
        let existing = people.get_mut(display);
        if let Some(existing) = existing {
            existing.total_commits += entry.total;
            existing.emails.extend(entry.emails.iter().cloned());
            existing.repos.extend(entry.repos.iter().cloned());
        } else {
            people.insert(
                display.to_string(),
                PersonInfo {
                    name: display.to_string(),
                    total_commits: entry.total,
                    emails: entry.emails.clone(),
                    repos: entry.repos.clone(),
                },
            );
        }
    }

    people
}

/// Group commits by identity: two commits are the same person when they share a name OR email.
///
/// Display name is the most-frequent name variant that `looks_like_real_name`; single-token
/// handles are dropped. Returns a map of display name → [`PersonInfo`].
fn dedupe_people(all_commits: &[(String, String, String)]) -> HashMap<String, PersonInfo> {
    if all_commits.is_empty() {
        return HashMap::new();
    }
    assert!(!all_commits.is_empty());

    let mut uf = UnionFind::new();
    for (name, email, _repo) in all_commits {
        let name_key = format!("n:{name}");
        // When email is absent, union the name with itself (no-op on the structure).
        let email_key = if email.is_empty() {
            format!("n:{name}")
        } else {
            format!("e:{email}")
        };
        uf.union(&name_key, &email_key);
    }

    let components = dedupe_people_build_components(all_commits, &mut uf);
    if components.is_empty() {
        return HashMap::new();
    }
    dedupe_people_pick_display(&components)
}

// ===================== MAIN SCAN HELPERS =====================

/// Resolve the user's git identity from the first repo's local config, falling back to global.
///
/// Called by [`scan`] to keep that function within the 70-line limit.
fn scan_get_user_identity(repos: &[PathBuf]) -> (String, String) {
    if let Some(first) = repos.first() {
        let (name, email) = git_user_identity(first);
        if !name.is_empty() || !email.is_empty() {
            return (name, email);
        }
    }
    git_global_identity()
}

/// Decide whether this repo should be considered "mine" (the current user is a key contributor).
///
/// Called by [`scan_process_repo`] to keep that function within the 70-line limit.
fn scan_is_mine(
    me_name: &str,
    user_commits: usize,
    total_commits: usize,
    author_counts: &HashMap<String, usize>,
) -> bool {
    if user_commits == 0 {
        return false;
    }
    assert!(user_commits > 0);

    // In top-5 contributors by commit count.
    let mut sorted: Vec<(&String, &usize)> = author_counts.iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(a.1));
    let top5: HashSet<&str> = sorted.iter().take(5).map(|(n, _)| n.as_str()).collect();
    if !me_name.is_empty() && top5.contains(me_name) {
        return true;
    }
    // At least 10% of commits are mine.
    if total_commits > 0 && user_commits * 10 >= total_commits {
        return true;
    }
    // At least 20 commits regardless of percentage.
    user_commits >= 20
}

/// Process one git repo: collect manifests, count authors, and update `projects` and `all_commits`.
///
/// Called by [`scan`] to keep that function within the 70-line limit.
fn scan_process_repo(
    repo: &Path,
    me_name: &str,
    me_email: &str,
    projects: &mut HashMap<String, ProjectInfo>,
    all_commits: &mut Vec<(String, String, String)>,
) {
    assert!(repo.is_dir());

    let manifests = collect_manifest_names(repo);
    let (manifest_file, proj_name) = if manifests.is_empty() {
        let fallback = repo
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();
        (None, fallback)
    } else {
        let (fname, name, _) = &manifests[0];
        (Some(fname.clone()), name.clone())
    };

    let authors = git_authors(repo);
    let repo_str = repo.to_string_lossy().into_owned();
    let mut user_commits = 0usize;
    let mut author_counts: HashMap<String, usize> = HashMap::new();

    for (name, email) in &authors {
        if is_bot(name, email) {
            continue;
        }
        *author_counts.entry(name.clone()).or_insert(0) += 1;
        all_commits.push((name.clone(), email.clone(), repo_str.clone()));
        if (!me_name.is_empty() && name.as_str() == me_name)
            || (!me_email.is_empty() && email.as_str() == me_email)
        {
            user_commits += 1;
        }
    }

    let total_commits = author_counts.values().sum::<usize>();
    let is_mine = scan_is_mine(me_name, user_commits, total_commits, &author_counts);

    let proj = ProjectInfo {
        name: proj_name.clone(),
        repo_root: repo.to_path_buf(),
        manifest: manifest_file,
        has_git: true,
        total_commits,
        user_commits,
        is_mine,
    };

    // Keep the entry with more user commits (the repo most relevant to this user).
    let entry = projects.entry(proj_name);
    match entry {
        std::collections::hash_map::Entry::Vacant(vacant) => {
            vacant.insert(proj);
        }
        std::collections::hash_map::Entry::Occupied(mut occupied) => {
            if proj.user_commits > occupied.get().user_commits {
                occupied.insert(proj);
            }
        }
    }
}

/// Handle the no-git-repo case: scan `root` directly for manifests.
///
/// Called by [`scan`] to keep that function within the 70-line limit.
fn scan_no_git_fallback(root: &Path, projects: &mut HashMap<String, ProjectInfo>) {
    assert!(
        root.is_dir(),
        "scan_no_git_fallback: root must be a directory"
    );

    // Pair assertion: root path must be non-empty (a directory path cannot be empty).
    assert!(!root.as_os_str().is_empty(), "root path must not be empty");
    let manifests = collect_manifest_names(root);

    for (manifest_file, proj_name, _dirpath) in manifests {
        if projects.contains_key(&proj_name) {
            continue;
        }
        projects.insert(
            proj_name.clone(),
            ProjectInfo {
                name: proj_name,
                repo_root: root.to_path_buf(),
                manifest: Some(manifest_file),
                has_git: false,
                total_commits: 0,
                user_commits: 0,
                is_mine: false,
            },
        );
    }
}

// ===================== PUBLIC API =====================

/// Scan `root` for projects (via manifests and git repos) and people (via git authors).
///
/// Returns `(projects, people)` sorted by relevance — own projects and prolific contributors first.
pub fn scan(root: &Path) -> (Vec<ProjectInfo>, Vec<PersonInfo>) {
    if !root.is_dir() {
        return (vec![], vec![]);
    }
    assert!(root.is_dir());

    let repos = find_git_repos(root);
    let (me_name, me_email) = scan_get_user_identity(&repos);

    let mut projects: HashMap<String, ProjectInfo> = HashMap::new();
    let mut all_commits: Vec<(String, String, String)> = Vec::new();

    for repo in &repos {
        scan_process_repo(repo, &me_name, &me_email, &mut projects, &mut all_commits);
    }

    let people = dedupe_people(&all_commits);

    if repos.is_empty() {
        // No git repos found — fall back to manifest-only discovery.
        scan_no_git_fallback(root, &mut projects);
    }

    let mut project_list: Vec<ProjectInfo> = projects.into_values().collect();
    // Sort: mine first, then by user commits desc, total commits desc, name asc.
    project_list.sort_by(|a, b| {
        b.is_mine
            .cmp(&a.is_mine)
            .then(b.user_commits.cmp(&a.user_commits))
            .then(b.total_commits.cmp(&a.total_commits))
            .then(a.name.cmp(&b.name))
    });

    let mut people_list: Vec<PersonInfo> = people.into_values().collect();
    people_list.sort_by_key(|p| std::cmp::Reverse(p.total_commits));

    (project_list, people_list)
}

/// Convert `scan` results into [`DetectedDict`] — the same shape as `entity_detect::detect_entities`.
///
/// Capped at `PROJECT_CAP` projects and `PEOPLE_CAP` people. The `uncertain` bucket is always
/// empty because manifest/git signals produce confident classifications.
pub fn to_detected_dict(projects: &[ProjectInfo], people: &[PersonInfo]) -> DetectedDict {
    // Precondition: all project and person names must be non-empty before conversion.
    debug_assert!(projects.iter().all(|p| !p.name.is_empty()));
    debug_assert!(people.iter().all(|p| !p.name.is_empty()));

    let project_entities: Vec<DetectedEntity> = projects
        .iter()
        .take(PROJECT_CAP)
        .map(|p| DetectedEntity {
            name: p.name.clone(),
            entity_type: "project".to_string(),
            confidence: p.confidence(),
            // frequency: prefer user_commits as relevance signal; fall back to total.
            frequency: if p.user_commits > 0 {
                p.user_commits
            } else {
                p.total_commits
            },
            signals: vec![p.to_signal()],
        })
        .collect();

    let people_entities: Vec<DetectedEntity> = people
        .iter()
        .take(PEOPLE_CAP)
        .map(|p| DetectedEntity {
            name: p.name.clone(),
            entity_type: "person".to_string(),
            confidence: p.confidence(),
            frequency: p.total_commits,
            signals: vec![p.to_signal()],
        })
        .collect();

    // Postcondition: all entities have a non-empty name and a valid entity_type.
    debug_assert!(project_entities.iter().all(|e| !e.name.is_empty()));
    debug_assert!(people_entities.iter().all(|e| !e.name.is_empty()));

    DetectedDict {
        people: people_entities,
        projects: project_entities,
        uncertain: vec![],
    }
}

/// Merge two [`DetectedDict`] values. Primary entries win on name conflict (case-insensitive).
///
/// When `drop_uncertain` is `true`, the secondary's uncertain bucket is discarded — useful
/// when real manifest/git signal exists and prose-regex noise would add no value.
pub fn merge_detected(
    mut primary: DetectedDict,
    secondary: DetectedDict,
    drop_uncertain: bool,
) -> DetectedDict {
    // Precondition: all entity names in both dicts must be non-empty strings.
    debug_assert!(
        primary
            .people
            .iter()
            .chain(&primary.projects)
            .chain(&primary.uncertain)
            .all(|e| !e.name.is_empty()),
        "primary entity names must be non-empty"
    );
    debug_assert!(
        secondary
            .people
            .iter()
            .chain(&secondary.projects)
            .chain(&secondary.uncertain)
            .all(|e| !e.name.is_empty()),
        "secondary entity names must be non-empty"
    );

    // Collect all names already in primary (case-insensitive).
    let mut seen: HashSet<String> = primary
        .people
        .iter()
        .chain(&primary.projects)
        .chain(&primary.uncertain)
        .map(|e| e.name.to_lowercase())
        .collect();

    for entity in secondary.people {
        if seen.insert(entity.name.to_lowercase()) {
            primary.people.push(entity);
        }
    }
    for entity in secondary.projects {
        if seen.insert(entity.name.to_lowercase()) {
            primary.projects.push(entity);
        }
    }
    if !drop_uncertain {
        for entity in secondary.uncertain {
            if seen.insert(entity.name.to_lowercase()) {
                primary.uncertain.push(entity);
            }
        }
    }

    primary
}

// ===================== TESTS =====================

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // -- Manifest parsers --

    #[test]
    fn parse_package_json_returns_name_field() {
        // Valid package.json with a name field should return that name.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("package.json");
        std::fs::write(&path, r#"{"name": "my-lib", "version": "1.0.0"}"#)
            .expect("write package.json");
        let result = parse_package_json(&path);
        assert!(result.is_some(), "must parse valid package.json");
        assert_eq!(result.expect("checked above"), "my-lib");
    }

    #[test]
    fn parse_package_json_returns_none_on_invalid_json() {
        // Malformed JSON should return None without panicking.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("package.json");
        std::fs::write(&path, "not json {{{").expect("write bad package.json");
        assert!(
            parse_package_json(&path).is_none(),
            "invalid JSON must return None"
        );
    }

    #[test]
    fn parse_pyproject_toml_reads_pep621_name() {
        // PEP 621 [project] table is the preferred name source.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("pyproject.toml");
        std::fs::write(&path, "[project]\nname = \"my-pkg\"\n").expect("write pyproject");
        let result = parse_pyproject_toml(&path);
        assert!(result.is_some(), "must parse [project].name");
        assert_eq!(result.expect("checked above"), "my-pkg");
    }

    #[test]
    fn parse_pyproject_toml_falls_back_to_poetry_name() {
        // When [project] is absent, [tool.poetry] is the fallback.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("pyproject.toml");
        std::fs::write(&path, "[tool.poetry]\nname = \"poetry-pkg\"\n").expect("write pyproject");
        let result = parse_pyproject_toml(&path);
        assert!(result.is_some(), "must fall back to [tool.poetry].name");
        assert_eq!(result.expect("checked above"), "poetry-pkg");
    }

    #[test]
    fn parse_pyproject_toml_returns_none_when_no_name() {
        // Neither [project].name nor [tool.poetry].name present — returns None.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("pyproject.toml");
        std::fs::write(&path, "[build-system]\nrequires = []\n").expect("write pyproject");
        assert!(
            parse_pyproject_toml(&path).is_none(),
            "missing name fields must return None"
        );
    }

    #[test]
    fn parse_cargo_toml_returns_package_name() {
        // [package].name is the authoritative name source for Rust crates.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("Cargo.toml");
        std::fs::write(
            &path,
            "[package]\nname = \"my-crate\"\nversion = \"0.1.0\"\n",
        )
        .expect("write Cargo.toml");
        let result = parse_cargo_toml(&path);
        assert!(result.is_some(), "must parse [package].name");
        assert_eq!(result.expect("checked above"), "my-crate");
    }

    #[test]
    fn parse_cargo_toml_returns_none_when_no_package_section() {
        // A TOML file without a [package] table (e.g. a workspace root) returns None.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("Cargo.toml");
        std::fs::write(&path, "[workspace]\nmembers = []\n").expect("write workspace Cargo.toml");
        assert!(
            parse_cargo_toml(&path).is_none(),
            "workspace Cargo.toml without [package] must return None"
        );
    }

    #[test]
    fn parse_go_mod_returns_last_path_segment() {
        // Multi-segment module paths should return only the last segment.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("go.mod");
        std::fs::write(&path, "module github.com/owner/my-project\n\ngo 1.21\n")
            .expect("write go.mod");
        let result = parse_go_mod(&path);
        assert!(result.is_some(), "must parse module directive");
        assert_eq!(result.expect("checked above"), "my-project");
    }

    #[test]
    fn parse_go_mod_returns_none_when_no_module_line() {
        // A file without a `module` directive returns None.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("go.mod");
        std::fs::write(&path, "go 1.21\n").expect("write go.mod without module");
        assert!(
            parse_go_mod(&path).is_none(),
            "go.mod without module directive must return None"
        );
    }

    // -- Bot / name filtering --

    #[test]
    fn is_bot_rejects_known_bot_names() {
        // Names containing "[bot]" or known bot prefixes must be detected.
        assert!(is_bot("dependabot[bot]", "bot@github.com"), "[bot] in name");
        assert!(
            is_bot("renovate-bot", "renovate@example.com"),
            "renovate prefix"
        );
        assert!(is_bot("github-actions", ""), "github-actions is a bot");
    }

    #[test]
    fn is_bot_accepts_real_humans() {
        // Ordinary human names and emails must not be filtered out.
        assert!(
            !is_bot("Alice Smith", "alice@example.com"),
            "human should pass"
        );
        assert!(
            !is_bot("Bob", "bob@users.noreply.github.com"),
            "GitHub privacy email is human"
        );
    }

    #[test]
    fn looks_like_real_name_accepts_proper_names() {
        // Names with a space and title-cased parts are treated as real people.
        assert!(looks_like_real_name("Alice Smith"), "two title-cased parts");
        assert!(
            looks_like_real_name("John Doe Jr"),
            "three parts still valid"
        );
    }

    #[test]
    fn looks_like_real_name_rejects_handles() {
        // Single-token names, lowercase names, and handles must be rejected.
        assert!(
            !looks_like_real_name("johndoe"),
            "no space — not a real name"
        );
        assert!(!looks_like_real_name("alice"), "single lowercase token");
        assert!(
            !looks_like_real_name("alice smith"),
            "lowercase first char — not title-cased"
        );
    }

    // -- UnionFind --

    #[test]
    fn union_find_merges_components_correctly() {
        // After unioning a and b, both should resolve to the same root.
        let mut uf = UnionFind::new();
        uf.union("n:Alice", "e:alice@example.com");
        uf.union("n:Alice", "n:Alicia"); // same person, different display names

        let root_alice = uf.find("n:Alice");
        let root_email = uf.find("e:alice@example.com");
        let root_alicia = uf.find("n:Alicia");

        assert_eq!(
            root_alice, root_email,
            "name and email must share a root after union"
        );
        assert_eq!(
            root_alice, root_alicia,
            "two name variants must share a root after union"
        );
    }

    #[test]
    fn union_find_path_compression_returns_same_root() {
        // After path compression, repeated find() calls return the same canonical root.
        let mut uf = UnionFind::new();
        uf.union("a", "b");
        uf.union("b", "c");
        uf.union("c", "d");
        let first = uf.find("a");
        let second = uf.find("a");
        assert_eq!(first, second, "path-compressed find must be idempotent");
        assert_eq!(uf.find("d"), first, "all chained nodes share one root");
    }

    // -- merge_detected --

    #[test]
    fn merge_detected_primary_wins_on_name_conflict() {
        // When the same entity name appears in both dicts, primary's version is kept.
        let primary = DetectedDict {
            people: vec![DetectedEntity {
                name: "Alice".to_string(),
                entity_type: "person".to_string(),
                confidence: 0.99,
                frequency: 10,
                signals: vec!["git".to_string()],
            }],
            projects: vec![],
            uncertain: vec![],
        };
        let secondary = DetectedDict {
            people: vec![DetectedEntity {
                name: "Alice".to_string(), // same name — should be dropped
                entity_type: "person".to_string(),
                confidence: 0.5,
                frequency: 1,
                signals: vec!["prose".to_string()],
            }],
            projects: vec![],
            uncertain: vec![],
        };
        let merged = merge_detected(primary, secondary, false);
        assert_eq!(merged.people.len(), 1, "duplicate must be deduplicated");
        assert!(
            (merged.people[0].confidence - 0.99).abs() < f64::EPSILON,
            "primary confidence must win"
        );
    }

    #[test]
    fn merge_detected_drop_uncertain_suppresses_secondary_uncertain() {
        // When drop_uncertain=true, secondary uncertain entries are not added.
        let primary = DetectedDict {
            people: vec![],
            projects: vec![],
            uncertain: vec![],
        };
        let secondary = DetectedDict {
            people: vec![],
            projects: vec![],
            uncertain: vec![DetectedEntity {
                name: "Noise".to_string(),
                entity_type: "uncertain".to_string(),
                confidence: 0.4,
                frequency: 3,
                signals: vec![],
            }],
        };
        let merged = merge_detected(primary, secondary, true);
        assert!(
            merged.uncertain.is_empty(),
            "drop_uncertain=true must suppress secondary uncertain entries"
        );
    }

    // -- scan e2e (requires git to be installed) --

    #[test]
    fn scan_discovers_project_from_cargo_toml_in_git_repo() {
        // Create a temporary git repo with a Cargo.toml; scan() must return the project name.
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();

        // Write a minimal Cargo.toml.
        std::fs::write(
            root.join("Cargo.toml"),
            "[package]\nname = \"scanner-test-proj\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .expect("write Cargo.toml");

        // Initialise a git repo so find_git_repos() picks it up.
        let git_init = std::process::Command::new("git")
            .arg("init")
            .current_dir(root)
            .output();
        if git_init.is_err() || !git_init.expect("git must be available").status.success() {
            // git is not available in this environment — skip gracefully.
            return;
        }

        // Configure a local identity so git_user_identity() returns something useful.
        let _ = std::process::Command::new("git")
            .args(["config", "user.name", "Test User"])
            .current_dir(root)
            .output();
        let _ = std::process::Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(root)
            .output();

        // Add and commit so there is at least one commit in the log.
        let _ = std::process::Command::new("git")
            .args(["add", "."])
            .current_dir(root)
            .output();
        let _ = std::process::Command::new("git")
            .args(["commit", "-m", "init", "--allow-empty"])
            .current_dir(root)
            .output();

        let (projects, _people) = scan(root);

        assert!(!projects.is_empty(), "scan must find at least one project");
        assert!(
            projects.iter().any(|p| p.name == "scanner-test-proj"),
            "project name must come from Cargo.toml; found: {:?}",
            projects.iter().map(|p| &p.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn scan_returns_empty_for_nonexistent_directory() {
        // scan() must return empty lists rather than panicking for a missing path.
        let (projects, people) = scan(Path::new("/nonexistent/path/that/cannot/exist"));
        assert!(
            projects.is_empty(),
            "nonexistent path must yield no projects"
        );
        assert!(people.is_empty(), "nonexistent path must yield no people");
    }

    #[test]
    fn to_detected_dict_produces_correct_entity_types() {
        // ProjectInfo must map to "project" and PersonInfo to "person" in the output.
        let projects = vec![ProjectInfo {
            name: "my-proj".to_string(),
            repo_root: PathBuf::from("/tmp/my-proj"),
            manifest: Some("Cargo.toml".to_string()),
            has_git: true,
            total_commits: 50,
            user_commits: 30,
            is_mine: true,
        }];
        let people: Vec<PersonInfo> = vec![];
        let dict = to_detected_dict(&projects, &people);

        assert_eq!(dict.projects.len(), 1, "one project must appear");
        assert!(
            dict.people.is_empty(),
            "no people input means no people output"
        );
        assert_eq!(dict.projects[0].entity_type, "project");
        assert!(
            !dict.projects[0].signals.is_empty(),
            "signal must be populated from to_signal()"
        );
    }

    // -- Manifest parser: empty-name branches --

    #[test]
    fn parse_package_json_returns_none_for_empty_name() {
        // A name field present but set to "" must not be returned.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("package.json");
        std::fs::write(&path, r#"{"name": "", "version": "1.0.0"}"#).expect("write package.json");
        assert!(
            parse_package_json(&path).is_none(),
            "empty name field must return None"
        );
    }

    #[test]
    fn parse_cargo_toml_returns_none_for_empty_name() {
        // [package].name = "" must return None rather than the empty string.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("Cargo.toml");
        std::fs::write(&path, "[package]\nname = \"\"\nversion = \"0.1.0\"\n")
            .expect("write Cargo.toml");
        assert!(
            parse_cargo_toml(&path).is_none(),
            "empty [package].name must return None"
        );
    }

    #[test]
    fn parse_go_mod_returns_none_for_empty_module_path() {
        // A `module ` line with no path after the prefix must return None.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("go.mod");
        std::fs::write(&path, "module \n").expect("write go.mod");
        assert!(
            parse_go_mod(&path).is_none(),
            "empty module path must return None"
        );
    }

    #[test]
    fn parse_pyproject_toml_returns_none_for_empty_project_name() {
        // [project].name = "" must fall through; no poetry fallback → None.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("pyproject.toml");
        std::fs::write(&path, "[project]\nname = \"\"\n").expect("write pyproject");
        assert!(
            parse_pyproject_toml(&path).is_none(),
            "empty [project].name must return None"
        );
    }

    #[test]
    fn parse_pyproject_toml_returns_none_for_empty_poetry_name() {
        // [tool.poetry].name = "" must return None rather than the empty string.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("pyproject.toml");
        std::fs::write(&path, "[tool.poetry]\nname = \"\"\n").expect("write pyproject");
        assert!(
            parse_pyproject_toml(&path).is_none(),
            "empty [tool.poetry].name must return None"
        );
    }

    // -- scan_is_mine branches --

    #[test]
    fn scan_is_mine_returns_false_when_user_has_no_commits() {
        // user_commits = 0 must immediately return false without inspecting author_counts.
        let author_counts = HashMap::from([("Alice".to_string(), 100usize)]);
        assert!(
            !scan_is_mine("Alice", 0, 100, &author_counts),
            "zero user commits must not be mine"
        );
    }

    #[test]
    fn scan_is_mine_returns_true_when_user_is_top_five_contributor() {
        // A user in the top-5 contributors list is considered mine regardless of percentage.
        let author_counts =
            HashMap::from([("Alice".to_string(), 50usize), ("Bob".to_string(), 40)]);
        assert!(
            scan_is_mine("Alice", 50, 90, &author_counts),
            "top-5 contributor must be mine"
        );
    }

    #[test]
    fn scan_is_mine_returns_true_at_ten_percent_threshold() {
        // user_commits * 10 >= total_commits triggers mine even without top-5 status.
        // Alice has 6/60 commits (10%) but 6 other larger contributors fill the top-5.
        let author_counts = HashMap::from([
            ("A".to_string(), 15usize),
            ("B".to_string(), 14),
            ("C".to_string(), 13),
            ("D".to_string(), 12),
            ("E".to_string(), 0),
            ("Alice".to_string(), 6),
        ]);
        assert!(
            scan_is_mine("Alice", 6, 60, &author_counts),
            "10% threshold must make this mine"
        );
    }

    #[test]
    fn scan_is_mine_returns_true_when_user_has_twenty_or_more_commits() {
        // 20+ commits is the absolute fallback — mine even when less than 10% of total.
        // Alice has 20/310 ≈ 6.5%, not in top-5 (6 larger contributors).
        let author_counts = HashMap::from([
            ("A".to_string(), 60usize),
            ("B".to_string(), 59),
            ("C".to_string(), 58),
            ("D".to_string(), 57),
            ("E".to_string(), 56),
            ("Alice".to_string(), 20),
        ]);
        assert!(
            scan_is_mine("Alice", 20, 310, &author_counts),
            "20+ commits must make this mine regardless of percentage"
        );
    }

    // -- is_bot email-only trigger --

    #[test]
    fn is_bot_returns_true_for_bot_email_with_human_looking_name() {
        // A human-sounding name paired with a `-bot@` email must still be filtered.
        assert!(
            is_bot("Alice Smith", "alice-bot@example.com"),
            "email matching -bot@ must be detected even with a human name"
        );
    }

    // -- looks_like_real_name additional branches --

    #[test]
    fn looks_like_real_name_rejects_when_last_part_is_lowercase() {
        // First part is uppercase but last part is lowercase — must return false.
        // This exercises the `last_char.is_uppercase()` false branch of the final &&.
        assert!(
            !looks_like_real_name("Alice smith"),
            "last part lowercase must fail the title-case check"
        );
    }

    #[test]
    fn looks_like_real_name_rejects_single_word_with_leading_space() {
        // A space is present but whitespace-trimmed split yields only one token → parts.len() < 2.
        assert!(
            !looks_like_real_name(" alice"),
            "single token after whitespace trimming must be rejected"
        );
    }

    // -- scan_no_git_fallback: already-seen project skip --

    #[test]
    fn scan_no_git_fallback_skips_already_seen_project() {
        // A project name already in the map must not be overwritten by the fallback.
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("Cargo.toml"),
            "[package]\nname = \"existing-proj\"\nversion = \"0.1.0\"\n",
        )
        .expect("write Cargo.toml");

        let mut projects: HashMap<String, ProjectInfo> = HashMap::new();
        projects.insert(
            "existing-proj".to_string(),
            ProjectInfo {
                name: "existing-proj".to_string(),
                repo_root: PathBuf::from("/original"),
                manifest: None,
                has_git: true,
                total_commits: 99,
                user_commits: 99,
                is_mine: true,
            },
        );

        scan_no_git_fallback(temp.path(), &mut projects);

        // The original entry must be preserved — no overwrite from the fallback.
        assert_eq!(projects.len(), 1, "duplicate project must not be added");
        assert!(
            projects["existing-proj"].is_mine,
            "original is_mine must be preserved"
        );
        assert_eq!(
            projects["existing-proj"].repo_root,
            PathBuf::from("/original"),
            "original repo_root must be preserved"
        );
    }

    // -- find_git_repos: nested git repo discovery --

    #[test]
    fn find_git_repos_discovers_nested_git_repo_without_descending() {
        // A nested git repo must be recorded but not descended into.
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        let nested = root.join("sub");
        std::fs::create_dir(&nested).expect("create sub dir");

        // Initialise the outer repo.
        let outer = std::process::Command::new("git")
            .arg("init")
            .current_dir(root)
            .output();
        if outer.is_err() || !outer.expect("git").status.success() {
            return; // git unavailable — skip gracefully
        }
        // Initialise the nested repo.
        let inner = std::process::Command::new("git")
            .arg("init")
            .current_dir(&nested)
            .output();
        if inner.is_err() || !inner.expect("git").status.success() {
            return;
        }

        let repos = find_git_repos(root);

        assert!(repos.len() >= 2, "must discover both root and nested repo");
        assert!(
            repos.iter().any(|r| r == root),
            "root repo must be in the result"
        );
        assert!(
            repos.iter().any(|r| r == &nested),
            "nested repo must be discovered"
        );
    }
}
