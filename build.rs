/// Build script: embeds the current git short SHA as the `GIT_SHORT_SHA` env
/// var. Falls back to `"unknown"` when the git directory or refs are missing.
///
/// Reads ref files directly from the filesystem instead of spawning an external
/// `git` process. Handles plain repos, submodules, and worktrees by resolving
/// the real git dir from the `.git` pointer file when necessary.
fn main() {
    let git_dir = resolve_git_dir().unwrap_or_else(|| std::path::PathBuf::from(".git"));
    println!(
        "cargo:rustc-env=GIT_SHORT_SHA={}",
        read_short_sha(&git_dir).unwrap_or_else(|| "unknown".to_owned())
    );
    println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join("refs/heads").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join("packed-refs").display()
    );
}

/// Resolve the real git directory for plain repos (`.git/` dir), submodules,
/// and worktrees (`.git` file containing `gitdir: <path>`).
///
/// Returns `None` only when `.git` is absent entirely.
fn resolve_git_dir() -> Option<std::path::PathBuf> {
    let git_path = std::path::Path::new(".git");
    if git_path.is_dir() {
        return Some(git_path.to_path_buf());
    }
    // .git is a file (submodule or worktree): "gitdir: <relative-or-absolute-path>"
    if git_path.is_file() {
        let content = std::fs::read_to_string(git_path).ok()?;
        let target = content.trim().strip_prefix("gitdir: ")?;
        let resolved = std::path::Path::new(target.trim());
        if resolved.is_dir() {
            return Some(resolved.to_path_buf());
        }
    }
    None
}

/// Return `true` if every byte in `s` is an ASCII hex digit (`0-9`, `a-f`, `A-F`).
///
/// Empty strings return `false`. Used to validate SHA strings so that malformed
/// ref content cannot pollute `GIT_SHORT_SHA`.
fn is_hex_sha(candidate: &str) -> bool {
    !candidate.is_empty() && candidate.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Read HEAD from `git_dir` and return the first 7 hex characters of the current
/// SHA, or `None` if HEAD is missing, unresolvable, or contains non-hex content.
///
/// Handles attached HEAD (`ref: refs/heads/<branch>`) and detached HEAD (raw SHA).
fn read_short_sha(git_dir: &std::path::Path) -> Option<String> {
    assert!(
        git_dir.is_dir(),
        "read_short_sha: git_dir must be a directory"
    );

    let head = std::fs::read_to_string(git_dir.join("HEAD")).ok()?;
    let head = head.trim();

    let full_sha = if let Some(ref_path) = head.strip_prefix("ref: ") {
        // Attached HEAD: resolve the symbolic ref to a full SHA.
        resolve_ref(git_dir, ref_path.trim())?
    } else {
        // Detached HEAD: HEAD itself must be a valid hex SHA — reject anything else
        // (e.g. a truncated write or an unexpected "ref: " prefix variant).
        if !is_hex_sha(head) {
            return None;
        }
        head.to_owned()
    };

    // Guard: a valid short SHA requires at least 7 validated hex characters.
    if full_sha.len() < 7 || !is_hex_sha(&full_sha) {
        return None;
    }

    Some(full_sha[..7].to_owned())
}

/// Resolve a ref name (e.g. `refs/heads/master`) to a full SHA string.
///
/// Tries the loose object file at `git_dir/<ref_name>` first, then falls back
/// to scanning `git_dir/packed-refs`. Validates the resolved value is hex before
/// returning it. Returns `None` if neither source resolves the ref.
fn resolve_ref(git_dir: &std::path::Path, ref_name: &str) -> Option<String> {
    assert!(
        !ref_name.is_empty(),
        "resolve_ref: ref_name must not be empty"
    );

    // Loose ref: git_dir/<ref_name> contains the full SHA on a single line.
    if let Ok(content) = std::fs::read_to_string(git_dir.join(ref_name)) {
        let sha = content.trim();
        if is_hex_sha(sha) {
            return Some(sha.to_owned());
        }
    }

    // Packed-refs fallback: each non-comment line is "<sha> <ref_name>".
    // Peeled-ref lines (starting with `^`) refer to the previous entry — skip.
    let packed = std::fs::read_to_string(git_dir.join("packed-refs")).ok()?;
    for line in packed.lines() {
        if line.starts_with('#') || line.starts_with('^') {
            continue;
        }
        if let Some((sha, name)) = line.split_once(' ')
            && name.trim() == ref_name
        {
            let sha = sha.trim();
            if is_hex_sha(sha) {
                return Some(sha.to_owned());
            }
        }
    }

    None
}
