/// Build script: embeds the current git short SHA as the `GIT_SHORT_SHA` env
/// var. Falls back to `"unknown"` when the git directory or refs are missing.
///
/// Reads ref files directly from the filesystem instead of spawning an external
/// `git` process. This removes the build-time dependency on git being in PATH.
fn main() {
    let sha = read_short_sha().unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=GIT_SHORT_SHA={sha}");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");
    println!("cargo:rerun-if-changed=.git/packed-refs");
}

/// Read HEAD and return the first 7 hex characters of the current SHA, or
/// `None` if the git directory is absent or any ref is unresolvable.
///
/// Handles both attached HEAD (`ref: refs/heads/<branch>`) and detached HEAD
/// (HEAD contains the full SHA directly).
fn read_short_sha() -> Option<String> {
    let head = std::fs::read_to_string(".git/HEAD").ok()?;
    let head = head.trim();

    let full_sha = if let Some(ref_path) = head.strip_prefix("ref: ") {
        // Attached HEAD: resolve the symbolic ref to a full SHA.
        resolve_ref(ref_path.trim())?
    } else {
        // Detached HEAD: HEAD itself contains the full SHA.
        head.to_owned()
    };

    // Guard: a valid short SHA requires at least 7 hex characters.
    if full_sha.len() < 7 {
        return None;
    }

    Some(full_sha[..7].to_owned())
}

/// Resolve a ref name (e.g. `refs/heads/master`) to a full SHA string.
///
/// Tries the loose object file at `.git/<ref_name>` first, then falls back
/// to scanning `.git/packed-refs`. Returns `None` if neither source resolves
/// the ref.
fn resolve_ref(ref_name: &str) -> Option<String> {
    assert!(
        !ref_name.is_empty(),
        "resolve_ref: ref_name must not be empty"
    );

    // Loose ref: .git/<ref_name> contains the full SHA on a single line.
    let loose_path = format!(".git/{ref_name}");
    if let Ok(content) = std::fs::read_to_string(&loose_path) {
        let sha = content.trim().to_owned();
        if !sha.is_empty() {
            return Some(sha);
        }
    }

    // Packed-refs fallback: each non-comment line is "<sha> <ref_name>".
    // Peeled-ref lines (starting with `^`) refer to the previous entry — skip.
    let packed = std::fs::read_to_string(".git/packed-refs").ok()?;
    for line in packed.lines() {
        if line.starts_with('#') || line.starts_with('^') {
            continue;
        }
        if let Some((sha, name)) = line.split_once(' ')
            && name.trim() == ref_name
        {
            return Some(sha.trim().to_owned());
        }
    }

    None
}
