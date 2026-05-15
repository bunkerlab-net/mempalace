//! `mempalace sync` — gitignore-aware drawer prune.
//!
//! Removes drawers whose `source_file` is now gitignored, missing on disk, or
//! lives outside the project roots the caller scoped the scan to. Reuses the
//! same gitignore evaluation the miner uses on the way in — the rules that
//! block ingest also drive cleanup. Mirrors `mempalace/sync.py`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use turso::Connection;

use crate::db::query_all;
use crate::error::{Error, Result};

/// Per-source-file removal count returned in the report. Surfaced via
/// `by_source` so callers can show the user which files are pruning the most
/// drawers.
type BySource = HashMap<String, usize>;

/// Aggregate result of a sync run.
#[derive(Debug, Default)]
pub struct SyncReport {
    /// Drawers inspected.
    pub scanned: usize,
    /// Drawers whose source file still exists and is not gitignored.
    pub kept: usize,
    /// Drawers whose source file is now gitignored (would be / was removed).
    pub gitignored: usize,
    /// Drawers whose source file no longer exists on disk (would be / was removed).
    pub missing: usize,
    /// Drawers with no `source_file` metadata (kept — likely manual entries).
    pub no_source: usize,
    /// Drawers whose source file lives outside every project root (kept).
    pub out_of_scope: usize,
    /// Drawers actually deleted (zero in dry-run mode).
    pub removed_drawers: usize,
    /// Closet rows actually deleted alongside drawers (zero in dry-run mode).
    pub removed_closets: usize,
    /// `true` when no deletions were performed.
    pub dry_run: bool,
    /// Counts of drawers per source file that were classified as removable.
    pub by_source: BySource,
}

/// Bucket a drawer was classified into. Drives both stats and the prune list.
enum DrawerBucket {
    Kept,
    Gitignored,
    Missing,
    NoSource,
    OutOfScope,
}

/// Registry rows are bookkeeping for the convo miner — preserve them so the next
/// mine pass doesn't re-chunk + re-embed the entire transcript.
fn is_registry_row(room: &str, ingest_mode: &str, drawer_id: &str) -> bool {
    room == "_registry" || ingest_mode == "registry" || drawer_id.starts_with("_reg_")
}

/// Return the longest project root that `source_file` lives under, mirroring
/// the longest-prefix-wins rule in `mempalace/sync.py`.
fn resolve_project_root<'a>(source_file: &Path, project_roots: &'a [PathBuf]) -> Option<&'a Path> {
    let mut best: Option<&Path> = None;
    for root in project_roots {
        if source_file.starts_with(root) {
            let longer =
                best.is_none_or(|current| root.as_os_str().len() > current.as_os_str().len());
            if longer {
                best = Some(root.as_path());
            }
        }
    }
    best
}

/// Load (or reuse) a per-root `Gitignore` matcher. Matchers are cached so a
/// large palace doesn't reparse the same `.gitignore` for every drawer.
fn matcher_for_root<'a>(
    matchers: &'a mut HashMap<PathBuf, ignore::gitignore::Gitignore>,
    root: &Path,
) -> &'a ignore::gitignore::Gitignore {
    matchers
        .entry(root.to_path_buf())
        .or_insert_with(|| ignore::gitignore::Gitignore::new(root.join(".gitignore")).0)
}

/// Classify one drawer by inspecting its source-file metadata.
fn classify_drawer(
    source_file: &str,
    room: &str,
    ingest_mode: &str,
    drawer_id: &str,
    project_roots: &[PathBuf],
    matchers: &mut HashMap<PathBuf, ignore::gitignore::Gitignore>,
) -> DrawerBucket {
    if is_registry_row(room, ingest_mode, drawer_id) {
        return DrawerBucket::Kept;
    }
    if source_file.is_empty() {
        return DrawerBucket::NoSource;
    }
    let source_path = Path::new(source_file);
    if !source_path.is_absolute() {
        return DrawerBucket::NoSource;
    }
    let Some(project_root) = resolve_project_root(source_path, project_roots) else {
        return DrawerBucket::OutOfScope;
    };
    if !source_path.exists() {
        return DrawerBucket::Missing;
    }
    let matcher = matcher_for_root(matchers, project_root);
    if matcher.matched(source_path, false).is_ignore() {
        return DrawerBucket::Gitignored;
    }
    DrawerBucket::Kept
}

/// Fetch every drawer's `(id, source_file, room, ingest_mode)`, optionally scoped to one wing.
async fn sync_query_drawers(
    connection: &Connection,
    wing: Option<&str>,
) -> Result<Vec<(String, String, String, String)>> {
    let rows = if let Some(wing_name) = wing {
        query_all(
            connection,
            "SELECT id, COALESCE(source_file, ''), COALESCE(room, ''), COALESCE(ingest_mode, '') \
             FROM drawers WHERE wing = ?1",
            [wing_name],
        )
        .await?
    } else {
        query_all(
            connection,
            "SELECT id, COALESCE(source_file, ''), COALESCE(room, ''), COALESCE(ingest_mode, '') \
             FROM drawers",
            (),
        )
        .await?
    };
    Ok(rows
        .iter()
        .map(|row| {
            let id: String = row.get(0).unwrap_or_default();
            let source_file: String = row.get(1).unwrap_or_default();
            let room: String = row.get(2).unwrap_or_default();
            let ingest_mode: String = row.get(3).unwrap_or_default();
            (id, source_file, room, ingest_mode)
        })
        .collect())
}

/// Run a sync pass over the palace.
///
/// `apply = false` returns the classification report without touching the
/// database. `apply = true` deletes the gitignored and missing drawers and the
/// closets whose `source_file` matches; at least one of `wing` or
/// `project_dirs` must be set so the caller can't accidentally prune every
/// wing in a multi-project palace.
pub async fn run(
    connection: &Connection,
    project_dirs: &[PathBuf],
    wing: Option<&str>,
    apply: bool,
) -> Result<SyncReport> {
    if apply && wing.is_none() && project_dirs.is_empty() {
        return Err(Error::Other(
            "sync --apply requires --dir or --wing so it cannot auto-prune \
             every wing in a multi-project palace; pass --wing or a project root"
                .to_string(),
        ));
    }

    // Canonicalize so longest-prefix matching uses real on-disk paths and not
    // symlinks. Falls back to the original path when canonicalization fails
    // (e.g. a dir was passed that no longer exists).
    let project_roots: Vec<PathBuf> = project_dirs
        .iter()
        .map(|p| std::fs::canonicalize(p).unwrap_or_else(|_| p.clone()))
        .collect();

    let mut report = SyncReport {
        dry_run: !apply,
        ..SyncReport::default()
    };
    let mut matchers: HashMap<PathBuf, ignore::gitignore::Gitignore> = HashMap::new();
    let mut removable_ids: Vec<String> = Vec::new();
    let mut removable_sources: HashSet<String> = HashSet::new();

    let drawers = sync_query_drawers(connection, wing).await?;

    for (drawer_id, source_file, room, ingest_mode) in &drawers {
        report.scanned += 1;
        let bucket = classify_drawer(
            source_file,
            room,
            ingest_mode,
            drawer_id,
            &project_roots,
            &mut matchers,
        );
        sync_apply_bucket(
            &mut report,
            &bucket,
            drawer_id,
            source_file,
            &mut removable_ids,
            &mut removable_sources,
        );
    }

    if !apply || removable_ids.is_empty() {
        return Ok(report);
    }

    sync_delete(connection, &removable_ids, &removable_sources, &mut report).await?;
    Ok(report)
}

/// Update `report` and the removable lists for one classification result.
///
/// Extracted from `run` so the main loop stays under the 70-line ceiling.
fn sync_apply_bucket(
    report: &mut SyncReport,
    bucket: &DrawerBucket,
    drawer_id: &str,
    source_file: &str,
    removable_ids: &mut Vec<String>,
    removable_sources: &mut HashSet<String>,
) {
    match bucket {
        DrawerBucket::Kept => report.kept += 1,
        DrawerBucket::Gitignored | DrawerBucket::Missing => {
            if matches!(bucket, DrawerBucket::Gitignored) {
                report.gitignored += 1;
            } else {
                report.missing += 1;
            }
            removable_ids.push(drawer_id.to_string());
            if !source_file.is_empty() {
                removable_sources.insert(source_file.to_string());
                *report.by_source.entry(source_file.to_string()).or_insert(0) += 1;
            }
        }
        DrawerBucket::NoSource => report.no_source += 1,
        DrawerBucket::OutOfScope => report.out_of_scope += 1,
    }
}

/// Apply the prune: delete drawers by id, then closets by `source_file`.
///
/// Counts deleted closets via a pre-DELETE COUNT so the report reflects what
/// actually changed rather than the size of the input list.
async fn sync_delete(
    connection: &Connection,
    removable_ids: &[String],
    removable_sources: &HashSet<String>,
    report: &mut SyncReport,
) -> Result<()> {
    // Run the whole prune inside a single transaction so a mid-loop failure
    // doesn't leave half the drawers deleted with their `drawer_words` /
    // `compressed` rows still pointing at them. On any error we ROLLBACK and
    // leave the palace in the state it was in before the apply began.
    connection.execute("BEGIN", ()).await?;
    let removed = match sync_delete_inner(connection, removable_ids, removable_sources).await {
        Ok(value) => value,
        Err(error) => {
            // Best-effort rollback; the original error wins regardless.
            let _ = connection.execute("ROLLBACK", ()).await;
            return Err(error);
        }
    };
    connection.execute("COMMIT", ()).await?;

    report.removed_drawers = removable_ids.len();
    report.removed_closets = removed.closets;
    Ok(())
}

/// Counts returned from [`sync_delete_inner`]. Kept separate from `SyncReport`
/// so the caller only commits report state after the transaction commits.
struct SyncDeleteCounts {
    closets: usize,
}

/// Perform the inner deletes inside the open transaction. Splitting this off
/// keeps `sync_delete` focused on transaction lifecycle.
async fn sync_delete_inner(
    connection: &Connection,
    removable_ids: &[String],
    removable_sources: &HashSet<String>,
) -> Result<SyncDeleteCounts> {
    for drawer_id in removable_ids {
        connection
            .execute(
                "DELETE FROM drawers WHERE id = ?1",
                turso::params![drawer_id.as_str()],
            )
            .await?;
        // drawer_words rows are tied to the drawer id; delete here so an FTS
        // rebuild doesn't surface stale entries for pruned drawers.
        connection
            .execute(
                "DELETE FROM drawer_words WHERE drawer_id = ?1",
                turso::params![drawer_id.as_str()],
            )
            .await?;
    }

    let mut removed_closets = 0usize;
    for source_file in removable_sources {
        let rows = query_all(
            connection,
            "SELECT COUNT(*) FROM compressed WHERE source_file = ?1",
            [source_file.as_str()],
        )
        .await?;
        let count_for_source: i64 = rows
            .first()
            .and_then(|row| row.get_value(0).ok())
            .and_then(|cell| cell.as_integer().copied())
            .unwrap_or(0);
        if count_for_source > 0 {
            connection
                .execute(
                    "DELETE FROM compressed WHERE source_file = ?1",
                    turso::params![source_file.as_str()],
                )
                .await?;
        }
        // Closet counts are display-only; cast safely from a guarded non-negative
        // i64 via try_from so 32-bit edge cases (>2^31 rows per source) saturate
        // rather than silently truncate. In practice a closet table is a small
        // derivative — the bound is defensive, not measured.
        let promoted = usize::try_from(count_for_source.max(0)).unwrap_or(usize::MAX);
        removed_closets += promoted;
    }
    Ok(SyncDeleteCounts {
        closets: removed_closets,
    })
}

/// Pretty-print a `SyncReport` to stdout, matching the Python CLI banner shape.
pub fn print_report(report: &SyncReport, palace_path: &Path, wing: Option<&str>, dirs: &[PathBuf]) {
    println!("\n{}", "=".repeat(55));
    println!("  MemPalace Sync — Gitignore-aware drawer prune");
    println!("{}", "=".repeat(55));
    println!("  Palace:   {}", palace_path.display());
    if let Some(wing_name) = wing {
        println!("  Wing:     {wing_name}");
    }
    for project_dir in dirs {
        println!("  Project:  {}", project_dir.display());
    }
    if report.dry_run {
        println!("  Mode:     DRY RUN (no deletions)");
    } else {
        println!("  Mode:     APPLY (deleting drawers)");
    }
    println!("{}", "-".repeat(55));

    let suffix = if report.dry_run {
        "(would remove)"
    } else {
        "(removed)"
    };
    println!("  Scanned:        {}", report.scanned);
    println!("  Kept:           {}", report.kept);
    println!("  Gitignored:     {}  {suffix}", report.gitignored);
    println!("  Missing:        {}  {suffix}", report.missing);
    println!("  No source:      {}  (kept)", report.no_source);
    println!("  Out of scope:   {}  (kept)", report.out_of_scope);

    if !report.by_source.is_empty() {
        let mut top: Vec<(&String, &usize)> = report.by_source.iter().collect();
        top.sort_by(|a, b| b.1.cmp(a.1));
        let label = if report.dry_run {
            "Top sources to remove"
        } else {
            "Top sources removed"
        };
        println!("\n  {label}:");
        for (source_file, count) in top.iter().take(5) {
            println!("    {source_file}  ({count})");
        }
    }

    if report.dry_run {
        if report.gitignored + report.missing > 0 {
            println!("\n  Re-run with --apply to commit these deletions.");
        }
    } else {
        println!(
            "\n  Removed {} drawers, {} closets.",
            report.removed_drawers, report.removed_closets
        );
    }
    println!("\n{}\n", "=".repeat(55));
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn is_registry_row_matches_known_sentinels() {
        assert!(is_registry_row("_registry", "", ""));
        assert!(is_registry_row("", "registry", ""));
        assert!(is_registry_row("", "", "_reg_abc"));
        // Pair assertion: a normal drawer must NOT be classified as a registry row.
        assert!(!is_registry_row("backend", "projects", "drawer_abc"));
    }

    #[test]
    fn resolve_project_root_picks_longest_prefix() {
        let outer = PathBuf::from("/repo");
        let inner = PathBuf::from("/repo/pkg");
        let roots = vec![outer.clone(), inner.clone()];
        let file = Path::new("/repo/pkg/src/main.rs");
        assert_eq!(resolve_project_root(file, &roots), Some(inner.as_path()));
    }

    #[test]
    fn resolve_project_root_returns_none_for_out_of_scope() {
        let roots = vec![PathBuf::from("/repo")];
        let file = Path::new("/elsewhere/main.rs");
        assert!(resolve_project_root(file, &roots).is_none());
    }

    #[test]
    fn classify_drawer_no_source_when_metadata_empty() {
        let mut matchers = HashMap::new();
        let bucket = classify_drawer("", "general", "projects", "drawer_1", &[], &mut matchers);
        assert!(matches!(bucket, DrawerBucket::NoSource));
    }

    #[test]
    fn classify_drawer_out_of_scope_when_source_outside_roots() {
        let roots = vec![PathBuf::from("/some-other-root")];
        let mut matchers = HashMap::new();
        let bucket = classify_drawer(
            "/elsewhere/file.rs",
            "general",
            "projects",
            "drawer_1",
            &roots,
            &mut matchers,
        );
        assert!(matches!(bucket, DrawerBucket::OutOfScope));
    }

    #[test]
    fn classify_drawer_missing_when_source_path_does_not_exist() {
        // Use a tempdir as the project root so we know the file genuinely does not exist.
        let temp = tempfile::tempdir().expect("tempdir");
        let roots = vec![temp.path().to_path_buf()];
        let missing_path = temp.path().join("absent_file.rs");
        let mut matchers = HashMap::new();
        let bucket = classify_drawer(
            missing_path.to_str().expect("path to_str"),
            "general",
            "projects",
            "drawer_1",
            &roots,
            &mut matchers,
        );
        assert!(matches!(bucket, DrawerBucket::Missing));
    }

    #[test]
    fn classify_drawer_kept_when_source_exists_and_not_ignored() {
        let temp = tempfile::tempdir().expect("tempdir");
        let real_file = temp.path().join("real.rs");
        std::fs::write(&real_file, b"fn main() {}").expect("write real file");
        let roots = vec![temp.path().to_path_buf()];
        let mut matchers = HashMap::new();
        let bucket = classify_drawer(
            real_file.to_str().expect("path to_str"),
            "general",
            "projects",
            "drawer_1",
            &roots,
            &mut matchers,
        );
        assert!(matches!(bucket, DrawerBucket::Kept));
    }

    #[test]
    fn classify_drawer_gitignored_when_source_matches_gitignore() {
        let temp = tempfile::tempdir().expect("tempdir");
        // Seed a .gitignore that matches *.log files at the root.
        std::fs::write(temp.path().join(".gitignore"), b"*.log\n").expect("write .gitignore");
        let ignored = temp.path().join("debug.log");
        std::fs::write(&ignored, b"oops").expect("write ignored file");
        let roots = vec![temp.path().to_path_buf()];
        let mut matchers = HashMap::new();
        let bucket = classify_drawer(
            ignored.to_str().expect("path to_str"),
            "general",
            "projects",
            "drawer_1",
            &roots,
            &mut matchers,
        );
        assert!(matches!(bucket, DrawerBucket::Gitignored));
    }

    #[test]
    fn classify_drawer_kept_for_registry_row_regardless_of_source() {
        // Registry sentinels must survive a sync pass even with bogus source paths
        // so the next mine doesn't redo work it already finished.
        let mut matchers = HashMap::new();
        let bucket = classify_drawer(
            "/not/a/real/path",
            "_registry",
            "registry",
            "_reg_abc",
            &[],
            &mut matchers,
        );
        assert!(matches!(bucket, DrawerBucket::Kept));
    }

    #[tokio::test]
    async fn run_apply_without_scope_returns_error() {
        // Defense-in-depth: apply mode demands explicit scoping.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = run(&connection, &[], None, true).await;
        assert!(result.is_err(), "apply without --dir or --wing must error");
    }

    #[tokio::test]
    async fn run_dry_run_classifies_drawers() {
        // Seed a drawer with a real source path inside a tempdir, and another with
        // a missing path. Dry-run should bucket them correctly and not delete anything.
        let temp = tempfile::tempdir().expect("tempdir");
        // Canonicalize the temp root so the drawer's source_file matches the
        // canonical form sync's `resolve_project_root` compares against. On
        // macOS std::env::temp_dir() returns `/var/folders/...` which
        // canonicalizes to `/private/var/folders/...`; without this the test
        // pseudo-paths would be classified as out_of_scope.
        let temp_root = std::fs::canonicalize(temp.path()).expect("canonicalize tempdir");

        let real_file = temp_root.join("real.rs");
        std::fs::write(&real_file, b"fn main() {}").expect("seed file");

        let (_db, connection) = crate::test_helpers::test_db().await;

        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "drawer_real",
                wing: "test",
                room: "general",
                content: "real content",
                source_file: real_file.to_str().expect("path to_str"),
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add real drawer");

        let missing_path = temp_root.join("ghost.rs");
        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "drawer_ghost",
                wing: "test",
                room: "general",
                content: "ghost content",
                source_file: missing_path.to_str().expect("path to_str"),
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add ghost drawer");

        let roots = vec![temp_root.clone()];
        let report = run(&connection, &roots, Some("test"), false)
            .await
            .expect("dry-run sync must succeed");

        assert_eq!(report.scanned, 2);
        assert_eq!(report.kept, 1, "real drawer must be kept");
        assert_eq!(report.missing, 1, "ghost drawer must be classified missing");
        assert_eq!(report.removed_drawers, 0, "dry-run must not delete");
        assert!(report.dry_run);
    }

    #[test]
    fn resolve_project_root_returns_none_when_no_roots() {
        // Empty roots list short-circuits without inspecting source_file at all.
        let file = Path::new("/anywhere/main.rs");
        assert!(resolve_project_root(file, &[]).is_none());
    }

    #[test]
    fn sync_apply_bucket_routes_kept_drawer_to_kept_count() {
        let mut report = SyncReport::default();
        let mut ids = Vec::new();
        let mut sources = HashSet::new();
        sync_apply_bucket(
            &mut report,
            &DrawerBucket::Kept,
            "drawer_1",
            "/x/file.rs",
            &mut ids,
            &mut sources,
        );
        assert_eq!(report.kept, 1);
        assert!(ids.is_empty(), "kept drawer must not be queued for removal");
        assert!(
            sources.is_empty(),
            "kept drawer must not record a removable source"
        );
    }

    #[test]
    fn sync_apply_bucket_routes_gitignored_to_removable_list() {
        let mut report = SyncReport::default();
        let mut ids = Vec::new();
        let mut sources = HashSet::new();
        sync_apply_bucket(
            &mut report,
            &DrawerBucket::Gitignored,
            "drawer_1",
            "/x/ignored.log",
            &mut ids,
            &mut sources,
        );
        assert_eq!(report.gitignored, 1);
        assert_eq!(ids, vec!["drawer_1".to_string()]);
        assert!(sources.contains("/x/ignored.log"));
        assert_eq!(report.by_source["/x/ignored.log"], 1);
    }

    #[test]
    fn sync_apply_bucket_routes_missing_with_no_source_does_not_record_source() {
        // A drawer classified as Missing but with an empty source string must
        // still be removable, just without polluting by_source / removable_sources.
        let mut report = SyncReport::default();
        let mut ids = Vec::new();
        let mut sources = HashSet::new();
        sync_apply_bucket(
            &mut report,
            &DrawerBucket::Missing,
            "drawer_1",
            "",
            &mut ids,
            &mut sources,
        );
        assert_eq!(report.missing, 1);
        assert_eq!(ids.len(), 1);
        assert!(sources.is_empty(), "blank source must not be tracked");
        assert!(report.by_source.is_empty());
    }

    #[test]
    fn sync_apply_bucket_routes_no_source_to_no_source_count() {
        let mut report = SyncReport::default();
        let mut ids = Vec::new();
        let mut sources = HashSet::new();
        sync_apply_bucket(
            &mut report,
            &DrawerBucket::NoSource,
            "drawer_1",
            "",
            &mut ids,
            &mut sources,
        );
        assert_eq!(report.no_source, 1);
        assert!(ids.is_empty());
    }

    #[test]
    fn sync_apply_bucket_routes_out_of_scope_to_out_of_scope_count() {
        let mut report = SyncReport::default();
        let mut ids = Vec::new();
        let mut sources = HashSet::new();
        sync_apply_bucket(
            &mut report,
            &DrawerBucket::OutOfScope,
            "drawer_1",
            "/outside",
            &mut ids,
            &mut sources,
        );
        assert_eq!(report.out_of_scope, 1);
        assert!(ids.is_empty());
    }

    #[test]
    fn print_report_does_not_panic_for_dry_run() {
        // print_report runs through every println! branch; the lib test runner
        // captures stdout. The point of the test is purely defensive — make sure
        // no formatting branch panics on a representative report.
        let mut by_source = HashMap::new();
        by_source.insert("/repo/a.log".to_string(), 3);
        by_source.insert("/repo/b.log".to_string(), 1);
        let report = SyncReport {
            scanned: 10,
            kept: 6,
            gitignored: 4,
            missing: 0,
            no_source: 0,
            out_of_scope: 0,
            removed_drawers: 0,
            removed_closets: 0,
            dry_run: true,
            by_source,
        };
        print_report(
            &report,
            Path::new("/tmp/palace.db"),
            Some("test"),
            &[PathBuf::from("/repo")],
        );
    }

    #[test]
    fn print_report_does_not_panic_for_apply_with_no_changes() {
        let report = SyncReport {
            scanned: 1,
            kept: 1,
            ..SyncReport::default()
        };
        print_report(&report, Path::new("/tmp/palace.db"), None, &[]);
    }

    #[test]
    fn print_report_does_not_panic_after_apply_with_removals() {
        let mut by_source = HashMap::new();
        by_source.insert("/repo/x.log".to_string(), 2);
        let report = SyncReport {
            scanned: 3,
            kept: 1,
            gitignored: 2,
            removed_drawers: 2,
            removed_closets: 1,
            dry_run: false,
            by_source,
            ..SyncReport::default()
        };
        print_report(
            &report,
            Path::new("/tmp/palace.db"),
            Some("test"),
            &[PathBuf::from("/repo")],
        );
    }

    #[tokio::test]
    async fn run_dry_run_with_no_drawers_returns_zero_counts() {
        // No drawers at all — every bucket count should be zero.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let report = run(&connection, &[], Some("missing-wing"), false)
            .await
            .expect("dry-run on empty palace must succeed");
        assert_eq!(report.scanned, 0);
        assert_eq!(report.kept, 0);
        assert_eq!(report.gitignored, 0);
        assert_eq!(report.missing, 0);
        assert!(report.dry_run);
    }

    #[tokio::test]
    async fn run_apply_purges_matching_closets() {
        // A drawer whose source_file is missing AND has a matching `compressed`
        // (closet) row triggers both deletions. Verifies the closet-purge branch.
        let temp = tempfile::tempdir().expect("tempdir");
        let temp_root = std::fs::canonicalize(temp.path()).expect("canonicalize tempdir");
        let (_db, connection) = crate::test_helpers::test_db().await;

        let ghost = temp_root.join("absent.rs");
        let source_str = ghost.to_str().expect("path to_str");

        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "drawer_with_closet",
                wing: "test",
                room: "general",
                content: "content",
                source_file: source_str,
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("seed drawer");

        // Insert a closet row directly with the matching source_file so the
        // closet-purge branch in `sync_delete` has work to do.
        connection
            .execute(
                "INSERT INTO compressed (id, wing, source_file, content) VALUES (?1, ?2, ?3, ?4)",
                turso::params!["closet_x", "test", source_str, "summary"],
            )
            .await
            .expect("seed closet");

        let roots = vec![temp_root.clone()];
        let report = run(&connection, &roots, Some("test"), true)
            .await
            .expect("apply sync must succeed");

        assert_eq!(report.removed_drawers, 1);
        assert_eq!(
            report.removed_closets, 1,
            "matching closet must be purged when its source is pruned"
        );

        let rows = query_all(
            &connection,
            "SELECT COUNT(*) FROM compressed WHERE source_file = ?1",
            [source_str],
        )
        .await
        .expect("count closets after apply");
        let remaining: i64 = rows
            .first()
            .and_then(|row| row.get_value(0).ok())
            .and_then(|cell| cell.as_integer().copied())
            .unwrap_or(0);
        assert_eq!(remaining, 0, "closet row must be purged");
    }

    #[tokio::test]
    async fn run_apply_deletes_missing_drawers() {
        let temp = tempfile::tempdir().expect("tempdir");
        let temp_root = std::fs::canonicalize(temp.path()).expect("canonicalize tempdir");
        let (_db, connection) = crate::test_helpers::test_db().await;

        let ghost = temp_root.join("vanished.rs");
        crate::palace::drawer::add_drawer(
            &connection,
            &crate::palace::drawer::DrawerParams {
                id: "drawer_vanished",
                wing: "test",
                room: "general",
                content: "vanished content",
                source_file: ghost.to_str().expect("path to_str"),
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("add vanished drawer");

        let roots = vec![temp_root.clone()];
        let report = run(&connection, &roots, Some("test"), true)
            .await
            .expect("apply sync must succeed");

        assert_eq!(report.missing, 1);
        assert_eq!(report.removed_drawers, 1);

        // Pair assertion: the deleted drawer must no longer be present.
        let rows = query_all(
            &connection,
            "SELECT COUNT(*) FROM drawers WHERE id = 'drawer_vanished'",
            (),
        )
        .await
        .expect("count drawers after apply");
        let remaining: i64 = rows
            .first()
            .and_then(|row| row.get_value(0).ok())
            .and_then(|cell| cell.as_integer().copied())
            .unwrap_or(0);
        assert_eq!(remaining, 0, "deleted drawer must not remain");
    }
}
