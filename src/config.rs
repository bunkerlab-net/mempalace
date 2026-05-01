//! Configuration loading for global (`$XDG_DATA_HOME/mempalace/config.json`)
//! and per-project (`mempalace.yaml`) settings.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Global mempalace configuration (`$XDG_DATA_HOME/mempalace/config.json`).
#[derive(Debug, Serialize, Deserialize)]
pub struct MempalaceConfig {
    /// Path to the palace `SQLite` database file.
    #[serde(default = "default_palace_path")]
    pub palace_path: PathBuf,

    /// Collection name (legacy from Python version).
    #[serde(default = "default_collection_name")]
    pub collection_name: String,

    /// Entity name → short code mappings for AAAK dialect compression.
    #[serde(default)]
    pub people_map: HashMap<String, String>,

    /// Stop hook saves silently via `systemMessage` instead of blocking the AI.
    /// Defaults to `true` (silent mode) so saves happen in the background.
    #[serde(default = "default_hook_silent_save")]
    pub hook_silent_save: bool,

    /// Stop hook sends a desktop notification via `notify-send` after a silent save.
    /// Defaults to `false`.
    #[serde(default)]
    pub hook_desktop_toast: bool,

    /// Language codes for multilingual entity detection (e.g. `["en", "de"]`).
    /// Defaults to `["en"]`.
    #[serde(default = "default_entity_languages")]
    pub entity_languages: Vec<String>,
}

fn default_palace_path() -> PathBuf {
    config_dir().join("palace.db")
}

fn default_collection_name() -> String {
    "mempalace_drawers".to_string()
}

fn default_hook_silent_save() -> bool {
    true
}

fn default_entity_languages() -> Vec<String> {
    vec!["en".to_string()]
}

/// Returns the mempalace data directory.
///
/// Resolution order:
///   1. `MEMPALACE_DIR` env var — explicit user, container, or test override.
///   2. `MEMPAL_DIR` env var — short alias accepted by the Python implementation.
///   3. `$XDG_DATA_HOME/mempalace` — XDG standard location.
///   4. `~/.local/share/mempalace` — XDG default fallback.
pub fn config_dir() -> PathBuf {
    for var in &["MEMPALACE_DIR", "MEMPAL_DIR"] {
        if let Ok(env_path) = std::env::var(var)
            && !env_path.is_empty()
        {
            return PathBuf::from(env_path);
        }
    }
    let data_directory = xdg_data_dir().join("mempalace");
    assert!(!data_directory.as_os_str().is_empty());
    assert!(data_directory.ends_with("mempalace"));
    data_directory
}

/// Returns `$XDG_DATA_HOME` if set to an absolute path, otherwise `$HOME/.local/share`.
///
/// A relative or empty `$XDG_DATA_HOME` is treated as unset, per XDG spec intent.
fn xdg_data_dir() -> PathBuf {
    let base_directory = if let Ok(xdg_data_home) = std::env::var("XDG_DATA_HOME")
        && !xdg_data_home.is_empty()
        && PathBuf::from(&xdg_data_home).is_absolute()
    {
        PathBuf::from(xdg_data_home)
    } else {
        home_dir().join(".local").join("share")
    };
    assert!(!base_directory.as_os_str().is_empty());
    // Negative space: path must not contain null bytes.
    assert!(!base_directory.to_string_lossy().contains('\0'));
    base_directory
}

/// Returns the legacy `~/.mempalace` directory, used only for migration detection.
pub fn legacy_dir() -> PathBuf {
    let legacy_directory = home_dir().join(".mempalace");
    assert!(!legacy_directory.as_os_str().is_empty());
    assert!(legacy_directory.ends_with(".mempalace"));
    legacy_directory
}

/// Returns the user's home directory.
///
/// Checks `HOME` (POSIX), then `USERPROFILE` (Windows), then
/// `HOMEDRIVE`+`HOMEPATH` (legacy Windows). Panics if none are set — a missing
/// home directory is a fatal misconfiguration that yields unusable XDG paths.
fn home_dir() -> PathBuf {
    let os_home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let path = std::env::var_os("HOMEPATH")?;
            Some(PathBuf::from(drive).join(path).into_os_string())
        });
    // A missing home directory is a programmer/environment error, not a
    // recoverable operating error — assert so both debug and release builds fail fast.
    assert!(
        os_home.is_some(),
        "HOME, USERPROFILE, or HOMEDRIVE+HOMEPATH must be set"
    );
    // unwrap_or_default() is unreachable after the assert above; used in place
    // of unwrap() because clippy::unwrap_used is denied in this project.
    let home = PathBuf::from(os_home.unwrap_or_default());
    assert!(!home.as_os_str().is_empty());
    home
}

/// Expand a leading `~` to the user's home directory.
///
/// Resolves the home directory by trying `HOME`, then `USERPROFILE`, then
/// `HOMEDRIVE` + `HOMEPATH` (Windows fallback). Uses `OsStr`-based path
/// component inspection to avoid lossy UTF-8 conversion. Returns the path
/// unchanged when no leading `~` is present or when the home directory cannot
/// be resolved.
pub(crate) fn expand_tilde(path: &Path) -> PathBuf {
    use std::ffi::OsStr;
    use std::path::Component;

    // Precondition: a caller should never pass an empty path.
    assert!(
        !path.as_os_str().is_empty(),
        "expand_tilde: path must not be empty"
    );

    let mut components = path.components();
    let first = components.next();

    let result = if first == Some(Component::Normal(OsStr::new("~"))) {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .or_else(|| {
                let drive = std::env::var_os("HOMEDRIVE")?;
                let home_path = std::env::var_os("HOMEPATH")?;
                Some(PathBuf::from(drive).join(home_path).into_os_string())
            });

        match home {
            Some(h) => {
                let rest: PathBuf = components.collect();
                PathBuf::from(h).join(rest)
            }
            None => path.to_path_buf(),
        }
    } else {
        path.to_path_buf()
    };

    // Postcondition: expansion never produces an empty path.
    debug_assert!(
        !result.as_os_str().is_empty(),
        "expand_tilde: result must not be empty"
    );
    result
}

/// Normalize a directory name into a wing slug.
///
/// Lowercases the name and replaces spaces and hyphens with underscores.
/// This is the single canonical rule used by every wing-slug producer
/// (`miner`, `convo_miner`, `room_detect`, `palace_graph`, `cli::init`) — matches
/// `mempalace/config.py::normalize_wing_name` verbatim.
pub fn normalize_wing_name(name: &str) -> String {
    assert!(
        !name.is_empty(),
        "normalize_wing_name: name must not be empty"
    );
    let result = name.to_lowercase().replace([' ', '-'], "_");
    assert!(!result.is_empty());
    assert!(!result.contains(' '));
    assert!(!result.contains('-'));
    result
}

/// Path to the global config file.
pub fn config_path() -> PathBuf {
    config_dir().join("config.json")
}

impl MempalaceConfig {
    /// Load config from the XDG data dir, or return defaults.
    pub fn load() -> Result<Self> {
        maybe_migrate()?;
        let path = config_path();
        if path.exists() {
            let data = std::fs::read_to_string(&path)?;
            let config: Self = serde_json::from_str(&data)?;
            Ok(config)
        } else {
            Ok(Self::default())
        }
    }

    /// Ensure the data directory and default config exist.
    pub fn init() -> Result<Self> {
        maybe_migrate()?;
        let directory = config_dir();
        std::fs::create_dir_all(&directory)?;

        let path = config_path();
        if path.exists() {
            let data = std::fs::read_to_string(&path)?;
            let config: Self = serde_json::from_str(&data)?;
            Ok(config)
        } else {
            let config = Self::default();
            let data = serde_json::to_string_pretty(&config)?;
            std::fs::write(&path, data)?;
            Ok(config)
        }
    }

    /// Write the current config to the standard config file path durably.
    ///
    /// Writes JSON to a temporary file in the config directory, fsyncs it,
    /// renames over `config.json` (atomic on the same filesystem), then fsyncs
    /// the parent directory so the rename itself survives a power loss. A
    /// crash between any of these steps either leaves the previous config
    /// intact or commits the new one — never a truncated middle state.
    pub fn save(&self) -> Result<()> {
        use std::io::Write as _;
        use std::sync::atomic::{AtomicU64, Ordering};

        // Per-call unique-suffix counter so concurrent saves cannot clobber
        // each other's temp file before either gets renamed. PID + a
        // monotonic atomic is enough for in-process uniqueness; `O_EXCL`
        // (`create_new`) catches the cross-process collision case by
        // forcing an error rather than silently overwriting another
        // save's intermediate file.
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        assert!(
            !self.palace_path.as_os_str().is_empty(),
            "palace_path must not be empty before saving config"
        );
        let directory = config_dir();
        std::fs::create_dir_all(&directory)?;
        let data = serde_json::to_string_pretty(self)?;

        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = directory.join(format!("config.json.tmp.{}.{unique}", std::process::id()));
        let mut tmp_file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)?;
        tmp_file.write_all(data.as_bytes())?;
        // Persist the file's bytes before the rename; without sync_all, a power
        // loss after the rename can leave the directory entry pointing at a
        // file whose data has not yet hit disk.
        tmp_file.sync_all()?;
        drop(tmp_file);

        std::fs::rename(&tmp_path, config_path())?;

        // Persist the directory entry too. On most filesystems the rename
        // itself is journalled separately from the file's data; fsyncing the
        // parent directory commits the rename so a subsequent crash cannot
        // resurrect the old name.
        if let Ok(dir_file) = std::fs::File::open(&directory) {
            let _ = dir_file.sync_all();
        }

        // Pair assertion: the file must exist after a successful rename.
        debug_assert!(config_path().exists(), "config.json must exist after save");
        Ok(())
    }

    /// Resolve the palace database path, checking env vars in priority order.
    ///
    /// Resolution order:
    ///   1. `MEMPALACE_PALACE_PATH` — primary override.
    ///   2. `MEMPAL_PALACE_PATH` — short alias accepted by the Python implementation.
    ///   3. `palace_path` from config file.
    ///
    /// Env values are tilde-expanded and resolved to absolute paths. Empty or
    /// whitespace-only values are treated as unset, because shells commonly
    /// export empty vars when clearing them.
    pub fn palace_db_path(&self) -> PathBuf {
        for var in &["MEMPALACE_PALACE_PATH", "MEMPAL_PALACE_PATH"] {
            if let Ok(env_path) = std::env::var(var)
                && !env_path.trim().is_empty()
            {
                let expanded = expand_tilde(Path::new(&env_path));
                // std::path::absolute is available since Rust 1.79 (MSRV is 1.88).
                // unwrap_or(expanded.clone()) falls back to the expanded path if
                // the OS call fails (e.g. path refers to a not-yet-created dir).
                let resolved = std::path::absolute(&expanded).unwrap_or(expanded);
                return resolved;
            }
        }
        assert!(
            !self.palace_path.as_os_str().is_empty(),
            "palace_path must not be empty"
        );
        self.palace_path.clone()
    }
}

impl Default for MempalaceConfig {
    fn default() -> Self {
        Self {
            palace_path: default_palace_path(),
            collection_name: default_collection_name(),
            people_map: HashMap::new(),
            hook_silent_save: default_hook_silent_save(),
            hook_desktop_toast: false,
            entity_languages: default_entity_languages(),
        }
    }
}

// --- Migration ---

/// Migrate from `~/.mempalace` to `$XDG_DATA_HOME/mempalace` if needed.
///
/// Idempotent: returns immediately if already migrated, legacy dir absent,
/// or either dir env var alias is set.
fn maybe_migrate() -> Result<()> {
    // MEMPALACE_DIR / MEMPAL_DIR means the caller manages paths — skip migration.
    for var in &["MEMPALACE_DIR", "MEMPAL_DIR"] {
        if let Ok(env_value) = std::env::var(var)
            && !env_value.is_empty()
        {
            return Ok(());
        }
    }

    // Without a resolvable home directory there is no legacy ~/.mempalace to
    // migrate from; skip silently rather than panicking inside legacy_dir().
    let home_resolvable = std::env::var_os("HOME").is_some()
        || std::env::var_os("USERPROFILE").is_some()
        || (std::env::var_os("HOMEDRIVE").is_some() && std::env::var_os("HOMEPATH").is_some());
    if !home_resolvable {
        return Ok(());
    }

    let source = legacy_dir();
    let destination = config_dir();

    // No legacy directory — nothing to migrate.
    if !source.exists() {
        return Ok(());
    }

    // Already migrated — config.json was moved last in maybe_migrate_inner(),
    // so its presence at the destination confirms full completion. A partial
    // migration (crash before config.json was moved) re-enters here and resumes
    // because source still contains config.json.
    if destination.join("config.json").exists() {
        return Ok(());
    }

    assert!(source != destination);
    assert!(source.exists());

    maybe_migrate_inner(&source, &destination)
}

/// Perform the actual migration: move files from `source` to `destination`.
///
/// `config.json` is moved last so that its presence at the destination acts as
/// an atomic completion marker. If this function is interrupted mid-run, the
/// next call re-enters and resumes: already-moved files are skipped because
/// their source paths no longer exist.
fn maybe_migrate_inner(source: &Path, destination: &Path) -> Result<()> {
    assert!(source.exists());
    assert_ne!(source, destination);

    std::fs::create_dir_all(destination)?;

    // Move all artifacts except config.json first. config.json moves last —
    // its arrival at the destination is the completion marker (see maybe_migrate).
    let files = [
        "identity.txt",
        "palace.db",
        "palace.db-wal",
        "palace.db-shm",
        "palace.db.bak",
    ];
    for name in &files {
        let source_file = source.join(name);
        if source_file.exists() {
            maybe_migrate_move_file(&source_file, &destination.join(name))?;
        }
    }

    let wal_source = source.join("wal");
    if wal_source.exists() {
        maybe_migrate_move_dir(&wal_source, &destination.join("wal"))?;
    }

    // Patch palace_path in config.json while it is still at the source so that
    // the moved copy already contains the correct path. Moving it after patching
    // keeps the patch and the move atomic with respect to the completion marker.
    let source_config = source.join("config.json");
    if source_config.exists() {
        let legacy_db = source.join("palace.db");
        let database_new = destination.join("palace.db");
        maybe_migrate_patch_config(&source_config, &legacy_db, &database_new)?;
        maybe_migrate_move_file(&source_config, &destination.join("config.json"))?;
    }

    // Remove the legacy directory if it is now empty.
    if std::fs::read_dir(source).is_ok_and(|mut entries| entries.next().is_none()) {
        let _ = std::fs::remove_dir(source);
    }

    eprintln!(
        "mempalace: migrated from {} to {}",
        source.display(),
        destination.display()
    );

    Ok(())
}

/// Move a directory's file contents from `source` to `destination`.
fn maybe_migrate_move_dir(source: &Path, destination: &Path) -> Result<()> {
    assert!(source.exists());
    assert_ne!(source, destination);

    std::fs::create_dir_all(destination)?;

    // Collect entries before iterating so the directory is not modified mid-walk.
    let entries: Vec<_> = std::fs::read_dir(source)?
        .filter_map(std::result::Result::ok)
        .collect();

    assert!(source.is_dir());

    for entry in &entries {
        let source_entry = entry.path();
        if source_entry.is_file() {
            maybe_migrate_move_file(&source_entry, &destination.join(entry.file_name()))?;
        }
    }

    // Remove source dir if now empty.
    if std::fs::read_dir(source).is_ok_and(|mut entries| entries.next().is_none()) {
        let _ = std::fs::remove_dir(source);
    }

    Ok(())
}

/// Move a single file, trying rename first, falling back to copy + delete.
///
/// Rename is fast and atomic on the same filesystem. Copy + delete handles
/// cross-filesystem moves (e.g. `~/.mempalace` on one mount, `~/.local` on another).
fn maybe_migrate_move_file(source: &Path, destination: &Path) -> Result<()> {
    assert!(source.exists());
    assert_ne!(source, destination);

    if std::fs::rename(source, destination).is_err() {
        std::fs::copy(source, destination)?;
        std::fs::remove_file(source)?;
    }

    // Pair assertion: destination must exist after the move.
    assert!(destination.exists());

    Ok(())
}

/// Update `palace_path` in config.json if it still points to the legacy DB location.
fn maybe_migrate_patch_config(
    config_path: &Path,
    legacy_db: &Path,
    database_new: &Path,
) -> Result<()> {
    assert!(config_path.exists());
    assert_ne!(legacy_db, database_new);

    let data = std::fs::read_to_string(config_path)?;
    let mut config: MempalaceConfig = match serde_json::from_str(&data) {
        Ok(c) => c,
        // Corrupted config — leave it alone; startup will surface the parse error.
        Err(_) => return Ok(()),
    };

    if config.palace_path == legacy_db {
        config.palace_path = database_new.to_path_buf();
        let patched = serde_json::to_string_pretty(&config)?;
        std::fs::write(config_path, &patched)?;
        // Pair assertion: patched value must round-trip correctly.
        debug_assert!(
            serde_json::from_str::<MempalaceConfig>(&patched)
                .is_ok_and(|c| c.palace_path.as_path() == database_new)
        );
    }

    Ok(())
}

// --- Per-project config ---

/// Per-project config (`mempalace.yaml`).
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectConfig {
    /// Wing name — the project-level namespace in the palace.
    pub wing: String,
    /// Room definitions for this project.
    pub rooms: Vec<RoomConfig>,
}

/// A room within a wing — a category for filing drawers.
#[derive(Debug, Serialize, Deserialize)]
pub struct RoomConfig {
    /// Room name (e.g. `"backend"`, `"frontend"`).
    pub name: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Keywords used for content-based room detection.
    #[serde(default)]
    pub keywords: Vec<String>,
}

impl ProjectConfig {
    /// Load from a mempalace.yaml file.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.extension().is_some_and(|e| e == "yaml" || e == "yml") {
            return Err(Error::Other(format!(
                "ProjectConfig::load: expected .yaml or .yml file, got: {}",
                path.display()
            )));
        }
        if !path.exists() {
            return Err(Error::ConfigNotFound(path.to_path_buf()));
        }
        let data = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&data)?;
        Ok(config)
    }
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_palace_path() {
        // default_palace_path() must return a path inside the XDG data directory.
        // Clear overrides so the function resolves through HOME/.local/share.
        temp_env::with_vars(
            [("MEMPALACE_DIR", None::<&str>), ("XDG_DATA_HOME", None)],
            || {
                let path = default_palace_path();
                let path_str = path.to_string_lossy();
                assert!(path_str.contains("mempalace"));
                assert!(path_str.ends_with("palace.db"));
            },
        );
    }

    #[test]
    fn config_dir_ends_with_mempalace() {
        // config_dir() must end with "mempalace" when resolved via the XDG default.
        // Clear overrides so the function uses HOME/.local/share/mempalace.
        temp_env::with_vars(
            [("MEMPALACE_DIR", None::<&str>), ("XDG_DATA_HOME", None)],
            || {
                assert!(config_dir().ends_with("mempalace"));
            },
        );
    }

    #[test]
    fn config_dir_respects_mempalace_dir_env_var() {
        // Verify that MEMPALACE_DIR overrides the XDG path. temp_env safely
        // sets the var for this test and restores the previous value afterwards,
        // preventing interference with concurrent tests.
        let tempdir = tempfile::tempdir().expect("failed to create temp dir");
        temp_env::with_var("MEMPALACE_DIR", Some(tempdir.path()), || {
            assert_eq!(config_dir(), tempdir.path());
        });
    }

    #[test]
    fn config_dir_uses_xdg_data_home() {
        // XDG_DATA_HOME should set the base for config_dir().
        let xdg_tempdir = tempfile::tempdir().expect("failed to create temp dir");
        temp_env::with_vars(
            [
                (
                    "XDG_DATA_HOME",
                    Some(xdg_tempdir.path().to_str().expect("valid path")),
                ),
                ("MEMPALACE_DIR", None),
            ],
            || {
                let result = config_dir();
                assert_eq!(result, xdg_tempdir.path().join("mempalace"));
            },
        );
    }

    #[test]
    fn xdg_data_home_relative_path_falls_back_to_default() {
        // A relative XDG_DATA_HOME must be ignored — only absolute paths are valid
        // per XDG spec intent. The fallback is $HOME/.local/share/mempalace.
        let home = tempfile::tempdir().expect("failed to create temp dir");
        temp_env::with_vars(
            [
                ("HOME", Some(home.path().to_str().expect("valid path"))),
                ("XDG_DATA_HOME", Some("relative/path")),
                ("MEMPALACE_DIR", None),
            ],
            || {
                let result = config_dir();
                assert_eq!(
                    result,
                    home.path().join(".local").join("share").join("mempalace")
                );
                assert!(!result.to_string_lossy().contains("relative/path"));
            },
        );
    }

    #[test]
    fn mempalace_dir_overrides_xdg_data_home() {
        // MEMPALACE_DIR takes priority over XDG_DATA_HOME.
        let mempalace_override = tempfile::tempdir().expect("failed to create temp dir");
        let xdg_tempdir = tempfile::tempdir().expect("failed to create temp dir");
        temp_env::with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(mempalace_override.path().to_str().expect("valid path")),
                ),
                (
                    "XDG_DATA_HOME",
                    Some(xdg_tempdir.path().to_str().expect("valid path")),
                ),
            ],
            || {
                assert_eq!(config_dir(), mempalace_override.path());
            },
        );
    }

    #[test]
    fn default_palace_path_uses_xdg_data_dir() {
        // The default palace.db path must be inside the XDG data dir, not ~/.mempalace.
        let xdg_tempdir = tempfile::tempdir().expect("failed to create temp dir");
        temp_env::with_vars(
            [
                (
                    "XDG_DATA_HOME",
                    Some(xdg_tempdir.path().to_str().expect("valid path")),
                ),
                ("MEMPALACE_DIR", None),
            ],
            || {
                let path = default_palace_path();
                assert_eq!(path, xdg_tempdir.path().join("mempalace").join("palace.db"));
                assert!(!path.to_string_lossy().contains(".mempalace"));
            },
        );
    }

    #[test]
    fn migrate_moves_files_from_legacy_dir() {
        // Set HOME to a temp dir, create a fake legacy ~/.mempalace, run migration,
        // verify files land in ~/.local/share/mempalace/.
        let home = tempfile::tempdir().expect("failed to create temp dir");
        let legacy = home.path().join(".mempalace");
        std::fs::create_dir_all(&legacy).expect("create legacy dir");
        std::fs::write(legacy.join("config.json"), r#"{"palace_path":"/old/palace.db","collection_name":"mempalace_drawers","people_map":{}}"#)
            .expect("write config.json");
        std::fs::write(legacy.join("identity.txt"), "I am a test palace.")
            .expect("write identity.txt");

        temp_env::with_vars(
            [
                ("HOME", Some(home.path().to_str().expect("valid path"))),
                ("MEMPALACE_DIR", None),
                ("XDG_DATA_HOME", None),
            ],
            || {
                maybe_migrate().expect("migration should succeed");

                let destination = home.path().join(".local").join("share").join("mempalace");
                assert!(destination.join("config.json").exists());
                assert!(destination.join("identity.txt").exists());
                // Legacy directory should be removed (it is now empty).
                assert!(!legacy.exists());
            },
        );
    }

    #[test]
    fn migrate_skipped_when_mempalace_dir_set() {
        // When MEMPALACE_DIR is set, migration must not run.
        let home = tempfile::tempdir().expect("failed to create temp dir");
        let legacy = home.path().join(".mempalace");
        std::fs::create_dir_all(&legacy).expect("create legacy dir");
        std::fs::write(legacy.join("config.json"), "{}").expect("write config.json");

        let override_dir = tempfile::tempdir().expect("create override dir");
        temp_env::with_vars(
            [
                ("HOME", Some(home.path().to_str().expect("valid path"))),
                (
                    "MEMPALACE_DIR",
                    Some(override_dir.path().to_str().expect("valid path")),
                ),
                ("XDG_DATA_HOME", None),
            ],
            || {
                maybe_migrate().expect("should return ok without migrating");
                // Legacy dir must still exist — migration was skipped.
                assert!(legacy.join("config.json").exists());
            },
        );
    }

    #[test]
    fn migrate_idempotent() {
        // Running migration twice must produce the same result with no errors.
        let home = tempfile::tempdir().expect("failed to create temp dir");
        let legacy = home.path().join(".mempalace");
        std::fs::create_dir_all(&legacy).expect("create legacy dir");
        std::fs::write(legacy.join("config.json"), r#"{"palace_path":"/old/palace.db","collection_name":"mempalace_drawers","people_map":{}}"#)
            .expect("write config.json");

        temp_env::with_vars(
            [
                ("HOME", Some(home.path().to_str().expect("valid path"))),
                ("MEMPALACE_DIR", None),
                ("XDG_DATA_HOME", None),
            ],
            || {
                maybe_migrate().expect("first migration");
                maybe_migrate().expect("second migration — must be idempotent");

                let destination = home.path().join(".local").join("share").join("mempalace");
                assert!(destination.join("config.json").exists());
            },
        );
    }

    #[test]
    fn migrate_resumes_after_partial_migration() {
        // Simulate a crash mid-migration: palace.db was already moved to the
        // destination but config.json is still in the legacy dir. The next run
        // must detect that config.json is still in the source (completion marker
        // absent at destination) and finish the migration rather than skipping it.
        let home = tempfile::tempdir().expect("failed to create temp dir");
        let legacy = home.path().join(".mempalace");
        let destination = home.path().join(".local").join("share").join("mempalace");

        std::fs::create_dir_all(&legacy).expect("create legacy dir");
        std::fs::create_dir_all(&destination).expect("create destination dir");

        // config.json is still in the legacy dir (not yet moved — migration incomplete).
        std::fs::write(
            legacy.join("config.json"),
            r#"{"palace_path":"/old/palace.db","collection_name":"mempalace_drawers","people_map":{}}"#,
        )
        .expect("write config.json");
        // palace.db was already moved to the destination in the previous partial run.
        std::fs::write(destination.join("palace.db"), b"fake db content").expect("write palace.db");

        temp_env::with_vars(
            [
                ("HOME", Some(home.path().to_str().expect("valid path"))),
                ("MEMPALACE_DIR", None),
                ("XDG_DATA_HOME", None),
            ],
            || {
                maybe_migrate().expect("migration must resume successfully");

                // Completion marker must now exist.
                assert!(destination.join("config.json").exists());
                // palace.db must still be present (was already there).
                assert!(destination.join("palace.db").exists());
                // Legacy dir must be gone (now empty).
                assert!(!legacy.exists());
            },
        );
    }

    #[test]
    fn migrate_patches_palace_path_when_pointing_to_legacy_default() {
        // maybe_migrate_patch_config() only rewrites palace_path when it equals
        // the legacy default (source.join("palace.db")). All other migration
        // tests use "/old/palace.db" which never matches, leaving this code path
        // untested. This test uses the real legacy default path to exercise it.
        let home = tempfile::tempdir().expect("failed to create temp dir");
        let legacy = home.path().join(".mempalace");
        std::fs::create_dir_all(&legacy).expect("create legacy dir");

        // Write config.json using serde to avoid hand-rolling JSON with a
        // path that may contain characters requiring escaping.
        let legacy_db = legacy.join("palace.db");
        let config_content = serde_json::to_string(&MempalaceConfig {
            palace_path: legacy_db,
            collection_name: "mempalace_drawers".to_string(),
            people_map: std::collections::HashMap::new(),
            ..MempalaceConfig::default()
        })
        .expect("serialize config");
        std::fs::write(legacy.join("config.json"), &config_content).expect("write config.json");

        temp_env::with_vars(
            [
                ("HOME", Some(home.path().to_str().expect("valid path"))),
                ("MEMPALACE_DIR", None),
                ("XDG_DATA_HOME", None),
            ],
            || {
                maybe_migrate().expect("migration should succeed");

                let destination = home.path().join(".local").join("share").join("mempalace");
                let database_new = destination.join("palace.db");

                let data = std::fs::read_to_string(destination.join("config.json"))
                    .expect("read migrated config.json");
                let config: MempalaceConfig =
                    serde_json::from_str(&data).expect("parse migrated config.json");

                // palace_path must now point to the XDG location, not the legacy one.
                assert_eq!(config.palace_path, database_new);
                assert!(!config.palace_path.to_string_lossy().contains(".mempalace"));
            },
        );
    }

    #[test]
    fn load_returns_default_when_no_config_file() {
        // MempalaceConfig::load() must return defaults when no config.json exists.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for load-default test");
        temp_env::with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(
                        temp_directory
                            .path()
                            .to_str()
                            .expect("temporary directory path must be valid UTF-8"),
                    ),
                ),
                (
                    "HOME",
                    Some(
                        temp_directory
                            .path()
                            .to_str()
                            .expect("temporary directory path must be valid UTF-8"),
                    ),
                ),
            ],
            || {
                let config = MempalaceConfig::load().expect(
                    "MempalaceConfig::load should return defaults when config.json is absent",
                );
                assert!(
                    !config.palace_path.as_os_str().is_empty(),
                    "palace_path must not be empty"
                );
                assert_eq!(config.collection_name, "mempalace_drawers");
            },
        );
    }

    #[test]
    fn load_reads_existing_config_file() {
        // MempalaceConfig::load() must parse an existing config.json.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for load-existing test");
        let config_json =
            r#"{"palace_path":"/custom/palace.db","collection_name":"custom","people_map":{}}"#;
        std::fs::write(temp_directory.path().join("config.json"), config_json)
            .expect("failed to write test config.json");

        temp_env::with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(
                        temp_directory
                            .path()
                            .to_str()
                            .expect("temporary directory path must be valid UTF-8"),
                    ),
                ),
                (
                    "HOME",
                    Some(
                        temp_directory
                            .path()
                            .to_str()
                            .expect("temporary directory path must be valid UTF-8"),
                    ),
                ),
            ],
            || {
                let config = MempalaceConfig::load()
                    .expect("MempalaceConfig::load should succeed when config.json exists");
                assert_eq!(config.collection_name, "custom");
                assert_eq!(
                    config.palace_path,
                    std::path::PathBuf::from("/custom/palace.db")
                );
            },
        );
    }

    #[test]
    fn init_creates_config_file_when_none_exists() {
        // MempalaceConfig::init() must create config.json when the directory is fresh.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for init-creates test");
        temp_env::with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(
                        temp_directory
                            .path()
                            .to_str()
                            .expect("temporary directory path must be valid UTF-8"),
                    ),
                ),
                (
                    "HOME",
                    Some(
                        temp_directory
                            .path()
                            .to_str()
                            .expect("temporary directory path must be valid UTF-8"),
                    ),
                ),
            ],
            || {
                let config = MempalaceConfig::init()
                    .expect("MempalaceConfig::init should succeed on a fresh directory");
                // config.json must have been written.
                assert!(
                    temp_directory.path().join("config.json").exists(),
                    "init must create config.json"
                );
                assert!(
                    !config.palace_path.as_os_str().is_empty(),
                    "palace_path must not be empty after init"
                );
            },
        );
    }

    #[test]
    fn init_reads_existing_config_when_present() {
        // MempalaceConfig::init() must read an existing config.json rather than overwriting it.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for init-reads-existing test");
        let config_json =
            r#"{"palace_path":"/existing/palace.db","collection_name":"existing","people_map":{}}"#;
        std::fs::write(temp_directory.path().join("config.json"), config_json)
            .expect("failed to write existing config.json for init test");

        temp_env::with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(
                        temp_directory
                            .path()
                            .to_str()
                            .expect("temporary directory path must be valid UTF-8"),
                    ),
                ),
                (
                    "HOME",
                    Some(
                        temp_directory
                            .path()
                            .to_str()
                            .expect("temporary directory path must be valid UTF-8"),
                    ),
                ),
            ],
            || {
                let config = MempalaceConfig::init()
                    .expect("MempalaceConfig::init should read existing config.json without error");
                assert_eq!(
                    config.collection_name, "existing",
                    "init must return existing config values"
                );
                assert_eq!(
                    config.palace_path,
                    std::path::PathBuf::from("/existing/palace.db")
                );
            },
        );
    }

    #[test]
    fn palace_db_path_env_var_overrides_config() {
        // MEMPALACE_PALACE_PATH must take priority over the config value.
        let config = MempalaceConfig {
            palace_path: std::path::PathBuf::from("/config/palace.db"),
            collection_name: "mempalace_drawers".to_string(),
            people_map: std::collections::HashMap::new(),
            ..MempalaceConfig::default()
        };
        temp_env::with_var("MEMPALACE_PALACE_PATH", Some("/env/override.db"), || {
            let path = config.palace_db_path();
            assert_eq!(
                path,
                std::path::PathBuf::from("/env/override.db"),
                "env var must override config palace_path"
            );
            assert!(
                !path.to_string_lossy().contains("config"),
                "result must not contain the config path"
            );
        });
    }

    #[test]
    fn project_config_load_wrong_extension_returns_error() {
        // ProjectConfig::load() must reject files that are not .yaml or .yml.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for wrong-extension test");
        let json_path = temp_directory.path().join("config.json");
        std::fs::write(&json_path, "wing: test\nrooms: []")
            .expect("failed to write test file with wrong extension");
        let result = ProjectConfig::load(&json_path);
        assert!(result.is_err(), "non-yaml extension must return Err");
        assert!(
            result.err().is_some_and(
                |error| error.to_string().contains("yaml") || error.to_string().contains("yml")
            ),
            "error must mention the expected extension"
        );
    }

    #[test]
    fn project_config_load_nonexistent_file_returns_error() {
        // ProjectConfig::load() must return Err when the file does not exist.
        let path = std::path::Path::new("/nonexistent/path/mempalace.yaml");
        let result = ProjectConfig::load(path);
        assert!(result.is_err(), "nonexistent file must return Err");
        assert!(
            result.err().is_some(),
            "error must be present for nonexistent file"
        );
    }

    #[test]
    fn maybe_migrate_moves_wal_directory() {
        // maybe_migrate_inner must move a "wal" subdirectory to the destination.
        let home_directory = tempfile::tempdir()
            .expect("failed to create temporary home directory for WAL migration test");
        let legacy_directory = home_directory.path().join(".mempalace");
        let wal_source_directory = legacy_directory.join("wal");
        std::fs::create_dir_all(&wal_source_directory)
            .expect("failed to create legacy wal directory");
        std::fs::write(wal_source_directory.join("frame.bin"), b"wal data")
            .expect("failed to write wal frame file");
        // config.json acts as the completion marker — move it last.
        std::fs::write(
            legacy_directory.join("config.json"),
            r#"{"palace_path":"/old/palace.db","collection_name":"mempalace_drawers","people_map":{}}"#,
        )
        .expect("failed to write config.json for WAL migration test");

        temp_env::with_vars(
            [
                (
                    "HOME",
                    Some(
                        home_directory
                            .path()
                            .to_str()
                            .expect("home directory path must be valid UTF-8"),
                    ),
                ),
                ("MEMPALACE_DIR", None),
                ("XDG_DATA_HOME", None),
            ],
            || {
                maybe_migrate()
                    .expect("migration should succeed for legacy directory with wal subdirectory");
                let destination = home_directory
                    .path()
                    .join(".local")
                    .join("share")
                    .join("mempalace");
                // wal directory must have been moved to the destination.
                assert!(
                    destination.join("wal").exists(),
                    "wal directory must exist at destination after migration"
                );
                assert!(
                    destination.join("wal").join("frame.bin").exists(),
                    "wal frame must be present in migrated wal directory"
                );
                assert!(
                    destination.join("config.json").exists(),
                    "config.json must exist as completion marker"
                );
            },
        );
    }

    #[test]
    fn maybe_migrate_patch_config_corrupted_json_is_noop() {
        // maybe_migrate_patch_config must silently ignore a corrupted config.json.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for corrupted-config test");
        let config_path = temp_directory.path().join("config.json");
        std::fs::write(&config_path, "not valid json {{{{")
            .expect("failed to write corrupted config.json for test");
        let legacy_db = std::path::Path::new("/old/palace.db");
        let database_new = std::path::Path::new("/new/palace.db");

        let result = maybe_migrate_patch_config(&config_path, legacy_db, database_new);

        // Corrupted JSON must be left unchanged (Ok returned, not Err).
        assert!(
            result.is_ok(),
            "corrupted config.json must not cause migration error"
        );
        let content = std::fs::read_to_string(&config_path).expect("config must still be readable");
        assert_eq!(
            content, "not valid json {{{{",
            "corrupted config.json must be left unchanged"
        );
    }

    #[test]
    fn project_config_yaml_round_trip() {
        let yaml = r"
wing: my_project
rooms:
  - name: backend
    description: Server code
    keywords:
      - api
      - server
  - name: frontend
    description: UI code
    keywords: []
";
        let config: ProjectConfig = serde_yaml::from_str(yaml).expect("parse yaml");
        assert_eq!(config.wing, "my_project");
        assert_eq!(config.rooms.len(), 2);
        assert_eq!(config.rooms[0].name, "backend");
        assert!(config.rooms[0].keywords.contains(&"api".to_string()));

        // Serialize back and deserialize to verify round-trip.
        let serialized = serde_yaml::to_string(&config).expect("serialize yaml");
        let config_roundtrip: ProjectConfig =
            serde_yaml::from_str(&serialized).expect("parse roundtrip yaml");
        assert_eq!(config.wing, config_roundtrip.wing);
        assert_eq!(config.rooms.len(), config_roundtrip.rooms.len());
        for (orig, rt) in config.rooms.iter().zip(config_roundtrip.rooms.iter()) {
            assert_eq!(orig.name, rt.name);
            assert_eq!(orig.description, rt.description);
            assert_eq!(orig.keywords, rt.keywords);
        }
    }

    // -- expand_tilde ---------------------------------------------------------

    #[test]
    fn expand_tilde_no_leading_tilde_returns_path_unchanged() {
        // A path with no leading ~ must be returned as-is.
        let path = std::path::Path::new("/absolute/path/to/file");
        let result = expand_tilde(path);
        assert_eq!(result, path, "absolute path must be returned unchanged");
        assert!(
            !result.to_string_lossy().starts_with("~/"),
            "result must not start with ~/"
        );
    }

    #[test]
    fn expand_tilde_relative_path_returned_unchanged() {
        // A relative path that does not start with ~ must be returned as-is.
        let path = std::path::Path::new("relative/path");
        let result = expand_tilde(path);
        assert_eq!(
            result, path,
            "relative path without ~ must be returned unchanged"
        );
        assert_eq!(result.to_string_lossy(), "relative/path");
    }

    #[test]
    fn expand_tilde_tilde_only_expands_to_home() {
        // A path of just "~" must expand to the HOME directory.
        temp_env::with_var("HOME", Some("/test/home"), || {
            let path = std::path::Path::new("~");
            let result = expand_tilde(path);
            assert_eq!(
                result,
                std::path::Path::new("/test/home"),
                "bare ~ must expand to HOME"
            );
            assert!(
                !result.to_string_lossy().contains('~'),
                "result must not contain ~"
            );
        });
    }

    #[test]
    fn expand_tilde_tilde_slash_path_appends_suffix() {
        // "~/foo/bar" must expand to "<HOME>/foo/bar".
        temp_env::with_var("HOME", Some("/test/home"), || {
            let path = std::path::Path::new("~/foo/bar");
            let result = expand_tilde(path);
            assert_eq!(
                result,
                std::path::Path::new("/test/home/foo/bar"),
                "~/foo/bar must expand to HOME/foo/bar"
            );
            assert!(
                result.starts_with("/test/home"),
                "result must start with HOME"
            );
        });
    }

    #[test]
    fn expand_tilde_no_home_set_returns_path_unchanged() {
        // When HOME is unset expand_tilde must return the path unchanged rather than panicking.
        // This covers the None branch of the home directory resolution chain.
        temp_env::with_vars(
            [
                ("HOME", None::<&str>),
                ("USERPROFILE", None::<&str>),
                ("HOMEDRIVE", None::<&str>),
                ("HOMEPATH", None::<&str>),
            ],
            || {
                let path = std::path::Path::new("~/no/home");
                let result = expand_tilde(path);
                // With no home env vars the expansion falls back to returning path as-is.
                assert_eq!(
                    result, path,
                    "expand_tilde must return the original path unchanged when HOME is unresolvable"
                );
                assert!(
                    !result.is_absolute(),
                    "result must remain a relative path when home is unresolvable"
                );
            },
        );
    }

    // -- palace_db_path normalization -----------------------------------------

    #[test]
    fn palace_db_path_env_var_expands_tilde() {
        // MEMPALACE_PALACE_PATH starting with ~ must be expanded to the home directory.
        let config = MempalaceConfig {
            palace_path: PathBuf::from("/config/palace.db"),
            collection_name: "mempalace_drawers".to_string(),
            people_map: std::collections::HashMap::new(),
            ..MempalaceConfig::default()
        };
        temp_env::with_vars(
            [
                ("MEMPALACE_PALACE_PATH", Some("~/palace.db")),
                ("HOME", Some("/myhome")),
            ],
            || {
                let path = config.palace_db_path();
                assert!(
                    path.starts_with("/myhome"),
                    "tilde must be expanded to HOME in MEMPALACE_PALACE_PATH"
                );
                assert!(
                    !path.to_string_lossy().contains('~'),
                    "result must not contain a literal tilde"
                );
            },
        );
    }

    #[test]
    fn palace_db_path_env_var_absolute_path_is_returned_as_is() {
        // An absolute MEMPALACE_PALACE_PATH must be returned unchanged (no ~ to expand).
        let config = MempalaceConfig {
            palace_path: PathBuf::from("/config/palace.db"),
            collection_name: "mempalace_drawers".to_string(),
            people_map: std::collections::HashMap::new(),
            ..MempalaceConfig::default()
        };
        temp_env::with_var(
            "MEMPALACE_PALACE_PATH",
            Some("/absolute/path/palace.db"),
            || {
                let path = config.palace_db_path();
                assert!(path.is_absolute(), "absolute env path must remain absolute");
                assert!(path.ends_with("palace.db"), "filename must be preserved");
            },
        );
    }

    #[test]
    fn palace_db_path_env_var_not_set_uses_config_value() {
        // When MEMPALACE_PALACE_PATH is absent the config field must be returned.
        let config = MempalaceConfig {
            palace_path: PathBuf::from("/config/palace.db"),
            collection_name: "mempalace_drawers".to_string(),
            people_map: std::collections::HashMap::new(),
            ..MempalaceConfig::default()
        };
        temp_env::with_var("MEMPALACE_PALACE_PATH", None::<&str>, || {
            let path = config.palace_db_path();
            assert_eq!(
                path,
                PathBuf::from("/config/palace.db"),
                "config palace_path must be returned when env var is absent"
            );
            assert!(path.is_absolute(), "config path must be absolute");
        });
    }

    #[test]
    fn config_dir_respects_mempal_dir_alias() {
        // MEMPAL_DIR (short alias) must resolve the same as MEMPALACE_DIR.
        let tempdir = tempfile::tempdir().expect("failed to create temp dir");
        temp_env::with_vars(
            [
                ("MEMPALACE_DIR", None::<&str>),
                (
                    "MEMPAL_DIR",
                    Some(tempdir.path().to_str().expect("valid path")),
                ),
            ],
            || {
                assert_eq!(config_dir(), tempdir.path());
            },
        );
    }

    #[test]
    fn mempalace_dir_takes_priority_over_mempal_dir_alias() {
        // MEMPALACE_DIR must win over MEMPAL_DIR when both are set.
        let primary = tempfile::tempdir().expect("failed to create temp dir");
        let alias = tempfile::tempdir().expect("failed to create temp dir");
        temp_env::with_vars(
            [
                (
                    "MEMPALACE_DIR",
                    Some(primary.path().to_str().expect("valid path")),
                ),
                (
                    "MEMPAL_DIR",
                    Some(alias.path().to_str().expect("valid path")),
                ),
            ],
            || {
                assert_eq!(config_dir(), primary.path());
                assert_ne!(config_dir(), alias.path());
            },
        );
    }

    #[test]
    fn palace_db_path_mempal_alias_overrides_config() {
        // MEMPAL_PALACE_PATH (short alias) must override the config field.
        let config = MempalaceConfig {
            palace_path: PathBuf::from("/config/palace.db"),
            collection_name: "mempalace_drawers".to_string(),
            people_map: std::collections::HashMap::new(),
            ..MempalaceConfig::default()
        };
        temp_env::with_vars(
            [
                ("MEMPALACE_PALACE_PATH", None::<&str>),
                ("MEMPAL_PALACE_PATH", Some("/alias/override.db")),
            ],
            || {
                let path = config.palace_db_path();
                assert_eq!(path, PathBuf::from("/alias/override.db"));
                assert!(!path.to_string_lossy().contains("config"));
            },
        );
    }

    #[test]
    fn mempalace_palace_path_takes_priority_over_mempal_alias() {
        // MEMPALACE_PALACE_PATH must win over MEMPAL_PALACE_PATH when both are set.
        let config = MempalaceConfig {
            palace_path: PathBuf::from("/config/palace.db"),
            collection_name: "mempalace_drawers".to_string(),
            people_map: std::collections::HashMap::new(),
            ..MempalaceConfig::default()
        };
        temp_env::with_vars(
            [
                ("MEMPALACE_PALACE_PATH", Some("/primary/palace.db")),
                ("MEMPAL_PALACE_PATH", Some("/alias/palace.db")),
            ],
            || {
                let path = config.palace_db_path();
                assert_eq!(path, PathBuf::from("/primary/palace.db"));
            },
        );
    }

    #[test]
    fn default_config_has_entity_languages_en() {
        // Default config must contain exactly ["en"] as entity_languages.
        let config = MempalaceConfig::default();
        assert_eq!(config.entity_languages, vec!["en".to_string()]);
        assert!(!config.entity_languages.is_empty());
    }

    #[test]
    fn entity_languages_round_trips_via_json() {
        // entity_languages must survive a JSON serialize/deserialize round-trip.
        let json = r#"{"palace_path":"/tmp/p.db","collection_name":"x","people_map":{},"entity_languages":["en","de"]}"#;
        let config: MempalaceConfig = serde_json::from_str(json).expect("parse json");
        assert_eq!(config.entity_languages, vec!["en", "de"]);
        assert_eq!(config.entity_languages.len(), 2);
    }

    #[test]
    fn entity_languages_defaults_when_absent_from_json() {
        // JSON without entity_languages must deserialize to the default ["en"].
        let json = r#"{"palace_path":"/tmp/p.db","collection_name":"x","people_map":{}}"#;
        let config: MempalaceConfig = serde_json::from_str(json).expect("parse json");
        assert_eq!(config.entity_languages, vec!["en".to_string()]);
        assert!(!config.entity_languages.is_empty());
    }

    // -- normalize_wing_name --------------------------------------------------

    #[test]
    fn normalize_wing_name_lowercases() {
        // Uppercase letters must be folded to lowercase.
        assert_eq!(normalize_wing_name("MyProject"), "myproject");
        assert_eq!(normalize_wing_name("SCREAMING"), "screaming");
    }

    #[test]
    fn normalize_wing_name_replaces_hyphens_with_underscores() {
        // Hyphens must become underscores — the canonical rule from Python.
        assert_eq!(normalize_wing_name("mempalace-rs"), "mempalace_rs");
        assert_eq!(normalize_wing_name("my-cool-project"), "my_cool_project");
    }

    #[test]
    fn normalize_wing_name_replaces_spaces_with_underscores() {
        // Spaces must become underscores.
        assert_eq!(normalize_wing_name("my project"), "my_project");
        assert_eq!(normalize_wing_name("hello world"), "hello_world");
    }

    #[test]
    fn normalize_wing_name_mixed_input() {
        // Mixed case, hyphens, and spaces all normalized in one pass.
        let result = normalize_wing_name("My-Cool Project");
        assert_eq!(result, "my_cool_project");
        assert!(!result.contains('-'));
        assert!(!result.contains(' '));
    }

    #[test]
    fn normalize_wing_name_already_normalized_is_idempotent() {
        // An already-slugged name must be returned unchanged.
        let slug = "my_project";
        assert_eq!(normalize_wing_name(slug), slug);
    }

    #[test]
    fn migrate_skipped_when_mempal_dir_set() {
        // When MEMPAL_DIR (alias) is set, migration must be skipped — same behaviour as MEMPALACE_DIR.
        let home = tempfile::tempdir().expect("failed to create temp dir");
        let legacy = home.path().join(".mempalace");
        std::fs::create_dir_all(&legacy).expect("create legacy dir");
        std::fs::write(legacy.join("config.json"), "{}").expect("write config.json");

        let override_dir = tempfile::tempdir().expect("create override dir");
        temp_env::with_vars(
            [
                ("HOME", Some(home.path().to_str().expect("valid path"))),
                ("MEMPALACE_DIR", None),
                (
                    "MEMPAL_DIR",
                    Some(override_dir.path().to_str().expect("valid path")),
                ),
                ("XDG_DATA_HOME", None),
            ],
            || {
                maybe_migrate().expect("should return ok without migrating");
                // Legacy dir must still exist — migration was skipped due to MEMPAL_DIR.
                assert!(legacy.join("config.json").exists());
            },
        );
    }

    #[test]
    fn palace_db_path_env_var_empty_falls_back_to_config() {
        // An empty MEMPALACE_PALACE_PATH must not panic — it must fall through to
        // the configured palace_path. Shells commonly export empty vars when users
        // clear them (e.g. `export MEMPALACE_PALACE_PATH=`).
        let config = MempalaceConfig {
            palace_path: PathBuf::from("/config/palace.db"),
            collection_name: "mempalace_drawers".to_string(),
            people_map: std::collections::HashMap::new(),
            ..MempalaceConfig::default()
        };
        temp_env::with_var("MEMPALACE_PALACE_PATH", Some(""), || {
            let path = config.palace_db_path();
            assert_eq!(
                path,
                PathBuf::from("/config/palace.db"),
                "empty env var must fall back to config palace_path"
            );
        });
        // Pair assertion: whitespace-only also falls back.
        temp_env::with_var("MEMPALACE_PALACE_PATH", Some("   "), || {
            let path = config.palace_db_path();
            assert_eq!(
                path,
                PathBuf::from("/config/palace.db"),
                "whitespace-only env var must fall back to config palace_path"
            );
        });
    }
}
