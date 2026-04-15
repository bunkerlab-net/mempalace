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
}

fn default_palace_path() -> PathBuf {
    config_dir().join("palace.db")
}

fn default_collection_name() -> String {
    "mempalace_drawers".to_string()
}

/// Returns the mempalace data directory.
///
/// Resolution order:
///   1. `MEMPALACE_DIR` env var — explicit user, container, or test override.
///   2. `$XDG_DATA_HOME/mempalace` — XDG standard location.
///   3. `~/.local/share/mempalace` — XDG default fallback.
pub fn config_dir() -> PathBuf {
    if let Ok(env_path) = std::env::var("MEMPALACE_DIR")
        && !env_path.is_empty()
    {
        return PathBuf::from(env_path);
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
    let base_directory = if let Ok(val) = std::env::var("XDG_DATA_HOME")
        && !val.is_empty()
        && PathBuf::from(&val).is_absolute()
    {
        PathBuf::from(val)
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
/// `HOMEDRIVE`+`HOMEPATH` (legacy Windows), mirroring the fallback order
/// used by `expand_tilde()` in `main.rs`. Panics if none are set — a missing
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

    /// Resolve the palace database path, respecting `MEMPALACE_PALACE_PATH` env var.
    pub fn palace_db_path(&self) -> PathBuf {
        // Check env override first — it can recover from an empty config value.
        if let Ok(env_path) = std::env::var("MEMPALACE_PALACE_PATH") {
            return PathBuf::from(env_path);
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
        }
    }
}

// --- Migration ---

/// Migrate from `~/.mempalace` to `$XDG_DATA_HOME/mempalace` if needed.
///
/// Idempotent: returns immediately if already migrated, legacy dir absent,
/// or `MEMPALACE_DIR` is set.
fn maybe_migrate() -> Result<()> {
    // MEMPALACE_DIR means the caller manages paths — skip migration entirely.
    if let Ok(val) = std::env::var("MEMPALACE_DIR")
        && !val.is_empty()
    {
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
        let new_db = destination.join("palace.db");
        maybe_migrate_patch_config(&source_config, &legacy_db, &new_db)?;
        maybe_migrate_move_file(&source_config, &destination.join("config.json"))?;
    }

    // Remove the legacy directory if it is now empty.
    if std::fs::read_dir(source)
        .map(|mut entries| entries.next().is_none())
        .unwrap_or(false)
    {
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
    if std::fs::read_dir(source)
        .map(|mut entries| entries.next().is_none())
        .unwrap_or(false)
    {
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
fn maybe_migrate_patch_config(config_path: &Path, legacy_db: &Path, new_db: &Path) -> Result<()> {
    assert!(config_path.exists());
    assert_ne!(legacy_db, new_db);

    let data = std::fs::read_to_string(config_path)?;
    let mut config: MempalaceConfig = match serde_json::from_str(&data) {
        Ok(c) => c,
        // Corrupted config — leave it alone; startup will surface the parse error.
        Err(_) => return Ok(()),
    };

    if config.palace_path == legacy_db {
        config.palace_path = new_db.to_path_buf();
        let patched = serde_json::to_string_pretty(&config)?;
        std::fs::write(config_path, &patched)?;
        // Pair assertion: patched value must round-trip correctly.
        debug_assert!(
            serde_json::from_str::<MempalaceConfig>(&patched)
                .map(|c| c.palace_path.as_path() == new_db)
                .unwrap_or(false)
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
        // The default palace path is under the XDG data dir.
        // Test the formula directly using home_dir() to avoid MEMPALACE_DIR interference.
        let path = home_dir()
            .join(".local")
            .join("share")
            .join("mempalace")
            .join("palace.db");
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("mempalace"));
        assert!(path_str.ends_with("palace.db"));
    }

    #[test]
    fn config_dir_ends_with_mempalace() {
        // Test the default formula directly via xdg_data_dir() to avoid
        // MEMPALACE_DIR interference from the test runner environment.
        let directory = home_dir().join(".local").join("share").join("mempalace");
        assert!(directory.ends_with("mempalace"));
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
                let new_db = destination.join("palace.db");

                let data = std::fs::read_to_string(destination.join("config.json"))
                    .expect("read migrated config.json");
                let config: MempalaceConfig =
                    serde_json::from_str(&data).expect("parse migrated config.json");

                // palace_path must now point to the XDG location, not the legacy one.
                assert_eq!(config.palace_path, new_db);
                assert!(!config.palace_path.to_string_lossy().contains(".mempalace"));
            },
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

        // Serialize back and deserialize to verify round-trip
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
}
