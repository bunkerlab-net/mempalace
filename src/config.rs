//! Configuration loading for global (`~/.mempalace/config.json`) and per-project (`mempalace.yaml`) settings.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Global mempalace configuration (`~/.mempalace/config.json`).
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

// Override for the config directory. Set once by `test_helpers::test_db()`
// before any tool writes, redirecting WAL and config writes away from the real
// `~/.mempalace` during test runs. The `OnceLock` makes this safe for parallel
// test execution: whichever test sets it first wins, and all subsequent calls
// to `config_dir()` see the same override.
static CONFIG_DIR_OVERRIDE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Override the config directory returned by `config_dir`.
///
/// Intended for test setup only. Must be called before any code that uses
/// `config_dir()`. Subsequent calls are silently ignored (`OnceLock` semantics).
// Called from test_helpers::redirect_config_dir() in the library target.
// The binary compiles test_helpers only under #[cfg(test)], so the binary
// linker sees this as unused — suppress rather than remove it.
#[allow(dead_code)]
pub fn set_config_dir_override(path: PathBuf) {
    let _ = CONFIG_DIR_OVERRIDE.set(path);
}

/// Returns the mempalace config directory (`~/.mempalace` by default).
///
/// If `set_config_dir_override` has been called (e.g. by test setup), that
/// path is returned instead.
pub fn config_dir() -> PathBuf {
    if let Some(path) = CONFIG_DIR_OVERRIDE.get() {
        return path.clone();
    }
    dirs_fallback().join(".mempalace")
}

/// Returns the user's home directory.
fn dirs_fallback() -> PathBuf {
    std::env::var("HOME").map_or_else(|_| PathBuf::from("."), PathBuf::from)
}

/// Path to the global config file.
pub fn config_path() -> PathBuf {
    config_dir().join("config.json")
}

impl MempalaceConfig {
    /// Load config from ~/.mempalace/config.json, or return defaults.
    pub fn load() -> Result<Self> {
        let path = config_path();
        if path.exists() {
            let data = std::fs::read_to_string(&path)?;
            let config: Self = serde_json::from_str(&data)?;
            Ok(config)
        } else {
            Ok(Self::default())
        }
    }

    /// Ensure the config directory and default config exist.
    pub fn init() -> Result<Self> {
        let directory = config_dir();
        std::fs::create_dir_all(&directory)?;

        let path = config_path();
        if path.exists() {
            Self::load()
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
        let config = MempalaceConfig::default();
        let path_str = config.palace_path.to_string_lossy();
        assert!(path_str.contains(".mempalace"));
        assert!(path_str.ends_with("palace.db"));
    }

    #[test]
    fn config_dir_ends_with_mempalace() {
        // Test the path formula directly rather than the public `config_dir()`,
        // which may return an override path set by test infrastructure.
        let directory = dirs_fallback().join(".mempalace");
        assert!(directory.to_string_lossy().ends_with(".mempalace"));
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
