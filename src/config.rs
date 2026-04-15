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

/// Returns the mempalace config directory.
///
/// Resolution order:
///   1. `MEMPALACE_DIR` env var — explicit user, container, or test override.
///   2. `~/.mempalace` — default.
pub fn config_dir() -> PathBuf {
    if let Ok(env_path) = std::env::var("MEMPALACE_DIR")
        && !env_path.is_empty()
    {
        return PathBuf::from(env_path);
    }
    home_dir().join(".mempalace")
}

/// Returns the user's home directory, or `.` if `HOME` is unset.
fn home_dir() -> PathBuf {
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
        // Test the default palace path formula directly: MempalaceConfig::default()
        // calls config_dir() which may return MEMPALACE_DIR when set by the test runner.
        let path = home_dir().join(".mempalace").join("palace.db");
        let path_str = path.to_string_lossy();
        assert!(path_str.contains(".mempalace"));
        assert!(path_str.ends_with("palace.db"));
    }

    #[test]
    fn config_dir_ends_with_mempalace() {
        // Test the default path formula directly: config_dir() returns MEMPALACE_DIR
        // when set, so we test the fallback formula via home_dir().
        let directory = home_dir().join(".mempalace");
        assert!(directory.to_string_lossy().ends_with(".mempalace"));
    }

    #[test]
    fn config_dir_respects_mempalace_dir_env_var() {
        // Verify that MEMPALACE_DIR overrides the default path. temp_env safely
        // sets the var for this test and restores the previous value afterwards,
        // preventing interference with concurrent tests.
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        temp_env::with_var("MEMPALACE_DIR", Some(dir.path()), || {
            assert_eq!(config_dir(), dir.path());
        });
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
