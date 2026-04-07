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

/// Returns ~/.mempalace/
pub fn config_dir() -> PathBuf {
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
            let cfg: Self = serde_json::from_str(&data)?;
            Ok(cfg)
        } else {
            Ok(Self::default())
        }
    }

    /// Ensure the config directory and default config exist.
    pub fn init() -> Result<Self> {
        let dir = config_dir();
        std::fs::create_dir_all(&dir)?;

        let path = config_path();
        if path.exists() {
            Self::load()
        } else {
            let cfg = Self::default();
            let data = serde_json::to_string_pretty(&cfg)?;
            std::fs::write(&path, data)?;
            Ok(cfg)
        }
    }

    /// Resolve the palace database path, respecting `MEMPALACE_PALACE_PATH` env var.
    pub fn palace_db_path(&self) -> PathBuf {
        if let Ok(env_path) = std::env::var("MEMPALACE_PALACE_PATH") {
            return PathBuf::from(env_path);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_palace_path() {
        let cfg = MempalaceConfig::default();
        let path_str = cfg.palace_path.to_string_lossy();
        assert!(path_str.contains(".mempalace"));
        assert!(path_str.ends_with("palace.db"));
    }

    #[test]
    fn config_dir_ends_with_mempalace() {
        let dir = config_dir();
        assert!(dir.to_string_lossy().ends_with(".mempalace"));
    }

    #[test]
    fn project_config_yaml_round_trip() {
        let yaml = r#"
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
"#;
        let cfg: ProjectConfig = serde_yaml::from_str(yaml).expect("parse yaml");
        assert_eq!(cfg.wing, "my_project");
        assert_eq!(cfg.rooms.len(), 2);
        assert_eq!(cfg.rooms[0].name, "backend");
        assert!(cfg.rooms[0].keywords.contains(&"api".to_string()));

        // Serialize back and deserialize to verify round-trip
        let serialized = serde_yaml::to_string(&cfg).expect("serialize yaml");
        let cfg_roundtrip: ProjectConfig =
            serde_yaml::from_str(&serialized).expect("parse roundtrip yaml");
        assert_eq!(cfg.wing, cfg_roundtrip.wing);
        assert_eq!(cfg.rooms.len(), cfg_roundtrip.rooms.len());
        for (orig, rt) in cfg.rooms.iter().zip(cfg_roundtrip.rooms.iter()) {
            assert_eq!(orig.name, rt.name);
            assert_eq!(orig.description, rt.description);
            assert_eq!(orig.keywords, rt.keywords);
        }
    }
}

impl ProjectConfig {
    /// Load from a mempalace.yaml file.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(Error::ConfigNotFound(path.to_path_buf()));
        }
        let data = std::fs::read_to_string(path)?;
        let cfg: Self = serde_yaml::from_str(&data)?;
        Ok(cfg)
    }
}
