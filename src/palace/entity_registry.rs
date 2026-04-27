//! Structured entity registry — `entity_registry.json` in mempalace config dir.
//!
//! Ports `entity_registry.py` from the Python reference implementation.
//! Tracks people, projects, and ambiguous-word flags in a single JSON file.
//!
//! Sources in priority order:
//!   1. Onboarding — explicit user entries (confidence 1.0).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::config::config_dir;
use crate::error::Result;

/// Confidence assigned to onboarding-sourced people and projects.
const CONFIDENCE_ONBOARDING: f64 = 1.0;

const _: () = assert!(CONFIDENCE_ONBOARDING > 0.0);

/// Words that are simultaneously common English words and personal names.
///
/// Names in this set are recorded as `ambiguous_flags` and require context
/// pattern matching before being resolved as a person.
static COMMON_ENGLISH_WORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "ever",
        "grace",
        "will",
        "bill",
        "mark",
        "april",
        "may",
        "june",
        "joy",
        "hope",
        "faith",
        "chance",
        "chase",
        "hunter",
        "dash",
        "flash",
        "star",
        "sky",
        "river",
        "brook",
        "lane",
        "art",
        "clay",
        "gil",
        "nat",
        "max",
        "rex",
        "ray",
        "jay",
        "rose",
        "violet",
        "lily",
        "ivy",
        "ash",
        "reed",
        "sage",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "january",
        "february",
        "march",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]
    .into_iter()
    .collect()
});

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

/// A person entry stored in the registry's `people` map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonEntry {
    pub source: String,
    #[serde(default)]
    pub contexts: Vec<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub relationship: String,
    pub confidence: f64,
    #[serde(default)]
    pub seen_count: u64,
    /// Canonical name when this entry is an alias (e.g. `"Maxwell"` → `"Max"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical: Option<String>,
}

/// Input record for [`EntityRegistry::seed`] — one person from onboarding.
pub struct SeedPerson {
    pub name: String,
    pub relationship: String,
    pub context: String,
    /// Optional shorter name / nickname (e.g. `"Max"` for `"Maxwell"`).
    pub nickname: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal serde target
// ─────────────────────────────────────────────────────────────────────────────

/// On-disk JSON representation of the entity registry.
#[derive(Debug, Serialize, Deserialize)]
struct RegistryData {
    #[serde(default = "registry_data_default_version")]
    version: u32,
    #[serde(default = "registry_data_default_mode")]
    mode: String,
    #[serde(default)]
    people: HashMap<String, PersonEntry>,
    #[serde(default)]
    projects: Vec<String>,
    #[serde(default)]
    ambiguous_flags: Vec<String>,
}

/// Returns the default version number for a freshly constructed `RegistryData`.
fn registry_data_default_version() -> u32 {
    1
}

/// Returns the default mode string for a freshly constructed `RegistryData`.
fn registry_data_default_mode() -> String {
    "personal".to_string()
}

impl Default for RegistryData {
    /// Constructs an empty registry with version 1 and mode `"personal"`.
    fn default() -> Self {
        Self {
            version: registry_data_default_version(),
            mode: registry_data_default_mode(),
            people: HashMap::new(),
            projects: Vec::new(),
            ambiguous_flags: Vec::new(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityRegistry — main public type
// ─────────────────────────────────────────────────────────────────────────────

/// Structured personal entity registry persisted at `entity_registry.json`.
///
/// Knows the difference between "Riley" (a person) and "ever" (an adverb).
/// Sources in priority order: onboarding → learned.
pub struct EntityRegistry {
    data: RegistryData,
    path: PathBuf,
}

// ─────────────────────────────────────────────────────────────────────────────
// Public free functions
// ─────────────────────────────────────────────────────────────────────────────

/// Returns the path to `entity_registry.json` in the mempalace config directory.
///
/// Respects `MEMPALACE_DIR` and XDG conventions via [`config_dir`].
pub fn entity_registry_path() -> PathBuf {
    let path = config_dir().join("entity_registry.json");
    assert!(!path.as_os_str().is_empty());
    assert!(path.ends_with("entity_registry.json"));
    path
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityRegistry — public impl
// ─────────────────────────────────────────────────────────────────────────────

impl EntityRegistry {
    /// Load the registry from disk.
    ///
    /// Returns a fresh empty registry when the file is absent, unreadable, or
    /// contains invalid JSON. No error is propagated.
    pub fn load() -> Self {
        let path = entity_registry_path();
        assert!(!path.as_os_str().is_empty());
        let data = entity_registry_load_data(&path);
        assert!(data.version > 0, "registry version must be positive");
        Self { data, path }
    }

    /// Seed the registry from onboarding data and persist to disk.
    ///
    /// Clears existing `people` and `projects`; ambiguous flags are rebuilt
    /// from the new people set.
    pub fn seed(&mut self, mode: &str, people: &[SeedPerson], projects: &[String]) -> Result<()> {
        assert!(!mode.is_empty(), "seed: mode must not be empty");
        // Sanity bound — realistic onboarding produces far fewer entries.
        assert!(
            people.len() <= 10_000,
            "seed: people list is unreasonably large"
        );

        self.data.mode = mode.to_string();
        self.data.projects = projects.to_vec();
        self.data.people.clear();

        for person in people {
            let name = person.name.trim();
            if name.is_empty() {
                continue;
            }
            self.seed_person_entry(name, person);
            if let Some(nickname) = person.nickname.as_deref() {
                let nick = nickname.trim();
                if !nick.is_empty() && nick != name {
                    self.seed_nickname_entry(nick, name, person);
                }
            }
        }

        self.seed_ambiguous_flags();

        // Pair assertion: every non-empty person name must appear in the registry.
        debug_assert!(
            people
                .iter()
                .filter(|person| !person.name.trim().is_empty())
                .all(|person| self.data.people.contains_key(person.name.trim())),
            "seed: all input people must be in registry after seed"
        );

        self.save()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityRegistry — private helpers (all named seed_*, etc.)
// ─────────────────────────────────────────────────────────────────────────────

impl EntityRegistry {
    /// Persist the registry data to disk.
    ///
    /// Creates parent directories as needed. Applies `chmod 0o600` on Unix
    /// so only the current user can read or write the file.
    fn save(&self) -> Result<()> {
        assert!(
            !self.path.as_os_str().is_empty(),
            "save: path must not be empty"
        );

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(&self.data)?;
        std::fs::write(&self.path, json.as_bytes())?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600));
        }

        // Pair assertion: registry must exist on disk after write.
        debug_assert!(self.path.exists(), "save: registry must exist after write");
        Ok(())
    }

    /// Insert one canonical `PersonEntry` from onboarding data.
    ///
    /// Called for each non-empty person in [`EntityRegistry::seed`].
    fn seed_person_entry(&mut self, name: &str, person: &SeedPerson) {
        assert!(
            !name.is_empty(),
            "seed_person_entry: name must not be empty"
        );

        let ctx = if person.context.is_empty() {
            "personal".to_string()
        } else {
            person.context.clone()
        };
        let aliases = person
            .nickname
            .as_deref()
            .filter(|nick| {
                let nick = nick.trim();
                !nick.is_empty() && nick != name
            })
            .map(|nick| vec![nick.to_string()])
            .unwrap_or_default();

        self.data.people.insert(
            name.to_string(),
            PersonEntry {
                source: "onboarding".to_string(),
                contexts: vec![ctx],
                aliases,
                relationship: person.relationship.clone(),
                confidence: CONFIDENCE_ONBOARDING,
                seen_count: 0,
                canonical: None,
            },
        );

        // Pair assertion: the name must be present immediately after insert.
        debug_assert!(self.data.people.contains_key(name));
    }

    /// Insert an alias `PersonEntry` pointing back to `canonical`.
    ///
    /// Called when a [`SeedPerson`] has a non-empty nickname. The alias entry
    /// carries `canonical: Some(canonical.to_string())` so lookups can resolve
    /// aliases to their canonical form.
    fn seed_nickname_entry(&mut self, nickname: &str, canonical: &str, person: &SeedPerson) {
        assert!(
            !nickname.is_empty(),
            "seed_nickname_entry: nickname must not be empty"
        );
        assert!(
            !canonical.is_empty(),
            "seed_nickname_entry: canonical must not be empty"
        );

        let ctx = if person.context.is_empty() {
            "personal".to_string()
        } else {
            person.context.clone()
        };
        self.data.people.insert(
            nickname.to_string(),
            PersonEntry {
                source: "onboarding".to_string(),
                contexts: vec![ctx],
                aliases: vec![canonical.to_string()],
                relationship: person.relationship.clone(),
                confidence: CONFIDENCE_ONBOARDING,
                seen_count: 0,
                canonical: Some(canonical.to_string()),
            },
        );
    }

    /// Rebuild `ambiguous_flags` from the current `people` map.
    ///
    /// A name is ambiguous when its lowercase form appears in `COMMON_ENGLISH_WORDS`.
    /// Called at the end of [`EntityRegistry::seed`].
    fn seed_ambiguous_flags(&mut self) {
        let flags: Vec<String> = self
            .data
            .people
            .keys()
            .filter(|name| COMMON_ENGLISH_WORDS.contains(name.to_lowercase().as_str()))
            .map(|name| name.to_lowercase())
            .collect();

        // Flags must be a subset of (lower-cased) people keys.
        assert!(
            flags.len() <= self.data.people.len(),
            "seed_ambiguous_flags: flags count must not exceed people count"
        );
        self.data.ambiguous_flags = flags;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Module-level private helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Load registry data from `path`, falling back to defaults on any error.
///
/// Returns `RegistryData::default()` when the file is absent, unreadable, or
/// malformed. Never propagates an error.
fn entity_registry_load_data(path: &Path) -> RegistryData {
    assert!(!path.as_os_str().is_empty());
    // Negative space: path must point to a file or not exist, not a directory.
    assert!(!path.is_dir());

    if !path.exists() {
        return RegistryData::default();
    }
    let Ok(text) = std::fs::read_to_string(path) else {
        return RegistryData::default();
    };
    let Ok(data) = serde_json::from_str::<RegistryData>(&text) else {
        return RegistryData::default();
    };
    data
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Create a fresh registry backed by a temporary directory.
    fn test_registry() -> (tempfile::TempDir, EntityRegistry) {
        let temp = tempfile::tempdir().expect("tempdir");
        let registry = temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), EntityRegistry::load);
        (temp, registry)
    }

    // ── entity_registry_path ──────────────────────────────────────────────────

    #[test]
    fn path_ends_with_entity_registry_json() {
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let path = entity_registry_path();
            assert!(path.ends_with("entity_registry.json"));
            assert!(!path.as_os_str().is_empty());
        });
    }

    // ── EntityRegistry::load ─────────────────────────────────────────────────

    #[test]
    fn load_returns_empty_registry_when_file_absent() {
        let (_temp, registry) = test_registry();
        assert!(
            registry.data.people.is_empty(),
            "fresh registry must have no people"
        );
        assert!(
            registry.data.projects.is_empty(),
            "fresh registry must have no projects"
        );
    }

    #[test]
    fn load_returns_empty_registry_on_corrupt_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("entity_registry.json");
        std::fs::write(&path, b"{ not valid json").expect("write corrupt file");
        let registry = temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), EntityRegistry::load);
        assert!(
            registry.data.people.is_empty(),
            "corrupt file must produce empty registry"
        );
    }

    // ── seed ─────────────────────────────────────────────────────────────────

    #[test]
    fn seed_populates_people_and_projects() {
        let (temp, mut registry) = test_registry();
        let people = vec![SeedPerson {
            name: "Alice".to_string(),
            relationship: "friend".to_string(),
            context: "personal".to_string(),
            nickname: None,
        }];
        let projects = vec!["MemPalace".to_string()];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry
                .seed("personal", &people, &projects)
                .expect("seed must succeed");
        });
        assert!(
            registry.data.people.contains_key("Alice"),
            "Alice must be in registry"
        );
        assert_eq!(registry.data.projects, vec!["MemPalace"]);
    }

    #[test]
    fn seed_registers_nickname_as_alias_entry() {
        let (temp, mut registry) = test_registry();
        let people = vec![SeedPerson {
            name: "Maxwell".to_string(),
            relationship: "colleague".to_string(),
            context: "work".to_string(),
            nickname: Some("Max".to_string()),
        }];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry
                .seed("work", &people, &[])
                .expect("seed must succeed");
        });
        assert!(
            registry.data.people.contains_key("Maxwell"),
            "Maxwell must be present"
        );
        assert!(
            registry.data.people.contains_key("Max"),
            "Max alias must be present"
        );
        assert_eq!(
            registry.data.people["Max"].canonical,
            Some("Maxwell".to_string()),
            "Max must point to Maxwell"
        );
    }

    #[test]
    fn seed_flags_ambiguous_names() {
        let (temp, mut registry) = test_registry();
        let people = vec![SeedPerson {
            name: "May".to_string(),
            relationship: "sister".to_string(),
            context: "personal".to_string(),
            nickname: None,
        }];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry
                .seed("personal", &people, &[])
                .expect("seed must succeed");
        });
        assert!(
            registry.data.ambiguous_flags.contains(&"may".to_string()),
            "May must be flagged as ambiguous"
        );
    }
}
