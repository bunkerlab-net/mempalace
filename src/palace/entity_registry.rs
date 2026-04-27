//! Structured entity registry — `entity_registry.json` in mempalace config dir.
//!
//! Ports `entity_registry.py` from the Python reference implementation.
//! Tracks people, projects, and ambiguous-word flags in a single JSON file.
//!
//! Sources in priority order:
//!   1. Onboarding — explicit user entries (confidence 1.0).
//!   2. Learned — inferred from session history with high confidence.
//!   3. Researched — opt-in Wikipedia lookup (never fires without `allow_network`).

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Duration;

use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::config_dir;
use crate::error::Result;
use crate::palace::entity_detect::detect_entities;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Confidence assigned to onboarding-sourced people and projects.
const CONFIDENCE_ONBOARDING: f64 = 1.0;
const _: () = assert!(CONFIDENCE_ONBOARDING > 0.0);

/// Confidence assigned to wiki-confirmed people.
const CONFIDENCE_WIKI: f64 = 0.90;
const _: () = assert!(CONFIDENCE_WIKI > 0.0 && CONFIDENCE_WIKI < 1.0);

/// Timeout for outbound Wikipedia HTTP requests.
const WIKIPEDIA_TIMEOUT_SECS: u64 = 5;
const _: () = assert!(WIKIPEDIA_TIMEOUT_SECS > 0);

/// Maximum excerpt length stored in `wiki_cache` per entry.
const WIKI_SUMMARY_MAX: usize = 200;
const _: () = assert!(WIKI_SUMMARY_MAX > 0);

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

/// Regex format strings that indicate a word is used as a person name.
///
/// `{name}` is replaced with `regex::escape(name_lower)` before compilation.
/// Each pattern is matched case-insensitively against the surrounding sentence.
const PERSON_CONTEXT_PATTERNS: &[&str] = &[
    r"\b{name}\s+said\b",
    r"\b{name}\s+told\b",
    r"\b{name}\s+asked\b",
    r"\b{name}\s+laughed\b",
    r"\b{name}\s+smiled\b",
    r"\b{name}\s+was\b",
    r"\b{name}\s+is\b",
    r"\b{name}\s+called\b",
    r"\b{name}\s+texted\b",
    r"\bwith\s+{name}\b",
    r"\bsaw\s+{name}\b",
    r"\bcalled\s+{name}\b",
    r"\btook\s+{name}\b",
    r"\bpicked\s+up\s+{name}\b",
    r"\bdrop(?:ped)?\s+(?:off\s+)?{name}\b",
    r"\b{name}(?:'s|s')\b",
    r"\bhey\s+{name}\b",
    r"\bthanks?\s+{name}\b",
    r"(?m)^{name}[:\s]",
    r"\bmy\s+(?:son|daughter|kid|child|brother|sister|friend|partner|colleague|coworker)\s+{name}\b",
];

/// Regex format strings that indicate a word is NOT being used as a person name.
///
/// `{name}` is replaced with `regex::escape(name_lower)` before compilation.
const CONCEPT_CONTEXT_PATTERNS: &[&str] = &[
    r"\bhave\s+you\s+{name}\b",
    r"\bif\s+you\s+{name}\b",
    r"\b{name}\s+since\b",
    r"\b{name}\s+again\b",
    r"\bnot\s+{name}\b",
    r"\b{name}\s+more\b",
    r"\bwould\s+{name}\b",
    r"\bcould\s+{name}\b",
    r"\bwill\s+{name}\b",
    r"(?:the\s+)?{name}\s+(?:of|in|at|for|to)\b",
];

/// Wikipedia extract phrases that indicate the page describes a personal name.
const NAME_INDICATOR_PHRASES: &[&str] = &[
    "given name",
    "personal name",
    "first name",
    "forename",
    "masculine name",
    "feminine name",
    "boy's name",
    "girl's name",
    "male name",
    "female name",
    "irish name",
    "welsh name",
    "scottish name",
    "gaelic name",
    "hebrew name",
    "arabic name",
    "norse name",
    "old english name",
    "is a name",
    "as a name",
    "name meaning",
    "name derived from",
    "legendary irish",
    "legendary welsh",
    "legendary scottish",
];

/// Wikipedia extract phrases that indicate the page describes a place.
const PLACE_INDICATOR_PHRASES: &[&str] = &[
    "city in",
    "town in",
    "village in",
    "municipality",
    "capital of",
    "district of",
    "county",
    "province",
    "region of",
    "island of",
    "mountain in",
    "river in",
];

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

/// Result of an entity [`EntityRegistry::lookup`] call.
pub struct LookupResult {
    /// Entity classification: `"person"`, `"project"`, `"place"`, `"concept"`, `"unknown"`.
    pub entity_type: String,
    pub confidence: f64,
    /// Source of the classification: `"onboarding"`, `"learned"`, `"wiki"`,
    /// `"context_disambiguated"`, `"none"`.
    pub source: String,
    /// Canonical name as stored in the registry (may differ in case from the query).
    pub name: String,
    /// Context tags from the person entry (e.g. `["personal"]`).
    pub contexts: Vec<String>,
    /// True when the word is ambiguous and the caller should provide more context.
    pub needs_disambiguation: bool,
    /// Set when context patterns resolved the ambiguity; value is `"context_patterns"`.
    pub disambiguated_by: Option<String>,
}

/// Wikipedia research cache entry stored under `wiki_cache` in the registry JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiCacheEntry {
    /// Classification inferred from the Wikipedia page: `"person"`, `"place"`, `"concept"`,
    /// `"ambiguous"`, or `"unknown"`.
    pub inferred_type: String,
    pub confidence: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wiki_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wiki_title: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub word: String,
    #[serde(default)]
    pub confirmed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
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
    /// Cached Wikipedia research results keyed by the queried word.
    #[serde(default)]
    wiki_cache: HashMap<String, WikiCacheEntry>,
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
            wiki_cache: HashMap::new(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityRegistry — main public type
// ─────────────────────────────────────────────────────────────────────────────

/// Structured personal entity registry persisted at `entity_registry.json`.
///
/// Knows the difference between "Riley" (a person) and "ever" (an adverb).
/// Sources in priority order: onboarding → learned → wiki-researched.
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

    /// Look up a word and return its entity classification.
    ///
    /// Checks people registry first, then projects, then the wiki cache. Context
    /// is used to disambiguate words that are also common English words.
    pub fn lookup(&self, word: &str, context: &str) -> LookupResult {
        assert!(!word.is_empty(), "lookup: word must not be empty");

        if let Some(result) = self.lookup_check_people(word, context) {
            assert!(!result.entity_type.is_empty());
            return result;
        }
        if let Some(result) = self.lookup_check_projects(word) {
            assert!(!result.entity_type.is_empty());
            return result;
        }
        if let Some(result) = self.lookup_check_wiki_cache(word) {
            assert!(!result.entity_type.is_empty());
            return result;
        }
        LookupResult {
            entity_type: "unknown".to_string(),
            confidence: 0.0,
            source: "none".to_string(),
            name: word.to_string(),
            contexts: vec![],
            needs_disambiguation: false,
            disambiguated_by: None,
        }
    }

    /// Research an unknown word, optionally via a Wikipedia outbound request.
    ///
    /// By default this is **local-only**: checks the wiki cache and returns
    /// `"unknown"` for uncached words. Pass `allow_network = true` to opt in
    /// to an outbound Wikipedia lookup. No data leaves the machine unless the
    /// caller explicitly requests it (privacy-by-architecture).
    pub fn research(
        &mut self,
        word: &str,
        auto_confirm: bool,
        allow_network: bool,
    ) -> WikiCacheEntry {
        assert!(!word.is_empty(), "research: word must not be empty");

        // Local-only path: return from cache without touching the network.
        if let Some(cached) = self.data.wiki_cache.get(word) {
            assert!(!cached.inferred_type.is_empty());
            return cached.clone();
        }

        if !allow_network {
            return WikiCacheEntry {
                inferred_type: "unknown".to_string(),
                confidence: 0.0,
                wiki_summary: None,
                wiki_title: None,
                word: word.to_string(),
                confirmed: false,
                confirmed_type: None,
                note: Some(
                    "network lookup disabled — pass allow_network=true to query Wikipedia"
                        .to_string(),
                ),
            };
        }

        // Network path — only reachable when the caller explicitly opted in.
        let mut entry = entity_registry_wikipedia_lookup(word);
        entry.word = word.to_string();
        entry.confirmed = auto_confirm;

        self.data.wiki_cache.insert(word.to_string(), entry.clone());

        // Pair assertion: word must be present in cache immediately after insert.
        debug_assert!(
            self.data.wiki_cache.contains_key(word),
            "research: entry must be in wiki_cache after insert"
        );

        // Best-effort save — a failed write does not invalidate the in-memory entry.
        let _ = self.save();
        entry
    }

    /// Mark a researched word as confirmed and optionally promote it to the people registry.
    ///
    /// If `entity_type` is `"person"`, the word is added to `people` with confidence
    /// [`CONFIDENCE_WIKI`] and to `ambiguous_flags` if the name is also a common word.
    pub fn confirm_research(
        &mut self,
        word: &str,
        entity_type: &str,
        relationship: &str,
        context: &str,
    ) -> Result<()> {
        assert!(!word.is_empty(), "confirm_research: word must not be empty");
        assert!(
            !entity_type.is_empty(),
            "confirm_research: entity_type must not be empty"
        );

        if let Some(cached) = self.data.wiki_cache.get_mut(word) {
            cached.confirmed = true;
            cached.confirmed_type = Some(entity_type.to_string());
        }

        if entity_type == "person" {
            let ctx = if context.is_empty() {
                "personal"
            } else {
                context
            };
            self.data.people.insert(
                word.to_string(),
                PersonEntry {
                    source: "wiki".to_string(),
                    contexts: vec![ctx.to_string()],
                    aliases: vec![],
                    relationship: relationship.to_string(),
                    confidence: CONFIDENCE_WIKI,
                    seen_count: 0,
                    canonical: None,
                },
            );

            // Pair assertion: person must be in registry immediately after insert.
            debug_assert!(
                self.data.people.contains_key(word),
                "confirm_research: person must be in registry after insert"
            );

            let word_lower = word.to_lowercase();
            if COMMON_ENGLISH_WORDS.contains(word_lower.as_str())
                && !self.data.ambiguous_flags.contains(&word_lower)
            {
                self.data.ambiguous_flags.push(word_lower);
            }
        }

        self.save()
    }

    /// Scan `text` for new entity candidates and add high-confidence people to the registry.
    ///
    /// Uses [`detect_entities`] via a temporary file. Returns the names of newly
    /// discovered candidates. `min_confidence` is clamped to `(0.0, 1.0]`.
    /// `languages` is forwarded to entity detection (BCP 47 tags such as `"en"`, `"de"`).
    pub fn learn_from_text(
        &mut self,
        text: &str,
        min_confidence: f64,
        languages: &[&str],
    ) -> Result<Vec<String>> {
        assert!(!text.is_empty(), "learn_from_text: text must not be empty");
        assert!(
            min_confidence > 0.0 && min_confidence <= 1.0,
            "learn_from_text: min_confidence must be in (0.0, 1.0]"
        );
        assert!(
            !languages.is_empty(),
            "learn_from_text: languages must not be empty"
        );

        // Write to a named temp file so detect_entities can read from a Path.
        let tmp_path = std::env::temp_dir().join(format!("mempalace_learn_{}.txt", Uuid::new_v4()));
        std::fs::write(&tmp_path, text.as_bytes())?;

        let paths: Vec<&Path> = vec![&tmp_path];
        let result = detect_entities(&paths, 1, languages);
        // Best-effort cleanup — a lingering temp file is harmless.
        let _ = std::fs::remove_file(&tmp_path);

        assert!(
            result.people.len() <= 10_000,
            "detect_entities people count must be bounded"
        );

        let new_names = self.learn_from_text_process(result.people, min_confidence);

        if !new_names.is_empty() {
            self.save()?;
        }

        // Pair assertion: all returned names must be in the people map.
        debug_assert!(
            new_names.iter().all(|n| self.data.people.contains_key(n)),
            "learn_from_text: all new candidates must be in people map"
        );

        Ok(new_names)
    }

    /// Extract known person names from a search query string.
    ///
    /// Uses word-boundary regex matching. Ambiguous names are only included
    /// when context patterns confirm person usage.
    pub fn extract_people_from_query(&self, query: &str) -> Vec<String> {
        assert!(
            !query.is_empty(),
            "extract_people_from_query: query must not be empty"
        );

        let mut found: Vec<String> = Vec::new();

        for (canonical, info) in &self.data.people {
            let all_names: Vec<&str> = std::iter::once(canonical.as_str())
                .chain(info.aliases.iter().map(String::as_str))
                .collect();

            for name in all_names {
                let escaped = regex::escape(name);
                let pattern = format!(r"(?i)\b{escaped}\b");
                let Ok(re) = Regex::new(&pattern) else {
                    continue;
                };
                if !re.is_match(query) {
                    continue;
                }
                if self.data.ambiguous_flags.contains(&name.to_lowercase()) {
                    let Some(result) = lookup_disambiguate(name, query, info) else {
                        continue;
                    };
                    if result.entity_type != "person" {
                        continue;
                    }
                }
                if !found.contains(canonical) {
                    found.push(canonical.clone());
                }
                // Matched via this name — no need to check remaining aliases.
                break;
            }
        }

        assert!(
            found.len() <= self.data.people.len(),
            "extract_people_from_query: result cannot exceed people count"
        );
        found
    }

    /// Returns a human-readable summary of the registry contents.
    pub fn summary(&self) -> String {
        assert!(
            self.data.version > 0,
            "summary: registry version must be positive"
        );

        let people_names: Vec<&str> = self.data.people.keys().map(String::as_str).collect();
        let preview = if people_names.len() > 8 {
            format!("{}...", people_names[..8].join(", "))
        } else {
            people_names.join(", ")
        };

        assert!(preview.len() <= 10_000, "summary: preview must be bounded");

        format!(
            "Mode: {}\nPeople: {} ({})\nProjects: {}\nAmbiguous flags: {}\nWiki cache: {} entries",
            self.data.mode,
            self.data.people.len(),
            preview,
            if self.data.projects.is_empty() {
                "(none)".to_string()
            } else {
                self.data.projects.join(", ")
            },
            if self.data.ambiguous_flags.is_empty() {
                "(none)".to_string()
            } else {
                self.data.ambiguous_flags.join(", ")
            },
            self.data.wiki_cache.len(),
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityRegistry — private helpers
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

    /// Check the people map for a word match, applying disambiguation for ambiguous names.
    ///
    /// Called by [`EntityRegistry::lookup`].
    fn lookup_check_people(&self, word: &str, context: &str) -> Option<LookupResult> {
        assert!(
            !word.is_empty(),
            "lookup_check_people: word must not be empty"
        );

        let word_lower = word.to_lowercase();
        for (canonical, info) in &self.data.people {
            let matches_canonical = word_lower == canonical.to_lowercase();
            let matches_alias = info
                .aliases
                .iter()
                .any(|alias| word_lower == alias.to_lowercase());
            if !matches_canonical && !matches_alias {
                continue;
            }
            // Ambiguous word — attempt context disambiguation first.
            if self.data.ambiguous_flags.contains(&word_lower)
                && !context.is_empty()
                && let Some(result) = lookup_disambiguate(word, context, info)
            {
                return Some(result);
            }
            return Some(LookupResult {
                entity_type: "person".to_string(),
                confidence: info.confidence,
                source: info.source.clone(),
                name: canonical.clone(),
                contexts: info.contexts.clone(),
                needs_disambiguation: false,
                disambiguated_by: None,
            });
        }
        None
    }

    /// Check the projects list for a word match.
    ///
    /// Called by [`EntityRegistry::lookup`].
    fn lookup_check_projects(&self, word: &str) -> Option<LookupResult> {
        assert!(
            !word.is_empty(),
            "lookup_check_projects: word must not be empty"
        );

        let word_lower = word.to_lowercase();
        for project in &self.data.projects {
            if word_lower != project.to_lowercase() {
                continue;
            }
            assert!(!project.is_empty(), "project entry must not be empty");
            return Some(LookupResult {
                entity_type: "project".to_string(),
                confidence: 1.0,
                source: "onboarding".to_string(),
                name: project.clone(),
                contexts: vec![],
                needs_disambiguation: false,
                disambiguated_by: None,
            });
        }
        None
    }

    /// Check the wiki cache for a confirmed entry matching `word`.
    ///
    /// Only confirmed entries are returned; unconfirmed research results are
    /// not surfaced as lookup results.
    fn lookup_check_wiki_cache(&self, word: &str) -> Option<LookupResult> {
        assert!(
            !word.is_empty(),
            "lookup_check_wiki_cache: word must not be empty"
        );

        let word_lower = word.to_lowercase();
        for (cached_word, entry) in &self.data.wiki_cache {
            if word_lower != cached_word.to_lowercase() || !entry.confirmed {
                continue;
            }
            assert!(
                !entry.inferred_type.is_empty(),
                "cached inferred_type must not be empty"
            );
            return Some(LookupResult {
                entity_type: entry.inferred_type.clone(),
                confidence: entry.confidence,
                source: "wiki".to_string(),
                name: word.to_string(),
                contexts: vec![],
                needs_disambiguation: false,
                disambiguated_by: None,
            });
        }
        None
    }

    /// Process detected entity candidates and insert high-confidence people.
    ///
    /// Helper for [`EntityRegistry::learn_from_text`]. Returns names of newly
    /// inserted people.
    fn learn_from_text_process(
        &mut self,
        candidates: Vec<crate::palace::entities::DetectedEntity>,
        min_confidence: f64,
    ) -> Vec<String> {
        assert!(
            min_confidence > 0.0,
            "learn_from_text_process: min_confidence must be positive"
        );

        let mode_context = if self.data.mode == "combo" {
            "personal".to_string()
        } else {
            self.data.mode.clone()
        };
        let mut new_names: Vec<String> = Vec::new();

        for entity in candidates {
            if entity.confidence < min_confidence {
                continue;
            }
            // Skip already-known entities.
            if self.data.people.contains_key(&entity.name)
                || self
                    .data
                    .projects
                    .iter()
                    .any(|p| p.eq_ignore_ascii_case(&entity.name))
            {
                continue;
            }
            let name_lower = entity.name.to_lowercase();
            // usize frequency fits in u64 on all supported platforms (u64 ≥ usize).
            #[allow(clippy::cast_possible_truncation)]
            self.data.people.insert(
                entity.name.clone(),
                PersonEntry {
                    source: "learned".to_string(),
                    contexts: vec![mode_context.clone()],
                    aliases: vec![],
                    relationship: String::new(),
                    confidence: entity.confidence,
                    seen_count: entity.frequency as u64,
                    canonical: None,
                },
            );
            if COMMON_ENGLISH_WORDS.contains(name_lower.as_str())
                && !self.data.ambiguous_flags.contains(&name_lower)
            {
                self.data.ambiguous_flags.push(name_lower);
            }
            new_names.push(entity.name);
        }

        assert!(
            new_names.len() <= 10_000,
            "learn_from_text_process: result set must be bounded"
        );
        new_names
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

/// Disambiguate a word that is both a registered person name and a common English word.
///
/// Scores context patterns for person usage vs concept usage and returns a
/// resolved `LookupResult` when one side wins. Returns `None` when the score
/// is tied, allowing the caller to fall through to the registered-person result.
fn lookup_disambiguate(
    name: &str,
    context: &str,
    person_info: &PersonEntry,
) -> Option<LookupResult> {
    assert!(
        !name.is_empty(),
        "lookup_disambiguate: name must not be empty"
    );
    assert!(
        !context.is_empty(),
        "lookup_disambiguate: context must not be empty"
    );

    let name_lower = name.to_lowercase();
    let ctx_lower = context.to_lowercase();
    let escaped = regex::escape(&name_lower);

    let person_score = PERSON_CONTEXT_PATTERNS
        .iter()
        .filter(|pat| {
            let pattern = pat.replace("{name}", &escaped);
            Regex::new(&pattern)
                .ok()
                .is_some_and(|re| re.is_match(&ctx_lower))
        })
        .count();

    let concept_score = CONCEPT_CONTEXT_PATTERNS
        .iter()
        .filter(|pat| {
            let pattern = pat.replace("{name}", &escaped);
            Regex::new(&pattern)
                .ok()
                .is_some_and(|re| re.is_match(&ctx_lower))
        })
        .count();

    assert!(person_score <= PERSON_CONTEXT_PATTERNS.len());
    assert!(concept_score <= CONCEPT_CONTEXT_PATTERNS.len());

    if person_score > concept_score {
        // person_score ≤ PERSON_CONTEXT_PATTERNS.len() ≤ 20; precision loss impossible.
        #[allow(clippy::cast_precision_loss)]
        let confidence = f64::min(0.95, 0.7 + person_score as f64 * 0.1);
        return Some(LookupResult {
            entity_type: "person".to_string(),
            confidence,
            source: person_info.source.clone(),
            name: name.to_string(),
            contexts: person_info.contexts.clone(),
            needs_disambiguation: false,
            disambiguated_by: Some("context_patterns".to_string()),
        });
    }
    if concept_score > person_score {
        // concept_score ≤ CONCEPT_CONTEXT_PATTERNS.len() ≤ 10; precision loss impossible.
        #[allow(clippy::cast_precision_loss)]
        let confidence = f64::min(0.90, 0.7 + concept_score as f64 * 0.1);
        return Some(LookupResult {
            entity_type: "concept".to_string(),
            confidence,
            source: "context_disambiguated".to_string(),
            name: name.to_string(),
            contexts: vec![],
            needs_disambiguation: false,
            disambiguated_by: Some("context_patterns".to_string()),
        });
    }
    // Tied — return None so the caller falls through to the registered-person result.
    None
}

/// Percent-encode a word for use as a Wikipedia REST API URL path segment.
///
/// Only unreserved characters (A-Z, a-z, 0-9, `-`, `_`, `.`, `~`) are kept
/// as-is; all other bytes are encoded as `%XX`.
fn entity_registry_url_encode(word: &str) -> String {
    // Unreserved RFC 3986 characters that never need percent-encoding.
    const UNRESERVED: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";

    assert!(
        !word.is_empty(),
        "entity_registry_url_encode: word must not be empty"
    );

    let mut encoded = String::with_capacity(word.len() * 3);
    for byte in word.as_bytes() {
        if UNRESERVED.contains(byte) {
            encoded.push(*byte as char);
        } else {
            encoded.push('%');
            // String::write_fmt is infallible; result is intentionally discarded.
            let _ = write!(encoded, "{byte:02X}");
        }
    }

    assert!(
        !encoded.is_empty(),
        "entity_registry_url_encode: result must not be empty"
    );
    encoded
}

/// Build a `WikiCacheEntry` representing a network failure or parse error.
///
/// Called by [`entity_registry_wikipedia_lookup`] for any non-404 error path.
fn entity_registry_wikipedia_error(word: &str) -> WikiCacheEntry {
    assert!(
        !word.is_empty(),
        "entity_registry_wikipedia_error: word must not be empty"
    );

    WikiCacheEntry {
        inferred_type: "unknown".to_string(),
        confidence: 0.0,
        wiki_summary: None,
        wiki_title: None,
        word: word.to_string(),
        confirmed: false,
        confirmed_type: None,
        note: None,
    }
}

/// Build a `WikiCacheEntry` with the common fields pre-filled.
///
/// All four classification paths in [`entity_registry_wikipedia_classify`] produce
/// the same struct layout; only `inferred_type`, `confidence`, and `note` vary.
fn entity_registry_wikipedia_classify_entry(
    inferred_type: &str,
    confidence: f64,
    summary: Option<String>,
    title: String,
    word: &str,
    note: Option<String>,
) -> WikiCacheEntry {
    assert!(
        !inferred_type.is_empty(),
        "classify_entry: inferred_type must not be empty"
    );
    assert!(!word.is_empty(), "classify_entry: word must not be empty");

    WikiCacheEntry {
        inferred_type: inferred_type.to_string(),
        confidence,
        wiki_summary: summary,
        wiki_title: Some(title),
        word: word.to_string(),
        confirmed: false,
        confirmed_type: None,
        note,
    }
}

/// Classify a Wikipedia REST API JSON response into a `WikiCacheEntry`.
///
/// Checks `page_type` for disambiguation pages, then `extract` text for name
/// and place indicator phrases. Falls back to `"concept"` if nothing matches.
fn entity_registry_wikipedia_classify(data: &serde_json::Value, word: &str) -> WikiCacheEntry {
    assert!(
        !word.is_empty(),
        "entity_registry_wikipedia_classify: word must not be empty"
    );

    let page_type = data
        .get("type")
        .and_then(|field| field.as_str())
        .unwrap_or("");
    let extract = data
        .get("extract")
        .and_then(|field| field.as_str())
        .unwrap_or("")
        .to_lowercase();
    let title = data
        .get("title")
        .and_then(|field| field.as_str())
        .unwrap_or(word)
        .to_string();
    let summary = extract
        .get(..extract.len().min(WIKI_SUMMARY_MAX))
        .map(str::to_string);

    assert!(
        extract.len() <= 1_000_000,
        "entity_registry_wikipedia_classify: extract must be bounded"
    );

    if page_type == "disambiguation" {
        let description = data
            .get("description")
            .and_then(|field| field.as_str())
            .unwrap_or("")
            .to_lowercase();
        let is_name = description.contains("name") || description.contains("given name");
        let note = is_name.then(|| "disambiguation page with name entries".to_string());
        let (kind, conf) = if is_name {
            ("person", 0.65)
        } else {
            ("ambiguous", 0.4)
        };
        return entity_registry_wikipedia_classify_entry(kind, conf, summary, title, word, note);
    }
    if NAME_INDICATOR_PHRASES
        .iter()
        .any(|phrase| extract.contains(*phrase))
    {
        let word_lower = word.to_lowercase();
        let is_direct = extract.contains(&format!("{word_lower} is a"))
            || extract.contains(&format!("{word_lower} (name"));
        let conf = if is_direct { 0.90 } else { 0.80 };
        return entity_registry_wikipedia_classify_entry(
            "person", conf, summary, title, word, None,
        );
    }
    if PLACE_INDICATOR_PHRASES
        .iter()
        .any(|phrase| extract.contains(*phrase))
    {
        return entity_registry_wikipedia_classify_entry("place", 0.80, summary, title, word, None);
    }
    // Found in Wikipedia but matches neither name nor place patterns.
    entity_registry_wikipedia_classify_entry("concept", 0.60, summary, title, word, None)
}

/// Fetch a Wikipedia REST API summary page and classify the result.
///
/// Caller is responsible for ensuring `allow_network = true` before calling.
/// A 404 response returns `inferred_type = "unknown"` with `confidence = 0.3`
/// (not found, but that says nothing definitively). Any other network error
/// returns `confidence = 0.0`.
fn entity_registry_wikipedia_lookup(word: &str) -> WikiCacheEntry {
    assert!(
        !word.is_empty(),
        "entity_registry_wikipedia_lookup: word must not be empty"
    );

    let encoded = entity_registry_url_encode(word);
    let url = format!("https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}");

    assert!(
        !url.is_empty(),
        "entity_registry_wikipedia_lookup: url must not be empty"
    );

    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(WIKIPEDIA_TIMEOUT_SECS)))
        .build()
        .new_agent();

    let response = agent.get(&url).header("User-Agent", "MemPalace/1.0").call();

    let text = match response {
        Err(ureq::Error::StatusCode(404)) => {
            return WikiCacheEntry {
                inferred_type: "unknown".to_string(),
                confidence: 0.3,
                wiki_summary: None,
                wiki_title: None,
                word: word.to_string(),
                confirmed: false,
                confirmed_type: None,
                note: Some("not found in Wikipedia".to_string()),
            };
        }
        Err(_) => return entity_registry_wikipedia_error(word),
        Ok(resp) => match resp.into_body().read_to_string() {
            Ok(t) => t,
            Err(_) => return entity_registry_wikipedia_error(word),
        },
    };

    let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) else {
        return entity_registry_wikipedia_error(word);
    };

    entity_registry_wikipedia_classify(&data, word)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
// Test code — .expect() is acceptable; float comparisons use exact constants only.
#[allow(clippy::expect_used)]
#[allow(clippy::float_cmp)]
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

    // ── lookup ───────────────────────────────────────────────────────────────

    #[test]
    fn lookup_finds_onboarded_person() {
        let (temp, mut registry) = test_registry();
        let people = vec![SeedPerson {
            name: "Riley".to_string(),
            relationship: "daughter".to_string(),
            context: "personal".to_string(),
            nickname: None,
        }];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry.seed("personal", &people, &[]).expect("seed");
        });
        let result = registry.lookup("Riley", "");
        assert_eq!(result.entity_type, "person", "Riley must resolve as person");
        assert_eq!(result.source, "onboarding");
        assert!(!result.needs_disambiguation);
    }

    #[test]
    fn lookup_finds_project() {
        let (temp, mut registry) = test_registry();
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry
                .seed("work", &[], &["MemPalace".to_string()])
                .expect("seed");
        });
        let result = registry.lookup("MemPalace", "");
        assert_eq!(
            result.entity_type, "project",
            "MemPalace must resolve as project"
        );
        assert_eq!(result.confidence, 1.0);
    }

    #[test]
    fn lookup_returns_unknown_for_unregistered_word() {
        let (_temp, registry) = test_registry();
        let result = registry.lookup("Xyzzy", "");
        assert_eq!(result.entity_type, "unknown");
        assert_eq!(result.confidence, 0.0);
    }

    // ── context disambiguation ────────────────────────────────────────────────

    #[test]
    fn disambiguate_resolves_person_with_name_context() {
        let (temp, mut registry) = test_registry();
        let people = vec![SeedPerson {
            name: "Will".to_string(),
            relationship: "brother".to_string(),
            context: "personal".to_string(),
            nickname: None,
        }];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry.seed("personal", &people, &[]).expect("seed");
        });
        // "Will said hello" — person context pattern fires.
        let result = registry.lookup("Will", "Will said hello to me");
        assert_eq!(result.entity_type, "person", "person context must win");
        assert_eq!(
            result.disambiguated_by.as_deref(),
            Some("context_patterns"),
            "disambiguation source must be context_patterns"
        );
    }

    #[test]
    fn disambiguate_resolves_concept_with_adverb_context() {
        let (temp, mut registry) = test_registry();
        let people = vec![SeedPerson {
            name: "Ever".to_string(),
            relationship: "colleague".to_string(),
            context: "work".to_string(),
            nickname: None,
        }];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry.seed("work", &people, &[]).expect("seed");
        });
        // "have you ever done this" — concept context pattern fires.
        let result = registry.lookup("ever", "have you ever done this");
        assert_eq!(result.entity_type, "concept", "concept context must win");
    }

    // ── research / confirm_research ───────────────────────────────────────────

    #[test]
    fn research_returns_unknown_when_network_disabled() {
        let (_temp, mut registry) = test_registry();
        let result = registry.research("Siobhan", false, false);
        assert_eq!(result.inferred_type, "unknown");
        assert!(!result.confirmed);
        assert!(
            result.note.is_some(),
            "disabled-network result must have a note"
        );
    }

    #[test]
    fn confirm_research_adds_person_to_registry() {
        let (temp, mut registry) = test_registry();
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry
                .confirm_research("Sam", "person", "colleague", "work")
                .expect("confirm must succeed");
        });
        assert!(
            registry.data.people.contains_key("Sam"),
            "Sam must be in registry after confirm"
        );
        assert_eq!(
            registry.data.people["Sam"].source, "wiki",
            "source must be wiki"
        );
    }

    // ── extract_people_from_query ─────────────────────────────────────────────

    #[test]
    fn extract_people_finds_registered_name_in_query() {
        let (temp, mut registry) = test_registry();
        let people = vec![SeedPerson {
            name: "Jordan".to_string(),
            relationship: "friend".to_string(),
            context: "personal".to_string(),
            nickname: None,
        }];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry.seed("personal", &people, &[]).expect("seed");
        });
        let found = registry.extract_people_from_query("What did Jordan say about the project?");
        assert!(
            found.contains(&"Jordan".to_string()),
            "Jordan must be found in query"
        );
    }

    #[test]
    fn extract_people_returns_empty_for_unknown_names() {
        let (_temp, registry) = test_registry();
        let found = registry.extract_people_from_query("What is the weather like today?");
        assert!(found.is_empty(), "no known names in generic query");
    }

    // ── summary ───────────────────────────────────────────────────────────────

    #[test]
    fn summary_contains_mode_and_people_count() {
        let (temp, mut registry) = test_registry();
        let people = vec![SeedPerson {
            name: "Alice".to_string(),
            relationship: "friend".to_string(),
            context: "personal".to_string(),
            nickname: None,
        }];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry.seed("personal", &people, &[]).expect("seed");
        });
        let summary = registry.summary();
        assert!(
            summary.contains("Mode: personal"),
            "summary must include mode"
        );
        assert!(
            summary.contains("People: "),
            "summary must include people count"
        );
    }

    // ── entity_registry_url_encode ────────────────────────────────────────────

    #[test]
    fn url_encode_leaves_ascii_letters_unchanged() {
        let encoded = entity_registry_url_encode("Riley");
        assert_eq!(encoded, "Riley", "ASCII letters must not be encoded");
        assert!(!encoded.is_empty());
    }

    #[test]
    fn url_encode_encodes_spaces() {
        let encoded = entity_registry_url_encode("Riley Smith");
        assert!(encoded.contains("%20"), "space must be encoded as %20");
        assert!(
            !encoded.contains(' '),
            "encoded result must have no raw spaces"
        );
    }

    // ── entity_registry_wikipedia_classify ───────────────────────────────────

    #[test]
    fn wikipedia_classify_name_page_returns_person() {
        let data = serde_json::json!({
            "type": "standard",
            "title": "Riley",
            "extract": "Riley is a given name of Irish origin."
        });
        let entry = entity_registry_wikipedia_classify(&data, "Riley");
        assert_eq!(
            entry.inferred_type, "person",
            "name page must classify as person"
        );
        assert!(entry.confidence >= 0.80);
    }

    #[test]
    fn wikipedia_classify_disambiguation_with_name_returns_person() {
        let data = serde_json::json!({
            "type": "disambiguation",
            "title": "Sam",
            "description": "given name",
            "extract": "Sam may refer to several things."
        });
        let entry = entity_registry_wikipedia_classify(&data, "Sam");
        assert_eq!(
            entry.inferred_type, "person",
            "name disambiguation must classify as person"
        );
        assert!(
            entry.note.is_some(),
            "disambiguation result must have a note"
        );
    }

    #[test]
    fn wikipedia_classify_city_returns_place() {
        let data = serde_json::json!({
            "type": "standard",
            "title": "May",
            "extract": "May is a city in the midwest of the United States."
        });
        let entry = entity_registry_wikipedia_classify(&data, "May");
        assert_eq!(
            entry.inferred_type, "place",
            "city page must classify as place"
        );
        assert!(entry.confidence >= 0.80);
    }

    #[test]
    fn wikipedia_classify_disambiguation_without_name_returns_ambiguous() {
        // A disambiguation page whose description does not mention "name" must yield
        // inferred_type="ambiguous" with confidence 0.40.
        let data = serde_json::json!({
            "type": "disambiguation",
            "title": "Mercury",
            "description": "multiple things including planet, element, and company",
            "extract": "Mercury may refer to several topics."
        });
        let entry = entity_registry_wikipedia_classify(&data, "Mercury");
        assert_eq!(
            entry.inferred_type, "ambiguous",
            "disambiguation without 'name' in description must be ambiguous"
        );
        assert!(
            (entry.confidence - 0.4).abs() < 1e-9,
            "ambiguous disambiguation must have 0.40 confidence"
        );
    }

    #[test]
    fn wikipedia_classify_concept_fallback_when_no_indicators_match() {
        // When the extract contains neither a NAME_INDICATOR_PHRASE nor a
        // PLACE_INDICATOR_PHRASE, the fallback type must be "concept".
        let data = serde_json::json!({
            "type": "standard",
            "title": "Photosynthesis",
            "extract": "Photosynthesis is a process by which plants convert light into energy."
        });
        let entry = entity_registry_wikipedia_classify(&data, "Photosynthesis");
        assert_eq!(
            entry.inferred_type, "concept",
            "page without name or place indicators must fall back to concept"
        );
        assert!(
            (entry.confidence - 0.60).abs() < 1e-9,
            "concept fallback must have 0.60 confidence"
        );
    }

    #[test]
    fn wikipedia_classify_indirect_name_indicator_yields_lower_confidence() {
        // "given name" appears in extract but "morgan is a" does not → is_direct=false → 0.80.
        let data = serde_json::json!({
            "type": "standard",
            "title": "Morgan",
            "extract": "The Welsh given name derived from ancient roots."
        });
        let entry = entity_registry_wikipedia_classify(&data, "Morgan");
        assert_eq!(entry.inferred_type, "person");
        // "morgan is a" not in extract → is_direct=false → conf=0.80
        assert!(
            (entry.confidence - 0.80).abs() < 1e-9,
            "indirect name indicator must yield 0.80 confidence, got {}",
            entry.confidence
        );
    }
}
