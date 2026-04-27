//! Structured entity registry — `entity_registry.json` in mempalace config dir.
//!
//! Ports `entity_registry.py` from the Python reference implementation.
//! Tracks people, projects, ambiguous-word flags, and a Wikipedia research
//! cache in a single JSON file.
//!
//! Sources in priority order:
//!   1. Onboarding — explicit user entries (confidence 1.0).
//!   2. Learned — inferred from session text with configurable minimum confidence.
//!   3. Wiki — Wikipedia REST API lookup (opt-in only; never called unless
//!      `allow_network = true` is passed to [`EntityRegistry::research`]).
//!
//! Wikipedia lookups honour the project's privacy-by-architecture principle:
//! no data leaves the machine unless the caller explicitly opts in.

// Public registry API is complete but callers (MCP tools, search, fact-checker)
// are ported in separate Band B/C work. Suppress dead_code until those callers land.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Duration;

use regex::Regex;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use crate::config::config_dir;
use crate::error::{Error as CrateError, Result};
use crate::palace::entities::DetectedEntity;
use crate::palace::entity_detect::detect_entities;

/// Wikipedia REST API endpoint for single-page summary lookups.
const WIKI_URL_BASE: &str = "https://en.wikipedia.org/api/rest_v1/page/summary/";

/// Characters of the Wikipedia extract stored in the cache per entry.
const WIKI_SUMMARY_MAX: usize = 200;

/// Timeout in seconds for Wikipedia HTTP requests.
const WIKI_TIMEOUT_SECS: u64 = 5;

/// Maximum context patterns applied per word during disambiguation.
const PATTERNS_LIMIT: usize = 50;

/// Confidence assigned to onboarding-sourced people and projects.
const CONFIDENCE_ONBOARDING: f64 = 1.0;

/// Confidence assigned to Wikipedia-confirmed entities added via `confirm_research`.
const CONFIDENCE_WIKI: f64 = 0.90;

const _: () = assert!(WIKI_SUMMARY_MAX > 0);
const _: () = assert!(WIKI_TIMEOUT_SECS > 0);
const _: () = assert!(PATTERNS_LIMIT > 0);
const _: () = assert!(CONFIDENCE_ONBOARDING > 0.0);
const _: () = assert!(CONFIDENCE_WIKI > 0.0);
// Compile-time guards: pattern lists must fit within the disambiguation limit.
const _: () = assert!(PERSON_CONTEXT_PATTERNS.len() <= PATTERNS_LIMIT);
const _: () = assert!(CONCEPT_CONTEXT_PATTERNS.len() <= PATTERNS_LIMIT);

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

/// Regex templates (substitute `{name}` → `regex::escape(name)`) that indicate
/// the target word is used as a person name in the surrounding context.
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
    r"^{name}[:\s]",
    r"\bmy\s+(?:son|daughter|kid|child|brother|sister|friend|partner|colleague|coworker)\s+{name}\b",
];

/// Regex templates that indicate the target word is NOT a person name in context.
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

/// Phrases in a Wikipedia summary that suggest the queried word is a personal name.
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

/// Phrases in a Wikipedia summary that suggest the queried word is a place name.
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

/// Classification of a looked-up or researched word.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Person,
    Project,
    Concept,
    Place,
    Ambiguous,
    Unknown,
}

impl EntityType {
    /// Returns the lowercase string label used in JSON output and diagnostics.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Person => "person",
            Self::Project => "project",
            Self::Concept => "concept",
            Self::Place => "place",
            Self::Ambiguous => "ambiguous",
            Self::Unknown => "unknown",
        }
    }
}

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

/// A cached Wikipedia research result stored in `wiki_cache`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiEntry {
    pub inferred_type: EntityType,
    pub confidence: f64,
    #[serde(default)]
    pub confirmed: bool,
    pub wiki_summary: Option<String>,
    pub wiki_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Result of a [`EntityRegistry::lookup`] call.
#[derive(Debug, Clone)]
pub struct LookupResult {
    pub entity_type: EntityType,
    pub confidence: f64,
    /// Source label: `"onboarding"`, `"learned"`, `"wiki"`, or `"none"`.
    pub source: String,
    /// Canonical registry name if found; otherwise the queried word.
    pub name: String,
    pub needs_disambiguation: bool,
    /// How the result was resolved for an ambiguous word.
    pub disambiguated_by: Option<String>,
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
    #[serde(default)]
    wiki_cache: HashMap<String, WikiEntry>,
}

fn registry_data_default_version() -> u32 {
    1
}

fn registry_data_default_mode() -> String {
    "personal".to_string()
}

impl Default for RegistryData {
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
/// Sources in priority order: onboarding → learned → wiki cache.
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
    /// Clears existing `people` and `projects`; wiki cache is preserved so
    /// past research results survive re-onboarding.
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

    /// Look up a word in the registry.
    ///
    /// Checks people → projects → wiki cache in priority order.
    /// `context` is used to disambiguate words that are both names and common
    /// English words; pass `""` to skip context-pattern matching.
    pub fn lookup(&self, word: &str, context: &str) -> LookupResult {
        assert!(!word.is_empty(), "lookup: word must not be empty");

        if let Some(result) = self.lookup_person(word, context) {
            return result;
        }
        if let Some(result) = self.lookup_project(word) {
            return result;
        }
        if let Some(result) = self.lookup_wiki_cache(word) {
            return result;
        }

        LookupResult {
            entity_type: EntityType::Unknown,
            confidence: 0.0,
            source: "none".to_string(),
            name: word.to_string(),
            needs_disambiguation: false,
            disambiguated_by: None,
        }
    }

    /// Research an unknown word.
    ///
    /// Default is **local-only**: checks the wiki cache and returns `Unknown`
    /// for uncached words. Pass `allow_network = true` to query Wikipedia.
    /// The result is cached and persisted regardless of `allow_network`.
    pub fn research(&mut self, word: &str, allow_network: bool) -> WikiEntry {
        assert!(!word.is_empty(), "research: word must not be empty");

        if let Some(cached) = self.data.wiki_cache.get(word) {
            return cached.clone();
        }

        if !allow_network {
            return WikiEntry {
                inferred_type: EntityType::Unknown,
                confidence: 0.0,
                confirmed: false,
                wiki_summary: None,
                wiki_title: None,
                note: Some(
                    "network lookup disabled; pass allow_network=true to query Wikipedia"
                        .to_string(),
                ),
            };
        }

        let mut entry = entity_registry_wikipedia_lookup(word);
        // Newly fetched entries require explicit confirmation before they affect lookup results.
        entry.confirmed = false;

        self.data.wiki_cache.insert(word.to_string(), entry.clone());
        // Best-effort persistence — caller is not required to handle save errors.
        let _ = self.save();

        assert!(
            self.data.wiki_cache.contains_key(word),
            "research: wiki_cache must contain the word after insertion"
        );
        entry
    }

    /// Mark a researched word as confirmed and optionally promote it to `people`.
    ///
    /// If `entity_type` is `"person"`, a `PersonEntry` is inserted with
    /// `source = "wiki"` and confidence [`CONFIDENCE_WIKI`].
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

        if let Some(entry) = self.data.wiki_cache.get_mut(word) {
            entry.confirmed = true;
        }

        if entity_type == "person" {
            let is_ambiguous = COMMON_ENGLISH_WORDS.contains(word.to_lowercase().as_str());
            self.data.people.insert(
                word.to_string(),
                PersonEntry {
                    source: "wiki".to_string(),
                    contexts: vec![if context.is_empty() {
                        "personal".to_string()
                    } else {
                        context.to_string()
                    }],
                    aliases: Vec::new(),
                    relationship: relationship.to_string(),
                    confidence: CONFIDENCE_WIKI,
                    seen_count: 0,
                    canonical: None,
                },
            );
            if is_ambiguous {
                let lower = word.to_lowercase();
                if !self.data.ambiguous_flags.contains(&lower) {
                    self.data.ambiguous_flags.push(lower);
                }
            }
        }

        // Pair assertion: confirmed entry must exist in cache after update.
        debug_assert!(
            self.data.wiki_cache.get(word).is_none_or(|e| e.confirmed),
            "confirm_research: cache entry must be confirmed after this call"
        );

        self.save()
    }

    /// Scan `text` for new person candidates and add confirmed ones to `people`.
    ///
    /// Returns names of newly learned people. Uses `detect_entities` via a
    /// temp file; entities already in the registry are skipped.
    pub fn learn_from_text(
        &mut self,
        text: &str,
        min_confidence: f64,
        languages: &[&str],
    ) -> Vec<String> {
        assert!(
            min_confidence > 0.0,
            "learn_from_text: min_confidence must be positive"
        );
        assert!(
            !languages.is_empty(),
            "learn_from_text: languages must not be empty"
        );

        if text.is_empty() {
            return Vec::new();
        }

        let candidates = learn_from_text_detect(text, languages);
        let mode = self.data.mode.clone();
        let mut new_names: Vec<String> = Vec::new();

        for entity in candidates {
            if entity.confidence < min_confidence {
                continue;
            }
            if self.data.people.contains_key(&entity.name)
                || self.data.projects.contains(&entity.name)
            {
                continue;
            }
            let lower = entity.name.to_lowercase();
            let is_ambiguous = COMMON_ENGLISH_WORDS.contains(lower.as_str());
            let entry = learn_from_text_make_entry(&entity, &mode);
            self.data.people.insert(entity.name.clone(), entry);
            if is_ambiguous && !self.data.ambiguous_flags.contains(&lower) {
                self.data.ambiguous_flags.push(lower);
            }
            new_names.push(entity.name);
        }

        if !new_names.is_empty() {
            let _ = self.save();
        }

        assert!(
            new_names.len() <= self.data.people.len(),
            "learn_from_text: new names cannot exceed total people count"
        );
        new_names
    }

    /// Extract known person names from a query string.
    ///
    /// Returns canonical names whose word-boundary regex matches the query.
    /// Ambiguous names are validated via context patterns before inclusion.
    pub fn extract_people_from_query(&self, query: &str) -> Vec<String> {
        assert!(
            !query.is_empty(),
            "extract_people_from_query: query must not be empty"
        );

        let mut found: Vec<String> = Vec::new();

        for (canonical, info) in &self.data.people {
            let names_to_check: Vec<&str> = std::iter::once(canonical.as_str())
                .chain(info.aliases.iter().map(String::as_str))
                .collect();

            for name in names_to_check {
                if extract_people_from_query_matches(name, query, &self.data.ambiguous_flags, info)
                    && !found.contains(canonical)
                {
                    found.push(canonical.clone());
                }
            }
        }

        assert!(
            found.len() <= self.data.people.len(),
            "extract_people_from_query: found cannot exceed registry size"
        );
        found
    }

    /// One-line summary of registry contents for display or logging.
    pub fn summary(&self) -> String {
        assert!(
            self.data.version > 0,
            "summary: registry version must be positive"
        );

        let people_keys: Vec<&str> = self
            .data
            .people
            .keys()
            .take(8)
            .map(String::as_str)
            .collect();
        let people_preview = if self.data.people.len() > 8 {
            format!("{}...", people_keys.join(", "))
        } else {
            people_keys.join(", ")
        };
        let projects_str = if self.data.projects.is_empty() {
            "(none)".to_string()
        } else {
            self.data.projects.join(", ")
        };
        let ambiguous_str = if self.data.ambiguous_flags.is_empty() {
            "(none)".to_string()
        } else {
            self.data.ambiguous_flags.join(", ")
        };

        format!(
            "Mode: {}\nPeople: {} ({})\nProjects: {}\nAmbiguous flags: {}\nWiki cache: {} entries",
            self.data.mode,
            self.data.people.len(),
            people_preview,
            projects_str,
            ambiguous_str,
            self.data.wiki_cache.len(),
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityRegistry — private helpers (all named seed_*, lookup_*, etc.)
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

    /// Look up `word` against the `people` map with optional context disambiguation.
    ///
    /// Returns `None` when no match is found, deferring to project/wiki lookup.
    fn lookup_person(&self, word: &str, context: &str) -> Option<LookupResult> {
        assert!(!word.is_empty());

        let word_lower = word.to_lowercase();

        for (canonical, info) in &self.data.people {
            let canonical_lower = canonical.to_lowercase();
            let alias_match = info.aliases.iter().any(|a| a.to_lowercase() == word_lower);
            if word_lower != canonical_lower && !alias_match {
                continue;
            }
            let is_ambiguous = self
                .data
                .ambiguous_flags
                .iter()
                .any(|flag| flag == &word_lower);
            if is_ambiguous
                && !context.is_empty()
                && let Some(resolved) = entity_registry_disambiguate(word, context, info)
            {
                return Some(resolved);
            }
            return Some(LookupResult {
                entity_type: EntityType::Person,
                confidence: info.confidence,
                source: info.source.clone(),
                name: canonical.clone(),
                needs_disambiguation: false,
                disambiguated_by: None,
            });
        }
        None
    }

    /// Look up `word` against the `projects` list.
    ///
    /// Returns `None` when no case-insensitive match is found.
    fn lookup_project(&self, word: &str) -> Option<LookupResult> {
        assert!(!word.is_empty());

        let word_lower = word.to_lowercase();
        for proj in &self.data.projects {
            if proj.to_lowercase() == word_lower {
                return Some(LookupResult {
                    entity_type: EntityType::Project,
                    confidence: CONFIDENCE_ONBOARDING,
                    source: "onboarding".to_string(),
                    name: proj.clone(),
                    needs_disambiguation: false,
                    disambiguated_by: None,
                });
            }
        }
        None
    }

    /// Look up `word` in the wiki cache, returning only confirmed entries.
    ///
    /// Unconfirmed entries (pending user review) are ignored by lookup.
    fn lookup_wiki_cache(&self, word: &str) -> Option<LookupResult> {
        assert!(!word.is_empty());

        let word_lower = word.to_lowercase();
        for (cached, entry) in &self.data.wiki_cache {
            if cached.to_lowercase() == word_lower && entry.confirmed {
                return Some(LookupResult {
                    entity_type: entry.inferred_type.clone(),
                    confidence: entry.confidence,
                    source: "wiki".to_string(),
                    name: word.to_string(),
                    needs_disambiguation: false,
                    disambiguated_by: None,
                });
            }
        }
        None
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

/// Apply context-pattern disambiguation for an ambiguous word.
///
/// Counts matches against `PERSON_CONTEXT_PATTERNS` and `CONCEPT_CONTEXT_PATTERNS`.
/// Returns a resolved `LookupResult` when one side dominates; `None` when tied.
fn entity_registry_disambiguate(
    word: &str,
    context: &str,
    person_entry: &PersonEntry,
) -> Option<LookupResult> {
    assert!(!word.is_empty());
    assert!(!context.is_empty());

    let name_lower = word.to_lowercase();
    let ctx_lower = context.to_lowercase();

    let person_score =
        entity_registry_count_pattern_matches(&name_lower, &ctx_lower, PERSON_CONTEXT_PATTERNS);
    let concept_score =
        entity_registry_count_pattern_matches(&name_lower, &ctx_lower, CONCEPT_CONTEXT_PATTERNS);

    if person_score > concept_score {
        // Cast usize to f64 for scoring: values are bounded by PATTERNS_LIMIT.
        #[allow(clippy::cast_precision_loss)]
        let confidence = (0.7_f64 + (person_score as f64) * 0.1).min(0.95);
        assert!(confidence > 0.0);
        assert!(confidence <= 1.0);
        return Some(LookupResult {
            entity_type: EntityType::Person,
            confidence,
            source: person_entry.source.clone(),
            name: word.to_string(),
            needs_disambiguation: false,
            disambiguated_by: Some("context_patterns".to_string()),
        });
    }

    if concept_score > person_score {
        // Cast usize to f64 for scoring: values are bounded by PATTERNS_LIMIT.
        #[allow(clippy::cast_precision_loss)]
        let confidence = (0.7_f64 + (concept_score as f64) * 0.1).min(0.90);
        assert!(confidence > 0.0);
        assert!(confidence <= 1.0);
        return Some(LookupResult {
            entity_type: EntityType::Concept,
            confidence,
            source: "context_disambiguated".to_string(),
            name: word.to_string(),
            needs_disambiguation: false,
            disambiguated_by: Some("context_patterns".to_string()),
        });
    }

    // Scores are equal — cannot disambiguate, let caller fall through to the
    // registered name (person result) as the Python implementation does.
    None
}

/// Count how many regex patterns in `patterns` match `context_lower`.
///
/// Each pattern contains `{name}`, which is replaced with the regex-escaped
/// `name_lower` before matching. Up to `PATTERNS_LIMIT` patterns are checked.
fn entity_registry_count_pattern_matches(
    name_lower: &str,
    context_lower: &str,
    patterns: &[&str],
) -> usize {
    assert!(!name_lower.is_empty());
    assert!(!patterns.is_empty());

    let escaped = regex::escape(name_lower);
    let limit = patterns.len().min(PATTERNS_LIMIT);
    let mut count: usize = 0;

    for pattern in patterns.iter().take(limit) {
        let re_str = pattern.replace("{name}", &escaped);
        if let Ok(re) = Regex::new(&re_str)
            && re.is_match(context_lower)
        {
            count += 1;
        }
    }

    assert!(count <= limit, "match count must not exceed pattern limit");
    count
}

/// Perform a Wikipedia summary lookup for `word` and return a `WikiEntry`.
///
/// Network errors and 404s are mapped to `EntityType::Unknown` rather than
/// propagated, so the registry always gets a usable result to cache.
fn entity_registry_wikipedia_lookup(word: &str) -> WikiEntry {
    assert!(!word.is_empty());

    match entity_registry_wikipedia_fetch(word) {
        Ok(value) => {
            let entry = entity_registry_wikipedia_parse(&value, word);
            assert!(entry.confidence >= 0.0);
            assert!(entry.confidence <= 1.0);
            entry
        }
        Err(error) => {
            let message = error.to_string();
            let is_not_found = message.contains("404") || message.contains("Not Found");
            WikiEntry {
                inferred_type: EntityType::Unknown,
                confidence: if is_not_found { 0.3 } else { 0.0 },
                confirmed: false,
                wiki_summary: None,
                wiki_title: None,
                note: Some(if is_not_found {
                    "not found in Wikipedia".to_string()
                } else {
                    format!("lookup failed: {message}")
                }),
            }
        }
    }
}

/// Make an HTTP GET request to the Wikipedia summary REST API.
///
/// Spaces in `word` are replaced with underscores per Wikipedia URL convention.
/// Returns the parsed JSON body on success, or a project `Error` on failure.
fn entity_registry_wikipedia_fetch(word: &str) -> Result<serde_json::Value> {
    assert!(!word.is_empty());
    // WIKI_TIMEOUT_SECS > 0 is guaranteed by the compile-time const assertion above.

    let title = word.replace(' ', "_");
    let url = format!("{WIKI_URL_BASE}{title}");

    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(WIKI_TIMEOUT_SECS)))
        .build()
        .new_agent();
    let response = agent
        .get(&url)
        .header("User-Agent", "MemPalace/1.0")
        .call()
        .map_err(|error| CrateError::Http(error.to_string()))?;
    let text = response
        .into_body()
        .read_to_string()
        .map_err(|error| CrateError::Http(error.to_string()))?;

    debug_assert!(
        !text.is_empty(),
        "Wikipedia response body must not be empty"
    );
    Ok(serde_json::from_str(&text)?)
}

/// Parse a Wikipedia summary JSON response into a `WikiEntry`.
///
/// Checks for disambiguation pages, name indicator phrases, and place indicator
/// phrases. Falls back to `Concept` when none match.
fn entity_registry_wikipedia_parse(value: &serde_json::Value, word: &str) -> WikiEntry {
    assert!(!word.is_empty());

    let page_type = value["type"].as_str().unwrap_or_default();
    let extract = value["extract"].as_str().unwrap_or_default().to_lowercase();
    let title = value["title"].as_str().unwrap_or(word).to_string();
    let summary = if extract.is_empty() {
        None
    } else {
        Some(extract[..extract.len().min(WIKI_SUMMARY_MAX)].to_string())
    };

    if page_type == "disambiguation" {
        let desc = value["description"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase();
        let has_name_hint = desc.contains("name") || desc.contains("given name");
        return WikiEntry {
            inferred_type: if has_name_hint {
                EntityType::Person
            } else {
                EntityType::Ambiguous
            },
            confidence: if has_name_hint { 0.65 } else { 0.4 },
            confirmed: false,
            wiki_summary: summary,
            wiki_title: Some(title),
            note: Some("disambiguation page".to_string()),
        };
    }

    if NAME_INDICATOR_PHRASES
        .iter()
        .any(|phrase| extract.contains(*phrase))
    {
        let word_lower = word.to_lowercase();
        let high_confidence = extract.contains(&format!("{word_lower} is a"))
            || extract.contains(&format!("{word_lower}(name"));
        return WikiEntry {
            inferred_type: EntityType::Person,
            confidence: if high_confidence { 0.90 } else { 0.80 },
            confirmed: false,
            wiki_summary: summary,
            wiki_title: Some(title),
            note: None,
        };
    }

    if PLACE_INDICATOR_PHRASES
        .iter()
        .any(|phrase| extract.contains(*phrase))
    {
        return WikiEntry {
            inferred_type: EntityType::Place,
            confidence: 0.80,
            confirmed: false,
            wiki_summary: summary,
            wiki_title: Some(title),
            note: None,
        };
    }

    WikiEntry {
        inferred_type: EntityType::Concept,
        confidence: 0.60,
        confirmed: false,
        wiki_summary: summary,
        wiki_title: Some(title),
        note: None,
    }
}

/// Detect person entity candidates in `text` by writing it to a temp file.
///
/// Returns only the `people` list from the detection result, bounded to the
/// temp file's content. Called by [`EntityRegistry::learn_from_text`].
fn learn_from_text_detect(text: &str, languages: &[&str]) -> Vec<DetectedEntity> {
    assert!(!text.is_empty());
    assert!(!languages.is_empty());

    let Ok(mut temp_file) = NamedTempFile::new() else {
        return Vec::new();
    };
    if temp_file.write_all(text.as_bytes()).is_err() {
        return Vec::new();
    }
    if temp_file.flush().is_err() {
        return Vec::new();
    }

    let temp_path = temp_file.path().to_path_buf();
    // temp_file remains alive (and the on-disk file exists) through detect_entities.
    let file_paths: Vec<&Path> = vec![temp_path.as_path()];
    let detection = detect_entities(&file_paths, 1, languages);

    assert!(
        detection.people.len() <= 1_000,
        "learn_from_text_detect: people candidates must be bounded"
    );
    detection.people
}

/// Build a `PersonEntry` for a learned entity.
///
/// Sets `source = "learned"` and derives context from `mode`. Called by
/// [`EntityRegistry::learn_from_text`].
fn learn_from_text_make_entry(entity: &DetectedEntity, mode: &str) -> PersonEntry {
    assert!(!entity.name.is_empty());
    assert!(!mode.is_empty());

    let ctx = if mode == "combo" { "personal" } else { mode };
    PersonEntry {
        source: "learned".to_string(),
        contexts: vec![ctx.to_string()],
        aliases: Vec::new(),
        relationship: String::new(),
        confidence: entity.confidence,
        // Cast usize to u64 for storage: frequency counts won't exceed u64::MAX.
        #[allow(clippy::cast_possible_truncation)]
        seen_count: entity.frequency as u64,
        canonical: None,
    }
}

/// Check if `name` word-boundary matches `query` and passes ambiguity validation.
///
/// Returns `true` when the regex matches AND (for ambiguous names) context
/// patterns indicate a person usage. Called by [`EntityRegistry::extract_people_from_query`].
fn extract_people_from_query_matches(
    name: &str,
    query: &str,
    ambiguous_flags: &[String],
    person_entry: &PersonEntry,
) -> bool {
    assert!(!name.is_empty());
    assert!(!query.is_empty());

    let pattern = format!(r"(?i)\b{}\b", regex::escape(name));
    let Ok(re) = Regex::new(&pattern) else {
        return false;
    };
    if !re.is_match(query) {
        return false;
    }

    let is_ambiguous = ambiguous_flags
        .iter()
        .any(|flag| flag == &name.to_lowercase());
    if is_ambiguous {
        matches!(
            entity_registry_disambiguate(name, query, person_entry),
            Some(LookupResult {
                entity_type: EntityType::Person,
                ..
            })
        )
    } else {
        true
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

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
    fn lookup_person_returns_person_type() {
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
        assert_eq!(result.entity_type, EntityType::Person);
        assert!((result.confidence - 1.0).abs() < f64::EPSILON);
        assert_eq!(result.source, "onboarding");
    }

    #[test]
    fn lookup_unknown_returns_unknown_type() {
        let (_temp, registry) = test_registry();
        let result = registry.lookup("Zephyrina", "");
        assert_eq!(result.entity_type, EntityType::Unknown);
        assert!(result.confidence < f64::EPSILON);
        assert_eq!(result.source, "none");
    }

    #[test]
    fn lookup_project_returns_project_type() {
        let (temp, mut registry) = test_registry();
        let projects = vec!["MemPalace".to_string()];
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            registry.seed("personal", &[], &projects).expect("seed");
        });
        let result = registry.lookup("mempalace", "");
        assert_eq!(result.entity_type, EntityType::Project);
        assert_eq!(result.name, "MemPalace");
    }

    // ── context disambiguation ────────────────────────────────────────────────

    #[test]
    fn disambiguate_person_score_wins_returns_person() {
        let entry = PersonEntry {
            source: "onboarding".to_string(),
            contexts: vec!["personal".to_string()],
            aliases: Vec::new(),
            relationship: String::new(),
            confidence: 1.0,
            seen_count: 0,
            canonical: None,
        };
        // "I went with May today" → strong person signal.
        let result = entity_registry_disambiguate("May", "I went with May today", &entry);
        assert!(result.is_some(), "person-context sentence must resolve");
        assert_eq!(result.expect("has result").entity_type, EntityType::Person);
    }

    #[test]
    fn disambiguate_concept_score_wins_returns_concept() {
        let entry = PersonEntry {
            source: "onboarding".to_string(),
            contexts: vec!["personal".to_string()],
            aliases: Vec::new(),
            relationship: String::new(),
            confidence: 1.0,
            seen_count: 0,
            canonical: None,
        };
        // "if you ever get the chance" → strong concept signal.
        let result = entity_registry_disambiguate("ever", "if you ever get the chance", &entry);
        assert!(result.is_some(), "concept-context sentence must resolve");
        assert_eq!(result.expect("has result").entity_type, EntityType::Concept);
    }

    // ── research (local-only) ────────────────────────────────────────────────

    #[test]
    fn research_without_network_returns_unknown() {
        let (_temp, mut registry) = test_registry();
        let entry = registry.research("Xylophone", false);
        assert_eq!(
            entry.inferred_type,
            EntityType::Unknown,
            "local-only research must return Unknown for uncached word"
        );
        assert!(
            entry.note.is_some(),
            "note must explain why network was not used"
        );
    }

    #[test]
    fn research_caches_result_after_network_call_stub() {
        // Verify that after inserting a mock entry into wiki_cache manually,
        // research returns the cached entry without a network call.
        let (_temp, mut registry) = test_registry();
        registry.data.wiki_cache.insert(
            "Lirael".to_string(),
            WikiEntry {
                inferred_type: EntityType::Person,
                confidence: 0.85,
                confirmed: true,
                wiki_summary: Some("character name".to_string()),
                wiki_title: Some("Lirael".to_string()),
                note: None,
            },
        );
        let entry = registry.research("Lirael", false);
        assert_eq!(entry.inferred_type, EntityType::Person);
        assert!((entry.confidence - 0.85).abs() < f64::EPSILON);
    }

    // ── count_pattern_matches ─────────────────────────────────────────────────

    #[test]
    fn count_pattern_matches_person_sentence() {
        let count = entity_registry_count_pattern_matches(
            "alice",
            "alice said hello to me",
            PERSON_CONTEXT_PATTERNS,
        );
        assert!(
            count > 0,
            "person-context sentence must match at least one pattern"
        );
    }

    #[test]
    fn count_pattern_matches_concept_sentence() {
        let count = entity_registry_count_pattern_matches(
            "ever",
            "have you ever wondered",
            CONCEPT_CONTEXT_PATTERNS,
        );
        assert!(
            count > 0,
            "concept-context sentence must match at least one pattern"
        );
    }

    // ── summary ──────────────────────────────────────────────────────────────

    #[test]
    fn summary_returns_non_empty_string() {
        let (_temp, registry) = test_registry();
        let text = registry.summary();
        assert!(!text.is_empty(), "summary must return a non-empty string");
        assert!(text.contains("Mode:"), "summary must include Mode label");
        assert!(
            text.contains("People:"),
            "summary must include People label"
        );
    }
}
