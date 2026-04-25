//! Global entity registry — `known_entities.json` in the mempalace data dir.
//!
//! The miner reads this registry at mine time to tag drawers with recognised
//! entity names. [`add_to_known_entities`] merges newly confirmed entities (from
//! `mempalace init`) into the registry without disturbing existing entries.
//!
//! Two on-disk shapes are supported for backwards compatibility with the Python tool:
//! - **List** `["Alice", "Bob"]` — the canonical shape written by this module.
//! - **Dict** `{"Alice": "CODE"}` — dialect-registry shape; new names are added as
//!   keys with `null` values so existing code assignments are preserved.
//!
//! Public API:
//! - [`registry_path`] — path to the registry file
//! - [`add_to_known_entities`] — merge entities into the registry

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use crate::config::config_dir;
use crate::error::Result;

// Safety bound: maximum names processed per category per call.
// In practice init pipelines produce < 30 entities; this limit guards against
// pathological inputs without truncating any realistic use case.
const NAMES_PER_CATEGORY_LIMIT: usize = 10_000;

const _: () = assert!(NAMES_PER_CATEGORY_LIMIT > 0);

// ===================== PUBLIC API =====================

/// Returns the path to the global known-entities registry file.
///
/// Resolves via `config_dir()`, which respects `MEMPALACE_DIR` and XDG conventions.
pub fn registry_path() -> PathBuf {
    let path = config_dir().join("known_entities.json");
    assert!(!path.as_os_str().is_empty());
    assert!(path.ends_with("known_entities.json"));
    path
}

/// Merge `entities_by_category` into the global known-entities registry.
///
/// - List-format categories: union case-insensitively, original order preserved.
/// - Dict-format categories: new names appended as keys with `null` values.
/// - Missing categories: created as a fresh deduplicated list.
/// - Corrupted or non-object registry JSON: starts fresh.
///
/// The registry file is chmod 0o600 after write so only the current user can
/// read it. Returns the registry path so callers can log or display it.
pub fn add_to_known_entities<S: std::hash::BuildHasher>(
    entities_by_category: &HashMap<String, Vec<String>, S>,
) -> Result<PathBuf> {
    let path = registry_path();
    assert!(!path.as_os_str().is_empty());

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut existing = add_to_known_entities_load(&path);

    for (category, names) in entities_by_category {
        if names.is_empty() {
            continue;
        }
        add_to_known_entities_merge_category(&mut existing, category, names);
    }

    let json = serde_json::to_string_pretty(&Value::Object(existing))?;
    std::fs::write(&path, json.as_bytes())?;

    // chmod 0o600: only the current user may read or write the entity registry.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }

    // Pair assertion: registry file must exist after write.
    debug_assert!(path.exists(), "registry must exist after write");

    Ok(path)
}

// ===================== PRIVATE HELPERS =====================

/// Load the existing registry as a JSON object, or return an empty object.
///
/// Returns an empty `Map` if the file is absent, unreadable, malformed, or
/// not a JSON object. Called by [`add_to_known_entities`].
fn add_to_known_entities_load(path: &Path) -> Map<String, Value> {
    assert!(!path.as_os_str().is_empty());
    // Negative space: path must not be a directory (we expect a file or absent).
    assert!(!path.is_dir());

    if !path.exists() {
        return Map::new();
    }
    let Ok(text) = std::fs::read_to_string(path) else {
        return Map::new();
    };
    let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
        return Map::new();
    };
    match parsed {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

/// Merge `names` into `category` inside `existing`.
///
/// Dispatches to list-merge, dict-merge, or new-category helpers based on the
/// current shape of the category value. Called by [`add_to_known_entities`].
fn add_to_known_entities_merge_category(
    existing: &mut Map<String, Value>,
    category: &str,
    names: &[String],
) {
    assert!(!category.is_empty());
    assert!(!names.is_empty());

    match existing.get_mut(category) {
        Some(Value::Array(list)) => {
            add_to_known_entities_merge_list(list, names);
        }
        Some(Value::Object(dict)) => {
            add_to_known_entities_merge_dict(dict, names);
        }
        _ => {
            // Missing or unrecognized shape — seed as a fresh deduplicated list.
            let deduped = add_to_known_entities_new_list(names);
            existing.insert(category.to_string(), Value::Array(deduped));
        }
    }
}

/// Append names to an existing list-format category, deduplicating case-insensitively.
///
/// Original order of pre-existing entries is preserved; new names are appended.
/// Called by [`add_to_known_entities_merge_category`] when the on-disk shape is a JSON array.
fn add_to_known_entities_merge_list(list: &mut Vec<Value>, names: &[String]) {
    assert!(!names.is_empty());

    let mut seen_lower: HashSet<String> = list
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_lowercase)
        .collect();

    let len_before = list.len();

    for name in names.iter().take(NAMES_PER_CATEGORY_LIMIT) {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_lowercase();
        if seen_lower.contains(&lower) {
            continue;
        }
        seen_lower.insert(lower);
        list.push(Value::String(trimmed.to_string()));
    }

    // Postcondition: list can only grow (dedup never removes pre-existing entries).
    debug_assert!(list.len() >= len_before);
}

/// Add names to an existing dict-format category as keys with `null` values.
///
/// The dict shape (`{"Alice": "CODE"}`) is the dialect-registry format. New names
/// are appended without overwriting existing code assignments.
/// Called by [`add_to_known_entities_merge_category`] when the on-disk shape is a JSON object.
fn add_to_known_entities_merge_dict(dict: &mut Map<String, Value>, names: &[String]) {
    assert!(!names.is_empty());

    let mut seen_lower: HashSet<String> = dict.keys().map(|k| k.to_lowercase()).collect();
    let len_before = dict.len();

    for name in names.iter().take(NAMES_PER_CATEGORY_LIMIT) {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_lowercase();
        if seen_lower.contains(&lower) {
            continue;
        }
        seen_lower.insert(lower);
        dict.insert(trimmed.to_string(), Value::Null);
    }

    // Postcondition: dict can only grow (dict-merge never removes existing keys).
    debug_assert!(dict.len() >= len_before);
}

/// Build a fresh deduplicated list from `names` for a new category.
///
/// Input order is preserved; case-insensitive duplicates are dropped after
/// the first occurrence. Called by [`add_to_known_entities_merge_category`]
/// when the category does not yet exist in the registry.
fn add_to_known_entities_new_list(names: &[String]) -> Vec<Value> {
    assert!(!names.is_empty());

    let mut seen: HashSet<String> = HashSet::new();
    let mut result: Vec<Value> = Vec::with_capacity(names.len().min(NAMES_PER_CATEGORY_LIMIT));

    for name in names.iter().take(NAMES_PER_CATEGORY_LIMIT) {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_lowercase();
        if seen.contains(&lower) {
            continue;
        }
        seen.insert(lower);
        result.push(Value::String(trimmed.to_string()));
    }

    // Postcondition: deduplicated list cannot be larger than the input.
    debug_assert!(result.len() <= names.len());
    result
}

// ===================== TESTS =====================

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn with_temp_registry<F: FnOnce(PathBuf)>(f: F) {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("known_entities.json");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || f(path));
    }

    // -- registry_path --

    #[test]
    fn registry_path_ends_with_known_entities_json() {
        // The registry path must end with the expected filename.
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let path = registry_path();
            assert!(path.ends_with("known_entities.json"));
            assert!(!path.as_os_str().is_empty());
        });
    }

    // -- add_to_known_entities: creation --

    #[test]
    fn add_creates_registry_when_absent() {
        // Registry must be created when it does not yet exist.
        with_temp_registry(|path| {
            assert!(!path.exists());
            let mut entities = HashMap::new();
            entities.insert(
                "people".to_string(),
                vec!["Alice".to_string(), "Bob".to_string()],
            );
            let result = add_to_known_entities(&entities).expect("add must succeed");
            assert!(result.ends_with("known_entities.json"));
            assert!(path.exists(), "registry must be created");
        });
    }

    #[test]
    fn add_returns_registry_path() {
        // Return value must be the registry path.
        with_temp_registry(|path| {
            let mut entities = HashMap::new();
            entities.insert("people".to_string(), vec!["Alice".to_string()]);
            let result = add_to_known_entities(&entities).expect("add must succeed");
            assert_eq!(result, path);
            assert!(result.ends_with("known_entities.json"));
        });
    }

    // -- add_to_known_entities: union / dedup --

    #[test]
    fn add_unions_with_existing_list_category() {
        // Existing entries must be preserved; new entries appended without duplicates.
        with_temp_registry(|path| {
            std::fs::write(&path, r#"{"people":["Alice","Bob"]}"#).expect("write");
            let mut entities = HashMap::new();
            entities.insert(
                "people".to_string(),
                vec!["Bob".to_string(), "Carol".to_string()],
            );
            add_to_known_entities(&entities).expect("add");
            let data: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
                    .expect("parse");
            let names: Vec<&str> = data["people"]
                .as_array()
                .expect("array")
                .iter()
                .filter_map(|entry| entry.as_str())
                .collect();
            assert_eq!(names, ["Alice", "Bob", "Carol"]);
            assert_eq!(names.len(), 3, "Bob must not be duplicated");
        });
    }

    #[test]
    fn add_case_insensitive_dedup_preserves_first_variant() {
        // "alice" and "ALICE" must not create a second entry when "Alice" already exists.
        with_temp_registry(|path| {
            std::fs::write(&path, r#"{"people":["Alice"]}"#).expect("write");
            let mut entities = HashMap::new();
            entities.insert(
                "people".to_string(),
                vec!["alice".to_string(), "ALICE".to_string(), "Bob".to_string()],
            );
            add_to_known_entities(&entities).expect("add");
            let data: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
                    .expect("parse");
            let names: Vec<&str> = data["people"]
                .as_array()
                .expect("array")
                .iter()
                .filter_map(|entry| entry.as_str())
                .collect();
            assert_eq!(names, ["Alice", "Bob"]);
            assert_eq!(names.len(), 2, "case variants must not duplicate Alice");
        });
    }

    #[test]
    fn add_preserves_untouched_categories() {
        // Categories not mentioned in input must be left unchanged.
        with_temp_registry(|path| {
            std::fs::write(&path, r#"{"people":["Alice"],"places":["Paris","Tokyo"]}"#)
                .expect("write");
            let mut entities = HashMap::new();
            entities.insert("people".to_string(), vec!["Bob".to_string()]);
            add_to_known_entities(&entities).expect("add");
            let data: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
                    .expect("parse");
            let places: Vec<&str> = data["places"]
                .as_array()
                .expect("places array")
                .iter()
                .filter_map(|entry| entry.as_str())
                .collect();
            assert_eq!(places, ["Paris", "Tokyo"], "places must be unchanged");
            assert_eq!(places.len(), 2);
        });
    }

    #[test]
    fn add_adds_new_categories() {
        // A category absent from the registry must be created as a new list.
        with_temp_registry(|path| {
            std::fs::write(&path, r#"{"people":["Alice"]}"#).expect("write");
            let mut entities = HashMap::new();
            entities.insert(
                "projects".to_string(),
                vec!["foo".to_string(), "bar".to_string()],
            );
            add_to_known_entities(&entities).expect("add");
            let data: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
                    .expect("parse");
            let projects: Vec<&str> = data["projects"]
                .as_array()
                .expect("projects array")
                .iter()
                .filter_map(|entry| entry.as_str())
                .collect();
            assert!(projects.contains(&"foo"), "foo must be present");
            assert!(projects.contains(&"bar"), "bar must be present");
        });
    }

    // -- add_to_known_entities: dict-format existing registry --

    #[test]
    fn add_dict_format_category_gets_new_keys() {
        // Dict-format categories must have new names appended as keys with null values.
        with_temp_registry(|path| {
            std::fs::write(&path, r#"{"people":{"Alice":"ALC","Bob":"BOB"}}"#).expect("write");
            let mut entities = HashMap::new();
            entities.insert(
                "people".to_string(),
                vec!["Alice".to_string(), "Carol".to_string()],
            );
            add_to_known_entities(&entities).expect("add");
            let data: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
                    .expect("parse");
            let obj = data["people"].as_object().expect("object");
            assert_eq!(
                obj["Alice"].as_str(),
                Some("ALC"),
                "existing code preserved"
            );
            assert_eq!(obj["Bob"].as_str(), Some("BOB"), "Bob untouched");
            assert!(obj.contains_key("Carol"), "Carol must be added");
            assert!(obj["Carol"].is_null(), "Carol gets null code");
        });
    }

    // -- error tolerance --

    #[test]
    fn add_malformed_registry_starts_fresh() {
        // A corrupted registry must be overwritten rather than causing an error.
        with_temp_registry(|path| {
            std::fs::write(&path, "{ not valid json").expect("write");
            let mut entities = HashMap::new();
            entities.insert("people".to_string(), vec!["Alice".to_string()]);
            add_to_known_entities(&entities).expect("add must succeed despite corruption");
            let data: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
                    .expect("parse");
            let names: Vec<&str> = data["people"]
                .as_array()
                .expect("array")
                .iter()
                .filter_map(|entry| entry.as_str())
                .collect();
            assert_eq!(names, ["Alice"]);
            assert!(!names.is_empty(), "fresh start must have Alice");
        });
    }

    #[test]
    fn add_skips_empty_names() {
        // Empty strings in the input list must not be written to the registry.
        with_temp_registry(|_path| {
            let mut entities = HashMap::new();
            entities.insert(
                "people".to_string(),
                vec!["Alice".to_string(), String::new(), "  ".to_string()],
            );
            let result_path = add_to_known_entities(&entities).expect("add");
            let data: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&result_path).expect("read"))
                    .expect("parse");
            let names: Vec<&str> = data["people"]
                .as_array()
                .expect("array")
                .iter()
                .filter_map(|entry| entry.as_str())
                .collect();
            assert_eq!(names, ["Alice"], "only Alice must be written");
            assert_eq!(names.len(), 1, "empty names must be skipped");
        });
    }

    #[test]
    fn add_dedupes_within_input() {
        // Duplicate names within the same input call must not be written twice.
        with_temp_registry(|_path| {
            let mut entities = HashMap::new();
            entities.insert(
                "people".to_string(),
                vec![
                    "Alice".to_string(),
                    "alice".to_string(),
                    "Alice".to_string(),
                ],
            );
            let result_path = add_to_known_entities(&entities).expect("add");
            let data: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&result_path).expect("read"))
                    .expect("parse");
            let names: Vec<&str> = data["people"]
                .as_array()
                .expect("array")
                .iter()
                .filter_map(|entry| entry.as_str())
                .collect();
            assert_eq!(names.len(), 1, "duplicates within input must be collapsed");
            assert_eq!(names[0], "Alice");
        });
    }
}
