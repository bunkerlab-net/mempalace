//! Source adapter registry (RFC 002 §3).
//!
//! Third-party adapters register themselves via [`register`]. Core resolves an
//! adapter by name via [`get_adapter`]. Unlike Python's entry-point discovery,
//! Rust adapters are registered explicitly — there is no automatic plugin scan.
//!
//! The default adapter name is `"filesystem"` (§3.3), preserved for future use
//! when the filesystem miner is migrated onto `SourceAdapter`.

use std::collections::HashMap;
use std::sync::{LazyLock, PoisonError, RwLock};

use super::adapter::SourceAdapter;

/// Stable name returned by [`resolve_adapter_name`] when no explicit name is given.
pub const DEFAULT_ADAPTER: &str = "filesystem";

const _: () = assert!(!DEFAULT_ADAPTER.is_empty());

/// Maps adapter name → constructor function.
type RegistryMap = HashMap<&'static str, fn() -> Box<dyn SourceAdapter>>;

/// Global adapter registry — instances are created fresh on each `get_adapter` call
/// to keep the registry itself free of mutable state.
static REGISTRY: LazyLock<RwLock<RegistryMap>> = LazyLock::new(|| RwLock::new(HashMap::new()));

/// Register `constructor` under `name`.
///
/// Explicit registration wins over any later call with the same name. Safe to
/// call from `main` before processing begins.
pub fn register(name: &'static str, constructor: fn() -> Box<dyn SourceAdapter>) {
    assert!(!name.is_empty(), "adapter name must not be empty");

    let mut map = REGISTRY.write().unwrap_or_else(PoisonError::into_inner);
    map.insert(name, constructor);

    assert!(
        map.contains_key(name),
        "register: adapter must be present after insert"
    );
}

/// Remove an adapter registration (primarily for tests).
pub fn unregister(name: &'static str) {
    assert!(!name.is_empty());
    let mut map = REGISTRY.write().unwrap_or_else(PoisonError::into_inner);
    map.remove(name);
}

/// Return a fresh adapter instance for `name`, or `None` if not registered.
///
/// Each call creates a new instance via the registered constructor.
pub fn get_adapter(name: &str) -> Option<Box<dyn SourceAdapter>> {
    assert!(!name.is_empty(), "get_adapter: name must not be empty");

    let map = REGISTRY.read().unwrap_or_else(PoisonError::into_inner);
    let constructor = map.get(name)?;
    let adapter = constructor();

    assert_eq!(
        adapter.name(),
        name,
        "get_adapter: adapter name must match registration key"
    );
    Some(adapter)
}

/// Return a sorted list of all registered adapter names.
pub fn available_adapters() -> Vec<String> {
    let map = REGISTRY.read().unwrap_or_else(PoisonError::into_inner);
    let mut names: Vec<String> = map.keys().map(|&name| name.to_string()).collect();
    names.sort_unstable();

    assert!(
        names.len() <= map.len(),
        "available_adapters: sorted list must not exceed registry size"
    );
    names
}

/// Resolve the adapter name per RFC 002 §3.3 priority order.
///
/// 1. Explicit `--source` flag (`explicit`)
/// 2. Per-source config value (`config_value`)
/// 3. Default (`DEFAULT_ADAPTER = "filesystem"`)
pub fn resolve_adapter_name<'a>(
    explicit: Option<&'a str>,
    config_value: Option<&'a str>,
) -> &'a str {
    [explicit, config_value]
        .into_iter()
        .flatten()
        .find(|name| !name.is_empty())
        .unwrap_or(DEFAULT_ADAPTER)
}

#[cfg(test)]
// Tests use .expect() for clarity on failure.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::sources::adapter::{AdapterSchema, DrawerRecord, SourceRef, SourceSummary};
    use std::collections::HashMap;

    // ── test adapter ──────────────────────────────────────────────────────

    struct FakeAdapter;
    impl SourceAdapter for FakeAdapter {
        fn name(&self) -> &'static str {
            "test_registry_fake"
        }
        fn adapter_version(&self) -> &'static str {
            "0.0.1"
        }
        fn ingest(&self, _source: &SourceRef) -> crate::error::Result<Vec<DrawerRecord>> {
            Ok(vec![])
        }
        fn describe_schema(&self) -> AdapterSchema {
            AdapterSchema {
                fields: HashMap::new(),
                version: "0.0.1".to_string(),
            }
        }
        fn source_summary(&self, _source: &SourceRef) -> SourceSummary {
            SourceSummary {
                description: "fake".to_string(),
                item_count: Some(0),
            }
        }
    }

    fn fake_constructor() -> Box<dyn SourceAdapter> {
        Box::new(FakeAdapter)
    }

    // ── register / get_adapter ─────────────────────────────────────────────

    #[test]
    fn register_and_get_adapter_round_trip() {
        register("test_registry_fake", fake_constructor);
        let adapter =
            get_adapter("test_registry_fake").expect("adapter must be found after registration");
        assert_eq!(adapter.name(), "test_registry_fake");
        unregister("test_registry_fake");
    }

    #[test]
    fn get_adapter_unknown_name_returns_none() {
        let result = get_adapter("__no_such_adapter_xyzzy__");
        assert!(result.is_none(), "unknown adapter must return None");
    }

    // ── available_adapters / unregister ──────────────────────────────────────

    #[test]
    fn available_adapters_returns_names_in_sorted_order() {
        // Register two adapters with keys that sort in a known order ("alpha" < "zeta").
        // Using unique prefixes to avoid races with the round-trip test.
        register("test_avail_zeta", fake_constructor);
        register("test_avail_alpha", fake_constructor);

        let names = available_adapters();
        let alpha_pos = names.iter().position(|n| n == "test_avail_alpha");
        let zeta_pos = names.iter().position(|n| n == "test_avail_zeta");

        assert!(alpha_pos.is_some(), "test_avail_alpha must be listed");
        assert!(zeta_pos.is_some(), "test_avail_zeta must be listed");
        assert!(
            alpha_pos.expect("alpha_pos must be Some after is_some check")
                < zeta_pos.expect("zeta_pos must be Some after is_some check"),
            "available_adapters must return names in ascending sorted order"
        );

        unregister("test_avail_alpha");
        unregister("test_avail_zeta");
    }

    #[test]
    fn unregister_makes_adapter_unavailable() {
        // Unique key so this test doesn't race with register_and_get_adapter_round_trip.
        register("test_avail_unregister_only", fake_constructor);
        unregister("test_avail_unregister_only");
        // get_adapter returns None via `?` before the name-check assertion fires.
        let result = get_adapter("test_avail_unregister_only");
        assert!(
            result.is_none(),
            "adapter must not be accessible after unregister"
        );
    }

    // ── resolve_adapter_name ───────────────────────────────────────────────

    #[test]
    fn resolve_adapter_explicit_wins_over_config() {
        let name = resolve_adapter_name(Some("explicit_src"), Some("config_src"));
        assert_eq!(name, "explicit_src");
    }

    #[test]
    fn resolve_adapter_config_wins_over_default() {
        // When explicit is None, the config value must be returned.
        let name = resolve_adapter_name(None, Some("config_src"));
        assert_eq!(name, "config_src");
    }

    #[test]
    fn resolve_adapter_empty_explicit_falls_through_to_config() {
        // An empty string explicit is skipped by the `!name.is_empty()` filter.
        let name = resolve_adapter_name(Some(""), Some("config_src"));
        assert_eq!(name, "config_src");
    }

    #[test]
    fn resolve_adapter_falls_back_to_default() {
        let name = resolve_adapter_name(None, None);
        assert_eq!(name, DEFAULT_ADAPTER);
    }
}
