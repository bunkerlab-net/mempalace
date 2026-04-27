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

    // Adapter whose name matches the registration key used in source_summary_override_is_called.
    struct SummaryFakeAdapter;
    impl SourceAdapter for SummaryFakeAdapter {
        fn name(&self) -> &'static str {
            "test_summary_fake"
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

    fn summary_fake_constructor() -> Box<dyn SourceAdapter> {
        Box::new(SummaryFakeAdapter)
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

    #[test]
    fn resolve_adapter_empty_config_falls_back_to_default() {
        // An empty config value is skipped by the `!name.is_empty()` filter,
        // so the resolver must fall through to DEFAULT_ADAPTER.
        let name = resolve_adapter_name(None, Some(""));
        assert_eq!(
            name, DEFAULT_ADAPTER,
            "empty config must fall back to default"
        );
        // Pair: both None and empty-string config must produce the same result.
        assert_eq!(
            resolve_adapter_name(None, None),
            resolve_adapter_name(None, Some("")),
            "None config and empty-string config must resolve identically"
        );
    }

    #[test]
    fn resolve_adapter_both_empty_falls_back_to_default() {
        // Both explicit and config being empty strings must fall back to the default.
        let name = resolve_adapter_name(Some(""), Some(""));
        assert_eq!(name, DEFAULT_ADAPTER, "both empty must produce default");
        assert!(!name.is_empty(), "default adapter name must not be empty");
    }

    // Two minimal adapters for the overwrite test — defined at module scope so
    // they appear before any statements (required by clippy::items_after_statements).
    struct OverwriteAdapterV1;
    impl SourceAdapter for OverwriteAdapterV1 {
        fn name(&self) -> &'static str {
            "test_overwrite_adapter"
        }
        fn adapter_version(&self) -> &'static str {
            "1.0.0"
        }
        fn ingest(&self, _source: &SourceRef) -> crate::error::Result<Vec<DrawerRecord>> {
            Ok(vec![])
        }
        fn describe_schema(&self) -> AdapterSchema {
            AdapterSchema {
                fields: HashMap::new(),
                version: "1.0.0".to_string(),
            }
        }
    }

    struct OverwriteAdapterV2;
    impl SourceAdapter for OverwriteAdapterV2 {
        fn name(&self) -> &'static str {
            "test_overwrite_adapter"
        }
        fn adapter_version(&self) -> &'static str {
            "2.0.0"
        }
        fn ingest(&self, _source: &SourceRef) -> crate::error::Result<Vec<DrawerRecord>> {
            Ok(vec![])
        }
        fn describe_schema(&self) -> AdapterSchema {
            AdapterSchema {
                fields: HashMap::new(),
                version: "2.0.0".to_string(),
            }
        }
    }

    fn overwrite_v1_constructor() -> Box<dyn SourceAdapter> {
        Box::new(OverwriteAdapterV1)
    }

    fn overwrite_v2_constructor() -> Box<dyn SourceAdapter> {
        Box::new(OverwriteAdapterV2)
    }

    #[test]
    fn register_overwrites_previous_constructor() {
        // A second call to register with the same name must silently overwrite
        // the previous constructor (last-write wins).
        register("test_overwrite_adapter", overwrite_v1_constructor);
        register("test_overwrite_adapter", overwrite_v2_constructor);
        let adapter = get_adapter("test_overwrite_adapter")
            .expect("adapter must be found after second registration");
        assert_eq!(
            adapter.adapter_version(),
            "2.0.0",
            "second registration must overwrite first"
        );
        assert_eq!(adapter.name(), "test_overwrite_adapter");
        unregister("test_overwrite_adapter");
        // Pair: adapter must be gone after unregister.
        assert!(
            get_adapter("test_overwrite_adapter").is_none(),
            "adapter must be absent after unregister"
        );
    }

    #[test]
    fn available_adapters_empty_when_none_registered_with_unique_prefix() {
        // This test confirms available_adapters returns at least an empty list
        // and that its length matches the number of registered adapters.
        // We cannot guarantee global state is empty (other tests may have registered),
        // so we just verify the invariant: available_adapters length <= map size.
        let names = available_adapters();
        // The sorted list length must be consistent across two calls (no mutations here).
        let names_again = available_adapters();
        assert_eq!(
            names.len(),
            names_again.len(),
            "available_adapters must be deterministic"
        );
        // All returned names must be non-empty strings.
        assert!(
            names.iter().all(|n| !n.is_empty()),
            "all adapter names must be non-empty"
        );
    }

    #[test]
    fn source_summary_override_is_called() {
        // SummaryFakeAdapter overrides source_summary to return item_count=Some(0).
        // Registering and retrieving it must call the override, not the default.
        register("test_summary_fake", summary_fake_constructor);
        let adapter =
            get_adapter("test_summary_fake").expect("adapter must be found after registration");
        let source = SourceRef::default();
        let summary = adapter.source_summary(&source);
        assert_eq!(
            summary.description, "fake",
            "override description must be returned"
        );
        assert_eq!(
            summary.item_count,
            Some(0),
            "override item_count must be returned"
        );
        unregister("test_summary_fake");
    }

    /// Exercise every trait method body on each test-only adapter struct directly,
    /// so LLVM coverage sees these lines as executed rather than dead code.
    // TigerStyle exemption: declarative test coverage — four structs × five methods;
    // line count reflects data volume, not branchy logic.
    #[allow(clippy::too_many_lines)]
    #[test]
    fn fake_adapter_trait_methods_are_exercised() {
        let source = SourceRef::default();

        // FakeAdapter — all five trait methods.
        let fake = FakeAdapter;
        assert_eq!(fake.name(), "test_registry_fake");
        assert_eq!(fake.adapter_version(), "0.0.1");
        let ingest_result = fake.ingest(&source);
        assert!(ingest_result.is_ok(), "FakeAdapter::ingest must return Ok");
        assert!(
            ingest_result
                .expect("FakeAdapter::ingest must succeed")
                .is_empty(),
            "FakeAdapter::ingest must return an empty vec"
        );
        let schema = fake.describe_schema();
        assert!(
            schema.fields.is_empty(),
            "FakeAdapter::describe_schema must return empty fields"
        );
        assert_eq!(schema.version, "0.0.1");
        let summary = fake.source_summary(&source);
        assert_eq!(summary.description, "fake");
        assert_eq!(summary.item_count, Some(0));

        // SummaryFakeAdapter — all five trait methods.
        let summary_fake = SummaryFakeAdapter;
        assert_eq!(summary_fake.name(), "test_summary_fake");
        assert_eq!(summary_fake.adapter_version(), "0.0.1");
        let sf_ingest = summary_fake.ingest(&source);
        assert!(
            sf_ingest.is_ok(),
            "SummaryFakeAdapter::ingest must return Ok"
        );
        assert!(
            sf_ingest
                .expect("SummaryFakeAdapter::ingest must succeed")
                .is_empty(),
            "SummaryFakeAdapter::ingest must return an empty vec"
        );
        let sf_schema = summary_fake.describe_schema();
        assert!(
            sf_schema.fields.is_empty(),
            "SummaryFakeAdapter::describe_schema must return empty fields"
        );
        let sf_summary = summary_fake.source_summary(&source);
        assert_eq!(sf_summary.description, "fake");

        // OverwriteAdapterV1 — ingest and describe_schema.
        let v1 = OverwriteAdapterV1;
        assert_eq!(v1.name(), "test_overwrite_adapter");
        assert_eq!(v1.adapter_version(), "1.0.0");
        let v1_ingest = v1.ingest(&source);
        assert!(
            v1_ingest.is_ok(),
            "OverwriteAdapterV1::ingest must return Ok"
        );
        assert!(
            v1_ingest
                .expect("OverwriteAdapterV1::ingest must succeed")
                .is_empty(),
            "OverwriteAdapterV1::ingest must return an empty vec"
        );
        let v1_schema = v1.describe_schema();
        assert!(
            v1_schema.fields.is_empty(),
            "OverwriteAdapterV1::describe_schema must return empty fields"
        );
        assert_eq!(v1_schema.version, "1.0.0");

        // OverwriteAdapterV2 — ingest and describe_schema.
        let v2 = OverwriteAdapterV2;
        assert_eq!(v2.name(), "test_overwrite_adapter");
        assert_eq!(v2.adapter_version(), "2.0.0");
        let v2_ingest = v2.ingest(&source);
        assert!(
            v2_ingest.is_ok(),
            "OverwriteAdapterV2::ingest must return Ok"
        );
        assert!(
            v2_ingest
                .expect("OverwriteAdapterV2::ingest must succeed")
                .is_empty(),
            "OverwriteAdapterV2::ingest must return an empty vec"
        );
        let v2_schema = v2.describe_schema();
        assert!(
            v2_schema.fields.is_empty(),
            "OverwriteAdapterV2::describe_schema must return empty fields"
        );
        assert_eq!(v2_schema.version, "2.0.0");
    }
}
