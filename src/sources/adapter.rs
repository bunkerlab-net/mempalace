//! Source adapter contract (RFC 002).
//!
//! Defines the read-side surface every source adapter must implement. A source
//! adapter extracts content from a specific origin (filesystem, git, Slack …)
//! and yields [`DrawerRecord`]s that core routes into the palace.
//!
//! The first-party miners (`palace::miner` and `palace::convo_miner`) are
//! migrated onto this interface in a follow-up; in this module we publish the
//! contract so third-party adapters can begin building against a stable surface.
//!
//! See Python `mempalace/sources/base.py` and RFC 002 for the authoritative spec.

use std::collections::HashMap;

use crate::error::Result;

// ─── Value objects ─────────────────────────────────────────────────────────

/// Identifies the source a caller wants to ingest (RFC 002 §2.1).
///
/// `local_path` is for filesystem-rooted sources; `uri` for URL-like references.
/// `options` carries adapter-specific non-secret config.
#[derive(Debug, Clone, Default)]
pub struct SourceRef {
    pub local_path: Option<String>,
    pub uri: Option<String>,
    pub options: HashMap<String, String>,
}

/// Adapter-supplied routing hint (RFC 002 §2.5).
///
/// Core uses these to decide which wing/room a drawer lands in.
#[derive(Debug, Clone, Default)]
pub struct RouteHint {
    pub wing: Option<String>,
    pub room: Option<String>,
}

/// Lightweight pointer yielded by lazy adapters for incremental-fetch support.
///
/// Core passes `existing_version` from the palace to
/// [`SourceAdapter::is_current`] before committing to a full extract.
#[derive(Debug, Clone)]
pub struct SourceItemMetadata {
    pub source_file: String,
    pub version: String,
    pub size_hint: Option<u64>,
    pub route_hint: Option<RouteHint>,
}

/// One drawer's worth of extracted content plus flat metadata.
///
/// `metadata` values must be flat scalars (RFC 001 §1.4). The `chunk_index`
/// starts at 0 for the first chunk from a given source file.
#[derive(Debug, Clone)]
pub struct DrawerRecord {
    pub content: String,
    pub source_file: String,
    pub chunk_index: u32,
    pub metadata: HashMap<String, String>,
    pub route_hint: Option<RouteHint>,
}

/// High-level summary of a source (RFC 002 §2.3).
#[derive(Debug, Clone)]
pub struct SourceSummary {
    pub description: String,
    pub item_count: Option<usize>,
}

/// Shape of a single per-adapter metadata field (RFC 002 §5.2).
#[derive(Debug, Clone)]
pub struct FieldSpec {
    pub field_type: FieldType,
    pub required: bool,
    pub description: String,
    pub indexed: bool,
}

/// Allowable types for a metadata field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    String,
    Int,
    Float,
    Bool,
    JsonString,
}

/// The per-adapter metadata schema (RFC 002 §5.2).
///
/// The schema is stable for a given `adapter_version`. Enterprises index on it;
/// core validates adapter output against it.
#[derive(Debug, Clone)]
pub struct AdapterSchema {
    pub fields: HashMap<String, FieldSpec>,
    pub version: String,
}

/// Ingest operation modes an adapter may declare support for (RFC 002 §4.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestMode {
    /// Project documentation, source code, configuration files.
    Projects,
    /// Conversation exports and chat transcripts.
    Convos,
    /// Exchange-by-exchange message extraction from transcripts.
    Exchange,
    /// General free-form text extraction.
    General,
}

/// Aggregate result of a completed ingest pipeline run.
///
/// Returned by higher-level orchestrators; individual adapter `ingest` calls
/// return `Vec<DrawerRecord>` directly.
#[derive(Debug, Clone, Default)]
pub struct IngestResult {
    /// Drawers successfully written to the palace.
    pub drawers_added: usize,
    /// Drawers skipped (already up-to-date per `is_current`).
    pub drawers_skipped: usize,
    /// Source files processed (success or partial).
    pub files_processed: usize,
    /// Non-fatal errors encountered; processing continued past each.
    pub errors: Vec<String>,
}

// ─── Schema validation ─────────────────────────────────────────────────────

/// Validate that `record` satisfies all required fields declared in `schema` (RFC-002 §5.2).
///
/// Returns `Err(SchemaConformance(...))` naming the first absent required field.
/// Optional fields are not checked — their absence is always valid.
pub fn validate_schema_conformance(
    record: &DrawerRecord,
    schema: &AdapterSchema,
) -> std::result::Result<(), crate::error::SourceAdapterError> {
    assert!(
        !record.source_file.is_empty(),
        "DrawerRecord source_file must not be empty"
    );
    assert!(
        !schema.version.is_empty(),
        "AdapterSchema version must not be empty"
    );

    for (field_name, spec) in &schema.fields {
        if !spec.required {
            continue;
        }
        if !record.metadata.contains_key(field_name) {
            return Err(crate::error::SourceAdapterError::SchemaConformance(
                format!("required field '{field_name}' is absent from DrawerRecord metadata"),
            ));
        }
    }
    Ok(())
}

// ─── Adapter contract ──────────────────────────────────────────────────────

/// Long-lived source adapter contract (RFC 002 §2).
///
/// Implementations are thread-safe: the same adapter instance may be called
/// concurrently for different `SourceRef` values. Construction is cheap —
/// defer all I/O and credential fetches to `ingest`.
pub trait SourceAdapter: Send + Sync {
    /// Stable adapter name used for registration and drawer metadata.
    fn name(&self) -> &'static str;

    /// Adapter semver (independent of the spec version).
    ///
    /// Recorded on every drawer so re-extract workflows can target drawers from
    /// a known-buggy adapter version.
    fn adapter_version(&self) -> &'static str;

    /// Extract content from `source` and return all `DrawerRecord`s.
    ///
    /// The return order matches the ingest order; chunk indices must be
    /// monotonically increasing per `source_file`.
    fn ingest(&self, source: &SourceRef) -> Result<Vec<DrawerRecord>>;

    /// Declare the structured metadata this adapter attaches (RFC 002 §5.2).
    fn describe_schema(&self) -> AdapterSchema;

    /// Return `true` if the palace already has an up-to-date copy of `item`.
    ///
    /// Default: always `false` (re-extract every time). Adapters advertising
    /// incremental support MUST override.
    fn is_current(&self, _item: &SourceItemMetadata, _existing_version: Option<&str>) -> bool {
        false
    }

    /// Describe a source without extracting its content.
    fn source_summary(&self, source: &SourceRef) -> SourceSummary {
        SourceSummary {
            description: self.name().to_string(),
            item_count: source.local_path.as_ref().map(|_| 0),
        }
    }

    /// RFC-002 spec version this adapter was built against.
    ///
    /// Default: `"RFC-002"`. Override to advertise a specific revision.
    fn spec_version(&self) -> &'static str {
        "RFC-002"
    }

    /// Capability tags declared by the adapter (e.g. `"incremental"`, `"streaming"`).
    ///
    /// Default: empty — no special capabilities beyond the baseline contract.
    fn capabilities(&self) -> Vec<String> {
        vec![]
    }

    /// Ingest modes this adapter can handle.
    ///
    /// Default: `[IngestMode::Projects]`.
    fn supported_modes(&self) -> Vec<IngestMode> {
        vec![IngestMode::Projects]
    }

    /// Named content transformations applied before yielding a `DrawerRecord`.
    ///
    /// Declared names must correspond to functions in `sources::transforms`.
    /// Default: empty — no declared transformations.
    fn declared_transformations(&self) -> Vec<String> {
        vec![]
    }

    /// Privacy class applied to drawers from this adapter.
    ///
    /// Default: `"internal"`. Override for adapters ingesting public or sensitive content.
    fn default_privacy_class(&self) -> &'static str {
        "internal"
    }
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // ── Minimal adapter for trait tests ───────────────────────────────────

    struct MinimalAdapter;
    impl SourceAdapter for MinimalAdapter {
        fn name(&self) -> &'static str {
            "minimal_test"
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
    }

    // ── SourceRef ──────────────────────────────────────────────────────────

    #[test]
    fn source_ref_default_is_empty() {
        let source_ref = SourceRef::default();
        assert!(source_ref.local_path.is_none());
        assert!(source_ref.uri.is_none());
        assert!(source_ref.options.is_empty());
    }

    #[test]
    fn source_ref_with_local_path_stores_value() {
        let source_ref = SourceRef {
            local_path: Some("/home/user/notes".to_string()),
            uri: None,
            options: HashMap::new(),
        };
        assert!(source_ref.local_path.is_some());
        assert_eq!(source_ref.local_path.as_deref(), Some("/home/user/notes"));
    }

    // ── DrawerRecord ───────────────────────────────────────────────────────

    #[test]
    fn drawer_record_chunk_index_starts_at_zero() {
        let record = DrawerRecord {
            content: "hello".to_string(),
            source_file: "foo.txt".to_string(),
            chunk_index: 0,
            metadata: HashMap::new(),
            route_hint: None,
        };
        assert_eq!(record.chunk_index, 0);
        assert!(!record.content.is_empty());
    }

    #[test]
    fn route_hint_default_is_empty() {
        let hint = RouteHint::default();
        assert!(hint.wing.is_none());
        assert!(hint.room.is_none());
    }

    // ── IngestMode ─────────────────────────────────────────────────────────

    #[test]
    fn ingest_mode_variant_equality() {
        // Each mode variant must compare equal only to itself.
        assert_eq!(IngestMode::Projects, IngestMode::Projects);
        assert_ne!(IngestMode::Projects, IngestMode::Convos);
        assert_ne!(IngestMode::Exchange, IngestMode::General);
    }

    // ── IngestResult ───────────────────────────────────────────────────────

    #[test]
    fn ingest_result_default_is_zero_counters() {
        // Default IngestResult must represent a no-op run with no errors.
        let result = IngestResult::default();
        assert_eq!(result.drawers_added, 0);
        assert_eq!(result.drawers_skipped, 0);
        assert_eq!(result.files_processed, 0);
        assert!(
            result.errors.is_empty(),
            "default IngestResult must have no errors"
        );
    }

    // ── Identity attrs ─────────────────────────────────────────────────────

    // ── validate_schema_conformance ────────────────────────────────────────

    #[test]
    fn validate_schema_conformance_passes_when_required_fields_present() {
        // A record with all required fields populated must pass validation.
        let mut schema_fields = HashMap::new();
        schema_fields.insert(
            "author".to_string(),
            FieldSpec {
                field_type: FieldType::String,
                required: true,
                description: "author name".to_string(),
                indexed: false,
            },
        );
        let schema = AdapterSchema {
            fields: schema_fields,
            version: "1.0".to_string(),
        };
        let mut metadata = HashMap::new();
        metadata.insert("author".to_string(), "Alice".to_string());
        let record = DrawerRecord {
            content: "test".to_string(),
            source_file: "doc.md".to_string(),
            chunk_index: 0,
            metadata,
            route_hint: None,
        };
        let result = validate_schema_conformance(&record, &schema);
        assert!(result.is_ok(), "record with required field must pass");
        // Pair: pure function must produce same result on repeated calls.
        assert!(
            validate_schema_conformance(&record, &schema).is_ok(),
            "validate_schema_conformance must be deterministic"
        );
    }

    #[test]
    fn validate_schema_conformance_fails_when_required_field_absent() {
        // A record missing a required field must return SchemaConformance error.
        let mut schema_fields = HashMap::new();
        schema_fields.insert(
            "author".to_string(),
            FieldSpec {
                field_type: FieldType::String,
                required: true,
                description: "author name".to_string(),
                indexed: false,
            },
        );
        let schema = AdapterSchema {
            fields: schema_fields,
            version: "1.0".to_string(),
        };
        let record = DrawerRecord {
            content: "test".to_string(),
            source_file: "doc.md".to_string(),
            chunk_index: 0,
            metadata: HashMap::new(),
            route_hint: None,
        };
        let result = validate_schema_conformance(&record, &schema);
        assert!(result.is_err(), "missing required field must fail");
        let err = result.expect_err("must be an error");
        let msg = err.to_string();
        assert!(msg.contains("author"), "error must name the missing field");
    }

    #[test]
    fn source_adapter_default_identity_attrs() {
        // Default identity methods must return RFC-002 specified values.
        let adapter = MinimalAdapter;
        assert_eq!(adapter.spec_version(), "RFC-002");
        assert!(adapter.capabilities().is_empty(), "default must be empty");
        assert_eq!(adapter.supported_modes(), vec![IngestMode::Projects]);
        assert!(
            adapter.declared_transformations().is_empty(),
            "default must be empty"
        );
        assert_eq!(adapter.default_privacy_class(), "internal");
    }

    #[test]
    fn is_current_default_returns_false() {
        // Default is_current must always return false so adapters without
        // incremental support re-extract on every run.
        let adapter = MinimalAdapter;
        let item = SourceItemMetadata {
            source_file: "test.rs".to_string(),
            version: "abc123".to_string(),
            size_hint: None,
            route_hint: None,
        };
        assert!(
            !adapter.is_current(&item, None),
            "default must be false with no existing version"
        );
        assert!(
            !adapter.is_current(&item, Some("abc123")),
            "default must be false even when versions match"
        );
    }

    #[test]
    fn source_summary_default_with_local_path_returns_zero_count() {
        // source_summary default implementation must return item_count=Some(0)
        // when a local_path is set, and None when local_path is absent.
        let adapter = MinimalAdapter;
        let source_with_path = SourceRef {
            local_path: Some("/tmp/notes".to_string()),
            uri: None,
            options: HashMap::new(),
        };
        let summary = adapter.source_summary(&source_with_path);
        assert_eq!(
            summary.item_count,
            Some(0),
            "local_path present must yield Some(0)"
        );
        assert!(
            !summary.description.is_empty(),
            "description must not be empty"
        );
    }

    #[test]
    fn source_summary_default_without_local_path_returns_none_count() {
        // source_summary default must return item_count=None when local_path is absent.
        let adapter = MinimalAdapter;
        let source_without_path = SourceRef {
            local_path: None,
            uri: Some("https://example.com".to_string()),
            options: HashMap::new(),
        };
        let summary = adapter.source_summary(&source_without_path);
        assert!(
            summary.item_count.is_none(),
            "absent local_path must yield None item_count"
        );
        assert_eq!(
            summary.description, "minimal_test",
            "description must match adapter name"
        );
    }

    #[test]
    fn validate_schema_conformance_optional_field_absent_passes() {
        // An absent optional field must not cause a validation failure —
        // only required fields are enforced.
        let mut schema_fields = HashMap::new();
        schema_fields.insert(
            "tags".to_string(),
            FieldSpec {
                field_type: FieldType::JsonString,
                required: false,
                description: "optional tags".to_string(),
                indexed: false,
            },
        );
        let schema = AdapterSchema {
            fields: schema_fields,
            version: "1.0".to_string(),
        };
        let record = DrawerRecord {
            content: "test".to_string(),
            source_file: "doc.md".to_string(),
            chunk_index: 0,
            metadata: HashMap::new(),
            route_hint: None,
        };
        let result = validate_schema_conformance(&record, &schema);
        assert!(result.is_ok(), "absent optional field must pass validation");
        // Pair: empty schema must also pass.
        let empty_schema = AdapterSchema {
            fields: HashMap::new(),
            version: "1.0".to_string(),
        };
        assert!(
            validate_schema_conformance(&record, &empty_schema).is_ok(),
            "empty schema must always pass"
        );
    }

    #[test]
    fn validate_schema_conformance_all_field_types_pass_when_present() {
        // All FieldType variants must be accepted when the field key is present
        // in record metadata — validate_schema_conformance only checks presence.
        let field_types = [
            ("str_field", FieldType::String),
            ("int_field", FieldType::Int),
            ("float_field", FieldType::Float),
            ("bool_field", FieldType::Bool),
            ("json_field", FieldType::JsonString),
        ];
        let mut schema_fields = HashMap::new();
        let mut metadata = HashMap::new();
        for (name, field_type) in field_types {
            schema_fields.insert(
                name.to_string(),
                FieldSpec {
                    field_type,
                    required: true,
                    description: "test field".to_string(),
                    indexed: false,
                },
            );
            metadata.insert(name.to_string(), "value".to_string());
        }
        let schema = AdapterSchema {
            fields: schema_fields,
            version: "2.0".to_string(),
        };
        let record = DrawerRecord {
            content: "test".to_string(),
            source_file: "multi.md".to_string(),
            chunk_index: 0,
            metadata,
            route_hint: None,
        };
        let result = validate_schema_conformance(&record, &schema);
        assert!(result.is_ok(), "all required field types present must pass");
        // Pair: removing one field must cause failure.
        let mut partial_record = record.clone();
        partial_record.metadata.remove("bool_field");
        assert!(
            validate_schema_conformance(&partial_record, &schema).is_err(),
            "removing a required field must cause failure"
        );
    }

    #[test]
    fn field_type_equality_covers_all_variants() {
        // Each FieldType variant must compare equal only to itself.
        assert_eq!(FieldType::String, FieldType::String);
        assert_eq!(FieldType::Int, FieldType::Int);
        assert_eq!(FieldType::Float, FieldType::Float);
        assert_eq!(FieldType::Bool, FieldType::Bool);
        assert_eq!(FieldType::JsonString, FieldType::JsonString);
        assert_ne!(FieldType::String, FieldType::Int);
        assert_ne!(FieldType::Float, FieldType::Bool);
    }

    #[test]
    fn source_item_metadata_stores_all_fields() {
        // SourceItemMetadata must retain all provided field values.
        let hint = RouteHint {
            wing: Some("projects".to_string()),
            room: Some("backend".to_string()),
        };
        let item = SourceItemMetadata {
            source_file: "src/main.rs".to_string(),
            version: "v1.2.3".to_string(),
            size_hint: Some(4096),
            route_hint: Some(hint),
        };
        assert_eq!(item.source_file, "src/main.rs");
        assert_eq!(item.version, "v1.2.3");
        assert_eq!(item.size_hint, Some(4096));
        assert!(item.route_hint.is_some(), "route_hint must be stored");
    }
}
