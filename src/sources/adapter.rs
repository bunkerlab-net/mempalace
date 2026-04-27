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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
