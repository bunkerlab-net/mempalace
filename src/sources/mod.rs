//! Source adapter contract and registry (RFC 002).
//!
//! This module publishes the contract third-party adapters target. The
//! first-party miners are migrated onto `SourceAdapter` in a follow-up.
//!
//! See `mempalace-py/mempalace/sources/` for the Python reference.

pub mod adapter;
pub mod context;
pub mod registry;
pub mod transforms;

pub use adapter::{
    AdapterSchema, DrawerRecord, FieldSpec, FieldType, IngestMode, IngestResult, RouteHint,
    SourceAdapter, SourceItemMetadata, SourceRef, SourceSummary, validate_schema_conformance,
};
pub use context::{NoOpProgressHook, PalaceContext, ProgressHook};
pub use registry::{
    DEFAULT_ADAPTER, available_adapters, get_adapter, register, resolve_adapter_name, unregister,
};
pub use transforms::{
    newline_normalize, strip_control_chars, utf8_replace_invalid, validate_content,
};
