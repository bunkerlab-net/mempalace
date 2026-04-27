//! Source adapter contract and registry (RFC 002).
//!
//! This module publishes the contract third-party adapters target. The
//! first-party miners are migrated onto `SourceAdapter` in a follow-up.
//!
//! See `mempalace-py/mempalace/sources/` for the Python reference.

pub mod adapter;
pub mod registry;

pub use adapter::{
    AdapterSchema, DrawerRecord, FieldSpec, FieldType, RouteHint, SourceAdapter,
    SourceItemMetadata, SourceRef, SourceSummary,
};
pub use registry::{
    DEFAULT_ADAPTER, available_adapters, get_adapter, register, resolve_adapter_name, unregister,
};
