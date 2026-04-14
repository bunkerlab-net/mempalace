//! Mempalace library — local-first AI memory palace backed by embedded `SQLite`.
//!
//! Re-exports modules so integration tests can access palace, MCP,
//! knowledge-graph, and normalization APIs. Not a public library API.

// Library target exists only for integration test access — doc-quality lints
// and `must_use_candidate` don't apply since the public API is the CLI binary,
// not this crate, and callers are integration tests that discard return values
// intentionally.
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate
)]

pub mod config;
pub mod db;
pub mod dialect;
pub mod error;
#[allow(dead_code)]
pub mod extract;
pub mod kg;
pub mod mcp;
pub mod normalize;
pub mod palace;
pub mod schema;
pub mod test_helpers;
