//! Core memory filing system — chunking, room detection, search, and graph traversal.

/// Maximum directory nesting depth for iterative directory walks. Prevents stack overflow
/// on pathological symlink graphs and enforces the no-recursion rule from the style guide.
pub const WALK_DEPTH_LIMIT: usize = 64;

pub mod chunker;
pub mod closet_llm;
pub mod closets;
pub mod convo_miner;
pub mod dedup;
pub mod diary_ingest;
pub mod drawer;
pub mod entities;
pub mod entity_confirm;
pub mod entity_detect;
pub mod entity_registry;
pub mod exporter;
pub mod fact_checker;
pub mod graph;
pub mod known_entities;
pub mod layers;
pub mod miner;
pub mod project_scanner;
pub mod query_sanitizer;
pub mod room_detect;
pub mod search;
pub mod session_scanner;
pub mod sweeper;
