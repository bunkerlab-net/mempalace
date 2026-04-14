//! Core memory filing system — chunking, room detection, search, and graph traversal.

/// Maximum directory nesting depth for iterative directory walks. Prevents stack overflow
/// on pathological symlink graphs and enforces the no-recursion rule from the style guide.
pub const WALK_DEPTH_LIMIT: usize = 64;

pub mod chunker;
pub mod convo_miner;
pub mod drawer;
#[allow(dead_code)]
pub mod entity_detect;
pub mod graph;
pub mod layers;
pub mod miner;
pub mod query_sanitizer;
pub mod room_detect;
pub mod search;
