//! Shared entity types used by `project_scanner` (production) and `entity_detect` (test-gated).
//!
//! Keeping `DetectedEntity` here breaks the would-be circular dependency: `entity_detect`
//! produces `DetectedEntity` values; `project_scanner` produces them too and groups them
//! into `DetectedDict`. Both modules import this type without importing each other.

/// A named entity (person, project, or uncertain) extracted from project signals or prose.
///
/// The `signals` field carries a human-readable evidence trail.
/// - For prose-detected entities it is empty.
/// - For manifest/git-detected entities it contains e.g. `"Cargo.toml, 30 of your commits"`.
pub struct DetectedEntity {
    pub name: String,
    pub entity_type: String, // "person", "project", or "uncertain"
    pub confidence: f64,
    pub frequency: usize,
    /// Evidence strings — empty for prose-detected entities, populated by `project_scanner`.
    pub signals: Vec<String>,
}
