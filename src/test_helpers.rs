// Test infrastructure — .expect() is acceptable with a descriptive message.
#![allow(clippy::expect_used)]

use std::path::PathBuf;
use std::sync::OnceLock;

use turso::Connection;

// Process-scoped config directory for tests. Kept as a `PathBuf` (not a
// `TempDir`) because `test_helpers` is compiled as a library module, where
// `tempfile` is not available (it is a dev-dependency only). The directory
// lives under `std::env::temp_dir()` and is cleaned up by the OS.
static TEST_CONFIG_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Redirect all config and WAL writes to a per-process temporary directory.
///
/// Uses `OnceLock` so the directory is created and registered exactly once,
/// regardless of how many tests call `test_db` concurrently — safe for
/// parallel test execution without any locking on the caller side.
fn redirect_config_dir() {
    TEST_CONFIG_DIR.get_or_init(|| {
        // Use the process ID so parallel test processes don't share the same
        // directory and overwrite each other's WAL files.
        let path = std::env::temp_dir().join(format!("mempalace_test_{}", std::process::id()));
        std::fs::create_dir_all(&path).expect("failed to create test config dir");
        crate::config::set_config_dir_override(path.clone());
        path
    });
}

/// Create an in-memory turso database with the full schema applied.
/// Returns (Database, Connection) tuple to keep the Database alive for the test lifetime.
pub async fn test_db() -> (turso::Database, Connection) {
    redirect_config_dir();
    let db = turso::Builder::new_local(":memory:")
        .experimental_triggers(true)
        .build()
        .await
        .expect("failed to create in-memory db");
    let connection = db.connect().expect("failed to connect");
    crate::schema::ensure_schema(&connection)
        .await
        .expect("failed to apply schema");
    (db, connection)
}
