fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
}

/// Verify that a process with `LIMBO_DISABLE_FILE_LOCK` set can open the
/// database even while another process holds the exclusive file lock.
/// Regression test for <https://github.com/bunkerlab-net/mempalace/issues/9>
///
/// POSIX `fcntl` locks are per-process, so a cross-process test is required
/// to exercise real locking behaviour. The parent opens normally (acquiring
/// the lock); the child is spawned with `LIMBO_DISABLE_FILE_LOCK=1` and must
/// succeed.
///
/// Child-process protocol (invoked via `_MEMPALACE_TEST_OPEN_PATH`):
///   exit 0 — open succeeded (expected)
///   exit 1 — open failed (unexpected)
#[allow(unsafe_code)]
#[test]
fn two_connections_to_same_file() {
    // --- Child-process path -------------------------------------------
    // Spawned with LIMBO_DISABLE_FILE_LOCK=1. Try to open the file and
    // report success or failure. Exit immediately so the test harness
    // does not run further tests in this subprocess.
    if let Ok(path) = std::env::var("_MEMPALACE_TEST_OPEN_PATH") {
        let ok = current_thread_runtime()
            .block_on(async { turso::Builder::new_local(&path).build().await.is_ok() });
        std::process::exit(i32::from(!ok));
    }

    // --- Parent-process path ------------------------------------------
    // Ensure the env var is not set so the parent acquires a real fcntl
    // lock. Without this, an externally set LIMBO_DISABLE_FILE_LOCK would
    // cause the parent to skip locking and make the test a no-op.
    //
    // SAFETY: nextest runs each integration test in its own subprocess, so
    // no other threads exist at this point.
    unsafe {
        std::env::remove_var("LIMBO_DISABLE_FILE_LOCK");
    }

    // Open the database normally (no LIMBO_DISABLE_FILE_LOCK), which
    // acquires an exclusive fcntl lock. Then spawn a child with the env
    // var set and confirm it can open the same file.
    current_thread_runtime().block_on(async {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let db_path = dir.path().join("palace.db");
        let path_str = db_path.to_str().expect("non-utf8 path");

        // Open the database; this acquires an exclusive fcntl lock on the file.
        let _db = turso::Builder::new_local(path_str)
            .build()
            .await
            .expect("parent open failed");

        // Spawn a child with LIMBO_DISABLE_FILE_LOCK=1 that tries to open the
        // same file while the parent holds the lock.
        // Exit code 0 means it succeeded (expected).
        let current_exe = std::env::current_exe().expect("failed to get current exe");
        let status = std::process::Command::new(current_exe)
            .env("_MEMPALACE_TEST_OPEN_PATH", path_str)
            .env("LIMBO_DISABLE_FILE_LOCK", "1")
            // Filter to this test so the child harness does not run other tests
            // before hitting the early-exit branch above.
            .args(["two_connections_to_same_file"])
            .status()
            .expect("failed to spawn child process");

        assert!(
            status.success(),
            "open with LIMBO_DISABLE_FILE_LOCK=1 should succeed even with another process holding the lock"
        );
    });
}

/// Verify that a second open of the same database file fails with a locking
/// error when `LIMBO_DISABLE_FILE_LOCK` is not set. This confirms that the
/// positive test above is actually testing something meaningful.
///
/// POSIX `fcntl` locks are per-process, so opening the file twice from the
/// same process does not produce a conflict. A subprocess is used to hold the
/// lock while the parent attempts a second open.
///
/// Child-process protocol (invoked via `_MEMPALACE_TEST_LOCK_PATH`):
///   exit 0 — open was blocked by the lock (expected)
///   exit 1 — open succeeded despite the lock (unexpected)
#[test]
fn second_open_fails_without_lock_disabled() {
    // --- Child-process path -------------------------------------------
    // When spawned as the lock-probe, try to open the file and report
    // whether the lock blocked us. Exit immediately so the test harness
    // does not run further tests in this subprocess.
    if let Ok(path) = std::env::var("_MEMPALACE_TEST_LOCK_PATH") {
        let blocked = current_thread_runtime()
            .block_on(async { turso::Builder::new_local(&path).build().await.is_err() });
        std::process::exit(i32::from(!blocked));
    }

    // --- Parent-process path ------------------------------------------
    current_thread_runtime().block_on(async {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let db_path = dir.path().join("palace.db");
        let path_str = db_path.to_str().expect("non-utf8 path");

        // Open the database; this acquires an exclusive fcntl lock on the file.
        let _db1 = turso::Builder::new_local(path_str)
            .build()
            .await
            .expect("first open failed");

        // Spawn a child process that tries to open the same file without the
        // env-var escape hatch. Exit code 0 means the lock correctly blocked it.
        let current_exe = std::env::current_exe().expect("failed to get current exe");
        let status = std::process::Command::new(current_exe)
            .env("_MEMPALACE_TEST_LOCK_PATH", path_str)
            .env_remove("LIMBO_DISABLE_FILE_LOCK")
            // Filter to this test so the child harness does not run other tests
            // before hitting the early-exit branch above.
            .args(["second_open_fails_without_lock_disabled"])
            .status()
            .expect("failed to spawn child process");

        assert!(
            status.success(),
            "second open should have been blocked by the exclusive file lock"
        );
    });
}
