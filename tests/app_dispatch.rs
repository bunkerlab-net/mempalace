//! Integration tests for the `app::run` dispatcher.
//!
//! Exercises CLI command paths through the real dispatch logic without requiring
//! a live palace database. Commands that need a DB (`Mine`, `Search`, `WakeUp`,
//! `Compress`, `Repair`, `Mcp`) are tested elsewhere; here we cover the routes
//! that only touch the filesystem.

// Test code — .expect() is acceptable with a descriptive message.
#![allow(clippy::expect_used)]

use std::path::PathBuf;

use mempalace::cli::{Cli, Command};

#[tokio::test]
async fn app_run_init_creates_config_with_yes_flag() {
    // Command::Init with yes=true must write mempalace.yaml without blocking on stdin.
    let temp_directory =
        tempfile::tempdir().expect("failed to create temporary directory for init test");

    let cli_instance = Cli {
        command: Command::Init {
            directory: temp_directory.path().to_path_buf(),
            yes: true,
            no_gitignore: false,
        },
    };

    mempalace::app::run(cli_instance)
        .await
        .expect("app::run with Command::Init should succeed");

    // mempalace.yaml must have been written.
    let config_path = temp_directory.path().join("mempalace.yaml");
    assert!(
        config_path.exists(),
        "mempalace.yaml must be created after init"
    );
    let config_content =
        std::fs::read_to_string(&config_path).expect("mempalace.yaml must be readable after init");
    assert!(
        config_content.contains("wing:"),
        "mempalace.yaml must contain a wing field"
    );
}

#[tokio::test]
async fn app_run_split_with_no_mega_files_returns_ok() {
    // Command::Split on a directory with no mega-files must return Ok harmlessly.
    let temp_directory =
        tempfile::tempdir().expect("failed to create temporary directory for split test");
    // Write a non-mega .txt file — only one session, below min_sessions threshold.
    std::fs::write(
        temp_directory.path().join("single.txt"),
        "Just some plain text content here.\nNo Claude Code session markers at all.",
    )
    .expect("failed to write single-session test file");

    let cli_instance = Cli {
        command: Command::Split {
            directory: temp_directory.path().to_path_buf(),
            output_dir: None,
            dry_run: false,
            min_sessions: 2,
            no_gitignore: false,
        },
    };

    mempalace::app::run(cli_instance)
        .await
        .expect("app::run with Command::Split should return Ok when no mega-files found");
    // The file must still exist — nothing was split.
    assert!(
        temp_directory.path().join("single.txt").exists(),
        "non-mega file must remain untouched"
    );
}

#[tokio::test]
async fn app_run_status_with_no_palace_returns_ok() {
    // Command::Status with MEMPALACE_DIR pointing to an empty temp directory must
    // return Ok and print "No palace found" rather than erroring.
    let temp_directory =
        tempfile::tempdir().expect("failed to create temporary directory for status test");
    let temp_directory_path_string = temp_directory
        .path()
        .to_str()
        .expect("temporary directory path must be valid UTF-8")
        .to_string();

    let cli_instance = Cli {
        command: Command::Status,
    };

    temp_env::async_with_vars(
        [
            ("MEMPALACE_DIR", Some(temp_directory_path_string.as_str())),
            ("MEMPALACE_PALACE_PATH", None),
        ],
        async {
            mempalace::app::run(cli_instance)
                .await
                .expect("app::run with Command::Status should return Ok when no palace exists");
        },
    )
    .await;
}

#[tokio::test]
async fn app_run_mine_with_projects_mode_and_db() {
    // Command::Mine with mode=projects must open the palace, mine files, and return Ok.
    let temp_directory =
        tempfile::tempdir().expect("failed to create temporary project directory for mine test");
    let palace_directory =
        tempfile::tempdir().expect("failed to create temporary palace directory for mine test");
    let palace_db_path = palace_directory.path().join("palace.db");
    let palace_db_path_string = palace_db_path
        .to_str()
        .expect("palace database path must be valid UTF-8")
        .to_string();

    // Write a source file and a mempalace.yaml config.
    std::fs::write(temp_directory.path().join("hello.rs"), "fn main() {}")
        .expect("failed to write test source file hello.rs");
    std::fs::write(
        temp_directory.path().join("mempalace.yaml"),
        "wing: test_project\nrooms:\n  - name: general\n    description: General\n    keywords: []",
    )
    .expect("failed to write mempalace.yaml for mine test");

    let cli_instance = Cli {
        command: Command::Mine {
            directory: temp_directory.path().to_path_buf(),
            mode: "projects".to_string(),
            extract_mode: "full".to_string(),
            wing: None,
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            no_gitignore: false,
        },
    };

    temp_env::async_with_vars(
        [
            (
                "MEMPALACE_DIR",
                Some(palace_directory.path().to_str().expect("valid UTF-8")),
            ),
            (
                "MEMPALACE_PALACE_PATH",
                Some(palace_db_path_string.as_str()),
            ),
        ],
        async {
            mempalace::app::run(cli_instance)
                .await
                .expect("app::run with Command::Mine projects should succeed");
        },
    )
    .await;

    // Palace database must have been created.
    assert!(palace_db_path.exists(), "palace.db must exist after mining");
}

#[tokio::test]
async fn app_run_mine_with_convos_mode_and_db() {
    // Command::Mine with mode=convos must scan a conversation directory and file chunks.
    let temp_directory = tempfile::tempdir()
        .expect("failed to create temporary conversation directory for mine test");
    let palace_directory = tempfile::tempdir()
        .expect("failed to create temporary palace directory for convos mine test");
    let palace_db_path = palace_directory.path().join("palace.db");
    let palace_db_path_string = palace_db_path
        .to_str()
        .expect("palace database path must be valid UTF-8")
        .to_string();

    // Write a conversation file with exchange markers.
    std::fs::write(
        temp_directory.path().join("conversation.txt"),
        "> user asks about system architecture and design patterns\n\
         The assistant explains the component structure and interface modules in detail.",
    )
    .expect("failed to write test conversation file");

    let cli_instance = Cli {
        command: Command::Mine {
            directory: temp_directory.path().to_path_buf(),
            mode: "convos".to_string(),
            extract_mode: "full".to_string(),
            wing: Some("test_convos".to_string()),
            agent: "test_agent".to_string(),
            limit: 0,
            dry_run: false,
            no_gitignore: false,
        },
    };

    temp_env::async_with_vars(
        [
            (
                "MEMPALACE_DIR",
                Some(palace_directory.path().to_str().expect("valid UTF-8")),
            ),
            (
                "MEMPALACE_PALACE_PATH",
                Some(palace_db_path_string.as_str()),
            ),
        ],
        async {
            mempalace::app::run(cli_instance)
                .await
                .expect("app::run with Command::Mine convos should succeed");
        },
    )
    .await;

    assert!(
        palace_db_path.exists(),
        "palace.db must exist after convos mining"
    );
}

#[tokio::test]
async fn app_run_search_with_palace() {
    // Command::Search must open the palace and execute a query.
    let palace_directory =
        tempfile::tempdir().expect("failed to create temporary palace directory for search test");
    let palace_db_path = palace_directory.path().join("palace.db");
    let palace_db_path_string = palace_db_path
        .to_str()
        .expect("palace database path must be valid UTF-8")
        .to_string();

    let cli_instance = Cli {
        command: Command::Search {
            query: "test query".to_string(),
            wing: None,
            room: None,
            results: 10,
        },
    };

    temp_env::async_with_vars(
        [
            (
                "MEMPALACE_DIR",
                Some(palace_directory.path().to_str().expect("valid UTF-8")),
            ),
            (
                "MEMPALACE_PALACE_PATH",
                Some(palace_db_path_string.as_str()),
            ),
        ],
        async {
            mempalace::app::run(cli_instance)
                .await
                .expect("app::run with Command::Search should succeed on an empty palace");
        },
    )
    .await;

    assert!(
        palace_db_path.exists(),
        "palace.db must exist after search creates it"
    );
}

#[tokio::test]
async fn app_run_wakeup_with_palace() {
    // Command::WakeUp must open the palace and generate wake-up context.
    let palace_directory =
        tempfile::tempdir().expect("failed to create temporary palace directory for wakeup test");
    let palace_db_path = palace_directory.path().join("palace.db");
    let palace_db_path_string = palace_db_path
        .to_str()
        .expect("palace database path must be valid UTF-8")
        .to_string();

    let cli_instance = Cli {
        command: Command::WakeUp { wing: None },
    };

    temp_env::async_with_vars(
        [
            (
                "MEMPALACE_DIR",
                Some(palace_directory.path().to_str().expect("valid UTF-8")),
            ),
            (
                "MEMPALACE_PALACE_PATH",
                Some(palace_db_path_string.as_str()),
            ),
        ],
        async {
            mempalace::app::run(cli_instance)
                .await
                .expect("app::run with Command::WakeUp should succeed on an empty palace");
        },
    )
    .await;

    // open_palace() must have created the database file as a side effect.
    assert!(
        palace_db_path.exists(),
        "palace.db must exist after wakeup creates it via open_palace"
    );
}

#[tokio::test]
async fn app_run_compress_with_palace() {
    // Command::Compress must open the palace and process drawers (no-op on empty).
    let palace_directory =
        tempfile::tempdir().expect("failed to create temporary palace directory for compress test");
    let palace_db_path = palace_directory.path().join("palace.db");
    let palace_db_path_string = palace_db_path
        .to_str()
        .expect("palace database path must be valid UTF-8")
        .to_string();

    let cli_instance = Cli {
        command: Command::Compress {
            wing: None,
            dry_run: true,
            config: None,
        },
    };

    temp_env::async_with_vars(
        [
            (
                "MEMPALACE_DIR",
                Some(palace_directory.path().to_str().expect("valid UTF-8")),
            ),
            (
                "MEMPALACE_PALACE_PATH",
                Some(palace_db_path_string.as_str()),
            ),
        ],
        async {
            mempalace::app::run(cli_instance)
                .await
                .expect("app::run with Command::Compress should succeed on an empty palace");
        },
    )
    .await;

    // open_palace() must have created the database file as a side effect.
    assert!(
        palace_db_path.exists(),
        "palace.db must exist after compress creates it via open_palace"
    );
}

#[tokio::test]
async fn app_run_repair_with_palace() {
    // Command::Repair must open the palace, back it up, and rebuild the index.
    let palace_directory =
        tempfile::tempdir().expect("failed to create temporary palace directory for repair test");
    let palace_db_path = palace_directory.path().join("palace.db");
    let palace_db_path_string = palace_db_path
        .to_str()
        .expect("palace database path must be valid UTF-8")
        .to_string();

    // Pre-create the palace so open_palace() can find it, and so repair can copy it.
    let palace_db_path_for_setup = PathBuf::from(&palace_db_path_string);
    {
        let (database, connection) = mempalace::db::open_db(&palace_db_path_string)
            .await
            .expect("failed to open palace database for test setup");
        mempalace::schema::ensure_schema(&connection)
            .await
            .expect("failed to apply schema for test setup");
        // Drop connection and database to release locks before repair runs.
        drop(connection);
        drop(database);
    }

    let cli_instance = Cli {
        command: Command::Repair,
    };

    temp_env::async_with_vars(
        [
            (
                "MEMPALACE_DIR",
                Some(palace_directory.path().to_str().expect("valid UTF-8")),
            ),
            (
                "MEMPALACE_PALACE_PATH",
                Some(palace_db_path_string.as_str()),
            ),
        ],
        async {
            mempalace::app::run(cli_instance)
                .await
                .expect("app::run with Command::Repair should succeed");
        },
    )
    .await;

    // Backup file must exist after repair.
    assert!(
        palace_db_path_for_setup.with_extension("db.bak").exists(),
        "palace.db.bak must exist after repair"
    );
}
