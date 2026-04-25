use crate::cli::{self, Cli, Command};
use crate::config::{MempalaceConfig, expand_tilde};
use crate::{db, error, mcp, palace, schema};

/// Open the palace DB, ensuring schema exists. Returns `(db, connection, path)`.
async fn open_palace() -> error::Result<(turso::Database, turso::Connection, std::path::PathBuf)> {
    let config = MempalaceConfig::init()?;
    let db_path = config.palace_db_path();
    let db_path_str = db_path.to_str().ok_or_else(|| {
        error::Error::Other(format!(
            "database path is not valid UTF-8: {}",
            db_path.display()
        ))
    })?;
    let (database, connection) = db::open_db(db_path_str).await?;
    schema::ensure_schema(&connection).await?;
    Ok((database, connection, db_path))
}

/// Dispatch a parsed CLI command to the appropriate subcommand handler.
// Each match arm dispatches a single CLI subcommand — splitting further would
// only scatter the top-level dispatch logic without reducing complexity.
#[allow(clippy::too_many_lines)]
pub async fn run(cli: Cli) -> error::Result<()> {
    match cli.command {
        Command::Status => {
            run_status().await?;
        }

        Command::Init {
            directory,
            yes,
            no_gitignore,
            llm,
            llm_provider,
            llm_model,
            llm_endpoint,
            llm_api_key,
        } => {
            let llm_opts = cli::init::LlmOpts {
                enabled: llm,
                provider: llm_provider,
                model: llm_model,
                endpoint: llm_endpoint,
                api_key: llm_api_key,
            };
            cli::init::run(&directory, yes, no_gitignore, &llm_opts)?;
        }

        Command::Mine {
            directory,
            mode,
            extract_mode,
            wing,
            agent,
            limit,
            dry_run,
            no_gitignore,
        } => {
            run_mine(
                directory,
                mode,
                extract_mode,
                wing,
                agent,
                limit,
                dry_run,
                no_gitignore,
            )
            .await?;
        }

        Command::Search {
            query,
            wing,
            room,
            results,
        } => {
            run_search(query, wing, room, results).await?;
        }

        Command::WakeUp { wing } => {
            run_wakeup(wing).await?;
        }

        Command::Compress {
            wing,
            dry_run,
            config,
        } => {
            run_compress(wing, dry_run, config).await?;
        }

        Command::Split {
            directory,
            output_dir,
            dry_run,
            sessions_min,
            no_gitignore,
        } => {
            run_split(
                &directory,
                output_dir.as_deref(),
                dry_run,
                sessions_min,
                no_gitignore,
            )?;
        }

        Command::Sweep { target, wing } => {
            run_sweep(target, wing).await?;
        }

        Command::Repair => {
            run_repair().await?;
        }

        Command::Mcp => {
            run_mcp().await?;
        }
    }

    Ok(())
}

/// Handle the `search` sub-command — opens the palace and runs the search.
async fn run_search(
    query: String,
    wing: Option<String>,
    room: Option<String>,
    results: usize,
) -> error::Result<()> {
    let (_db, connection, _path) = open_palace().await?;
    cli::search::run(
        &connection,
        &query,
        wing.as_deref(),
        room.as_deref(),
        results,
    )
    .await
}

/// Handle the `wakeup` sub-command — opens the palace and runs wake-up.
async fn run_wakeup(wing: Option<String>) -> error::Result<()> {
    let (_db, connection, _path) = open_palace().await?;
    cli::wakeup::run(&connection, wing.as_deref()).await
}

/// Handle the `split` sub-command — expands `~` then delegates to the splitter.
fn run_split(
    directory: &std::path::Path,
    output_dir: Option<&std::path::Path>,
    dry_run: bool,
    sessions_min: usize,
    no_gitignore: bool,
) -> error::Result<()> {
    // Expand ~ so that `mempalace split ~/convos` works as expected.
    let directory = expand_tilde(directory);
    let output_dir = output_dir.map(expand_tilde);
    cli::split::run(
        &directory,
        output_dir.as_deref(),
        dry_run,
        sessions_min,
        !no_gitignore,
    )
}

/// Handle the `repair` sub-command — opens the palace and runs repair.
async fn run_repair() -> error::Result<()> {
    let (_db, connection, palace_path) = open_palace().await?;
    cli::repair::run(&connection, &palace_path).await
}

/// Handle the `mcp` sub-command — opens the palace and starts the MCP server.
async fn run_mcp() -> error::Result<()> {
    let (_db, connection, _path) = open_palace().await?;
    mcp::run(&connection).await
}

/// Handle the `status` sub-command — opens the palace if it exists.
async fn run_status() -> error::Result<()> {
    let config = MempalaceConfig::load()?;
    let db_path = config.palace_db_path();

    if !db_path.exists() {
        println!("No palace found at {}", db_path.display());
        println!("Run `mempalace init <dir>` to get started.");
        return Ok(());
    }

    let db_path_str = db_path.to_str().ok_or_else(|| {
        error::Error::Other(format!(
            "database path is not valid UTF-8: {}",
            db_path.display()
        ))
    })?;
    let (_db, connection) = db::open_db(db_path_str).await?;
    cli::status::run(&connection).await
}

/// Handle the `compress` sub-command — compresses drawers into AAAK dialect format.
async fn run_compress(
    wing: Option<String>,
    dry_run: bool,
    config: Option<std::path::PathBuf>,
) -> error::Result<()> {
    let (_db, connection, _path) = open_palace().await?;
    let config_str = match config.as_ref() {
        Some(path) => Some(path.to_str().ok_or_else(|| {
            error::Error::Other(format!(
                "config path is not valid UTF-8: {}",
                path.display()
            ))
        })?),
        None => None,
    };
    cli::compress::run(&connection, wing.as_deref(), dry_run, config_str).await
}

/// Handle the `sweep` sub-command — expands `~` then delegates to the sweeper.
async fn run_sweep(target: std::path::PathBuf, wing: String) -> error::Result<()> {
    let target = expand_tilde(&target);
    let (_db, connection, _path) = open_palace().await?;
    cli::sweep::run(&connection, &target, &wing).await
}

/// Handle the `mine` sub-command — delegates to the correct miner by mode.
// Arguments mirror the CLI fields 1:1 — no meaningful grouping exists.
#[allow(clippy::too_many_arguments)]
async fn run_mine(
    directory: std::path::PathBuf,
    mode: String,
    extract_mode: String,
    wing: Option<String>,
    agent: String,
    limit: usize,
    dry_run: bool,
    no_gitignore: bool,
) -> error::Result<()> {
    let opts = palace::miner::MineParams {
        wing,
        agent,
        limit,
        dry_run,
        respect_gitignore: !no_gitignore,
    };
    match mode.as_str() {
        "projects" => {
            let (_db, connection, _path) = open_palace().await?;
            palace::miner::mine(&connection, &directory, &opts).await?;
        }
        "convos" => {
            let (_db, connection, _path) = open_palace().await?;
            palace::convo_miner::mine_convos(&connection, &directory, &extract_mode, &opts).await?;
        }
        other => {
            return Err(error::Error::Other(format!(
                "unknown mine mode: {other} (expected 'projects' or 'convos')"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // -- run_mine error path --------------------------------------------------

    #[tokio::test]
    async fn run_mine_unknown_mode_returns_error() {
        // An unrecognised mode must return Err without calling open_palace.
        // This avoids requiring a real palace DB for the error path.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for run_mine test");
        let result = run_mine(
            temp_directory.path().to_path_buf(),
            "invalid_mode".to_string(),
            "full".to_string(),
            None,
            "test_agent".to_string(),
            0,
            false,
            false,
        )
        .await;
        assert!(result.is_err(), "unknown mine mode must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("invalid_mode")),
            "error message must contain the unknown mode name"
        );
    }

    // -- run_sweep via Command dispatch ----------------------------------------

    #[tokio::test]
    async fn run_command_sweep_with_file_target_returns_ok() {
        // Exercises both the Command::Sweep dispatch arm and run_sweep end-to-end.
        // A real palace DB is created by open_palace() inside run_sweep, so we
        // point MEMPALACE_DIR at a temp directory to avoid polluting the real palace.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for Command::Sweep test");
        let temp_directory_path_string = temp_directory
            .path()
            .to_str()
            .expect("temporary directory path must be valid UTF-8")
            .to_string();

        // Write a minimal valid Claude JSONL record so the sweep has something to process.
        let jsonl_path = temp_directory.path().join("session.jsonl");
        let record = r#"{"type":"user","sessionId":"s1","uuid":"u1","message":{"role":"user","content":"hello"}}"#;
        std::fs::write(&jsonl_path, format!("{record}\n"))
            .expect("must write test JSONL file for sweep");

        temp_env::async_with_vars(
            [
                ("MEMPALACE_DIR", Some(temp_directory_path_string.as_str())),
                ("MEMPALACE_PALACE_PATH", None),
            ],
            async {
                let cli = Cli {
                    command: Command::Sweep {
                        target: jsonl_path,
                        wing: "conversations".to_string(),
                    },
                };
                let result = run(cli).await;
                assert!(
                    result.is_ok(),
                    "run must return Ok for Command::Sweep with a valid JSONL file"
                );
                // Pair assertion: the palace DB must have been created by open_palace.
                assert!(
                    temp_directory.path().join("palace.db").exists(),
                    "palace.db must exist after a successful sweep"
                );
            },
        )
        .await;
    }

    // -- run_status without a palace ------------------------------------------

    #[tokio::test]
    async fn run_status_with_no_palace_db_returns_ok() {
        // When the palace.db does not exist run_status must print a friendly message
        // and return Ok without panicking.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for run_status test");
        let temp_directory_path_string = temp_directory
            .path()
            .to_str()
            .expect("temporary directory path must be valid UTF-8")
            .to_string();
        temp_env::async_with_vars(
            [
                ("MEMPALACE_DIR", Some(temp_directory_path_string.as_str())),
                ("MEMPALACE_PALACE_PATH", None),
            ],
            async {
                let result = run_status().await;
                assert!(
                    result.is_ok(),
                    "run_status must return Ok when palace.db does not exist"
                );
                // Pair assertion: the temp directory must not have gained a palace.db.
                assert!(
                    !temp_directory.path().join("palace.db").exists(),
                    "run_status must not create a palace.db when reporting absence"
                );
            },
        )
        .await;
    }
}
