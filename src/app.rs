use crate::cli::{self, Cli, Command};
use crate::config::{MempalaceConfig, expand_tilde};
use crate::{db, error, mcp, palace, schema};

/// Open the palace DB, ensuring schema exists. Returns `(db, connection, path)`.
///
/// `palace_override` takes priority over `MEMPALACE_PALACE_PATH` and the config
/// file — mirrors the `--palace` CLI flag semantics.
async fn open_palace(
    palace_override: Option<&std::path::Path>,
) -> error::Result<(turso::Database, turso::Connection, std::path::PathBuf)> {
    let config = MempalaceConfig::init()?;
    let db_path = match palace_override {
        Some(path) => crate::config::expand_tilde(path),
        None => config.palace_db_path(),
    };
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
    // Extract the optional palace override before moving cli.command into the match.
    let palace = cli.palace;
    let palace_override = palace.as_deref();

    match cli.command {
        Command::Status => {
            run_status(palace_override).await?;
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
                palace_override,
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
            run_search(palace_override, query, wing, room, results).await?;
        }

        Command::WakeUp {
            wing,
            room,
            query,
            results,
        } => {
            run_wakeup(palace_override, wing, room, query, results).await?;
        }

        Command::Compress {
            wing,
            dry_run,
            config,
        } => {
            run_compress(palace_override, wing, dry_run, config).await?;
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
            run_sweep(palace_override, target, wing).await?;
        }

        Command::Dedup {
            wing,
            threshold,
            dry_run,
            stats,
        } => {
            run_dedup(palace_override, wing, threshold, dry_run, stats).await?;
        }

        Command::Repair => {
            run_repair(palace_override).await?;
        }

        Command::Mcp => {
            run_mcp(palace_override).await?;
        }

        Command::Export {
            output,
            wing,
            dry_run,
        } => {
            run_export(palace_override, output, wing, dry_run).await?;
        }

        Command::DiaryIngest {
            directory,
            wing,
            agent,
            force,
        } => {
            run_diary_ingest(palace_override, directory, wing, agent, force).await?;
        }

        Command::ClosetLlm {
            wing,
            sample,
            dry_run,
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
            run_closet_llm(palace_override, wing, sample, dry_run, llm_opts).await?;
        }

        Command::Instructions { name } => {
            cli::instructions::run(&name)?;
        }

        Command::Hook { hook, harness } => {
            cli::hook::run(&hook, &harness).await?;
        }

        Command::Onboard { directory } => {
            let directory = expand_tilde(&directory);
            cli::onboarding::run(&directory)?;
        }
    }

    Ok(())
}

/// Handle the `search` sub-command — opens the palace and runs the search.
async fn run_search(
    palace_override: Option<&std::path::Path>,
    query: String,
    wing: Option<String>,
    room: Option<String>,
    results: usize,
) -> error::Result<()> {
    let (_db, connection, _path) = open_palace(palace_override).await?;
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
async fn run_wakeup(
    palace_override: Option<&std::path::Path>,
    wing: Option<String>,
    room: Option<String>,
    query: Option<String>,
    results: usize,
) -> error::Result<()> {
    let (_db, connection, _path) = open_palace(palace_override).await?;
    cli::wakeup::run(
        &connection,
        wing.as_deref(),
        room.as_deref(),
        query.as_deref(),
        results,
    )
    .await
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

/// Handle the `dedup` sub-command — finds and removes near-duplicate drawers.
async fn run_dedup(
    palace_override: Option<&std::path::Path>,
    wing: Option<String>,
    threshold: f64,
    dry_run: bool,
    stats: bool,
) -> error::Result<()> {
    let (_db, connection, _path) = open_palace(palace_override).await?;
    cli::dedup::run(&connection, wing.as_deref(), threshold, dry_run, stats).await
}

/// Handle the `repair` sub-command — opens the palace and runs repair.
async fn run_repair(palace_override: Option<&std::path::Path>) -> error::Result<()> {
    let (_db, connection, palace_path) = open_palace(palace_override).await?;
    cli::repair::run(&connection, &palace_path).await
}

/// Handle the `mcp` sub-command — opens the palace and starts the MCP server.
async fn run_mcp(palace_override: Option<&std::path::Path>) -> error::Result<()> {
    let (_db, connection, _path) = open_palace(palace_override).await?;
    mcp::run(&connection).await
}

/// Handle the `status` sub-command — opens the palace if it exists.
///
/// `palace_override` takes priority over env vars and config, matching the
/// `--palace` CLI flag semantics used by `open_palace`.
async fn run_status(palace_override: Option<&std::path::Path>) -> error::Result<()> {
    let config = MempalaceConfig::load()?;
    let db_path = match palace_override {
        Some(path) => crate::config::expand_tilde(path),
        None => config.palace_db_path(),
    };

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
    palace_override: Option<&std::path::Path>,
    wing: Option<String>,
    dry_run: bool,
    config: Option<std::path::PathBuf>,
) -> error::Result<()> {
    let (_db, connection, _path) = open_palace(palace_override).await?;
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
async fn run_sweep(
    palace_override: Option<&std::path::Path>,
    target: std::path::PathBuf,
    wing: String,
) -> error::Result<()> {
    let target = expand_tilde(&target);
    let (_db, connection, _path) = open_palace(palace_override).await?;
    cli::sweep::run(&connection, &target, &wing).await
}

/// Handle the `diary-ingest` sub-command — ingests diary markdown files into the palace.
async fn run_diary_ingest(
    palace_override: Option<&std::path::Path>,
    directory: std::path::PathBuf,
    wing: String,
    agent: String,
    force: bool,
) -> error::Result<()> {
    let directory = expand_tilde(&directory);
    let (_db, connection, _path) = open_palace(palace_override).await?;
    cli::diary_ingest::run(&connection, &directory, &wing, &agent, force).await
}

/// Handle the `closet-llm` sub-command — regenerates closets using a configured LLM.
async fn run_closet_llm(
    palace_override: Option<&std::path::Path>,
    wing: Option<String>,
    sample: usize,
    dry_run: bool,
    llm_opts: cli::init::LlmOpts,
) -> error::Result<()> {
    let (_db, connection, _path) = open_palace(palace_override).await?;
    cli::closet_llm::run(&connection, wing.as_deref(), sample, dry_run, &llm_opts).await
}

/// Handle the `export` sub-command — exports palace drawers to markdown files.
async fn run_export(
    palace_override: Option<&std::path::Path>,
    output: std::path::PathBuf,
    wing: Option<String>,
    dry_run: bool,
) -> error::Result<()> {
    let output = expand_tilde(&output);
    let (_db, connection, _path) = open_palace(palace_override).await?;
    cli::export::run(&connection, &output, wing.as_deref(), dry_run).await
}

/// Handle the `mine` sub-command — delegates to the correct miner by mode.
// Arguments mirror the CLI fields 1:1 — no meaningful grouping exists.
#[allow(clippy::too_many_arguments)]
async fn run_mine(
    palace_override: Option<&std::path::Path>,
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
            let (_db, connection, _path) = open_palace(palace_override).await?;
            palace::miner::mine(&connection, &directory, &opts).await?;
        }
        "convos" => {
            let (_db, connection, _path) = open_palace(palace_override).await?;
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
            None,
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
                    palace: None,
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

    // -- open_palace with override --------------------------------------------

    #[tokio::test]
    async fn open_palace_with_override_creates_db_at_override_path() {
        // When palace_override is Some, open_palace must create the DB at that
        // path rather than using MEMPALACE_DIR or config defaults.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for open_palace override test");
        let override_path = temp_directory.path().join("override_palace.db");
        assert!(
            !override_path.exists(),
            "override DB must not exist before open_palace"
        );

        let result = open_palace(Some(&override_path)).await;
        assert!(
            result.is_ok(),
            "open_palace with override must succeed: {:?}",
            result.err()
        );
        // Pair assertion: the DB must have been created at the override path.
        assert!(
            override_path.exists(),
            "DB must be created at the override path"
        );
    }

    // -- run_status with palace override --------------------------------------

    #[tokio::test]
    async fn run_status_with_palace_override_reports_missing_db() {
        // When palace_override points to a non-existent DB, run_status must
        // print a friendly message and return Ok (same behaviour as env var override).
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for run_status override test");
        let override_path = temp_directory.path().join("missing_override.db");
        assert!(
            !override_path.exists(),
            "override DB must not exist before run_status"
        );

        let result = run_status(Some(&override_path)).await;
        assert!(
            result.is_ok(),
            "run_status with palace override pointing at non-existent DB must return Ok"
        );
        // Pair assertion: run_status must not create the DB when reporting absence.
        assert!(
            !override_path.exists(),
            "run_status must not create a DB when reporting absence"
        );
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
                let result = run_status(None).await;
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
