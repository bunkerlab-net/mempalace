mod cli;
mod config;
mod db;
mod dialect;
mod error;
#[allow(dead_code)]
mod extract;
mod kg;
mod mcp;
mod normalize;
mod palace;
mod schema;
#[cfg(test)]
#[allow(dead_code)]
mod test_helpers;

use clap::Parser;
use cli::{Cli, Command};
use config::MempalaceConfig;

// Disable turso/limbo's exclusive file lock before the Tokio runtime spawns
// worker threads. This allows multiple mempalace processes (e.g. concurrent
// MCP servers or CLI commands) to open the same database concurrently; WAL
// mode provides the concurrency control at the protocol level.
// See: https://github.com/bunkerlab-net/mempalace/issues/9
//
// SAFETY: set_var is unsafe because it is not thread-safe, but this runs
// before the Tokio runtime is built and before any other threads exist.
#[allow(unsafe_code)]
// tokio runtime build failure is unrecoverable — no Result to propagate to.
#[allow(clippy::expect_used)]
fn main() {
    unsafe {
        std::env::set_var("LIMBO_DISABLE_FILE_LOCK", "1");
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
        .block_on(async {
            let cli = Cli::parse();
            if let Err(e) = run(cli).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        });
}

/// Expand a leading `~` to the user's home directory.
///
/// Resolves the home directory by trying `HOME`, then `USERPROFILE`, then
/// `HOMEDRIVE` + `HOMEPATH` (Windows fallback). Uses `OsStr`-based path
/// component inspection to avoid lossy UTF-8 conversion.
fn expand_tilde(path: &std::path::Path) -> std::path::PathBuf {
    use std::ffi::OsStr;
    use std::path::Component;

    let mut components = path.components();
    let first = components.next();

    if first != Some(Component::Normal(OsStr::new("~"))) {
        return path.to_path_buf();
    }

    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let home_path = std::env::var_os("HOMEPATH")?;
            Some(
                std::path::PathBuf::from(drive)
                    .join(home_path)
                    .into_os_string(),
            )
        });

    match home {
        Some(h) => {
            let rest: std::path::PathBuf = components.collect();
            std::path::PathBuf::from(h).join(rest)
        }
        None => path.to_path_buf(),
    }
}

/// Open the palace DB, ensuring schema exists. Returns `(db, connection, path)`.
async fn open_palace() -> error::Result<(turso::Database, turso::Connection, std::path::PathBuf)> {
    let config = MempalaceConfig::init()?;
    let db_path = config.palace_db_path();
    let (db, connection) = db::open_db(db_path.to_str().unwrap_or(":memory:")).await?;
    schema::ensure_schema(&connection).await?;
    Ok((db, connection, db_path))
}

// Each match arm is a single CLI subcommand — splitting this into separate
// functions would not reduce complexity, only scatter the dispatch logic.
#[allow(clippy::too_many_lines)]
async fn run(cli: Cli) -> error::Result<()> {
    match cli.command {
        Command::Status => {
            run_status().await?;
        }

        Command::Init {
            directory,
            yes,
            no_gitignore,
        } => {
            cli::init::run(&directory, yes, no_gitignore)?;
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
            let (_db, connection, _path) = open_palace().await?;
            cli::search::run(
                &connection,
                &query,
                wing.as_deref(),
                room.as_deref(),
                results,
            )
            .await?;
        }

        Command::WakeUp { wing } => {
            let (_db, connection, _path) = open_palace().await?;
            cli::wakeup::run(&connection, wing.as_deref()).await?;
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
            min_sessions,
            no_gitignore,
        } => {
            // Expand ~ so that `mempalace split ~/convos` works as expected.
            let directory = expand_tilde(&directory);
            let output_dir = output_dir.as_deref().map(expand_tilde);
            cli::split::run(
                &directory,
                output_dir.as_deref(),
                dry_run,
                min_sessions,
                !no_gitignore,
            )?;
        }

        Command::Repair => {
            let (_db, connection, palace_path) = open_palace().await?;
            cli::repair::run(&connection, &palace_path).await?;
        }

        Command::Mcp => {
            let (_db, connection, _path) = open_palace().await?;
            mcp::run(&connection).await?;
        }
    }

    Ok(())
}

/// Handle the `status` sub-command — opens the palace read-only if it exists.
async fn run_status() -> error::Result<()> {
    let config = MempalaceConfig::load()?;
    let db_path = config.palace_db_path();

    if !db_path.exists() {
        println!("No palace found at {}", db_path.display());
        println!("Run `mempalace init <dir>` to get started.");
        return Ok(());
    }

    let (_db, connection) = db::open_db(db_path.to_str().unwrap_or(":memory:")).await?;
    cli::status::run(&connection).await
}

/// Handle the `compress` sub-command — compresses drawers into AAAK dialect format.
async fn run_compress(
    wing: Option<String>,
    dry_run: bool,
    config: Option<std::path::PathBuf>,
) -> error::Result<()> {
    let (_db, connection, _path) = open_palace().await?;
    cli::compress::run(
        &connection,
        wing.as_deref(),
        dry_run,
        config.as_ref().and_then(|p| p.to_str()),
    )
    .await
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
            eprintln!("unknown mine mode: {other} (expected 'projects' or 'convos')");
            std::process::exit(1);
        }
    }
    Ok(())
}
