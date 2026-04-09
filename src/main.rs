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

/// Open the palace DB, ensuring schema exists. Returns `(db, conn, path)`.
async fn open_palace() -> error::Result<(turso::Database, turso::Connection, std::path::PathBuf)> {
    let cfg = MempalaceConfig::init()?;
    let db_path = cfg.palace_db_path();
    let (db, conn) = db::open_db(db_path.to_str().unwrap_or(":memory:")).await?;
    schema::ensure_schema(&conn).await?;
    Ok((db, conn, db_path))
}

async fn run(cli: Cli) -> error::Result<()> {
    match cli.command {
        Command::Status => {
            let cfg = MempalaceConfig::load()?;
            let db_path = cfg.palace_db_path();

            if !db_path.exists() {
                println!("No palace found at {}", db_path.display());
                println!("Run `mempalace init <dir>` to get started.");
                return Ok(());
            }

            let (_db, conn) = db::open_db(db_path.to_str().unwrap_or(":memory:")).await?;
            cli::status::run(&conn).await?;
        }

        Command::Init { dir, yes } => {
            cli::init::run(&dir, yes)?;
        }

        Command::Mine {
            dir,
            mode,
            extract_mode,
            wing,
            agent,
            limit,
            dry_run,
            no_gitignore,
        } => {
            let opts = palace::miner::MineParams {
                wing,
                agent,
                limit,
                dry_run,
                respect_gitignore: !no_gitignore,
            };
            match mode.as_str() {
                "projects" => {
                    let (_db, conn, _path) = open_palace().await?;
                    palace::miner::mine(&conn, &dir, &opts).await?;
                }
                "convos" => {
                    let (_db, conn, _path) = open_palace().await?;
                    palace::convo_miner::mine_convos(&conn, &dir, &extract_mode, &opts).await?;
                }
                other => {
                    eprintln!("unknown mine mode: {other} (expected 'projects' or 'convos')");
                    std::process::exit(1);
                }
            }
        }

        Command::Search {
            query,
            wing,
            room,
            results,
        } => {
            let (_db, conn, _path) = open_palace().await?;
            cli::search::run(&conn, &query, wing.as_deref(), room.as_deref(), results).await?;
        }

        Command::WakeUp { wing } => {
            let (_db, conn, _path) = open_palace().await?;
            cli::wakeup::run(&conn, wing.as_deref()).await?;
        }

        Command::Compress {
            wing,
            dry_run,
            config,
        } => {
            let (_db, conn, _path) = open_palace().await?;
            cli::compress::run(
                &conn,
                wing.as_deref(),
                dry_run,
                config.as_ref().and_then(|p| p.to_str()),
            )
            .await?;
        }

        Command::Split {
            dir,
            output_dir,
            dry_run,
            min_sessions,
        } => {
            cli::split::run(&dir, output_dir.as_deref(), dry_run, min_sessions)?;
        }

        Command::Repair => {
            let (_db, conn, palace_path) = open_palace().await?;
            cli::repair::run(&conn, &palace_path).await?;
        }

        Command::Mcp => {
            let (_db, conn, _path) = open_palace().await?;
            mcp::run(&conn).await?;
        }
    }

    Ok(())
}
