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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

/// Open the palace DB, ensuring schema exists.
async fn open_palace() -> error::Result<(turso::Database, turso::Connection)> {
    let cfg = MempalaceConfig::init()?;
    let db_path = cfg.palace_db_path();
    let (db, conn) = db::open_db(db_path.to_str().unwrap_or(":memory:")).await?;
    schema::ensure_schema(&conn).await?;
    Ok((db, conn))
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

        Command::Init { dir, yes: _ } => {
            cli::init::run(&dir)?;
        }

        Command::Mine {
            dir,
            mode,
            extract_mode,
            wing,
            agent,
            limit,
            dry_run,
        } => {
            let opts = palace::miner::MineParams {
                wing,
                agent,
                limit,
                dry_run,
            };
            match mode.as_str() {
                "projects" => {
                    let (_db, conn) = open_palace().await?;
                    palace::miner::mine(&conn, &dir, &opts).await?;
                }
                "convos" => {
                    let (_db, conn) = open_palace().await?;
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
            let (_db, conn) = open_palace().await?;
            cli::search::run(&conn, &query, wing.as_deref(), room.as_deref(), results).await?;
        }

        Command::WakeUp { wing } => {
            let (_db, conn) = open_palace().await?;
            cli::wakeup::run(&conn, wing.as_deref()).await?;
        }

        Command::Compress {
            wing,
            dry_run,
            config,
        } => {
            let (_db, conn) = open_palace().await?;
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

        Command::Mcp => {
            let (_db, conn) = open_palace().await?;
            mcp::run(&conn).await?;
        }
    }

    Ok(())
}
