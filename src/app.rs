use crate::cli::{self, Cli, Command};
use crate::config::MempalaceConfig;
use crate::{db, error, mcp, palace, schema};

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
    let db_path_str = db_path.to_str().ok_or_else(|| {
        error::Error::Other(format!(
            "database path is not valid UTF-8: {}",
            db_path.display()
        ))
    })?;
    let (db, connection) = db::open_db(db_path_str).await?;
    schema::ensure_schema(&connection).await?;
    Ok((db, connection, db_path))
}

// Each match arm is a single CLI subcommand — splitting this into separate
// functions would not reduce complexity, only scatter the dispatch logic.
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

    // -- expand_tilde ---------------------------------------------------------

    #[test]
    fn expand_tilde_no_leading_tilde_returns_path_unchanged() {
        // A path with no leading ~ must be returned as-is.
        let path = std::path::Path::new("/absolute/path/to/file");
        let result = expand_tilde(path);
        assert_eq!(result, path, "absolute path must be returned unchanged");
        assert!(
            !result.to_string_lossy().starts_with("~/"),
            "result must not start with ~/"
        );
    }

    #[test]
    fn expand_tilde_relative_path_returned_unchanged() {
        // A relative path that does not start with ~ must be returned as-is.
        let path = std::path::Path::new("relative/path");
        let result = expand_tilde(path);
        assert_eq!(
            result, path,
            "relative path without ~ must be returned unchanged"
        );
        assert_eq!(result.to_string_lossy(), "relative/path");
    }

    #[test]
    fn expand_tilde_tilde_only_expands_to_home() {
        // A path of just "~" must expand to the HOME directory.
        temp_env::with_var("HOME", Some("/test/home"), || {
            let path = std::path::Path::new("~");
            let result = expand_tilde(path);
            assert_eq!(
                result,
                std::path::Path::new("/test/home"),
                "bare ~ must expand to HOME"
            );
            assert!(
                !result.to_string_lossy().contains('~'),
                "result must not contain ~"
            );
        });
    }

    #[test]
    fn expand_tilde_tilde_slash_path_appends_suffix() {
        // "~/foo/bar" must expand to "<HOME>/foo/bar".
        temp_env::with_var("HOME", Some("/test/home"), || {
            let path = std::path::Path::new("~/foo/bar");
            let result = expand_tilde(path);
            assert_eq!(
                result,
                std::path::Path::new("/test/home/foo/bar"),
                "~/foo/bar must expand to HOME/foo/bar"
            );
            assert!(
                result.starts_with("/test/home"),
                "result must start with HOME"
            );
        });
    }

    #[test]
    fn expand_tilde_no_home_set_returns_path_unchanged() {
        // When HOME is unset expand_tilde must return the path unchanged rather than panicking.
        // This covers the None branch of the home directory resolution chain.
        temp_env::with_vars(
            [
                ("HOME", None::<&str>),
                ("USERPROFILE", None::<&str>),
                ("HOMEDRIVE", None::<&str>),
                ("HOMEPATH", None::<&str>),
            ],
            || {
                let path = std::path::Path::new("~/no/home");
                let result = expand_tilde(path);
                // With no home env vars the expansion falls back to returning path as-is.
                assert_eq!(
                    result, path,
                    "expand_tilde must return the original path unchanged when HOME is unresolvable"
                );
                assert!(
                    !result.is_absolute(),
                    "result must remain a relative path when home is unresolvable"
                );
            },
        );
    }

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
