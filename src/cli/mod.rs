//! CLI command definitions and handlers for the `mempalace` binary.

pub mod compress;
pub mod init;
pub mod search;
pub mod split;
pub mod status;
pub mod wakeup;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "mempalace",
    version,
    about = "A memory palace for AI assistants"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Initialize a new palace from a project directory
    Init {
        /// Path to project directory
        dir: PathBuf,

        /// Auto-accept detected rooms without prompting (non-interactive / CI mode)
        #[arg(long)]
        yes: bool,
    },

    /// Mine files into the palace
    Mine {
        /// Path to project directory
        dir: PathBuf,

        /// Mining mode: projects or convos
        #[arg(long, default_value = "projects")]
        mode: String,

        /// Extraction mode for convos: exchange or general
        #[arg(long, default_value = "exchange")]
        extract_mode: String,

        /// Override the wing name (default: from mempalace.yaml or directory name)
        #[arg(long)]
        wing: Option<String>,

        /// Agent name recorded on each drawer (default: mempalace)
        #[arg(long, default_value = "mempalace")]
        agent: String,

        /// Maximum number of files to process; 0 means no limit
        #[arg(long, default_value = "0")]
        limit: usize,

        /// Preview what would be filed without writing to the palace
        #[arg(long)]
        dry_run: bool,
    },

    /// Search the palace
    Search {
        /// Search query
        query: String,

        /// Filter by wing
        #[arg(long)]
        wing: Option<String>,

        /// Filter by room
        #[arg(long)]
        room: Option<String>,

        /// Number of results
        #[arg(long, default_value = "10")]
        results: usize,
    },

    /// Generate wake-up context (L0 + L1)
    WakeUp {
        /// Filter by wing
        #[arg(long)]
        wing: Option<String>,
    },

    /// Compress drawers using AAAK dialect
    Compress {
        /// Filter by wing
        #[arg(long)]
        wing: Option<String>,

        /// Dry run — show stats without writing
        #[arg(long)]
        dry_run: bool,

        /// Path to dialect config
        #[arg(long)]
        config: Option<PathBuf>,
    },

    /// Split concatenated mega-files into per-session files
    Split {
        /// Path to directory containing files to split
        dir: PathBuf,

        /// Output directory
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// Dry run — preview without writing
        #[arg(long)]
        dry_run: bool,

        /// Minimum sessions to trigger split
        #[arg(long, default_value = "2")]
        min_sessions: usize,
    },

    /// Show palace overview and stats
    Status,

    /// Run as MCP server (JSON-RPC over stdio)
    Mcp,
}
