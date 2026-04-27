//! CLI command definitions and handlers for the `mempalace` binary.

pub mod closet_llm;
pub mod compress;
pub mod dedup;
pub mod diary_ingest;
pub mod export;
pub mod hook;
pub mod init;
pub mod instructions;
pub mod onboarding;
pub mod repair;
pub mod search;
pub mod split;
pub mod status;
pub mod sweep;
pub mod wakeup;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "mempalace",
    version = concat!(env!("CARGO_PKG_VERSION"), "-", env!("GIT_SHORT_SHA")),
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
        directory: PathBuf,

        /// Auto-accept detected rooms without prompting (non-interactive / CI mode)
        #[arg(long, short = 'y')]
        yes: bool,

        /// Disable .gitignore filtering (include all files regardless of gitignore rules)
        #[arg(long)]
        no_gitignore: bool,

        /// Enable LLM-assisted entity refinement
        #[arg(long)]
        llm: bool,

        /// LLM provider: ollama, openai-compat, or anthropic
        #[arg(long, default_value = "ollama")]
        llm_provider: String,

        /// LLM model name (e.g. gemma3:4b for Ollama, claude-haiku-4-5-20251001 for Anthropic)
        #[arg(long, default_value = "gemma3:4b")]
        llm_model: String,

        /// LLM API endpoint URL (required for openai-compat, optional for others)
        #[arg(long)]
        llm_endpoint: Option<String>,

        /// LLM API key (for anthropic or authenticated openai-compat endpoints)
        #[arg(long)]
        llm_api_key: Option<String>,
    },

    /// Mine files into the palace
    Mine {
        /// Path to project directory
        directory: PathBuf,

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

        /// Disable .gitignore filtering (include all files regardless of gitignore rules)
        #[arg(long)]
        no_gitignore: bool,
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

    /// Generate wake-up context (L0 + L1; L2 when --wing/--room is given; L3 with --query)
    WakeUp {
        /// Filter by wing (also enables L2 on-demand recall for that wing)
        #[arg(long)]
        wing: Option<String>,

        /// Filter by room within the wing (requires --wing; enables L2 room-scoped recall)
        #[arg(long)]
        room: Option<String>,

        /// Run a keyword search query and append L3 deep-search results
        #[arg(long)]
        query: Option<String>,

        /// Number of results for L2 recall and L3 search
        #[arg(long, default_value = "20")]
        results: usize,
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
        directory: PathBuf,

        /// Output directory
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// Dry run — preview without writing
        #[arg(long)]
        dry_run: bool,

        /// Minimum sessions to trigger split
        #[arg(long, default_value = "2", alias = "min-sessions")]
        sessions_min: usize,

        /// Disable .gitignore filtering (include all files regardless of gitignore rules)
        #[arg(long)]
        no_gitignore: bool,
    },

    /// Tandem miner: catch messages the primary miner missed
    ///
    /// Sweeps a `.jsonl` transcript file or directory, inserting one drawer
    /// per user/assistant message not already present.  Idempotent: re-running
    /// the same target is a safe no-op.
    Sweep {
        /// Path to a `.jsonl` transcript file or a directory to scan recursively
        target: PathBuf,

        /// Wing to file drawers under
        #[arg(long, default_value = "conversations")]
        wing: String,
    },

    /// Show palace overview and stats
    Status,

    /// Detect and remove near-duplicate drawers using Jaccard similarity
    Dedup {
        /// Only deduplicate drawers in this wing
        #[arg(long)]
        wing: Option<String>,

        /// Jaccard similarity threshold; pairs above this are considered duplicates
        #[arg(long, default_value = "0.85")]
        threshold: f64,

        /// Show what would be deleted without actually deleting
        #[arg(long)]
        dry_run: bool,

        /// Print stats only without deleting
        #[arg(long)]
        stats: bool,
    },

    /// Rebuild the inverted index (repair corrupted palace)
    Repair,

    /// Run as MCP server (JSON-RPC over stdio)
    Mcp,

    /// Export palace drawers to markdown files on disk
    Export {
        /// Output directory (default: ./palace-export)
        #[arg(long, default_value = "palace-export")]
        output: std::path::PathBuf,

        /// Filter by wing
        #[arg(long)]
        wing: Option<String>,

        /// Preview what would be exported without writing files
        #[arg(long)]
        dry_run: bool,
    },

    /// Print packaged skill instructions for a named `MemPalace` command
    Instructions {
        /// Instruction name: help, init, mine, search, or status
        name: String,
    },

    /// First-run interactive setup wizard — seeds your entity registry
    Onboard {
        /// Directory to scan for additional entity candidates (default: current dir)
        #[arg(long, short = 'd', default_value = ".")]
        directory: PathBuf,
    },

    /// Run a hook handler (session-start, stop, or precompact)
    Hook {
        /// Hook name: session-start, stop, or precompact
        #[arg(long)]
        hook: String,

        /// Harness name: claude-code or codex
        #[arg(long, default_value = "claude-code")]
        harness: String,
    },

    /// Ingest on-disk markdown diary files (`YYYY-MM-DD*.md`) into the palace
    DiaryIngest {
        /// Path to the directory containing diary markdown files
        directory: PathBuf,

        /// Wing to file diary drawers under
        #[arg(long, default_value = "diary")]
        wing: String,

        /// Agent name recorded on each drawer
        #[arg(long, default_value = "mempalace")]
        agent: String,

        /// Re-ingest all sections even if already filed
        #[arg(long)]
        force: bool,
    },

    /// Regenerate compressed closets using a configured LLM for richer topic extraction
    ClosetLlm {
        /// Limit regeneration to a specific wing (default: all wings)
        #[arg(long)]
        wing: Option<String>,

        /// Only process the first N drawers; 0 means all
        #[arg(long, default_value = "0")]
        sample: usize,

        /// Preview work without calling the LLM or writing to the palace
        #[arg(long)]
        dry_run: bool,

        /// Enable LLM (required)
        #[arg(long)]
        llm: bool,

        /// LLM provider: ollama, openai-compat, or anthropic
        #[arg(long, default_value = "ollama")]
        llm_provider: String,

        /// LLM model name (e.g. llama3:8b, gpt-4o-mini, claude-haiku-4-5-20251001)
        #[arg(long, default_value = "llama3:8b")]
        llm_model: String,

        /// LLM API endpoint URL (required for openai-compat, optional for others)
        #[arg(long)]
        llm_endpoint: Option<String>,

        /// LLM API key (for anthropic or authenticated openai-compat endpoints)
        #[arg(long)]
        llm_api_key: Option<String>,
    },
}
