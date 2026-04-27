mod app;
mod cli;
mod config;
mod db;
mod dialect;
mod error;
mod i18n;
mod kg;
mod llm;
mod mcp;
mod normalize;
mod palace;
mod schema;
#[cfg(test)]
mod test_helpers;

use clap::Parser;
use cli::Cli;

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
            if let Err(e) = app::run(cli).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        });
}
