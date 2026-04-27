//! `mempalace closet-llm` — regenerate closets using a configured LLM.
//!
//! Reads all drawers (or a wing subset), sends each to the configured LLM via
//! the OpenAI-compatible Chat Completions API, and replaces its entry in the
//! `compressed` table with a topic-dense LLM-generated summary.
//!
//! Regex compression is always the fallback; this command supplements it for
//! richer topic extraction, foreign-language content, and contextual references.

use turso::Connection;

use crate::cli::init::LlmOpts;
use crate::error::Result;
use crate::llm::client::LlmProvider;
use crate::llm::get_provider;
use crate::palace::closet_llm;

/// Timeout for LLM calls — closets may be larger than entity refinement batches.
const LLM_TIMEOUT_SECS: u64 = 120;

const _: () = assert!(LLM_TIMEOUT_SECS > 0);

/// Run the `closet-llm` command.
///
/// Requires a configured LLM provider (via `llm_opts`). In `dry_run` mode,
/// drawers are listed but the LLM is not called and nothing is written.
/// `sample` limits processing to the first N drawers (0 = all).
pub async fn run(
    connection: &Connection,
    wing: Option<&str>,
    sample: usize,
    dry_run: bool,
    llm_opts: &LlmOpts,
) -> Result<()> {
    assert!(sample < 1_000_001, "run: sample must be <= 1_000_000");

    if !llm_opts.enabled {
        eprintln!("LLM not configured. Pass --llm and --llm-provider / --llm-model.");
        return Ok(());
    }
    assert!(
        !llm_opts.provider.is_empty(),
        "run: provider must not be empty when enabled"
    );
    assert!(
        !llm_opts.model.is_empty(),
        "run: model must not be empty when enabled"
    );

    let provider = get_provider(
        &llm_opts.provider,
        &llm_opts.model,
        llm_opts.endpoint.clone(),
        llm_opts.api_key.clone(),
        LLM_TIMEOUT_SECS,
    )?;

    let (available, message) = provider.check_available();
    if !available {
        eprintln!("LLM unavailable: {message}");
        return Ok(());
    }

    run_with_provider(
        connection,
        wing,
        sample,
        dry_run,
        provider.as_ref(),
        &llm_opts.provider,
        &llm_opts.model,
    )
    .await
}

/// Inner execution after the LLM provider is confirmed available.
///
/// Logs progress messages, calls `regenerate_closets`, asserts stat invariants,
/// and logs completion. Extracted from `run` so tests can inject a mock provider
/// directly without going through the `check_available` network probe.
async fn run_with_provider(
    connection: &Connection,
    wing: Option<&str>,
    sample: usize,
    dry_run: bool,
    provider: &dyn LlmProvider,
    provider_name: &str,
    model_name: &str,
) -> Result<()> {
    assert!(
        sample < 1_000_001,
        "run_with_provider: sample must be <= 1_000_000"
    );
    assert!(
        !provider_name.is_empty(),
        "run_with_provider: provider_name must not be empty"
    );
    assert!(
        !model_name.is_empty(),
        "run_with_provider: model_name must not be empty"
    );

    if dry_run {
        eprintln!("Dry run — no LLM calls will be made and nothing will be written.");
    } else {
        eprintln!("Regenerating closets via {provider_name} ({model_name})...");
    }

    let stats = closet_llm::regenerate_closets(connection, wing, sample, dry_run, provider).await?;

    assert!(
        stats.processed + stats.failed + stats.skipped_dry_run
            <= if sample > 0 { sample } else { usize::MAX },
        "run_with_provider: stat totals must not exceed sample limit"
    );

    if dry_run {
        eprintln!(
            "Done (dry run). {} drawers would be processed.",
            stats.skipped_dry_run
        );
    } else {
        eprintln!(
            "Done. {} processed, {} failed.",
            stats.processed, stats.failed
        );
    }

    Ok(())
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // Mock provider that always succeeds — used to exercise run_with_provider.
    struct OkProvider;
    impl crate::llm::client::LlmProvider for OkProvider {
        fn classify(
            &self,
            _system: &str,
            _user: &str,
            _json_mode: bool,
        ) -> crate::error::Result<crate::llm::client::LlmResponse> {
            Ok(crate::llm::client::LlmResponse {
                text: r#"{"topics":["test"],"quotes":[],"summary":"A test summary."}"#.to_string(),
            })
        }
        fn check_available(&self) -> (bool, String) {
            (true, "ok".to_string())
        }
        fn name(&self) -> &'static str {
            "mock-ok"
        }
    }

    #[tokio::test]
    async fn run_without_llm_enabled_returns_ok() {
        // When LLM is disabled, run must return Ok without hitting the database.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = LlmOpts::default(); // enabled = false
        let result = run(&connection, None, 0, false, &opts).await;
        assert!(result.is_ok(), "disabled LLM must not return an error");
    }

    #[tokio::test]
    async fn run_dry_run_with_no_drawers_returns_ok() {
        // Dry run on an empty palace should succeed with zero stats.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let opts = LlmOpts {
            enabled: true,
            provider: "ollama".to_string(),
            model: "llama3:8b".to_string(),
            endpoint: Some("http://localhost:11434/v1".to_string()),
            api_key: None,
        };
        // Provider is not reachable in tests — run will log "LLM unavailable" and return Ok.
        let result = run(&connection, None, 0, true, &opts).await;
        assert!(result.is_ok(), "unreachable provider must not panic");
    }

    #[tokio::test]
    async fn run_with_provider_dry_run_returns_ok() {
        // Dry run via the inner helper must return Ok and produce zero processed drawers.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = run_with_provider(
            &connection,
            None,
            0,
            true,
            &OkProvider,
            "mock-ok",
            "mock-model",
        )
        .await;
        assert!(result.is_ok(), "dry_run run_with_provider must return Ok");
        // Pair assertion: a successful dry_run on an empty palace must not Err.
        assert!(result.err().is_none(), "no error must be returned");
    }

    #[tokio::test]
    async fn run_with_provider_live_run_empty_palace_returns_ok() {
        // A live run via the inner helper on an empty palace must return Ok with zero stats.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = run_with_provider(
            &connection,
            None,
            0,
            false,
            &OkProvider,
            "mock-ok",
            "mock-model",
        )
        .await;
        assert!(result.is_ok(), "live run on empty palace must return Ok");
        // Pair assertion: success path must not leave an error.
        assert!(result.err().is_none(), "no error must be returned");
    }
}
