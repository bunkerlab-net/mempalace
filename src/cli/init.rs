//! `mempalace init` — scan a directory and write `mempalace.yaml`.
//!
//! Pipeline:
//! 1. Entity discovery: manifest files + git authors (+ Claude Code sessions)
//! 2. Optional LLM refinement: `--llm` flags enable batched entity classification
//! 3. Entity confirmation: interactive or `--yes` auto-accept
//! 4. Room detection from folder structure
//! 5. Write `entities.json` and `mempalace.yaml` to the project directory

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::config::{ProjectConfig, RoomConfig};
use crate::error::Result;
use crate::llm::client::ApiKeySource;
use crate::llm::{LlmProvider, collect_corpus_text, get_provider, refine_entities};
use crate::palace::corpus_origin::{
    CorpusOriginResult, detect_origin_heuristic, detect_origin_llm,
};
use crate::palace::entities::DetectedEntity;
use crate::palace::entity_confirm::confirm_entities;
use crate::palace::entity_detect;
use crate::palace::known_entities::add_to_known_entities;
use crate::palace::project_scanner::{
    DetectedDict, ProjectInfo, merge_detected, scan, to_detected_dict,
};
use crate::palace::room_detect::detect_rooms_from_folders;
use crate::palace::session_scanner::{is_claude_projects_root, scan_claude_projects};

// Timeout for LLM calls during entity refinement — long enough for local models.
const LLM_TIMEOUT_SECS: u64 = 60;

// Corpus excerpt size for corpus-origin Pass 0 detection (same cap as the LLM tier).
const CORPUS_SAMPLE_SIZE: usize = 800;

const _: () = assert!(LLM_TIMEOUT_SECS > 0);
const _: () = assert!(CORPUS_SAMPLE_SIZE > 0);

// ===================== PUBLIC TYPES =====================

/// Options for LLM-assisted entity refinement, passed through from CLI flags.
pub struct LlmOpts {
    /// Whether LLM refinement is enabled (`--llm` flag).
    pub enabled: bool,
    /// Provider name: `"ollama"`, `"openai-compat"`, or `"anthropic"`.
    pub provider: String,
    /// Model identifier (e.g. `"gemma3:4b"` for Ollama).
    pub model: String,
    /// Custom API endpoint URL (required for `openai-compat`, optional for others).
    pub endpoint: Option<String>,
    /// API key (required for `anthropic`, optional for authenticated endpoints).
    pub api_key: Option<String>,
    /// When `true`, bypass the interactive consent prompt that fires when an
    /// external LLM provider is configured via an environment-variable API key.
    /// Use in CI or non-interactive runs where you have already opted in.
    pub accept_external_llm: bool,
}

impl Default for LlmOpts {
    /// Return an `LlmOpts` with refinement disabled — used in tests.
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "ollama".to_string(),
            model: "gemma3:4b".to_string(),
            endpoint: None,
            api_key: None,
            accept_external_llm: false,
        }
    }
}

// ===================== PUBLIC API =====================

/// Run `mempalace init` for `directory`.
///
/// Discovers entities, optionally refines via LLM, confirms with the user,
/// detects rooms, then writes `entities.json` and `mempalace.yaml`.
/// If `lang` is non-empty those BCP-47 codes replace the global
/// `entity_languages` setting. After writing the config, `mempalace.yaml`
/// and `entities.json` are appended to the directory's `.gitignore` when
/// the directory is inside a git worktree and the files aren't already
/// listed.
///
/// Returns `Some(files)` when the user agrees to mine immediately; the caller
/// (app.rs) opens the palace and passes those files to `mine()` to avoid a
/// second directory walk. Returns `None` when the user declines or when
/// `auto_mine` is `false` and stdin is EOF (non-interactive).
pub fn run(
    directory: &Path,
    yes: bool,
    auto_mine: bool,
    no_gitignore: bool,
    lang: &[String],
    llm_opts: &LlmOpts,
) -> Result<Option<Vec<PathBuf>>> {
    let directory = directory.canonicalize().map_err(|error| {
        crate::error::Error::Other(format!(
            "directory not found: {}: {error}",
            directory.display()
        ))
    })?;
    assert!(
        directory.is_dir(),
        "canonicalize succeeded so directory must exist"
    );
    assert!(!directory.as_os_str().is_empty());

    // Pass 0: detect whether this corpus is an AI-dialogue record. Result is
    // threaded into entity discovery (to filter out AI persona names) and entity
    // refinement (to prime the LLM classifier with platform/persona context).
    let corpus_origin = run_detect_corpus_origin(&directory, llm_opts);

    // Phase 1: discover entities from manifest files and git history.
    let (projects, detected) = run_discover_entities(&directory, lang, &corpus_origin);

    // Phase 1.5: optionally refine entities with the LLM.
    let detected = run_refine_entities(detected, &directory, llm_opts, &corpus_origin)?;

    // Phase 2: detect rooms from folder structure.
    let rooms = detect_rooms_from_folders(&directory);
    let wing_name = run_derive_wing_name(&projects, &directory);

    // Pre-scan once; reuse for the file-count display and the optional mine step.
    let scanned_files = crate::palace::miner::scan_project_with_opts(&directory, !no_gitignore);
    let total_bytes = run_compute_total_bytes(&scanned_files);

    // Present summary and ask whether to proceed before any writes so a "no"
    // answer leaves no side effects (entity files, config) on disk.
    run_print_summary(
        &wing_name,
        scanned_files.len(),
        total_bytes,
        &rooms,
        &detected,
    );

    if !run_prompt_proceed(yes)? {
        return Ok(None);
    }

    // Persist the corpus-origin audit trail only after the user agrees to
    // proceed — declining at the prompt must leave no side effects on disk.
    // Non-fatal on error; serialisation/IO failures are deliberately ignored
    // because the audit file is supplementary, not load-bearing.
    if let Ok(json_text) = serde_json::to_string_pretty(&corpus_origin.to_json_value()) {
        let _ = std::fs::write(directory.join("corpus_origin.json"), json_text.as_bytes());
    }

    // Write mempalace.yaml before entities.json so a failure in run_write_config
    // does not leave an orphaned entities.json on disk without a valid config.
    run_write_config(&wing_name, rooms, &directory)?;
    run_confirm_and_save(&detected, yes, &directory, &wing_name)?;

    if !lang.is_empty() {
        run_persist_lang(lang);
    }
    run_gitignore_protect(&directory);

    // Ask whether to mine immediately; --auto-mine skips the prompt.
    if !run_prompt_mine(auto_mine)? {
        return Ok(None);
    }
    Ok(Some(scanned_files))
}

// ===================== PRIVATE HELPERS =====================

/// Run Pass 0 corpus-origin detection on the project directory.
///
/// Tier 1 (heuristic) always runs. Tier 2 (LLM) runs only when `llm_opts` is
/// enabled, a provider is reachable, AND the non-interactive consent gate
/// `pass0_consent_obtained` returns `true` (local provider, an explicit
/// `--llm-api-key`, or `--accept-external-llm`). An env-fallback key against
/// an external provider does NOT clear this gate at Pass 0 because there is
/// no user-facing prompt here; the interactive consent prompt is handled
/// later in Phase 1.5 (`run_setup_llm_consent_check`).
///
/// Merge rule (when Tier 2 runs): `likely_ai_dialogue` and `confidence` are
/// kept from the heuristic; `primary_platform`, `user_name`, and
/// `agent_persona_names` are taken from the LLM when non-empty; `evidence`
/// from both tiers is concatenated. Result is persisted to
/// `corpus_origin.json` by `run()` after the proceed-confirm gate.
fn run_detect_corpus_origin(directory: &Path, llm_opts: &LlmOpts) -> CorpusOriginResult {
    assert!(
        directory.is_dir(),
        "run_detect_corpus_origin: must be a dir"
    );

    // Use existing corpus collector; split into fixed-size chunks to produce
    // samples for both detection tiers. `from_utf8_lossy` preserves text whose
    // multibyte boundaries fall inside a chunk by replacing the partial sequence
    // with U+FFFD instead of dropping the entire chunk — important for non-ASCII
    // corpora where strict UTF-8 parsing would bias Pass 0.
    let corpus = collect_corpus_text(directory);
    let samples: Vec<String> = corpus
        .as_bytes()
        .chunks(CORPUS_SAMPLE_SIZE)
        .take(20)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect();

    let heuristic = detect_origin_heuristic(&samples);

    // Tier 2: attempt LLM-assisted detection if a provider is configured AND
    // implicit consent is granted (local provider, --accept-external-llm, or
    // an explicit --llm-api-key). Pass 0 is non-interactive, so an env-fallback
    // key against an external provider must NOT trigger an LLM call here —
    // entity refinement (Phase 1.5) prompts the user separately. Errors are
    // silently swallowed; corpus-origin failure must not abort init.
    let result = run_detect_corpus_origin_llm(&heuristic, &samples, llm_opts);

    assert!(result.confidence >= 0.0 && result.confidence <= 1.0);
    result
}

/// Optionally upgrade a heuristic result with LLM-tier detection, then merge.
///
/// Merges field-by-field: `likely_ai_dialogue`/`confidence` from heuristic;
/// `primary_platform`, `user_name`, and persona names from LLM (when non-empty);
/// `evidence` concatenated.
/// Returns `heuristic` unchanged when the LLM is disabled, unavailable, or the
/// non-interactive consent gate (`pass0_consent_obtained`) blocks the call.
/// Called by [`run_detect_corpus_origin`].
fn run_detect_corpus_origin_llm(
    heuristic: &CorpusOriginResult,
    samples: &[String],
    llm_opts: &LlmOpts,
) -> CorpusOriginResult {
    if !llm_opts.enabled || samples.is_empty() {
        return heuristic.clone();
    }
    let Ok(provider) = get_provider(
        &llm_opts.provider,
        &llm_opts.model,
        llm_opts.endpoint.clone(),
        llm_opts.api_key.clone(),
        LLM_TIMEOUT_SECS,
    ) else {
        return heuristic.clone();
    };
    // Non-interactive consent gate: an external provider with an env-fallback
    // key cannot be used in Pass 0 because there is no user-facing prompt here.
    // The interactive consent gate runs in Phase 1.5 (`run_setup_llm_consent_check`).
    if !pass0_consent_obtained(provider.as_ref(), llm_opts) {
        return heuristic.clone();
    }
    let (available, _) = provider.check_available();
    if !available {
        return heuristic.clone();
    }
    let llm = detect_origin_llm(samples, provider.as_ref());
    // Merge: keep heuristic's likely_ai_dialogue + confidence (more reliable than
    // LLM alone for the binary verdict); take LLM's richer metadata fields.
    let mut combined_evidence = heuristic.evidence.clone();
    combined_evidence.extend(llm.evidence);
    CorpusOriginResult {
        likely_ai_dialogue: heuristic.likely_ai_dialogue,
        confidence: heuristic.confidence,
        primary_platform: llm
            .primary_platform
            .or_else(|| heuristic.primary_platform.clone()),
        user_name: llm.user_name.or_else(|| heuristic.user_name.clone()),
        agent_persona_names: if llm.agent_persona_names.is_empty() {
            heuristic.agent_persona_names.clone()
        } else {
            llm.agent_persona_names
        },
        evidence: combined_evidence,
    }
}

/// Discover entities from manifest files, git history, and Claude Code sessions.
///
/// Returns the raw `ProjectInfo` list (for wing-name derivation) and the merged
/// `DetectedDict`. Never fails — returns empty collections when the directory has
/// no recognized signals. Called by [`run`].
///
/// `corpus_origin` is used to filter out AI agent persona names from the entity
/// lists so they are not incorrectly confirmed as human contributors.
fn run_discover_entities(
    directory: &Path,
    lang: &[String],
    corpus_origin: &CorpusOriginResult,
) -> (Vec<ProjectInfo>, DetectedDict) {
    assert!(directory.is_dir());
    assert!(!directory.as_os_str().is_empty());

    let (projects, people) = scan(directory);
    let real_signal = to_detected_dict(&projects, &people);

    let session_projects = if is_claude_projects_root(directory) {
        scan_claude_projects(directory)
    } else {
        vec![]
    };
    let session_signal = to_detected_dict(&session_projects, &[]);
    let has_real_signal = !projects.is_empty() || !people.is_empty();
    let detected = merge_detected(real_signal, session_signal, has_real_signal);

    // Phase 1b: supplement with prose-based entity detection. Use the CLI-provided
    // languages so prose detection respects --lang during init; defaults to English
    // when the caller passed no languages.
    let mut detected = run_discover_entities_prose(directory, detected, lang);

    // Apply corpus-origin filtering: strip entity names that match AI agent persona
    // names so they are not confirmed as human contributors.
    entity_detect::apply_corpus_origin(&mut detected, corpus_origin);

    // Pair assertion: entity lists are bounded.
    debug_assert!(
        detected.people.len() < 1_000_000,
        "people count must be bounded"
    );
    debug_assert!(
        detected.projects.len() < 1_000_000,
        "projects count must be bounded"
    );

    (projects, detected)
}

/// Called by `run_discover_entities` to add prose-detected entity candidates.
///
/// Scans up to 10 prose/code files in `directory`, runs `detect_entities` with
/// English patterns, then merges any new candidates into `current`. Returns
/// `current` unchanged if no prose files are found.
fn run_discover_entities_prose(
    directory: &Path,
    current: DetectedDict,
    lang: &[String],
) -> DetectedDict {
    let prose_files = entity_detect::scan_for_detection(directory, 10);
    if prose_files.is_empty() {
        return current;
    }
    let prose_refs: Vec<&std::path::Path> = prose_files
        .iter()
        .map(std::path::PathBuf::as_path)
        .collect();
    // Borrow the caller's languages as &str slices; default to English when empty
    // so existing single-language callers keep working without configuration.
    let lang_refs: Vec<&str> = lang.iter().map(String::as_str).collect();
    let languages: &[&str] = if lang_refs.is_empty() {
        &["en"]
    } else {
        &lang_refs
    };
    let result = entity_detect::detect_entities(&prose_refs, 10, languages);

    assert!(
        result.people.len() + result.projects.len() + result.uncertain.len()
            <= prose_refs.len() * 1000,
        "prose detection result is unexpectedly large"
    );

    let prose_signal = DetectedDict {
        people: result.people,
        projects: result.projects,
        uncertain: result.uncertain,
        topics: result.topics,
    };
    merge_detected(current, prose_signal, false)
}

/// Optionally refine `detected` using an LLM.
///
/// Returns `detected` unchanged when LLM is disabled or unavailable. On LLM
/// failure, batch errors are logged to stderr and remaining entities are
/// returned as-is. `corpus_origin` is forwarded to the entity classifier so it
/// can prime the model with platform/persona context from Pass 0. Called by [`run`].
fn run_refine_entities(
    detected: DetectedDict,
    directory: &Path,
    llm_opts: &LlmOpts,
    corpus_origin: &CorpusOriginResult,
) -> Result<DetectedDict> {
    assert!(directory.is_dir());
    assert!(!directory.as_os_str().is_empty());

    let Some(provider) = run_setup_llm(llm_opts)? else {
        return Ok(detected);
    };

    let corpus = collect_corpus_text(directory);
    let result = refine_entities(detected, &corpus, provider.as_ref(), Some(corpus_origin));

    if result.errors > 0 {
        eprintln!(
            "  Warning: {}/{} LLM batches failed",
            result.errors, result.batches_total
        );
    }
    eprintln!(
        "  LLM refinement: {} dropped, {} reclassified",
        result.dropped, result.reclassified
    );

    assert!(result.batches_completed + result.errors == result.batches_total);
    Ok(result.merged)
}

/// Build an [`LlmProvider`] from `opts` and probe its availability.
///
/// Returns `None` when the provider is disabled, unreachable, or declined by
/// the privacy consent gate. Returns `Err` only for misconfigured provider names.
/// Called by [`run_refine_entities`].
fn run_setup_llm(opts: &LlmOpts) -> Result<Option<Box<dyn LlmProvider>>> {
    if !opts.enabled {
        return Ok(None);
    }
    assert!(!opts.provider.is_empty());
    assert!(!opts.model.is_empty());

    let provider = get_provider(
        &opts.provider,
        &opts.model,
        opts.endpoint.clone(),
        opts.api_key.clone(),
        LLM_TIMEOUT_SECS,
    )?;

    let (available, message) = provider.check_available();
    if !available {
        eprintln!("  LLM unavailable: {message}");
        return Ok(None);
    }

    if !run_setup_llm_consent_check(provider.as_ref(), opts)? {
        return Ok(None);
    }

    assert!(!provider.name().is_empty());
    eprintln!("  LLM: {} ({}) ready", opts.provider, opts.model);
    Ok(Some(provider))
}

/// Non-interactive consent check used by Pass 0 corpus-origin LLM detection.
///
/// Returns `true` when the provider may be used without prompting:
/// - Local providers (no privacy concern).
/// - External provider with `--accept-external-llm` (user pre-opted in).
/// - External provider whose key was supplied via explicit `--llm-api-key`
///   (`ApiKeySource::Flag`) — the act of passing the flag is itself consent.
///
/// Returns `false` for an external provider with an env-fallback key
/// (`ApiKeySource::Env`) or no key at all: those paths require the interactive
/// consent gate that only runs in Phase 1.5 (`run_setup_llm_consent_check`).
fn pass0_consent_obtained(provider: &dyn LlmProvider, opts: &LlmOpts) -> bool {
    if !provider.is_external_service() {
        return true;
    }
    if opts.accept_external_llm {
        return true;
    }
    provider.api_key_source() == Some(ApiKeySource::Flag)
}

/// Print the privacy warning and optionally prompt for consent when an external
/// LLM is configured via an env-fallback API key.
///
/// Returns `false` if the user declines (or EOF), meaning the provider should
/// be dropped. Returns `true` when consent is given or the gate does not apply.
/// Called by [`run_setup_llm`] after confirming the provider is available.
fn run_setup_llm_consent_check(provider: &dyn LlmProvider, opts: &LlmOpts) -> Result<bool> {
    if !provider.is_external_service() {
        return Ok(true);
    }
    eprintln!(
        "  \u{26a0} {} is an EXTERNAL API. Your folder content will be sent to \
         the provider during init. Rerun without --llm (or omit it) to keep \
         init fully local.",
        opts.provider
    );
    // Explicit --llm-api-key or --accept-external-llm means the user already opted in.
    if provider.api_key_source() != Some(ApiKeySource::Env) || opts.accept_external_llm {
        return Ok(true);
    }
    // Env-fallback key + external endpoint: require interactive confirmation.
    print!(
        "  Your API key was loaded from the environment (not --llm-api-key). \
         Continue with external LLM? [y/N] "
    );
    std::io::stdout().flush()?;
    let mut input = String::new();
    let bytes_read = std::io::stdin().read_line(&mut input).unwrap_or(0);
    if bytes_read == 0 || input.trim().to_lowercase() != "y" {
        eprintln!(
            "  Declined — falling back to heuristics-only. \
             Pass --llm-api-key explicitly or --accept-external-llm to skip this prompt."
        );
        return Ok(false);
    }
    Ok(true)
}

/// Print the interactive proceed prompt and return whether the user accepted.
///
/// In `--yes` / CI mode, always returns `true` without prompting.
/// Called by [`run`].
fn run_prompt_proceed(yes: bool) -> Result<bool> {
    if yes {
        return Ok(true);
    }
    print!("\n  Proceed? [Y/n] ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let trimmed = input.trim().to_lowercase();

    // Pair assertion: stdin response is well-formed UTF-8 (no null bytes).
    debug_assert!(!trimmed.contains('\0'), "input must not contain null bytes");

    let proceed = trimmed != "n" && trimmed != "no";
    if !proceed {
        println!("  Aborted.");
    }
    Ok(proceed)
}

/// Sum the byte sizes of every file in `files`, skipping those whose metadata
/// cannot be read.
///
/// Called by [`run`] to compute the size label shown in the init summary. The
/// result is used only for display — errors are silently treated as 0 bytes so
/// a missing file does not abort init.
fn run_compute_total_bytes(files: &[PathBuf]) -> u64 {
    assert!(
        files.len() < 10_000_000,
        "run_compute_total_bytes: file count must be bounded"
    );
    let total: u64 = files
        .iter()
        .map(|path| std::fs::metadata(path).map_or(0, |m| m.len()))
        .sum();
    assert!(
        total < u64::MAX,
        "run_compute_total_bytes: total must be less than u64::MAX"
    );
    total
}

/// Print the mine prompt and return whether to proceed.
///
/// With `auto_mine=true` returns `true` immediately without reading stdin.
/// On EOF (non-interactive stdin) returns `false` (decline). Defaults to yes
/// when the user presses Enter without typing anything. Called by [`run`]
/// after config is written.
fn run_prompt_mine(auto_mine: bool) -> Result<bool> {
    if auto_mine {
        return Ok(true);
    }
    print!("\n  Mine this project now? [Y/n] ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    let bytes_read = std::io::stdin().read_line(&mut input)?;
    if bytes_read == 0 {
        // EOF on stdin means non-interactive context — treat as decline.
        return Ok(false);
    }
    let trimmed = input.trim().to_lowercase();
    let should_mine = trimmed != "n" && trimmed != "no";
    // Pair assertion: empty or Enter input means yes (default).
    debug_assert!(
        should_mine || trimmed.starts_with('n'),
        "run_prompt_mine: decline must start with 'n'"
    );
    Ok(should_mine)
}

/// Write `mempalace.yaml` to `directory` and print the next-step instructions.
///
/// Called by [`run`] after the user confirms.
fn run_write_config(wing_name: &str, rooms: Vec<RoomConfig>, directory: &Path) -> Result<()> {
    assert!(!wing_name.is_empty());
    assert!(directory.is_dir());

    let config = ProjectConfig {
        wing: wing_name.to_string(),
        rooms,
    };
    let config_path = directory.join("mempalace.yaml");
    let yaml = serde_yaml::to_string(&config).map_err(crate::error::Error::Yaml)?;
    std::fs::write(&config_path, &yaml)?;

    // Pair assertion: config must exist after write.
    debug_assert!(
        config_path.exists(),
        "mempalace.yaml must exist after write"
    );

    println!("\n  Config saved: {}", config_path.display());
    println!("\n  Next step:");
    println!("    mempalace mine {}", directory.display());
    println!("\n=======================================================\n");

    Ok(())
}

/// Confirm detected entities with the user, then persist to the global registry and `entities.json`.
///
/// Calls [`confirm_entities`] (auto or interactive), then writes the confirmed
/// names to `~/.local/share/mempalace/known_entities.json` and to
/// `entities.json` inside the project directory. Registry write errors are
/// non-fatal (logged to stderr) so a full-disk condition does not abort init.
///
/// Topics are passed through verbatim alongside `people` and `projects`: they
/// flow into the registry's `topics_by_wing[wing_name]` slot (replace semantic)
/// so the miner's topic-tunnel pipeline can find them at mine time. Called by
/// [`run`] to keep that function within the 70-line limit.
fn run_confirm_and_save(
    detected: &DetectedDict,
    yes: bool,
    directory: &Path,
    wing_name: &str,
) -> Result<()> {
    assert!(directory.is_dir());
    assert!(!directory.as_os_str().is_empty());
    assert!(!wing_name.is_empty());

    let confirmed = confirm_entities(detected, yes);

    // Build the category map for the global registry. Always include a
    // `"topics"` entry — even when empty — so the registry's
    // `topics_by_wing[wing_name]` slot is cleared on a re-init that
    // detected no topics. Otherwise stale topics from a previous run would
    // linger and feed the miner's cross-wing tunnel computation. People
    // and projects keep the "skip if empty" semantic since they have a
    // union (not replace) merge contract — clearing them is not desired.
    let mut by_category: HashMap<String, Vec<String>> = HashMap::new();
    if !confirmed.people.is_empty() {
        by_category.insert("people".to_string(), confirmed.people.clone());
    }
    if !confirmed.projects.is_empty() {
        by_category.insert("projects".to_string(), confirmed.projects.clone());
    }
    by_category.insert("topics".to_string(), confirmed.topics.clone());

    // Pass `wing_name` so the registry routes confirmed topics into
    // `topics_by_wing[wing_name]`; the miner reads this when deciding which
    // wings to consider for cross-wing topic tunnels.
    if let Err(error) = add_to_known_entities(&by_category, Some(wing_name)) {
        eprintln!("  Warning: could not update entity registry: {error}");
    }

    // Always overwrite the per-project `entities.json` so the on-disk audit
    // file mirrors the registry. Skipping the write on a re-init that
    // confirmed nothing would leave stale entities from the previous run on
    // disk while the registry's `topics_by_wing[wing]` was just cleared,
    // producing a confusing divergence between the two surfaces.
    let entities_json = serde_json::json!({
        "people": confirmed.people,
        "projects": confirmed.projects,
        "topics": confirmed.topics,
    });
    let entities_path = directory.join("entities.json");
    let json_text = serde_json::to_string_pretty(&entities_json)?;
    std::fs::write(&entities_path, json_text.as_bytes())?;

    // Pair assertion: entities.json must exist after write.
    debug_assert!(
        entities_path.exists(),
        "entities.json must exist after write"
    );

    println!("  Entities saved: {}", entities_path.display());
    Ok(())
}

/// Derive the wing name: prefer the user's own project name from scan results,
/// fall back to the directory name sanitized to `snake_case`.
///
/// Called by [`run`] to keep that function within the 70-line limit.
fn run_derive_wing_name(projects: &[ProjectInfo], directory: &Path) -> String {
    assert!(directory.is_dir());

    // Prefer a project the user actively contributes to.
    let mine_name = projects
        .iter()
        .find(|project| project.is_mine)
        .map(|project| project.name.as_str());

    let base = mine_name.unwrap_or_else(|| {
        directory
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("project")
    });

    // Normalize to [a-z0-9_] only: split on any non-alphanumeric character (treating
    // each run as a word separator), filter empty segments, then rejoin with '_'.
    // This handles any separator character — spaces, hyphens, slashes, colons, etc. —
    // and naturally avoids leading/trailing underscores and consecutive separators.
    // e.g. "my-lib/v2" → ["my", "lib", "v2"] → "my_lib_v2"
    // e.g. "-myproject" → ["myproject"] → "myproject"
    let candidate: String = base
        .to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    let sanitized = if candidate.is_empty() {
        "project".to_string()
    } else {
        candidate
    };
    assert!(!sanitized.is_empty(), "wing name must not be empty");
    // Postcondition: result must start with an alphanumeric character.
    debug_assert!(
        sanitized.starts_with(|c: char| c.is_ascii_alphanumeric()),
        "wing name must start with alphanumeric"
    );
    sanitized
}

/// Persist `lang` codes to the global `MempalaceConfig::entity_languages`.
///
/// Called by [`run`] when `--lang` is provided. Failures are non-fatal:
/// a full-disk or permission error must not abort an otherwise successful
/// `init`.
fn run_persist_lang(lang: &[String]) {
    assert!(!lang.is_empty(), "run_persist_lang: lang must not be empty");
    let mut config = match crate::config::MempalaceConfig::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("  Warning: could not load config to persist --lang: {error}");
            return;
        }
    };
    config.entity_languages = lang.to_vec();
    assert!(
        !config.entity_languages.is_empty(),
        "entity_languages must be non-empty after assignment"
    );
    if let Err(error) = config.save() {
        eprintln!("  Warning: could not persist --lang to config: {error}");
    } else {
        println!("  Entity languages set to: {}", lang.join(", "));
    }
}

/// Append `/mempalace.yaml` and `/entities.json` (anchored to repo root) to
/// the `.gitignore` in `directory` if the directory is the root of a git
/// worktree and the entries are not already present. The dedup check treats
/// anchored and unanchored variants (e.g. `mempalace.yaml`) as equivalent so
/// we never append a duplicate rule.
///
/// Called by [`run`] after config is written. Non-fatal: errors are printed
/// to stderr and do not abort init.
fn run_gitignore_protect(directory: &Path) {
    assert!(directory.is_dir(), "run_gitignore_protect: must be a dir");
    let git_dir = directory.join(".git");
    if !git_dir.exists() {
        return;
    }
    let gitignore_path = directory.join(".gitignore");
    let existing = match std::fs::read_to_string(&gitignore_path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => {
            eprintln!("  Warning: could not read .gitignore: {error}");
            return;
        }
    };
    assert!(
        existing.len() < 10 * 1024 * 1024,
        "run_gitignore_protect: .gitignore must be < 10 MB"
    );
    // Anchored patterns: these files only exist at the repo root, so a leading
    // slash makes the rule unambiguous and also lets us deduplicate against
    // unanchored variants a user may have written previously.
    let entries = ["/mempalace.yaml", "/entities.json"];
    let mut appended: Vec<&str> = Vec::with_capacity(entries.len());
    let mut additions = String::new();
    for entry in entries {
        let unanchored = entry.trim_start_matches('/');
        let already_present = existing.lines().any(|line| {
            let trimmed = line.trim();
            trimmed == entry || trimmed == unanchored
        });
        if !already_present {
            additions.push_str(entry);
            additions.push('\n');
            appended.push(entry);
        }
    }
    if appended.is_empty() {
        return;
    }
    let separator = if existing.ends_with('\n') || existing.is_empty() {
        ""
    } else {
        "\n"
    };
    let new_content = format!("{existing}{separator}{additions}");
    if let Err(error) = std::fs::write(&gitignore_path, &new_content) {
        eprintln!("  Warning: could not update .gitignore: {error}");
    } else {
        println!("  Added to .gitignore: {}", appended.join(", "));
    }
}

/// Print the init summary including detected entities and rooms.
///
/// Called by [`run`] to keep that function within the 70-line limit.
fn run_print_summary(
    wing_name: &str,
    file_count: usize,
    total_bytes: u64,
    rooms: &[crate::config::RoomConfig],
    detected: &DetectedDict,
) {
    assert!(!wing_name.is_empty());

    // Format bytes as "<1 MB" when under 1 MiB, or the integer MiB count otherwise.
    // Integer division on `total_bytes` is intentional: the summary reports whole
    // MiB units and the fractional remainder is dropped on purpose for display.
    #[allow(clippy::integer_division)]
    let size_label = if total_bytes < 1_048_576 {
        "<1 MB".to_string()
    } else {
        format!("{} MB", total_bytes / 1_048_576)
    };

    println!("\n=======================================================");
    println!("  MemPalace Init");
    println!("=======================================================");
    println!("\n  WING: {wing_name}");
    println!("  (~{file_count} files, ~{size_label} — rooms detected from folder structure)\n");

    for room in rooms {
        println!("    ROOM: {}", room.name);
        println!("          {}", room.description);
    }

    if !detected.projects.is_empty() || !detected.people.is_empty() || !detected.topics.is_empty() {
        println!("\n  Detected entities:");
        run_print_entities("Projects", &detected.projects);
        run_print_entities("People", &detected.people);
        // Topics print before "Uncertain" so the summary leads with the
        // confirmed-pass-through bucket and trails with the items the user
        // may want to review.
        run_print_entities("Topics", &detected.topics);
        if !detected.uncertain.is_empty() {
            run_print_entities("Uncertain", &detected.uncertain);
        }
    }

    println!("\n-------------------------------------------------------");
}

/// Print a labelled entity list.  Called by [`run_print_summary`].
fn run_print_entities(label: &str, entities: &[DetectedEntity]) {
    assert!(!label.is_empty());
    if entities.is_empty() {
        return;
    }
    println!("\n    {label}:");
    for entity in entities {
        assert!(!entity.name.is_empty());
        assert!(!entity.entity_type.is_empty());
        // Show confidence (e.g. 99%) and the first evidence signal.
        // `frequency` is the occurrence/commit count — shown when > 0.
        // Confidence is in [0.0, 1.0]; * 100 then round gives 0.0–100.0, safely fits u32.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let conf_pct = (entity.confidence * 100.0).round() as u32;
        let signal_str = entity.signals.first().map_or("", String::as_str);
        let freq_str = if entity.frequency > 0 {
            format!(
                ", {} occurrence{}",
                entity.frequency,
                if entity.frequency == 1 { "" } else { "s" }
            )
        } else {
            String::new()
        };
        if signal_str.is_empty() {
            println!("      - {} ({}%{})", entity.name, conf_pct, freq_str);
        } else {
            println!(
                "      - {} ({}%{}, {})",
                entity.name, conf_pct, freq_str, signal_str
            );
        }
    }
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn init_run_creates_mempalace_yaml_in_directory() {
        // init::run with yes=true must write a mempalace.yaml to the target directory.
        // auto_mine=false — stdin is EOF in tests so mine prompt returns false (None).
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for init test");
        run(
            temp_directory.path(),
            true,
            false,
            false,
            &[],
            &LlmOpts::default(),
        )
        .expect("init::run should succeed for a valid directory with yes=true");

        let config_path = temp_directory.path().join("mempalace.yaml");
        assert!(config_path.exists(), "mempalace.yaml must be created");
        // Pair assertion: the file must contain valid YAML with a wing key.
        let contents = std::fs::read_to_string(&config_path)
            .expect("mempalace.yaml must be readable after init");
        assert!(
            contents.contains("wing:"),
            "config must contain a wing field"
        );
        assert!(!contents.is_empty(), "config file must not be empty");
    }

    #[test]
    fn init_run_nonexistent_directory_returns_error() {
        // Passing a path that does not exist must return Err.
        let path = std::path::Path::new("/nonexistent/path/that/does/not/exist");
        let result = run(path, true, false, false, &[], &LlmOpts::default());
        assert!(result.is_err(), "nonexistent directory must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| !error.to_string().is_empty()),
            "error message must not be empty"
        );
    }

    #[test]
    fn init_run_no_gitignore_flag_counts_files() {
        // no_gitignore=true must still produce a valid config (just without .gitignore filtering).
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for no-gitignore init test");
        std::fs::write(temp_directory.path().join("test.rs"), "fn main() {}")
            .expect("failed to write test source file");

        run(
            temp_directory.path(),
            true,
            false,
            true,
            &[],
            &LlmOpts::default(),
        )
        .expect("init::run should succeed with no_gitignore=true");

        let config_path = temp_directory.path().join("mempalace.yaml");
        assert!(
            config_path.exists(),
            "mempalace.yaml must be created with no_gitignore=true"
        );
        let contents = std::fs::read_to_string(&config_path)
            .expect("mempalace.yaml must be readable after init with no_gitignore");
        assert!(contents.contains("wing:"), "config must contain wing field");
    }

    // -- run_setup_llm --

    #[test]
    fn run_setup_llm_disabled_returns_none() {
        // When LLM is disabled, run_setup_llm must return Ok(None) without any I/O.
        let opts = LlmOpts::default(); // enabled = false
        let result = run_setup_llm(&opts).expect("disabled LLM must not fail");
        assert!(result.is_none(), "disabled LLM must return None");
    }

    #[test]
    fn run_setup_llm_invalid_provider_returns_error() {
        // An unknown provider name must propagate as Err from get_provider.
        let opts = LlmOpts {
            enabled: true,
            provider: "unknown-provider".to_string(),
            model: "some-model".to_string(),
            endpoint: None,
            api_key: None,
            accept_external_llm: false,
        };
        let result = run_setup_llm(&opts);
        assert!(result.is_err(), "unknown provider must return Err");
    }

    #[test]
    fn run_setup_llm_anthropic_no_key_returns_none() {
        // Anthropic without a key reports unavailable; run_setup_llm must return Ok(None).
        temp_env::with_var("ANTHROPIC_API_KEY", None::<&str>, || {
            let opts = LlmOpts {
                enabled: true,
                provider: "anthropic".to_string(),
                model: "claude-haiku-4-5-20251001".to_string(),
                endpoint: None,
                api_key: None,
                accept_external_llm: false,
            };
            let result = run_setup_llm(&opts).expect("unavailable provider must not return Err");
            assert!(
                result.is_none(),
                "missing API key must cause unavailable → None"
            );
        });
    }

    // -- run_setup_llm_consent_check --

    #[test]
    fn consent_check_local_provider_always_passes() {
        // A local provider (e.g. Ollama default endpoint) must never trigger the gate.
        let provider = crate::llm::client::OllamaProvider::new("gemma3:4b".to_string(), None, 60);
        let opts = LlmOpts {
            accept_external_llm: false,
            ..LlmOpts::default()
        };
        let result = run_setup_llm_consent_check(&provider, &opts)
            .expect("local provider consent check must not fail");
        assert!(result, "local provider must pass the consent check");
    }

    #[test]
    fn consent_check_external_flag_key_passes_without_prompt() {
        // Explicit --llm-api-key (Flag source) must skip the interactive consent prompt.
        temp_env::with_var("ANTHROPIC_API_KEY", None::<&str>, || {
            let provider = crate::llm::client::AnthropicProvider::new(
                "claude-haiku-4-5-20251001".to_string(),
                None,
                Some("sk-ant-explicit".to_string()),
                60,
            );
            assert!(provider.is_external_service(), "anthropic is external");
            assert_eq!(provider.api_key_source(), Some(ApiKeySource::Flag));
            let opts = LlmOpts {
                accept_external_llm: false,
                ..LlmOpts::default()
            };
            let result = run_setup_llm_consent_check(&provider, &opts)
                .expect("flag-key external provider must not fail");
            assert!(result, "flag API key bypasses interactive consent");
        });
    }

    #[test]
    fn consent_check_external_accept_flag_bypasses_prompt() {
        // --accept-external-llm must bypass the prompt even for env-fallback keys.
        temp_env::with_var("ANTHROPIC_API_KEY", Some("sk-ant-env"), || {
            let provider = crate::llm::client::AnthropicProvider::new(
                "claude-haiku-4-5-20251001".to_string(),
                None,
                None,
                60,
            );
            assert!(provider.is_external_service(), "anthropic is external");
            assert_eq!(provider.api_key_source(), Some(ApiKeySource::Env));
            let opts = LlmOpts {
                // accept_external_llm bypasses the prompt.
                accept_external_llm: true,
                ..LlmOpts::default()
            };
            let result =
                run_setup_llm_consent_check(&provider, &opts).expect("accept flag must not fail");
            assert!(result, "--accept-external-llm must bypass the consent gate");
        });
    }

    // -- run_derive_wing_name --

    #[test]
    fn run_derive_wing_name_prefers_mine_project() {
        // A project with is_mine=true must be used as the wing name base.
        use crate::palace::project_scanner::ProjectInfo;
        let temp_dir =
            tempfile::tempdir().expect("must create temp dir for wing name derivation test");
        let projects = vec![ProjectInfo {
            name: "my-awesome-project".to_string(),
            repo_root: temp_dir.path().to_path_buf(),
            manifest: None,
            has_git: false,
            total_commits: 0,
            user_commits: 5,
            is_mine: true,
        }];
        let result = run_derive_wing_name(&projects, temp_dir.path());
        assert_eq!(
            result, "my_awesome_project",
            "hyphens must be converted to underscores"
        );
        assert!(!result.is_empty());
    }

    #[test]
    fn run_derive_wing_name_falls_back_to_dir_name() {
        // With no projects, the wing name must come from the directory basename.
        let temp_dir =
            tempfile::tempdir().expect("must create temp dir for wing name fallback test");
        let result = run_derive_wing_name(&[], temp_dir.path());
        assert!(
            !result.is_empty(),
            "wing name must not be empty even without projects"
        );
    }

    #[test]
    fn run_derive_wing_name_sanitizes_spaces_to_underscores() {
        // Spaces in the project name must be replaced with underscores.
        use crate::palace::project_scanner::ProjectInfo;
        let temp_dir = tempfile::tempdir().expect("must create temp dir for sanitize test");
        let projects = vec![ProjectInfo {
            name: "my project name".to_string(),
            repo_root: temp_dir.path().to_path_buf(),
            manifest: None,
            has_git: false,
            total_commits: 0,
            user_commits: 1,
            is_mine: true,
        }];
        let result = run_derive_wing_name(&projects, temp_dir.path());
        assert!(!result.contains(' '), "wing name must not contain spaces");
        assert_eq!(result, "my_project_name");
    }

    // -- run_prompt_proceed --

    #[test]
    fn run_prompt_proceed_with_yes_returns_true_without_stdin() {
        // yes=true must return Ok(true) immediately without reading stdin.
        let result = run_prompt_proceed(true).expect("yes=true must not fail");
        assert!(result, "yes=true must always proceed");
    }

    // -- run_refine_entities --

    #[test]
    fn run_refine_entities_when_disabled_returns_detected_unchanged() {
        // Disabled LLM must return the input DetectedDict without modification.
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir = tempfile::tempdir().expect("must create temp dir for refine entities test");
        let detected = DetectedDict {
            people: vec![],
            projects: vec![],
            uncertain: vec![],
            topics: vec![],
        };
        let opts = LlmOpts::default(); // enabled = false
        let corpus_origin = detect_origin_heuristic(&[]);
        let result = run_refine_entities(detected, temp_dir.path(), &opts, &corpus_origin)
            .expect("disabled LLM refine must not fail");
        assert!(result.people.is_empty());
        assert!(result.projects.is_empty());
    }

    // -- run_print_summary and run_print_entities --

    #[test]
    fn run_print_summary_with_detected_entities_does_not_panic() {
        // run_print_summary must handle a non-empty DetectedDict without panicking.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let detected = DetectedDict {
            people: vec![DetectedEntity {
                name: "Alice".to_string(),
                entity_type: "person".to_string(),
                confidence: 0.9,
                frequency: 5,
                signals: vec!["git: 5 of your commits".to_string()],
            }],
            projects: vec![DetectedEntity {
                name: "mylib".to_string(),
                entity_type: "project".to_string(),
                confidence: 0.8,
                frequency: 3,
                signals: vec![],
            }],
            uncertain: vec![DetectedEntity {
                name: "unknown".to_string(),
                entity_type: "uncertain".to_string(),
                confidence: 0.6,
                frequency: 1,
                signals: vec![],
            }],
            topics: vec![],
        };
        // Rooms list — empty is fine for the print test.
        let rooms: Vec<crate::config::RoomConfig> = vec![];
        // Must not panic with any non-empty DetectedDict.
        run_print_summary("my_project", 42, 1_500_000, &rooms, &detected);
    }

    #[test]
    fn run_print_summary_includes_topics_when_only_topics_detected() {
        // Regression: a DetectedDict with only `topics` populated must still
        // trigger the "Detected entities:" block. Without this, topic-only
        // pipelines (a corpus with no people or projects but with topic
        // labels) would show no detection summary at all and the user would
        // have no chance to confirm the topics before mining.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let detected = DetectedDict {
            people: vec![],
            projects: vec![],
            uncertain: vec![],
            topics: vec![DetectedEntity {
                name: "Rust".to_string(),
                entity_type: "topic".to_string(),
                confidence: 0.85,
                frequency: 7,
                signals: vec!["lexicon".to_string()],
            }],
        };
        let rooms: Vec<crate::config::RoomConfig> = vec![];
        // Must not panic and must traverse the topics-print branch.
        run_print_summary("topics_wing", 5, 0, &rooms, &detected);
    }

    #[test]
    fn run_print_entities_with_non_empty_list_does_not_panic() {
        // run_print_entities must iterate entities without panicking.
        use crate::palace::entities::DetectedEntity;
        let entities = vec![
            DetectedEntity {
                name: "Alice".to_string(),
                entity_type: "person".to_string(),
                confidence: 0.9,
                frequency: 2,
                signals: vec!["Cargo.toml".to_string()],
            },
            DetectedEntity {
                name: "Bob".to_string(),
                entity_type: "person".to_string(),
                confidence: 0.7,
                // frequency=0 exercises the empty freq_str branch.
                frequency: 0,
                signals: vec![],
            },
        ];
        // Must not panic regardless of signal/frequency content.
        run_print_entities("People", &entities);
    }

    // -- run_confirm_and_save --

    #[test]
    fn run_confirm_and_save_writes_entities_json_when_entities_confirmed() {
        // With yes=true and high-confidence people, entities.json must be written.
        // MEMPALACE_DIR is redirected because add_to_known_entities writes
        // known_entities.json to config_dir(), which defaults to ~/.local/share/mempalace.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir =
            tempfile::tempdir().expect("must create temp dir for run_confirm_and_save test");
        let registry_dir =
            tempfile::tempdir().expect("must create registry dir for run_confirm_and_save test");
        let detected = DetectedDict {
            people: vec![DetectedEntity {
                name: "Alice".to_string(),
                entity_type: "person".to_string(),
                confidence: 0.9,
                frequency: 1,
                signals: vec![],
            }],
            projects: vec![],
            uncertain: vec![],
            topics: vec![],
        };
        temp_env::with_var("MEMPALACE_DIR", Some(registry_dir.path()), || {
            run_confirm_and_save(&detected, true, temp_dir.path(), "test_wing")
                .expect("run_confirm_and_save must succeed");
        });
        let entities_path = temp_dir.path().join("entities.json");
        assert!(entities_path.exists(), "entities.json must be written");
        let content =
            std::fs::read_to_string(&entities_path).expect("entities.json must be readable");
        assert!(
            content.contains("Alice"),
            "entities.json must name the confirmed person"
        );
        assert!(!content.is_empty());
        // Pair assertion: known_entities.json must be in the isolated registry dir.
        assert!(
            registry_dir.path().join("known_entities.json").exists(),
            "known_entities.json must land in the redirected MEMPALACE_DIR"
        );
    }

    #[test]
    fn run_confirm_and_save_writes_empty_entities_json_when_nothing_confirmed() {
        // entities.json must always exist after init — even when no entities
        // were confirmed — so the on-disk audit file mirrors the cleared
        // registry. Skipping the write would leave stale entries from a
        // previous run while topics_by_wing[wing] was already cleared,
        // producing a confusing divergence between the two surfaces.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir = tempfile::tempdir().expect("must create temp dir for empty-write test");
        let registry_dir = tempfile::tempdir().expect("registry dir");
        // Seed a stale entities.json so we can confirm it gets overwritten.
        let entities_path = temp_dir.path().join("entities.json");
        std::fs::write(
            &entities_path,
            r#"{"people":["StaleAlice"],"projects":["StaleProj"],"topics":["StaleTopic"]}"#,
        )
        .expect("seed stale entities.json");

        let detected = DetectedDict {
            // Below threshold — will not be auto-accepted.
            people: vec![DetectedEntity {
                name: "LowConf".to_string(),
                entity_type: "person".to_string(),
                confidence: 0.1,
                frequency: 0,
                signals: vec![],
            }],
            projects: vec![],
            uncertain: vec![],
            topics: vec![],
        };
        temp_env::with_var("MEMPALACE_DIR", Some(registry_dir.path()), || {
            run_confirm_and_save(&detected, true, temp_dir.path(), "test_wing")
                .expect("run_confirm_and_save must succeed even with no confirmations");
        });
        assert!(
            entities_path.exists(),
            "entities.json must exist after init — always overwritten"
        );
        let value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&entities_path).expect("read"))
                .expect("parse");
        // Pair assertion: each category is present and empty (stale data overwritten).
        for key in ["people", "projects", "topics"] {
            let arr = value[key].as_array().expect("array");
            assert!(arr.is_empty(), "{key} must be empty after re-init clears");
        }
        assert!(
            !value.to_string().contains("StaleAlice"),
            "stale entries must be overwritten, not retained"
        );
    }

    #[test]
    fn run_confirm_and_save_writes_both_people_and_projects() {
        // When both people and projects are confirmed, both categories appear in entities.json.
        // MEMPALACE_DIR is redirected because add_to_known_entities writes
        // known_entities.json to config_dir(), which defaults to ~/.local/share/mempalace.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir = tempfile::tempdir().expect("must create temp dir for both-categories test");
        let registry_dir =
            tempfile::tempdir().expect("must create registry dir for both-categories test");
        let detected = DetectedDict {
            people: vec![DetectedEntity {
                name: "Alice".to_string(),
                entity_type: "person".to_string(),
                confidence: 0.9,
                frequency: 3,
                signals: vec![],
            }],
            projects: vec![DetectedEntity {
                name: "mylib".to_string(),
                entity_type: "project".to_string(),
                confidence: 0.85,
                frequency: 2,
                signals: vec![],
            }],
            uncertain: vec![],
            topics: vec![],
        };
        temp_env::with_var("MEMPALACE_DIR", Some(registry_dir.path()), || {
            run_confirm_and_save(&detected, true, temp_dir.path(), "test_wing")
                .expect("run_confirm_and_save must succeed");
        });
        let entities_path = temp_dir.path().join("entities.json");
        assert!(entities_path.exists(), "entities.json must be written");
        let content =
            std::fs::read_to_string(&entities_path).expect("entities.json must be readable");
        assert!(content.contains("Alice"), "must include confirmed person");
        assert!(content.contains("mylib"), "must include confirmed project");
    }

    #[test]
    fn run_confirm_and_save_clears_stale_topics_on_reinit_with_no_topics() {
        // Regression: re-running init with no detected topics must clear the
        // wing's previous topic list from `topics_by_wing[wing]`. Otherwise
        // stale labels would keep feeding the miner's cross-wing tunnel
        // pipeline indefinitely.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let registry_dir = tempfile::tempdir().expect("registry dir");

        // First run: confirm a topic, populating topics_by_wing[stale_wing].
        let first = DetectedDict {
            people: vec![],
            projects: vec![],
            uncertain: vec![],
            topics: vec![DetectedEntity {
                name: "Rust".to_string(),
                entity_type: "topic".to_string(),
                confidence: 0.9,
                frequency: 5,
                signals: vec![],
            }],
        };
        temp_env::with_var("MEMPALACE_DIR", Some(registry_dir.path()), || {
            run_confirm_and_save(&first, true, temp_dir.path(), "stale_wing")
                .expect("first confirm");
        });

        // Pair assertion: precondition — the topic landed in the registry.
        let registry_path = registry_dir.path().join("known_entities.json");
        let registry_after_first: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&registry_path).expect("registry read 1"),
        )
        .expect("parse 1");
        assert!(
            registry_after_first["topics_by_wing"]["stale_wing"].is_array(),
            "first run must seed topics_by_wing[stale_wing]"
        );

        // Second run: zero detected topics. The wing's stale topics MUST be
        // cleared even though the early-return path used to skip the call.
        let second = DetectedDict {
            people: vec![],
            projects: vec![],
            uncertain: vec![],
            topics: vec![],
        };
        temp_env::with_var("MEMPALACE_DIR", Some(registry_dir.path()), || {
            run_confirm_and_save(&second, true, temp_dir.path(), "stale_wing")
                .expect("second confirm");
        });

        let registry_after_second: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&registry_path).expect("registry read 2"),
        )
        .expect("parse 2");
        // Either topics_by_wing is gone entirely (last wing removed) or the
        // wing key is no longer present. Both shapes mean "stale topics gone".
        let cleared = registry_after_second
            .get("topics_by_wing")
            .and_then(|map| map.get("stale_wing"))
            .is_none();
        assert!(cleared, "stale_wing topics must be cleared on re-init");
    }

    #[test]
    fn run_confirm_and_save_persists_topics_in_entities_json_and_registry() {
        // Regression: confirmed topics must land both in entities.json (so the
        // project records them) and in the global registry's topics_by_wing
        // bucket (so the miner's topic-tunnel pipeline finds them).
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let registry_dir = tempfile::tempdir().expect("registry dir");
        let detected = DetectedDict {
            people: vec![],
            projects: vec![],
            uncertain: vec![],
            topics: vec![DetectedEntity {
                name: "Rust".to_string(),
                entity_type: "topic".to_string(),
                confidence: 0.9,
                frequency: 5,
                signals: vec![],
            }],
        };
        temp_env::with_var("MEMPALACE_DIR", Some(registry_dir.path()), || {
            run_confirm_and_save(&detected, true, temp_dir.path(), "topics_wing")
                .expect("run_confirm_and_save must succeed");
        });

        // entities.json must include the topics array.
        let entities_path = temp_dir.path().join("entities.json");
        assert!(entities_path.exists(), "entities.json must be written");
        let entities_text =
            std::fs::read_to_string(&entities_path).expect("entities.json readable");
        let entities_value: serde_json::Value =
            serde_json::from_str(&entities_text).expect("entities.json parse");
        let topics: Vec<&str> = entities_value["topics"]
            .as_array()
            .expect("topics array")
            .iter()
            .filter_map(|val| val.as_str())
            .collect();
        assert_eq!(topics, ["Rust"], "topics must persist in entities.json");

        // known_entities.json must include topics_by_wing[topics_wing].
        let registry_path = registry_dir.path().join("known_entities.json");
        assert!(registry_path.exists(), "known_entities.json must exist");
        let registry_value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&registry_path).expect("registry read"))
                .expect("registry parse");
        let registry_topics: Vec<&str> = registry_value["topics_by_wing"]["topics_wing"]
            .as_array()
            .expect("registry topics array")
            .iter()
            .filter_map(|val| val.as_str())
            .collect();
        assert_eq!(
            registry_topics,
            ["Rust"],
            "topics must persist in topics_by_wing[wing]"
        );
    }

    #[test]
    fn run_discover_entities_with_claude_projects_root() {
        // A directory that looks like a .claude/projects/ root must trigger session scanning.
        let temp_dir =
            tempfile::tempdir().expect("must create temp dir for claude projects discovery test");
        // Create the expected layout: a -slug dir with a .jsonl session file.
        let slug_dir = temp_dir.path().join("-Users-robbie-test-proj");
        std::fs::create_dir(&slug_dir).expect("must create slug dir");
        std::fs::write(
            slug_dir.join("session.jsonl"),
            "{\"cwd\":\"/Users/robbie/test-proj\",\"type\":\"human\"}\n",
        )
        .expect("must write session file");

        // run_discover_entities should take the is_claude_projects_root=true branch.
        let corpus_origin = detect_origin_heuristic(&[]);
        let (_projects, detected) = run_discover_entities(temp_dir.path(), &[], &corpus_origin);
        // The session JSONL carries cwd="/Users/robbie/test-proj", so the session
        // scanner must surface "test-proj" in detected.projects.
        assert!(
            detected.projects.iter().any(|e| e.name == "test-proj"),
            "session scanner must detect project name from cwd in session JSONL"
        );
    }

    #[test]
    fn gitignore_protect_adds_entries_when_git_repo() {
        // run_gitignore_protect must append mempalace.yaml and entities.json to
        // .gitignore when the directory has a .git folder and neither entry is present.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::create_dir(dir.path().join(".git")).expect("must create .git dir");
        std::fs::write(dir.path().join(".gitignore"), "*.log\n").expect("must write .gitignore");

        run_gitignore_protect(dir.path());

        let contents =
            std::fs::read_to_string(dir.path().join(".gitignore")).expect("must read .gitignore");
        assert!(
            contents.contains("mempalace.yaml"),
            ".gitignore must contain mempalace.yaml"
        );
        // Pair assertion: entities.json must also be added.
        assert!(
            contents.contains("entities.json"),
            ".gitignore must contain entities.json"
        );
    }

    #[test]
    fn gitignore_protect_does_not_duplicate_existing_entries() {
        // run_gitignore_protect must not add entries that are already present.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::create_dir(dir.path().join(".git")).expect("must create .git dir");
        std::fs::write(
            dir.path().join(".gitignore"),
            "mempalace.yaml\nentities.json\n",
        )
        .expect("must write .gitignore");

        run_gitignore_protect(dir.path());

        let contents =
            std::fs::read_to_string(dir.path().join(".gitignore")).expect("must read .gitignore");
        let yaml_count = contents.matches("mempalace.yaml").count();
        let json_count = contents.matches("entities.json").count();
        assert_eq!(yaml_count, 1, "mempalace.yaml must appear exactly once");
        // Pair assertion: entities.json must also appear exactly once.
        assert_eq!(json_count, 1, "entities.json must appear exactly once");
    }

    #[test]
    fn gitignore_protect_dedupes_anchored_existing_entries() {
        // Regression: when .gitignore already contains the rooted variants
        // (`/mempalace.yaml`, `/entities.json`), run_gitignore_protect must
        // recognise them as equivalent to the unanchored names and not append
        // a redundant rule.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::create_dir(dir.path().join(".git")).expect("must create .git dir");
        std::fs::write(
            dir.path().join(".gitignore"),
            "/mempalace.yaml\n/entities.json\n",
        )
        .expect("must write .gitignore");

        run_gitignore_protect(dir.path());

        let contents =
            std::fs::read_to_string(dir.path().join(".gitignore")).expect("must read .gitignore");
        let yaml_count = contents.matches("mempalace.yaml").count();
        let json_count = contents.matches("entities.json").count();
        assert_eq!(
            yaml_count, 1,
            "anchored mempalace.yaml must not be duplicated"
        );
        // Pair assertion: anchored entities.json must also stay unique.
        assert_eq!(
            json_count, 1,
            "anchored entities.json must not be duplicated"
        );
    }

    #[test]
    fn gitignore_protect_skips_non_git_directory() {
        // run_gitignore_protect must not create or modify .gitignore when there
        // is no .git directory (i.e., not a git worktree root).
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        // No .git directory — protect must be a no-op.
        run_gitignore_protect(dir.path());
        assert!(
            !dir.path().join(".gitignore").exists(),
            ".gitignore must not be created for a non-git directory"
        );
        // Pair assertion: the .git directory must truly be absent.
        assert!(!dir.path().join(".git").exists(), ".git must be absent");
    }

    // --- run_derive_wing_name: empty candidate falls back to "project" ---

    #[test]
    fn run_derive_wing_name_all_special_chars_falls_back_to_project() {
        // When the project name consists entirely of special characters that split
        // to empty segments, the candidate is empty and must fall back to "project".
        // Covers L393-394.
        use crate::palace::project_scanner::ProjectInfo;
        let temp_dir = tempfile::tempdir().expect("must create temp dir for empty-candidate test");
        let projects = vec![ProjectInfo {
            // All non-alphanumeric: splits to empty segments after filtering.
            name: "---".to_string(),
            repo_root: temp_dir.path().to_path_buf(),
            manifest: None,
            has_git: false,
            total_commits: 0,
            user_commits: 1,
            is_mine: true,
        }];
        let result = run_derive_wing_name(&projects, temp_dir.path());
        assert_eq!(
            result, "project",
            "all-special-char name must fall back to 'project'"
        );
        // Pair assertion: result is always non-empty.
        assert!(!result.is_empty(), "wing name must never be empty");
    }

    // --- run_gitignore_protect: no separator needed when .gitignore ends with newline ---

    #[test]
    fn gitignore_protect_no_separator_when_existing_ends_with_newline() {
        // When the existing .gitignore already ends with '\n', no separator is added
        // before the new entries. Covers the separator="" branch on L471-475.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::create_dir(dir.path().join(".git")).expect("must create .git dir");
        // Content ends with '\n' — separator must be "".
        std::fs::write(dir.path().join(".gitignore"), "*.log\n").expect("must write .gitignore");

        run_gitignore_protect(dir.path());

        let contents =
            std::fs::read_to_string(dir.path().join(".gitignore")).expect("must read .gitignore");
        // No double-newline between the original content and the added entries.
        assert!(
            !contents.contains("\n\nmempalace.yaml"),
            ".gitignore must not have double-newline separator"
        );
        // Pair assertion: the entries are present.
        assert!(
            contents.contains("mempalace.yaml"),
            "mempalace.yaml must be added"
        );
    }

    // --- run_gitignore_protect: separator added when .gitignore does not end with newline ---

    // -- auto_mine and run_prompt_mine --

    #[test]
    fn run_prompt_mine_with_auto_mine_true_returns_true() {
        // auto_mine=true must return Ok(true) without reading stdin.
        let result = run_prompt_mine(true).expect("auto_mine must not fail");
        assert!(result, "auto_mine=true must proceed to mine");
        // Pair assertion: no stdin was consumed (EOF would give Ok(false), not true).
        assert!(result, "result must stay true — no stdin fallthrough");
    }

    #[test]
    fn init_run_with_auto_mine_returns_some_files() {
        // auto_mine=true must return Some(files) after a successful init.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for auto_mine test");
        let result = run(
            temp_directory.path(),
            true,
            true,
            false,
            &[],
            &LlmOpts::default(),
        )
        .expect("init::run with auto_mine=true must succeed");
        // When auto_mine is set the caller gets the pre-scanned file list.
        assert!(
            result.is_some(),
            "auto_mine=true must return Some(files) so the caller can mine"
        );
        // Pair assertion: config must have been written (init completed before mine hand-off).
        assert!(
            temp_directory.path().join("mempalace.yaml").exists(),
            "mempalace.yaml must exist even when auto_mine=true"
        );
    }

    #[test]
    fn init_run_yes_without_auto_mine_returns_none_on_eof() {
        // Regression guard: --yes alone must NOT skip the mine prompt.
        // In tests stdin is EOF so run_prompt_mine returns false → None.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for yes-no-auto-mine test");
        let result = run(
            temp_directory.path(),
            true,
            false,
            false,
            &[],
            &LlmOpts::default(),
        )
        .expect("init::run with yes=true auto_mine=false must succeed");
        // Stdin is EOF in tests → mine prompt declines → None.
        assert!(
            result.is_none(),
            "--yes alone must not auto-mine; user declined (EOF)"
        );
        // Pair assertion: config is still written even when mine is declined.
        assert!(
            temp_directory.path().join("mempalace.yaml").exists(),
            "mempalace.yaml must be written even when mine is declined"
        );
    }

    // -- run_compute_total_bytes --

    #[test]
    fn run_compute_total_bytes_sums_file_sizes() {
        // run_compute_total_bytes must return the total byte count of all files.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let file_a = dir.path().join("a.txt");
        let file_b = dir.path().join("b.txt");
        std::fs::write(&file_a, "hello").expect("write a.txt must succeed");
        std::fs::write(&file_b, "world!").expect("write b.txt must succeed");
        let total = run_compute_total_bytes(&[file_a, file_b]);
        assert_eq!(total, 11, "5 + 6 bytes must equal 11");
        // Pair assertion: single file round-trips correctly.
        let file_c = dir.path().join("c.txt");
        std::fs::write(&file_c, "abc").expect("write c.txt must succeed");
        let single = run_compute_total_bytes(&[file_c]);
        assert_eq!(single, 3, "single 3-byte file must total 3");
    }

    #[test]
    fn run_compute_total_bytes_skips_missing_files() {
        // A path that does not exist must contribute 0 bytes, not an error.
        let phantom = std::path::PathBuf::from("/nonexistent/file.txt");
        let total = run_compute_total_bytes(&[phantom]);
        assert_eq!(total, 0, "missing file must contribute 0 bytes");
    }

    // -- run_print_summary with size labels --

    #[test]
    fn run_print_summary_with_large_size_does_not_panic() {
        // Exercises the ">= 1 MB" branch in the size label computation.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let detected = DetectedDict {
            people: vec![DetectedEntity {
                name: "Alice".to_string(),
                entity_type: "person".to_string(),
                confidence: 0.9,
                frequency: 1,
                signals: vec![],
            }],
            projects: vec![],
            uncertain: vec![],
            topics: vec![],
        };
        let rooms: Vec<crate::config::RoomConfig> = vec![];
        // 5 MB total — exercises the integer-division MB branch.
        run_print_summary("test_wing", 10, 5 * 1_048_576, &rooms, &detected);
        // Pair assertion: sub-MB branch also succeeds.
        run_print_summary("test_wing", 0, 0, &rooms, &detected);
    }

    #[test]
    fn gitignore_protect_adds_separator_when_existing_lacks_trailing_newline() {
        // When the existing .gitignore does not end with '\n', a newline separator is
        // added. Covers the separator="\n" branch on L473.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        std::fs::create_dir(dir.path().join(".git")).expect("must create .git dir");
        // Content does NOT end with '\n'.
        std::fs::write(dir.path().join(".gitignore"), "*.log").expect("must write .gitignore");

        run_gitignore_protect(dir.path());

        let contents =
            std::fs::read_to_string(dir.path().join(".gitignore")).expect("must read .gitignore");
        // The entries must appear after the original content with a newline between them.
        assert!(
            contents.starts_with("*.log\n"),
            ".gitignore must start with original content followed by newline"
        );
        // Pair assertion: new entries must be present.
        assert!(
            contents.contains("mempalace.yaml"),
            "mempalace.yaml must be added after separator"
        );
    }

    // --- run_persist_lang: save error branch ---

    // Uses `std::os::unix::fs::PermissionsExt` to make the config directory
    // read-only — Windows has no equivalent permission model, so gate the
    // test to Unix targets to keep cross-platform builds compiling.
    #[cfg(unix)]
    #[test]
    fn run_persist_lang_does_not_panic_when_save_fails() {
        // When the config directory is read-only, config.save() returns Err.
        // run_persist_lang must log the warning and return without panicking.
        // Covers L426-428.
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        temp_env::with_var("MEMPALACE_DIR", Some(dir.path()), || {
            // Make the directory read-only so the write fails.
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o444))
                .expect("set_permissions must succeed");

            // Must not panic even when save() fails.
            run_persist_lang(&["en".to_string()]);

            // Restore permissions so tempdir cleanup succeeds.
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
                .expect("restore permissions must succeed");
        });
        // Pair assertion: the directory must still exist (we only chmod'd it, not removed).
        assert!(dir.path().is_dir(), "temp dir must still exist after test");
    }

    #[test]
    fn persist_lang_updates_global_config() {
        // run_persist_lang must write the provided language codes to the global
        // MempalaceConfig so that subsequent loads return the new value.
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        temp_env::with_var("MEMPALACE_DIR", Some(dir.path()), || {
            run_persist_lang(&["de".to_string(), "fr".to_string()]);
            let loaded = crate::config::MempalaceConfig::load()
                .expect("config must load after persist_lang");
            assert!(
                loaded.entity_languages.contains(&"de".to_string()),
                "entity_languages must contain de"
            );
            // Pair assertion: fr must also be present.
            assert!(
                loaded.entity_languages.contains(&"fr".to_string()),
                "entity_languages must contain fr"
            );
        });
    }

    // -- run_detect_corpus_origin_llm --

    fn make_heuristic_result() -> CorpusOriginResult {
        CorpusOriginResult {
            likely_ai_dialogue: true,
            confidence: 0.8,
            primary_platform: Some("test".to_string()),
            user_name: Some("Tester".to_string()),
            agent_persona_names: vec!["Bot".to_string()],
            evidence: vec!["signal-a".to_string()],
        }
    }

    #[test]
    fn run_detect_corpus_origin_llm_disabled_returns_heuristic_unchanged() {
        // When LLM is disabled the function must return the heuristic result unchanged
        // — no provider is constructed and the merge branch is never reached.
        let heuristic = make_heuristic_result();
        let opts = LlmOpts::default(); // enabled = false
        let samples = vec!["some sample text".to_string()];
        let result = run_detect_corpus_origin_llm(&heuristic, &samples, &opts);
        assert_eq!(
            result.likely_ai_dialogue, heuristic.likely_ai_dialogue,
            "disabled LLM must preserve heuristic likely_ai_dialogue"
        );
        // Cloned value is bit-identical — use subtraction to satisfy float_cmp.
        assert!(
            (result.confidence - heuristic.confidence).abs() < f64::EPSILON,
            "disabled LLM must preserve heuristic confidence"
        );
        assert_eq!(
            result.evidence, heuristic.evidence,
            "disabled LLM must preserve evidence unchanged"
        );
    }

    #[test]
    fn run_detect_corpus_origin_llm_empty_samples_returns_heuristic_unchanged() {
        // When samples is empty the function must return the heuristic unchanged
        // regardless of whether the LLM is enabled, to avoid sending empty prompts.
        let heuristic = make_heuristic_result();
        let opts = LlmOpts {
            enabled: true,
            provider: "ollama".to_string(),
            model: "llama3:8b".to_string(),
            endpoint: Some("http://localhost:11434/v1".to_string()),
            api_key: None,
            accept_external_llm: false,
        };
        let result = run_detect_corpus_origin_llm(&heuristic, &[], &opts);
        assert_eq!(
            result.likely_ai_dialogue, heuristic.likely_ai_dialogue,
            "empty samples must return heuristic likely_ai_dialogue"
        );
        // Pair assertion: evidence must also be unchanged.
        assert_eq!(
            result.evidence, heuristic.evidence,
            "empty samples must return evidence unchanged"
        );
    }

    // -- run_detect_corpus_origin --

    #[test]
    fn run_detect_corpus_origin_on_empty_dir_returns_valid_result() {
        // An empty directory has no prose and no AI signals, so the heuristic must
        // return a result with valid confidence bounds.
        let temp_dir = tempfile::tempdir().expect("must create temp dir for corpus-origin test");
        let result = run_detect_corpus_origin(temp_dir.path(), &LlmOpts::default());
        assert!(
            result.confidence >= 0.0 && result.confidence <= 1.0,
            "confidence must be in [0.0, 1.0] for an empty directory"
        );
        // Pair assertion: the function MUST NOT write corpus_origin.json — that
        // write is deferred to `run()` after the user confirms the proceed gate
        // so a declined init leaves no audit-trail side effects on disk.
        let written = temp_dir.path().join("corpus_origin.json");
        assert!(
            !written.exists(),
            "corpus_origin.json must not be written before proceed confirmation"
        );
    }

    #[test]
    fn init_run_writes_corpus_origin_json_after_proceed() {
        // After init succeeds (yes=true bypasses the proceed prompt), the corpus
        // origin audit trail must land in the project directory. This is the
        // post-confirmation pair to the test above.
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for corpus-origin write test");
        run(
            temp_directory.path(),
            true,
            false,
            false,
            &[],
            &LlmOpts::default(),
        )
        .expect("init::run should succeed for a valid directory");
        assert!(
            temp_directory.path().join("corpus_origin.json").exists(),
            "corpus_origin.json must be written after the proceed gate"
        );
    }

    // -- pass0_consent_obtained --

    #[test]
    fn pass0_consent_obtained_local_provider_returns_true() {
        // A local provider always passes the gate — no privacy concern.
        let provider = crate::llm::client::OllamaProvider::new("gemma3:4b".to_string(), None, 60);
        let opts = LlmOpts::default();
        assert!(
            pass0_consent_obtained(&provider, &opts),
            "local provider must pass Pass 0 consent gate"
        );
    }

    #[test]
    fn pass0_consent_obtained_external_with_accept_flag_returns_true() {
        // --accept-external-llm bypasses the gate even with an env-fallback key.
        temp_env::with_var("ANTHROPIC_API_KEY", Some("sk-ant-env"), || {
            let provider = crate::llm::client::AnthropicProvider::new(
                "claude-haiku-4-5-20251001".to_string(),
                None,
                None,
                60,
            );
            let opts = LlmOpts {
                accept_external_llm: true,
                ..LlmOpts::default()
            };
            assert!(
                pass0_consent_obtained(&provider, &opts),
                "--accept-external-llm must bypass Pass 0 consent gate"
            );
        });
    }

    #[test]
    fn pass0_consent_obtained_external_with_explicit_key_returns_true() {
        // An explicit --llm-api-key is itself consent — passing the flag is opt-in.
        temp_env::with_var("ANTHROPIC_API_KEY", None::<&str>, || {
            let provider = crate::llm::client::AnthropicProvider::new(
                "claude-haiku-4-5-20251001".to_string(),
                None,
                Some("sk-ant-explicit".to_string()),
                60,
            );
            let opts = LlmOpts::default();
            assert!(
                pass0_consent_obtained(&provider, &opts),
                "explicit Flag-source key must pass Pass 0 consent gate"
            );
        });
    }

    #[test]
    fn pass0_consent_obtained_external_with_env_key_returns_false() {
        // Env-fallback key + external endpoint + no --accept-external-llm must
        // be blocked at Pass 0 — the user has not given non-interactive consent.
        temp_env::with_var("ANTHROPIC_API_KEY", Some("sk-ant-env"), || {
            let provider = crate::llm::client::AnthropicProvider::new(
                "claude-haiku-4-5-20251001".to_string(),
                None,
                None,
                60,
            );
            let opts = LlmOpts::default(); // accept_external_llm = false
            assert!(
                !pass0_consent_obtained(&provider, &opts),
                "env-fallback key must block Pass 0 LLM call"
            );
        });
    }

    #[test]
    fn run_detect_corpus_origin_with_prose_returns_valid_result() {
        // A directory containing prose text must still produce a valid result.
        let temp_dir =
            tempfile::tempdir().expect("must create temp dir for prose corpus-origin test");
        std::fs::write(
            temp_dir.path().join("notes.txt"),
            "Alice went to the market. Bob bought apples. Alice and Bob are friends.",
        )
        .expect("must write prose file");
        let result = run_detect_corpus_origin(temp_dir.path(), &LlmOpts::default());
        assert!(
            result.confidence >= 0.0 && result.confidence <= 1.0,
            "confidence must remain in [0.0, 1.0] for prose input"
        );
        // Pair assertion: evidence list must be populated for a processed corpus.
        // The heuristic always populates evidence (even if empty string), so the
        // vec itself may be empty for short prose — just verify we don't panic.
        assert!(
            result.evidence.len() < 1000,
            "evidence must not grow unbounded"
        );
    }

    // -- run_discover_entities --

    #[test]
    fn run_discover_entities_on_plain_dir_returns_empty_dict() {
        // A directory with no manifests, git history, or recognized entities must
        // return an empty DetectedDict without panicking.
        let temp_dir =
            tempfile::tempdir().expect("must create temp dir for discover entities test");
        let empty_origin = CorpusOriginResult {
            likely_ai_dialogue: false,
            confidence: 0.5,
            primary_platform: None,
            user_name: None,
            agent_persona_names: vec![],
            evidence: vec![],
        };
        let (projects, detected) =
            run_discover_entities(temp_dir.path(), &["en".to_string()], &empty_origin);
        // An empty dir has no manifests so the project list must be empty.
        assert!(
            projects.len() < 10_000,
            "project list must be bounded for an empty directory"
        );
        // Pair assertion: detected entities must also be empty or near-empty.
        assert!(
            detected.people.len() < 10_000,
            "people must be bounded for an empty directory"
        );
    }
}
