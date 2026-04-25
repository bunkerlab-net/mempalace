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
use std::path::Path;

use crate::config::{ProjectConfig, RoomConfig};
use crate::error::Result;
use crate::llm::{LlmProvider, collect_corpus_text, get_provider, refine_entities};
use crate::palace::entities::DetectedEntity;
use crate::palace::entity_confirm::confirm_entities;
use crate::palace::known_entities::add_to_known_entities;
use crate::palace::project_scanner::{
    DetectedDict, ProjectInfo, merge_detected, scan, to_detected_dict,
};
use crate::palace::room_detect::detect_rooms_from_folders;
use crate::palace::session_scanner::{is_claude_projects_root, scan_claude_projects};

// Timeout for LLM calls during entity refinement — long enough for local models.
const LLM_TIMEOUT_SECS: u64 = 60;

const _: () = assert!(LLM_TIMEOUT_SECS > 0);

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
        }
    }
}

// ===================== PUBLIC API =====================

/// Run `mempalace init` for `directory`.
///
/// Discovers entities, optionally refines via LLM, confirms with the user,
/// detects rooms, then writes `entities.json` and `mempalace.yaml`.
pub fn run(directory: &Path, yes: bool, no_gitignore: bool, llm_opts: &LlmOpts) -> Result<()> {
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

    // Phase 1: discover entities from manifest files and git history.
    let (projects, detected) = run_discover_entities(&directory);

    // Phase 1.5: optionally refine entities with the LLM.
    let detected = run_refine_entities(detected, &directory, llm_opts)?;

    // Phase 2: detect rooms from folder structure.
    let rooms = detect_rooms_from_folders(&directory);
    let wing_name = run_derive_wing_name(&projects, &directory);
    let file_count = crate::palace::miner::scan_project_with_opts(&directory, !no_gitignore).len();

    run_print_summary(&wing_name, file_count, &rooms, &detected);
    run_confirm_and_save(&detected, yes, &directory)?;

    if !run_prompt_proceed(yes)? {
        return Ok(());
    }

    run_write_config(&wing_name, rooms, &directory)
}

// ===================== PRIVATE HELPERS =====================

/// Discover entities from manifest files, git history, and Claude Code sessions.
///
/// Returns the raw `ProjectInfo` list (for wing-name derivation) and the merged
/// `DetectedDict`. Never fails — returns empty collections when the directory has
/// no recognized signals. Called by [`run`].
fn run_discover_entities(directory: &Path) -> (Vec<ProjectInfo>, DetectedDict) {
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

/// Optionally refine `detected` using an LLM.
///
/// Returns `detected` unchanged when LLM is disabled or unavailable. On LLM
/// failure, batch errors are logged to stderr and remaining entities are
/// returned as-is. Called by [`run`].
fn run_refine_entities(
    detected: DetectedDict,
    directory: &Path,
    llm_opts: &LlmOpts,
) -> Result<DetectedDict> {
    assert!(directory.is_dir());
    assert!(!directory.as_os_str().is_empty());

    let Some(provider) = run_setup_llm(llm_opts)? else {
        return Ok(detected);
    };

    let corpus = collect_corpus_text(directory);
    let result = refine_entities(detected, &corpus, provider.as_ref());

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
/// Returns `None` when the provider is disabled or unreachable, logging the
/// reason to stderr. Returns `Err` only for misconfigured provider names.
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

    assert!(!provider.name().is_empty());
    eprintln!("  LLM: {} ({}) ready", opts.provider, opts.model);
    Ok(Some(provider))
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
/// Called by [`run`] to keep that function within the 70-line limit.
fn run_confirm_and_save(detected: &DetectedDict, yes: bool, directory: &Path) -> Result<()> {
    assert!(directory.is_dir());
    assert!(!directory.as_os_str().is_empty());

    let confirmed = confirm_entities(detected, yes);

    if confirmed.people.is_empty() && confirmed.projects.is_empty() {
        return Ok(());
    }

    // Build the category map for the global registry.
    let mut by_category: HashMap<String, Vec<String>> = HashMap::new();
    if !confirmed.people.is_empty() {
        by_category.insert("people".to_string(), confirmed.people.clone());
    }
    if !confirmed.projects.is_empty() {
        by_category.insert("projects".to_string(), confirmed.projects.clone());
    }

    if let Err(error) = add_to_known_entities(&by_category) {
        eprintln!("  Warning: could not update entity registry: {error}");
    }

    // Write entities.json into the project directory.
    let entities_json = serde_json::json!({
        "people": confirmed.people,
        "projects": confirmed.projects,
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

    let sanitized = base.to_lowercase().replace([' ', '-'], "_");
    assert!(!sanitized.is_empty(), "wing name must not be empty");
    sanitized
}

/// Print the init summary including detected entities and rooms.
///
/// Called by [`run`] to keep that function within the 70-line limit.
// TigerStyle exemption: declarative display logic — entity lists are data, not logic.
#[allow(clippy::too_many_lines)]
fn run_print_summary(
    wing_name: &str,
    file_count: usize,
    rooms: &[crate::config::RoomConfig],
    detected: &DetectedDict,
) {
    assert!(!wing_name.is_empty());

    println!("\n=======================================================");
    println!("  MemPalace Init");
    println!("=======================================================");
    println!("\n  WING: {wing_name}");
    println!("  ({file_count} files found, rooms detected from folder structure)\n");

    for room in rooms {
        println!("    ROOM: {}", room.name);
        println!("          {}", room.description);
    }

    if !detected.projects.is_empty() || !detected.people.is_empty() {
        println!("\n  Detected entities:");
        run_print_entities("Projects", &detected.projects);
        run_print_entities("People", &detected.people);
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
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for init test");
        run(temp_directory.path(), true, false, &LlmOpts::default())
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
        let result = run(path, true, false, &LlmOpts::default());
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

        run(temp_directory.path(), true, true, &LlmOpts::default())
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
}
