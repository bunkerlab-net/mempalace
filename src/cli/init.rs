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
use crate::palace::entity_detect;
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
/// If `lang` is non-empty those BCP-47 codes replace the global
/// `entity_languages` setting. After writing the config, `mempalace.yaml`
/// and `entities.json` are appended to the directory's `.gitignore` when
/// the directory is inside a git worktree and the files aren't already
/// listed.
pub fn run(
    directory: &Path,
    yes: bool,
    no_gitignore: bool,
    lang: &[String],
    llm_opts: &LlmOpts,
) -> Result<()> {
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

    // Present summary and ask whether to proceed before any writes so a "no"
    // answer leaves no side effects (entity files, config) on disk.
    run_print_summary(&wing_name, file_count, &rooms, &detected);

    if !run_prompt_proceed(yes)? {
        return Ok(());
    }

    // Write mempalace.yaml before entities.json so a failure in run_write_config
    // does not leave an orphaned entities.json on disk without a valid config.
    run_write_config(&wing_name, rooms, &directory)?;
    run_confirm_and_save(&detected, yes, &directory)?;

    if !lang.is_empty() {
        run_persist_lang(lang);
    }
    run_gitignore_protect(&directory);
    Ok(())
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

    // Phase 1b: supplement with prose-based entity detection.
    // Languages default to English; entity_languages config is read by the
    // hook/MCP path where the config is already loaded.
    let detected = run_discover_entities_prose(directory, detected);

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
fn run_discover_entities_prose(directory: &Path, current: DetectedDict) -> DetectedDict {
    let prose_files = entity_detect::scan_for_detection(directory, 10);
    if prose_files.is_empty() {
        return current;
    }
    let prose_refs: Vec<&std::path::Path> = prose_files
        .iter()
        .map(std::path::PathBuf::as_path)
        .collect();
    let result = entity_detect::detect_entities(&prose_refs, 10, &["en"]);

    assert!(
        result.people.len() + result.projects.len() + result.uncertain.len()
            <= prose_refs.len() * 1000,
        "prose detection result is unexpectedly large"
    );

    let prose_signal = DetectedDict {
        people: result.people,
        projects: result.projects,
        uncertain: result.uncertain,
    };
    merge_detected(current, prose_signal, false)
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
    let mut config = crate::config::MempalaceConfig::load().unwrap_or_default();
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

/// Append `mempalace.yaml` and `entities.json` to the `.gitignore` in
/// `directory` if the directory is the root of a git worktree and the
/// entries are not already present.
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
    let existing = std::fs::read_to_string(&gitignore_path).unwrap_or_default();
    assert!(
        existing.len() < 10 * 1024 * 1024,
        "run_gitignore_protect: .gitignore must be < 10 MB"
    );
    let entries = ["mempalace.yaml", "entities.json"];
    let mut appended: Vec<&str> = Vec::with_capacity(entries.len());
    let mut additions = String::new();
    for entry in entries {
        if !existing.lines().any(|line| line.trim() == entry) {
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
        run(temp_directory.path(), true, false, &[], &LlmOpts::default())
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
        let result = run(path, true, false, &[], &LlmOpts::default());
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

        run(temp_directory.path(), true, true, &[], &LlmOpts::default())
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
            };
            let result = run_setup_llm(&opts).expect("unavailable provider must not return Err");
            assert!(
                result.is_none(),
                "missing API key must cause unavailable → None"
            );
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
        };
        let opts = LlmOpts::default(); // enabled = false
        let result = run_refine_entities(detected, temp_dir.path(), &opts)
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
        };
        // Rooms list — empty is fine for the print test.
        let rooms: Vec<crate::config::RoomConfig> = vec![];
        // Must not panic with any non-empty DetectedDict.
        run_print_summary("my_project", 42, &rooms, &detected);
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
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir =
            tempfile::tempdir().expect("must create temp dir for run_confirm_and_save test");
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
        };
        run_confirm_and_save(&detected, true, temp_dir.path())
            .expect("run_confirm_and_save must succeed");
        let entities_path = temp_dir.path().join("entities.json");
        assert!(entities_path.exists(), "entities.json must be written");
        let content =
            std::fs::read_to_string(&entities_path).expect("entities.json must be readable");
        assert!(
            content.contains("Alice"),
            "entities.json must name the confirmed person"
        );
        assert!(!content.is_empty());
    }

    #[test]
    fn run_confirm_and_save_skips_write_when_nothing_confirmed() {
        // With no entities above the confidence threshold, entities.json must not be written.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir = tempfile::tempdir().expect("must create temp dir for skip-write test");
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
        };
        run_confirm_and_save(&detected, true, temp_dir.path())
            .expect("run_confirm_and_save must succeed even with no confirmations");
        let entities_path = temp_dir.path().join("entities.json");
        assert!(
            !entities_path.exists(),
            "entities.json must NOT be written when nothing is confirmed"
        );
    }

    #[test]
    fn run_confirm_and_save_writes_both_people_and_projects() {
        // When both people and projects are confirmed, both categories appear in entities.json.
        use crate::palace::entities::DetectedEntity;
        use crate::palace::project_scanner::DetectedDict;
        let temp_dir = tempfile::tempdir().expect("must create temp dir for both-categories test");
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
        };
        run_confirm_and_save(&detected, true, temp_dir.path())
            .expect("run_confirm_and_save must succeed");
        let entities_path = temp_dir.path().join("entities.json");
        assert!(entities_path.exists(), "entities.json must be written");
        let content =
            std::fs::read_to_string(&entities_path).expect("entities.json must be readable");
        assert!(content.contains("Alice"), "must include confirmed person");
        assert!(content.contains("mylib"), "must include confirmed project");
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
        let (_projects, detected) = run_discover_entities(temp_dir.path());
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
}
