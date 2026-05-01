//! `mempalace onboard` — first-run interactive setup wizard.
//!
//! Asks the user about their world (people, projects, wings) and seeds the
//! entity registry and AAAK bootstrap markdown files. Mirrors Python's
//! `onboarding.py` module.

use std::collections::HashMap;
use std::io::{BufRead, Write as _};
use std::path::Path;

use crate::config::config_dir;
use crate::error::Result;
use crate::palace::entities::DetectedEntity;
use crate::palace::entity_detect::{detect_entities, scan_for_detection};
use crate::palace::entity_registry::{EntityRegistry, SeedPerson as RegistrySeedPerson};
use crate::palace::known_entities::add_to_known_entities;

/// Maximum people that can be added in a single onboarding session.
const PEOPLE_LIMIT: usize = 100;
/// Maximum projects that can be added in a single onboarding session.
const PROJECTS_LIMIT: usize = 100;
/// Maximum files scanned during entity auto-detection.
const SCAN_FILES_MAX: usize = 200;
/// Minimum confidence to include an auto-detected entity for review.
const DETECT_CONFIDENCE_MIN: f64 = 0.7;

const _: () = assert!(PEOPLE_LIMIT > 0);
const _: () = assert!(PROJECTS_LIMIT > 0);
const _: () = assert!(SCAN_FILES_MAX > 0);
const _: () = assert!(DETECT_CONFIDENCE_MIN > 0.0);

static DEFAULT_WINGS_WORK: &[&str] = &["projects", "clients", "team", "decisions", "research"];
static DEFAULT_WINGS_PERSONAL: &[&str] = &[
    "family",
    "health",
    "creative",
    "reflections",
    "relationships",
];
static DEFAULT_WINGS_COMBO: &[&str] = &[
    "family",
    "work",
    "health",
    "creative",
    "projects",
    "reflections",
];

// Names that are also common English words — surface as ambiguity warnings.
// Mirrors Python's entity_registry.COMMON_ENGLISH_WORDS.
static AMBIGUOUS_NAMES: &[&str] = &[
    "ever",
    "grace",
    "will",
    "bill",
    "mark",
    "april",
    "may",
    "june",
    "joy",
    "hope",
    "faith",
    "chance",
    "chase",
    "hunter",
    "dash",
    "flash",
    "star",
    "sky",
    "river",
    "brook",
    "lane",
    "art",
    "clay",
    "gil",
    "nat",
    "max",
    "rex",
    "ray",
    "jay",
    "rose",
    "violet",
    "lily",
    "ivy",
    "ash",
    "reed",
    "sage",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "january",
    "february",
    "march",
    "july",
    "august",
];

/// A person collected during the onboarding flow.
pub struct PersonEntry {
    pub name: String,
    pub relationship: String,
    pub context: String,
    pub nickname: String,
}

/// Run the `onboard` command — interactive first-run setup wizard.
///
/// Collects mode, people, projects, and wings from stdin. Optionally scans
/// `directory` for additional name candidates. Seeds the entity registry and
/// generates AAAK bootstrap markdown files in the mempalace config directory.
pub fn run(directory: &Path) -> Result<()> {
    println!("\n  Welcome to MemPalace! Let's set up your palace.\n");

    let mut reader = std::io::BufReader::new(std::io::stdin());

    let mode = onboarding_ask_mode(&mut reader)?;
    let people = onboarding_ask_people(&mode, &mut reader)?;
    let projects = onboarding_ask_projects(&mode, &mut reader)?;
    let wings = onboarding_ask_wings(&mode, &mut reader)?;

    let all_people = onboarding_maybe_scan(directory, people, &mode, &mut reader)?;

    let ambiguous = onboarding_warn_ambiguous(&all_people);
    if !ambiguous.is_empty() {
        println!("\n  Heads up — these names are also common English words:");
        println!("    {}", ambiguous.join(", "));
        println!("  MemPalace will check context before treating them as names.\n");
    }

    let registry_path = onboarding_seed_registry(&all_people, &projects)?;
    onboarding_seed_entity_registry(&all_people, &projects, &mode)?;
    onboarding_generate_aaak_bootstrap(&all_people, &projects, &wings, &mode)?;

    let config = config_dir();
    println!("\n  Setup complete!");
    println!("  Registry:      {}", registry_path.display());
    println!(
        "  AAAK entities: {}",
        config.join("aaak_entities.md").display()
    );
    println!(
        "  Critical facts: {}",
        config.join("critical_facts.md").display()
    );
    println!("\n  Your AI will know your world from the first session.\n");

    assert!(!registry_path.as_os_str().is_empty());
    Ok(())
}

/// Print the three mode choices and read the user's selection.
///
/// Returns one of `"work"`, `"personal"`, or `"combo"`. Loops until valid.
fn onboarding_ask_mode(reader: &mut impl BufRead) -> Result<String> {
    println!("  How are you using MemPalace?");
    println!("    1. Work — clients, projects, team");
    println!("    2. Personal — family, health, relationships");
    println!("    3. Combo — work and personal together");
    println!();

    loop {
        let input = onboarding_readline("Your choice [1/2/3]", None, reader)?;
        let mode = match input.trim() {
            "1" => "work",
            "2" => "personal",
            "3" | "" => "combo",
            _ => {
                println!("  Please enter 1, 2, or 3.");
                continue;
            }
        };
        assert!(!mode.is_empty());
        return Ok(mode.to_string());
    }
}

/// Collect people from stdin, looping until the user enters an empty line.
///
/// For "combo" mode the context (personal vs work) is asked per person.
/// Returns up to `PEOPLE_LIMIT` entries.
fn onboarding_ask_people(mode: &str, reader: &mut impl BufRead) -> Result<Vec<PersonEntry>> {
    assert!(!mode.is_empty());
    println!(
        "\n  Who are the people in your {}? (enter to finish)",
        if mode == "work" { "team" } else { "life" }
    );
    let mut people: Vec<PersonEntry> = Vec::new();

    loop {
        assert!(people.len() <= PEOPLE_LIMIT);
        if people.len() >= PEOPLE_LIMIT {
            break;
        }
        let entry = onboarding_ask_people_one(mode, people.len() + 1, reader)?;
        if entry.name.is_empty() {
            break;
        }
        people.push(entry);
    }

    assert!(people.len() <= PEOPLE_LIMIT);
    Ok(people)
}

/// Read one person from stdin (name + optional nickname, relationship, context).
///
/// Returns a `PersonEntry` with an empty `name` when the user presses enter
/// to signal end-of-input.
fn onboarding_ask_people_one(
    mode: &str,
    index: usize,
    reader: &mut impl BufRead,
) -> Result<PersonEntry> {
    assert!(!mode.is_empty());
    assert!(index > 0);

    let name = onboarding_readline(&format!("  Person {index}"), None, reader)?;
    if name.is_empty() {
        return Ok(PersonEntry {
            name: String::new(),
            relationship: String::new(),
            context: String::new(),
            nickname: String::new(),
        });
    }
    let nickname = onboarding_readline(
        &format!("  Nickname for {name}? (or enter to skip)"),
        Some(""),
        reader,
    )?;
    let relationship = onboarding_readline("  Relationship/role", Some(""), reader)?;
    let context = match mode {
        "personal" => "personal".to_string(),
        "work" => "work".to_string(),
        _ => {
            // Lowercase before matching so "W"/"P" are accepted as readily as
            // "w"/"p"; the prompt makes no claim about case sensitivity.
            let raw = onboarding_readline("  Context (p=personal / w=work)", Some("p"), reader)?
                .to_ascii_lowercase();
            if raw.starts_with('w') {
                "work".to_string()
            } else {
                "personal".to_string()
            }
        }
    };

    assert!(!name.is_empty());
    Ok(PersonEntry {
        name,
        relationship,
        context,
        nickname,
    })
}

/// Collect project names from stdin until the user enters an empty line.
///
/// Returns up to `PROJECTS_LIMIT` project name strings.
fn onboarding_ask_projects(mode: &str, reader: &mut impl BufRead) -> Result<Vec<String>> {
    assert!(!mode.is_empty());
    if mode == "personal" {
        println!("\n  Any creative projects or personal goals? (enter to skip)");
    } else {
        println!("\n  What are your main projects? (enter to finish)");
    }

    let mut projects: Vec<String> = Vec::new();
    loop {
        assert!(projects.len() <= PROJECTS_LIMIT);
        if projects.len() >= PROJECTS_LIMIT {
            break;
        }
        let name = onboarding_readline(&format!("  Project {}", projects.len() + 1), None, reader)?;
        if name.is_empty() {
            break;
        }
        projects.push(name);
    }

    assert!(projects.len() <= PROJECTS_LIMIT);
    Ok(projects)
}

/// Show the default wings for the selected mode and allow the user to override.
///
/// The user may accept the defaults (enter) or type comma-separated names.
fn onboarding_ask_wings(mode: &str, reader: &mut impl BufRead) -> Result<Vec<String>> {
    assert!(!mode.is_empty());
    let defaults: &[&str] = match mode {
        "work" => DEFAULT_WINGS_WORK,
        "personal" => DEFAULT_WINGS_PERSONAL,
        _ => DEFAULT_WINGS_COMBO,
    };

    assert!(!defaults.is_empty());
    println!("\n  Default wings for {mode} mode: {}", defaults.join(", "));

    let input = onboarding_readline(
        "  Customize wings (comma-separated, or enter to accept defaults)",
        Some(""),
        reader,
    )?;

    let parsed: Vec<String> = if input.is_empty() {
        Vec::new()
    } else {
        input
            .split(',')
            .map(|wing_str| wing_str.trim().to_string())
            .filter(|wing_str| !wing_str.is_empty())
            .collect()
    };

    // Fall back to the defaults when the user typed only whitespace or commas.
    // The earlier `assert!` would have panicked here — onboarding is interactive,
    // so a malformed input must lead to a usable wings list, not a crash.
    let wings: Vec<String> = if parsed.is_empty() {
        defaults
            .iter()
            .map(|wing_str| (*wing_str).to_string())
            .collect()
    } else {
        parsed
    };

    assert!(
        !wings.is_empty(),
        "wings list must be non-empty after fallback to defaults"
    );
    Ok(wings)
}

/// Optionally scan `directory` for additional entity candidates and prompt the user.
///
/// Skips if the user declines the scan prompt. Returns the augmented people list.
fn onboarding_maybe_scan(
    directory: &Path,
    people: Vec<PersonEntry>,
    mode: &str,
    reader: &mut impl BufRead,
) -> Result<Vec<PersonEntry>> {
    assert!(!mode.is_empty());
    if !onboarding_readline_yn(
        "Scan your files for additional names we might have missed?",
        true,
        reader,
    )? {
        return Ok(people);
    }
    let config = crate::config::MempalaceConfig::load().unwrap_or_default();
    // Fall back to "en" so an empty `entity_languages` (e.g. a hand-edited
    // config or a future default change) cannot trip the non-empty languages
    // assertion in `onboarding_scan_directory` and crash an interactive flow.
    let language_refs: Vec<&str> = if config.entity_languages.is_empty() {
        vec!["en"]
    } else {
        config.entity_languages.iter().map(String::as_str).collect()
    };
    assert!(!language_refs.is_empty());
    let detected = onboarding_scan_directory(directory, &people, &language_refs);
    if detected.is_empty() {
        println!("  No additional candidates found.");
        return Ok(people);
    }
    onboarding_prompt_detected(people, detected, mode, reader)
}

/// Scan `directory` for entity candidates not already in `known_people`.
///
/// Returns only candidates whose confidence meets `DETECT_CONFIDENCE_MIN`.
fn onboarding_scan_directory(
    directory: &Path,
    known_people: &[PersonEntry],
    languages: &[&str],
) -> Vec<DetectedEntity> {
    assert!(
        !languages.is_empty(),
        "onboarding_scan_directory: languages must not be empty"
    );
    assert!(
        known_people.len() <= PEOPLE_LIMIT,
        "people list must not exceed PEOPLE_LIMIT"
    );

    let known_lower: std::collections::HashSet<String> =
        known_people.iter().map(|p| p.name.to_lowercase()).collect();

    let path_bufs = scan_for_detection(directory, SCAN_FILES_MAX);
    if path_bufs.is_empty() {
        return vec![];
    }
    let path_refs: Vec<&Path> = path_bufs.iter().map(std::path::PathBuf::as_path).collect();
    let result = detect_entities(&path_refs, SCAN_FILES_MAX, languages);

    result
        .people
        .into_iter()
        .filter(|e| {
            e.confidence >= DETECT_CONFIDENCE_MIN && !known_lower.contains(&e.name.to_lowercase())
        })
        .collect()
}

/// Prompt the user to add auto-detected candidates to the people list.
///
/// Shows each candidate's name and confidence, then asks (p)erson or (s)kip.
fn onboarding_prompt_detected(
    mut people: Vec<PersonEntry>,
    detected: Vec<DetectedEntity>,
    mode: &str,
    reader: &mut impl BufRead,
) -> Result<Vec<PersonEntry>> {
    assert!(!mode.is_empty());
    println!("\n  Found {} additional name candidates:\n", detected.len());

    for entity in &detected {
        println!(
            "    {:20} confidence={:.0}%",
            entity.name,
            entity.confidence * 100.0
        );
    }
    println!();
    if !onboarding_readline_yn("  Add any of these to your registry?", false, reader)? {
        return Ok(people);
    }

    for entity in detected {
        if people.len() >= PEOPLE_LIMIT {
            break;
        }
        // Lowercase for tolerance: "P"/"S" should behave like "p"/"s".
        let choice = onboarding_readline(
            &format!("    {} — (p)erson, (s)kip?", entity.name),
            Some("s"),
            reader,
        )?
        .to_ascii_lowercase();
        if !choice.starts_with('p') {
            continue;
        }
        let relationship = onboarding_readline("    Relationship/role", Some(""), reader)?;
        let context = match mode {
            "personal" => "personal".to_string(),
            "work" => "work".to_string(),
            _ => {
                // Same case-insensitive policy as `onboarding_ask_people_one`.
                let raw =
                    onboarding_readline("    Context (p=personal / w=work)", Some("p"), reader)?
                        .to_ascii_lowercase();
                if raw.starts_with('w') {
                    "work".to_string()
                } else {
                    "personal".to_string()
                }
            }
        };
        people.push(PersonEntry {
            name: entity.name,
            relationship,
            context,
            nickname: String::new(),
        });
    }

    assert!(people.len() <= PEOPLE_LIMIT);
    Ok(people)
}

/// Return people whose names clash with common English words.
///
/// Used to surface an ambiguity warning so the user understands the risk.
fn onboarding_warn_ambiguous(people: &[PersonEntry]) -> Vec<String> {
    let ambiguous_set: std::collections::HashSet<&str> = AMBIGUOUS_NAMES.iter().copied().collect();

    assert!(!ambiguous_set.is_empty());

    people
        .iter()
        .filter(|p| ambiguous_set.contains(p.name.to_lowercase().as_str()))
        .map(|p| p.name.clone())
        .collect()
}

/// Merge people and projects into the global known-entities registry.
///
/// Returns the path written so the caller can display it to the user.
fn onboarding_seed_registry(
    people: &[PersonEntry],
    projects: &[String],
) -> Result<std::path::PathBuf> {
    let mut by_category: HashMap<String, Vec<String>> = HashMap::new();

    let person_names: Vec<String> = people.iter().map(|p| p.name.clone()).collect();
    if !person_names.is_empty() {
        by_category.insert("people".to_string(), person_names);
    }

    let project_names: Vec<String> = projects.to_vec();
    if !project_names.is_empty() {
        by_category.insert("projects".to_string(), project_names);
    }

    // Nicknames are stored as aliases under the "aliases" category.
    let alias_names: Vec<String> = people
        .iter()
        .filter(|p| !p.nickname.is_empty())
        .map(|p| format!("{}={}", p.nickname, p.name))
        .collect();
    if !alias_names.is_empty() {
        by_category.insert("aliases".to_string(), alias_names);
    }

    assert!(
        !by_category.is_empty() || people.is_empty(),
        "non-empty people must populate registry"
    );
    add_to_known_entities(&by_category, None)
}

/// Seed the structured `EntityRegistry` (`entity_registry.json`) from onboarding data.
///
/// Converts `PersonEntry` slices into [`RegistrySeedPerson`] records and calls
/// [`EntityRegistry::seed`], which persists the richer per-entity metadata
/// (source, contexts, relationship, confidence) alongside the flat name list
/// already written by [`onboarding_seed_registry`].
fn onboarding_seed_entity_registry(
    people: &[PersonEntry],
    projects: &[String],
    mode: &str,
) -> Result<()> {
    assert!(
        !mode.is_empty(),
        "onboarding_seed_entity_registry: mode must not be empty"
    );
    assert!(
        people.len() <= PEOPLE_LIMIT,
        "onboarding_seed_entity_registry: people count exceeds limit"
    );

    let seed_people: Vec<RegistrySeedPerson> = people
        .iter()
        .filter(|person| !person.name.trim().is_empty())
        .map(|person| RegistrySeedPerson {
            name: person.name.clone(),
            relationship: person.relationship.clone(),
            context: person.context.clone(),
            nickname: if person.nickname.is_empty() {
                None
            } else {
                Some(person.nickname.clone())
            },
        })
        .collect();

    let mut registry = EntityRegistry::load();
    registry.seed(mode, &seed_people, projects)?;

    // Pair assertion: converted list is a subset of the input (filter can only shrink it).
    debug_assert!(
        seed_people.len() <= people.len(),
        "onboarding_seed_entity_registry: seed_people must be a subset of input people"
    );
    Ok(())
}

/// Generate AAAK entity codes for each person: first 3 letters uppercase.
///
/// Collision is resolved by extending to 4 characters.
fn onboarding_generate_entity_codes(people: &[PersonEntry]) -> Vec<(String, String)> {
    let mut used: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut codes: Vec<(String, String)> = Vec::with_capacity(people.len());

    for person in people {
        assert!(!person.name.is_empty());
        let base: String = person
            .name
            .chars()
            .take(3)
            .collect::<String>()
            .to_uppercase();
        let code = if used.contains(&base) {
            // Progressively widen the prefix until unique; fall back to numeric suffix.
            let name_chars: Vec<char> = person.name.chars().collect();
            let mut candidate = String::new();
            let mut found = false;
            for length in 4..=name_chars.len() {
                candidate = name_chars[..length]
                    .iter()
                    .collect::<String>()
                    .to_uppercase();
                if !used.contains(&candidate) {
                    found = true;
                    break;
                }
            }
            if !found {
                // All prefix lengths are taken; append a numeric suffix.
                let mut suffix = 1usize;
                loop {
                    candidate = format!(
                        "{}{}",
                        name_chars[..name_chars.len().min(4)]
                            .iter()
                            .collect::<String>()
                            .to_uppercase(),
                        suffix
                    );
                    if !used.contains(&candidate) {
                        break;
                    }
                    suffix += 1;
                    assert!(
                        suffix < 1000,
                        "onboarding_generate_entity_codes: too many collisions"
                    );
                }
            }
            candidate
        } else {
            base
        };
        used.insert(code.clone());
        codes.push((person.name.clone(), code));
    }

    assert_eq!(codes.len(), people.len());
    codes
}

/// Write the AAAK entities markdown and critical facts bootstrap files.
///
/// Both files are written to the mempalace config directory.
fn onboarding_generate_aaak_bootstrap(
    people: &[PersonEntry],
    projects: &[String],
    wings: &[String],
    mode: &str,
) -> Result<()> {
    assert!(!mode.is_empty());
    assert!(!wings.is_empty());

    let codes = onboarding_generate_entity_codes(people);
    let config = config_dir();
    std::fs::create_dir_all(&config)?;

    onboarding_write_aaak_entities_file(&config, people, projects, &codes)?;
    onboarding_write_critical_facts_file(&config, people, projects, wings, mode, &codes)?;

    assert!(config.join("aaak_entities.md").exists() || people.is_empty());
    Ok(())
}

/// Write `aaak_entities.md` — the AAAK entity code registry.
fn onboarding_write_aaak_entities_file(
    config: &Path,
    people: &[PersonEntry],
    projects: &[String],
    codes: &[(String, String)],
) -> Result<()> {
    assert_eq!(codes.len(), people.len());
    let mut lines: Vec<String> = vec![
        "# AAAK Entity Registry".to_string(),
        "# Auto-generated by mempalace onboard. Update as needed.".to_string(),
        String::new(),
        "## People".to_string(),
    ];

    for (i, person) in people.iter().enumerate() {
        let code = &codes[i].1;
        if person.relationship.is_empty() {
            lines.push(format!("  {code}={}", person.name));
        } else {
            lines.push(format!(
                "  {code}={} ({})",
                person.name, person.relationship
            ));
        }
    }

    if !projects.is_empty() {
        lines.push(String::new());
        lines.push("## Projects".to_string());
        // Track codes already issued to people so project codes can't collide with
        // them, and across projects so two projects sharing a 4-char prefix get
        // distinct deterministic suffixes.
        let mut used: std::collections::HashSet<String> =
            codes.iter().map(|(_, code)| code.clone()).collect();
        for proj in projects {
            let base: String = proj.chars().take(4).collect::<String>().to_uppercase();
            let mut candidate = base.clone();
            let mut suffix: usize = 1;
            while used.contains(&candidate) {
                candidate = format!("{base}{suffix}");
                suffix += 1;
                assert!(
                    suffix < 1000,
                    "onboarding project code collision suffix exceeded"
                );
            }
            used.insert(candidate.clone());
            lines.push(format!("  {candidate}={proj}"));
        }
    }

    lines.extend([
        String::new(),
        "## AAAK Quick Reference".to_string(),
        "  Symbols: ♡=love ★=importance ⚠=warning →=relationship |=separator".to_string(),
        "  Structure: KEY:value | GROUP(details) | entity.attribute".to_string(),
        "  Read naturally — expand codes, treat *markers* as emotional context.".to_string(),
    ]);

    std::fs::write(config.join("aaak_entities.md"), lines.join("\n"))?;
    Ok(())
}

/// Write `critical_facts.md` — the pre-palace facts bootstrap.
fn onboarding_write_critical_facts_file(
    config: &Path,
    people: &[PersonEntry],
    projects: &[String],
    wings: &[String],
    mode: &str,
    codes: &[(String, String)],
) -> Result<()> {
    assert!(!mode.is_empty());
    assert!(!wings.is_empty());
    assert_eq!(codes.len(), people.len());

    let mut lines: Vec<String> = vec![
        "# Critical Facts (bootstrap — will be enriched after mining)".to_string(),
        String::new(),
    ];

    let personal: Vec<_> = people
        .iter()
        .enumerate()
        .filter(|(_, p)| p.context == "personal")
        .collect();
    let work: Vec<_> = people
        .iter()
        .enumerate()
        .filter(|(_, p)| p.context == "work")
        .collect();

    if !personal.is_empty() {
        lines.push("## People (personal)".to_string());
        for (i, person) in &personal {
            let code = &codes[*i].1;
            let rel = if person.relationship.is_empty() {
                String::new()
            } else {
                format!(" — {}", person.relationship)
            };
            lines.push(format!("- **{}** ({code}){rel}", person.name));
        }
        lines.push(String::new());
    }

    if !work.is_empty() {
        lines.push("## People (work)".to_string());
        for (i, person) in &work {
            let code = &codes[*i].1;
            let rel = if person.relationship.is_empty() {
                String::new()
            } else {
                format!(" — {}", person.relationship)
            };
            lines.push(format!("- **{}** ({code}){rel}", person.name));
        }
        lines.push(String::new());
    }

    if !projects.is_empty() {
        lines.push("## Projects".to_string());
        for proj in projects {
            lines.push(format!("- **{proj}**"));
        }
        lines.push(String::new());
    }

    lines.extend([
        "## Palace".to_string(),
        format!("Wings: {}", wings.join(", ")),
        format!("Mode: {mode}"),
        String::new(),
        "*This file will be enriched by mining.*".to_string(),
    ]);

    std::fs::write(config.join("critical_facts.md"), lines.join("\n"))?;
    Ok(())
}

/// Print a prompt to stdout and read one trimmed line from `reader`.
///
/// When `default` is `Some("")` the prompt shows nothing extra; when
/// `default` is `Some(val)` it shows `[val]` to guide the user.
fn onboarding_readline(
    prompt: &str,
    default: Option<&str>,
    reader: &mut impl BufRead,
) -> Result<String> {
    assert!(!prompt.is_empty());
    let suffix = match default {
        Some(default_val) if !default_val.is_empty() => format!(" [{default_val}]"),
        _ => String::new(),
    };
    print!("  {prompt}{suffix}: ");
    std::io::stdout().flush()?;

    let mut input = String::new();
    reader.read_line(&mut input)?;
    let trimmed = input.trim().to_string();

    // When user presses enter on a defaulted field, return the default value.
    // unwrap_or_default() for Option<&str> returns "" when None, preserving the
    // same behaviour as falling through to Ok(trimmed) when no default was set.
    if trimmed.is_empty() {
        return Ok(default.unwrap_or_default().to_string());
    }

    assert!(
        !trimmed.contains('\0'),
        "stdin input must not contain null bytes"
    );
    Ok(trimmed)
}

/// Print a yes/no prompt and return true for yes, false for no.
///
/// The default answer is used when the user presses enter without input.
fn onboarding_readline_yn(
    prompt: &str,
    default_yes: bool,
    reader: &mut impl BufRead,
) -> Result<bool> {
    assert!(!prompt.is_empty());
    let hint = if default_yes { "Y/n" } else { "y/N" };
    print!("  {prompt} [{hint}]: ");
    std::io::stdout().flush()?;

    let mut input = String::new();
    reader.read_line(&mut input)?;
    let trimmed = input.trim().to_lowercase();

    let answer = if trimmed.is_empty() {
        default_yes
    } else {
        trimmed.starts_with('y')
    };

    assert!(
        !trimmed.contains('\0'),
        "stdin input must not contain null bytes"
    );
    Ok(answer)
}

#[cfg(test)]
// Test code — .expect() with a descriptive message is acceptable; panics are the correct failure mode.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // ── onboarding_generate_entity_codes ────────────────────────────────

    #[test]
    fn entity_codes_uses_first_three_chars_uppercase() {
        let people = vec![PersonEntry {
            name: "Alice".to_string(),
            relationship: String::new(),
            context: "personal".to_string(),
            nickname: String::new(),
        }];
        let codes = onboarding_generate_entity_codes(&people);
        assert_eq!(codes.len(), 1);
        assert_eq!(codes[0].1, "ALI");
    }

    #[test]
    fn entity_codes_collision_uses_four_chars() {
        // Two names sharing the same 3-char prefix must not both get the same code.
        let people = vec![
            PersonEntry {
                name: "Alice".to_string(),
                relationship: String::new(),
                context: String::new(),
                nickname: String::new(),
            },
            PersonEntry {
                name: "Alicia".to_string(),
                relationship: String::new(),
                context: String::new(),
                nickname: String::new(),
            },
        ];
        let codes = onboarding_generate_entity_codes(&people);
        assert_eq!(codes.len(), 2);
        assert_ne!(
            codes[0].1, codes[1].1,
            "collision must produce distinct codes"
        );
        assert_eq!(codes[1].1, "ALIC");
    }

    // ── onboarding_warn_ambiguous ────────────────────────────────────────

    #[test]
    fn warn_ambiguous_flags_common_word_names() {
        let people = vec![
            PersonEntry {
                name: "Grace".to_string(),
                relationship: String::new(),
                context: String::new(),
                nickname: String::new(),
            },
            PersonEntry {
                name: "Unusual".to_string(),
                relationship: String::new(),
                context: String::new(),
                nickname: String::new(),
            },
        ];
        let ambiguous = onboarding_warn_ambiguous(&people);
        assert_eq!(ambiguous.len(), 1, "only Grace is a common English word");
        assert_eq!(ambiguous[0], "Grace");
    }

    #[test]
    fn warn_ambiguous_returns_empty_for_distinct_names() {
        let people = vec![PersonEntry {
            name: "Xiomara".to_string(),
            relationship: String::new(),
            context: String::new(),
            nickname: String::new(),
        }];
        let ambiguous = onboarding_warn_ambiguous(&people);
        assert!(ambiguous.is_empty(), "distinctive name must not be flagged");
    }

    // ── onboarding_generate_entity_codes — collision fallback ─────────────

    #[test]
    fn entity_codes_numeric_suffix_when_all_prefixes_collide() {
        // Three identical names exhaust every prefix length, so the third must
        // fall through to the numeric-suffix branch of the collision resolver.
        let people = vec![
            PersonEntry {
                name: "Al".to_string(),
                relationship: String::new(),
                context: String::new(),
                nickname: String::new(),
            },
            PersonEntry {
                name: "Al".to_string(),
                relationship: String::new(),
                context: String::new(),
                nickname: String::new(),
            },
            PersonEntry {
                name: "Al".to_string(),
                relationship: String::new(),
                context: String::new(),
                nickname: String::new(),
            },
        ];
        let codes = onboarding_generate_entity_codes(&people);
        assert_eq!(codes.len(), 3);
        let unique: std::collections::HashSet<&str> =
            codes.iter().map(|(_, code)| code.as_str()).collect();
        assert_eq!(unique.len(), 3, "every code must be unique");
        // The third code falls through to the numeric-suffix branch — it must
        // include a digit, because the prefix-widening loop runs out of room.
        assert!(
            codes
                .iter()
                .any(|(_, code)| code.chars().any(char::is_numeric)),
            "numeric-suffix branch must be exercised"
        );
    }

    // ── PersonEntry helper ────────────────────────────────────────────────

    fn person_entry(name: &str, relationship: &str, context: &str, nickname: &str) -> PersonEntry {
        PersonEntry {
            name: name.to_string(),
            relationship: relationship.to_string(),
            context: context.to_string(),
            nickname: nickname.to_string(),
        }
    }

    // ── onboarding_seed_registry ──────────────────────────────────────────

    #[test]
    fn seed_registry_writes_people_projects_and_aliases() {
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let people = vec![
                person_entry("Alice", "engineer", "work", "Ali"),
                person_entry("Bob", "friend", "personal", ""),
            ];
            let projects = vec!["Mempalace".to_string()];
            let path =
                onboarding_seed_registry(&people, &projects).expect("seed registry must succeed");
            assert!(path.exists(), "registry file must be created");
            let raw = std::fs::read_to_string(&path).expect("read registry");
            // Aliases section is only written when at least one person has a
            // nickname — Alice's "Ali" must therefore land under "aliases".
            assert!(raw.contains("Alice"), "Alice must be listed");
            assert!(raw.contains("Bob"), "Bob must be listed");
            assert!(raw.contains("Mempalace"), "project must be listed");
            assert!(raw.contains("Ali=Alice"), "alias must be encoded");
        });
    }

    #[test]
    fn seed_registry_with_empty_inputs_writes_empty_registry() {
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let path =
                onboarding_seed_registry(&[], &[]).expect("empty seed registry must succeed");
            // The known-entities writer creates the file even when no
            // categories were merged, so its parent path must exist.
            assert!(path.parent().is_some_and(std::path::Path::exists));
        });
    }

    // ── onboarding_seed_entity_registry ────────────────────────────────────

    #[test]
    fn seed_entity_registry_writes_json_at_config_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let people = vec![
                person_entry("Alice", "engineer", "work", "Ali"),
                // An empty-name entry must be filtered out before seeding —
                // the registry seed asserts non-empty trimmed names.
                person_entry("   ", "", "", ""),
            ];
            let projects = vec!["Mempalace".to_string()];
            onboarding_seed_entity_registry(&people, &projects, "work")
                .expect("seed entity registry must succeed");
            let registry = temp.path().join("entity_registry.json");
            assert!(registry.exists(), "entity_registry.json must be written");
            let raw = std::fs::read_to_string(&registry).expect("read registry");
            assert!(raw.contains("\"Alice\""), "Alice must appear in registry");
            assert!(
                !raw.contains("\"   \""),
                "whitespace-only name must be filtered out"
            );
        });
    }

    // ── onboarding_write_aaak_entities_file ────────────────────────────────

    #[test]
    fn write_aaak_entities_file_renders_people_and_projects() {
        let temp = tempfile::tempdir().expect("tempdir");
        let people = vec![
            person_entry("Alice", "engineer", "work", ""),
            person_entry("Bob", "", "personal", ""),
        ];
        let projects = vec!["Mempalace".to_string(), "Mempal".to_string()];
        let codes = onboarding_generate_entity_codes(&people);
        onboarding_write_aaak_entities_file(temp.path(), &people, &projects, &codes)
            .expect("write aaak_entities.md must succeed");
        let contents = std::fs::read_to_string(temp.path().join("aaak_entities.md"))
            .expect("read aaak_entities.md");
        assert!(contents.contains("# AAAK Entity Registry"));
        assert!(
            contents.contains("ALI=Alice (engineer)"),
            "person with relationship must show parens"
        );
        assert!(
            contents.contains("BOB=Bob"),
            "person without relationship must omit parens"
        );
        assert!(contents.contains("## Projects"));
        assert!(
            contents.contains("MEMP=Mempalace"),
            "first project gets the bare 4-char prefix"
        );
        assert!(
            contents.contains("MEMP1=Mempal"),
            "colliding project must get a numeric suffix"
        );
        assert!(contents.contains("## AAAK Quick Reference"));
    }

    #[test]
    fn write_aaak_entities_file_skips_projects_section_when_empty() {
        let temp = tempfile::tempdir().expect("tempdir");
        let people = vec![person_entry("Alice", "", "personal", "")];
        let codes = onboarding_generate_entity_codes(&people);
        onboarding_write_aaak_entities_file(temp.path(), &people, &[], &codes)
            .expect("write aaak_entities.md must succeed");
        let contents = std::fs::read_to_string(temp.path().join("aaak_entities.md"))
            .expect("read aaak_entities.md");
        assert!(
            !contents.contains("## Projects"),
            "empty projects must not render a section header"
        );
    }

    // ── onboarding_write_critical_facts_file ───────────────────────────────

    #[test]
    fn write_critical_facts_file_separates_personal_and_work() {
        let temp = tempfile::tempdir().expect("tempdir");
        let people = vec![
            person_entry("Alice", "engineer", "work", ""),
            person_entry("Bob", "sister", "personal", ""),
        ];
        let projects = vec!["Mempalace".to_string()];
        let wings = vec!["family".to_string(), "work".to_string()];
        let codes = onboarding_generate_entity_codes(&people);
        onboarding_write_critical_facts_file(
            temp.path(),
            &people,
            &projects,
            &wings,
            "combo",
            &codes,
        )
        .expect("write critical_facts.md must succeed");
        let contents = std::fs::read_to_string(temp.path().join("critical_facts.md"))
            .expect("read critical_facts.md");
        assert!(contents.contains("## People (personal)"));
        assert!(contents.contains("## People (work)"));
        assert!(contents.contains("## Projects"));
        assert!(contents.contains("Wings: family, work"));
        assert!(contents.contains("Mode: combo"));
        assert!(
            contents.contains("**Bob** (BOB) — sister"),
            "personal section must include relationship"
        );
        assert!(
            contents.contains("**Alice** (ALI) — engineer"),
            "work section must include relationship"
        );
    }

    #[test]
    fn write_critical_facts_file_omits_empty_sections() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wings = vec!["projects".to_string()];
        onboarding_write_critical_facts_file(temp.path(), &[], &[], &wings, "work", &[])
            .expect("write critical_facts.md must succeed");
        let contents = std::fs::read_to_string(temp.path().join("critical_facts.md"))
            .expect("read critical_facts.md");
        assert!(!contents.contains("## People"));
        assert!(!contents.contains("## Projects"));
        assert!(contents.contains("Wings: projects"));
    }

    // ── onboarding_generate_aaak_bootstrap ────────────────────────────────

    #[test]
    fn generate_aaak_bootstrap_writes_both_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        temp_env::with_var("MEMPALACE_DIR", Some(temp.path()), || {
            let people = vec![person_entry("Alice", "engineer", "work", "")];
            let projects = vec!["Mempalace".to_string()];
            let wings = vec!["work".to_string()];
            onboarding_generate_aaak_bootstrap(&people, &projects, &wings, "work")
                .expect("generate bootstrap must succeed");
            assert!(temp.path().join("aaak_entities.md").exists());
            assert!(temp.path().join("critical_facts.md").exists());
        });
    }

    // ── onboarding_scan_directory ─────────────────────────────────────────

    #[test]
    fn scan_directory_returns_empty_when_directory_has_no_prose() {
        let temp = tempfile::tempdir().expect("tempdir");
        // Directory has no .md/.txt/code files, so scan_for_detection returns
        // an empty list and the early-return in scan_directory fires.
        let detected = onboarding_scan_directory(temp.path(), &[], &["en"]);
        assert!(detected.is_empty(), "empty directory yields no candidates");
    }

    #[test]
    fn scan_directory_filters_known_people() {
        let temp = tempfile::tempdir().expect("tempdir");
        // Seed a markdown file with multiple mentions of two names — Alice is
        // already known so must be filtered out, while Beatrice should pass
        // through if confidence meets the minimum threshold.
        let mut prose = String::new();
        for _ in 0..6 {
            prose.push_str(
                "Alice met Beatrice at the cafe. Beatrice is the new lead. \
                Alice asked Beatrice about plans. ",
            );
        }
        std::fs::write(temp.path().join("notes.md"), prose).expect("write notes");
        let known = vec![person_entry("Alice", "", "", "")];
        let detected = onboarding_scan_directory(temp.path(), &known, &["en"]);
        // Postcondition: Alice (already known) must not appear in results.
        assert!(
            detected
                .iter()
                .all(|entity| entity.name.to_lowercase() != "alice"),
            "known names must be filtered out"
        );
    }

    // ── onboarding_readline ────────────────────────────────────────────────

    #[test]
    fn onboarding_readline_returns_trimmed_input() {
        // Standard input is trimmed and returned as-is.
        let mut reader = std::io::Cursor::new(b"  hello  \n");
        let result = onboarding_readline("test", None, &mut reader).expect("must succeed");
        assert_eq!(result, "hello", "trimmed input must be returned");
        // Pair assertion: no surrounding whitespace must remain.
        assert!(!result.starts_with(' ') && !result.ends_with(' '));
    }

    #[test]
    fn onboarding_readline_empty_input_returns_default() {
        // An empty line when a default is set must return the default string.
        let mut reader = std::io::Cursor::new(b"\n");
        let result =
            onboarding_readline("test", Some("fallback"), &mut reader).expect("must succeed");
        assert_eq!(result, "fallback", "empty input must yield the default");
    }

    #[test]
    fn onboarding_readline_empty_input_no_default_returns_empty() {
        // An empty line with no default must return an empty string.
        let mut reader = std::io::Cursor::new(b"\n");
        let result = onboarding_readline("test", None, &mut reader).expect("must succeed");
        assert!(
            result.is_empty(),
            "empty input with no default must return empty string"
        );
    }

    // ── onboarding_readline_yn ─────────────────────────────────────────────

    #[test]
    fn onboarding_readline_yn_y_returns_true() {
        let mut reader = std::io::Cursor::new(b"y\n");
        let answer = onboarding_readline_yn("continue?", false, &mut reader).expect("must succeed");
        assert!(answer, "'y' must return true");
    }

    #[test]
    fn onboarding_readline_yn_n_returns_false() {
        let mut reader = std::io::Cursor::new(b"n\n");
        let answer = onboarding_readline_yn("continue?", true, &mut reader).expect("must succeed");
        assert!(!answer, "'n' must return false");
    }

    #[test]
    fn onboarding_readline_yn_empty_uses_default() {
        // Empty input must fall through to the default_yes value.
        let mut reader_yes = std::io::Cursor::new(b"\n");
        assert!(
            onboarding_readline_yn("continue?", true, &mut reader_yes).expect("must succeed"),
            "empty with default_yes=true must return true"
        );
        let mut reader_no = std::io::Cursor::new(b"\n");
        assert!(
            !onboarding_readline_yn("continue?", false, &mut reader_no).expect("must succeed"),
            "empty with default_yes=false must return false"
        );
    }

    // ── onboarding_ask_mode ───────────────────────────────────────────────

    #[test]
    fn ask_mode_choice_1_returns_work() {
        let mut reader = std::io::Cursor::new(b"1\n");
        let mode = onboarding_ask_mode(&mut reader).expect("must succeed");
        assert_eq!(mode, "work", "choice 1 must return work mode");
    }

    #[test]
    fn ask_mode_choice_2_returns_personal() {
        let mut reader = std::io::Cursor::new(b"2\n");
        let mode = onboarding_ask_mode(&mut reader).expect("must succeed");
        assert_eq!(mode, "personal", "choice 2 must return personal mode");
    }

    #[test]
    fn ask_mode_choice_3_returns_combo() {
        let mut reader = std::io::Cursor::new(b"3\n");
        let mode = onboarding_ask_mode(&mut reader).expect("must succeed");
        assert_eq!(mode, "combo", "choice 3 must return combo mode");
    }

    #[test]
    fn ask_mode_empty_returns_combo() {
        // An empty line defaults to combo (the "3 | ''" arm in the match).
        let mut reader = std::io::Cursor::new(b"\n");
        let mode = onboarding_ask_mode(&mut reader).expect("must succeed");
        assert_eq!(mode, "combo", "empty input must default to combo");
    }

    #[test]
    fn ask_mode_invalid_then_valid_loops() {
        // An invalid choice must prompt again; the second valid entry is used.
        let mut reader = std::io::Cursor::new(b"9\n1\n");
        let mode = onboarding_ask_mode(&mut reader).expect("must succeed");
        assert_eq!(
            mode, "work",
            "loop must continue until a valid choice is entered"
        );
    }

    // ── onboarding_ask_people_one ─────────────────────────────────────────

    #[test]
    fn ask_people_one_empty_name_returns_empty_entry() {
        // An immediately empty line signals end-of-input.
        let mut reader = std::io::Cursor::new(b"\n");
        let entry = onboarding_ask_people_one("work", 1, &mut reader).expect("must succeed");
        assert!(
            entry.name.is_empty(),
            "empty input must produce an empty-name entry"
        );
    }

    #[test]
    fn ask_people_one_work_mode_fills_entry() {
        // In work mode no context prompt is shown.
        // Input: name, empty nickname, relationship.
        let mut reader = std::io::Cursor::new(b"Alice\n\nengineer\n");
        let entry = onboarding_ask_people_one("work", 1, &mut reader).expect("must succeed");
        assert_eq!(entry.name, "Alice");
        assert_eq!(entry.context, "work", "work mode must set context to work");
        assert_eq!(entry.relationship, "engineer");
        // Pair assertion: empty nickname line must produce empty nickname.
        assert!(
            entry.nickname.is_empty(),
            "empty nick line must produce empty nickname"
        );
    }

    #[test]
    fn ask_people_one_personal_mode_sets_context() {
        // Personal mode sets context automatically — no prompt.
        let mut reader = std::io::Cursor::new(b"Bob\n\nfriend\n");
        let entry = onboarding_ask_people_one("personal", 1, &mut reader).expect("must succeed");
        assert_eq!(
            entry.context, "personal",
            "personal mode must set context automatically"
        );
        assert_eq!(entry.name, "Bob");
    }

    #[test]
    fn ask_people_one_combo_mode_work_context() {
        // In combo mode 'w' selects work context.
        // Input: name, empty nick, empty rel, context='w'.
        let mut reader = std::io::Cursor::new(b"Carol\n\n\nw\n");
        let entry = onboarding_ask_people_one("combo", 1, &mut reader).expect("must succeed");
        assert_eq!(
            entry.context, "work",
            "w input must produce work context in combo mode"
        );
    }

    #[test]
    fn ask_people_one_combo_mode_personal_context() {
        // In combo mode 'p' (or default) selects personal context.
        let mut reader = std::io::Cursor::new(b"Dave\n\n\np\n");
        let entry = onboarding_ask_people_one("combo", 1, &mut reader).expect("must succeed");
        assert_eq!(
            entry.context, "personal",
            "p input must produce personal context in combo mode"
        );
    }

    // ── onboarding_ask_people ─────────────────────────────────────────────

    #[test]
    fn ask_people_stops_on_empty_name() {
        // After Alice is added, an empty line stops the loop.
        // Input: name, empty nick, empty rel → entry added; then empty name → stop.
        let mut reader = std::io::Cursor::new(b"Alice\n\n\n\n");
        let people = onboarding_ask_people("work", &mut reader).expect("must succeed");
        assert_eq!(people.len(), 1, "one person must be collected");
        assert_eq!(people[0].name, "Alice");
        // Pair assertion: the loop exit condition is the empty name, not a limit hit.
        assert!(people.len() < PEOPLE_LIMIT, "limit must not have been hit");
    }

    // ── onboarding_ask_projects ───────────────────────────────────────────

    #[test]
    fn ask_projects_stops_on_empty_input() {
        // Two projects then an empty line to stop.
        let mut reader = std::io::Cursor::new(b"Alpha\nBeta\n\n");
        let projects = onboarding_ask_projects("work", &mut reader).expect("must succeed");
        assert_eq!(projects.len(), 2, "two projects must be collected");
        assert_eq!(projects[0], "Alpha");
        assert_eq!(projects[1], "Beta");
    }

    #[test]
    fn ask_projects_personal_mode_empty_yields_none() {
        // Personal mode shows a different prompt but behaves identically.
        let mut reader = std::io::Cursor::new(b"\n");
        let projects = onboarding_ask_projects("personal", &mut reader).expect("must succeed");
        assert!(
            projects.is_empty(),
            "empty input in personal mode must yield no projects"
        );
    }

    // ── onboarding_ask_wings ──────────────────────────────────────────────

    #[test]
    fn ask_wings_empty_input_returns_work_defaults() {
        // An empty line must fall back to the mode-specific default wings.
        let mut reader = std::io::Cursor::new(b"\n");
        let wings = onboarding_ask_wings("work", &mut reader).expect("must succeed");
        let expected: Vec<String> = DEFAULT_WINGS_WORK
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(
            wings, expected,
            "empty input must return default work wings"
        );
        assert!(!wings.is_empty(), "default wings must be non-empty");
    }

    #[test]
    fn ask_wings_custom_input_overrides_defaults() {
        // A comma-separated list must replace the defaults entirely.
        let mut reader = std::io::Cursor::new(b"alpha,beta,gamma\n");
        let wings = onboarding_ask_wings("work", &mut reader).expect("must succeed");
        assert_eq!(
            wings,
            vec!["alpha", "beta", "gamma"],
            "custom wings must be returned verbatim"
        );
    }

    #[test]
    fn ask_wings_personal_mode_returns_personal_defaults() {
        let mut reader = std::io::Cursor::new(b"\n");
        let wings = onboarding_ask_wings("personal", &mut reader).expect("must succeed");
        let expected: Vec<String> = DEFAULT_WINGS_PERSONAL
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(
            wings, expected,
            "empty input in personal mode must return personal defaults"
        );
    }

    // ── onboarding_prompt_detected ─────────────────────────────────────────

    fn detected_entity(name: &str) -> DetectedEntity {
        DetectedEntity {
            name: name.to_string(),
            entity_type: "person".to_string(),
            confidence: 0.9,
            frequency: 3,
            signals: vec![],
        }
    }

    #[test]
    fn prompt_detected_skip_all_when_user_declines() {
        // 'n' to "Add any?" must return people list unchanged.
        let detected = vec![detected_entity("Carol")];
        let people = vec![person_entry("Alice", "", "work", "")];
        let mut reader = std::io::Cursor::new(b"n\n");
        let result = onboarding_prompt_detected(people, detected, "work", &mut reader)
            .expect("must succeed");
        assert_eq!(
            result.len(),
            1,
            "declining must leave people list unchanged"
        );
        assert_eq!(result[0].name, "Alice");
        // Pair assertion: Carol must not be in the result.
        assert!(
            result.iter().all(|p| p.name != "Carol"),
            "Carol must not be added"
        );
    }

    #[test]
    fn prompt_detected_accept_adds_person_and_skips_other() {
        // 'y' to add any, 'p' to accept Carol, 's' to skip Dave.
        let detected = vec![detected_entity("Carol"), detected_entity("Dave")];
        let people = vec![person_entry("Alice", "", "work", "")];
        // y=add any; p=accept Carol; engineer=rel; s=skip Dave.
        let mut reader = std::io::Cursor::new(b"y\np\nengineer\ns\n");
        let result = onboarding_prompt_detected(people, detected, "work", &mut reader)
            .expect("must succeed");
        assert_eq!(
            result.len(),
            2,
            "accepting one entity must add it to people"
        );
        assert_eq!(result[1].name, "Carol", "accepted entity must be Carol");
        assert_eq!(result[1].relationship, "engineer");
        // Pair assertion: skipped entity must not appear.
        assert!(
            result.iter().all(|p| p.name != "Dave"),
            "skipped entity must not be added"
        );
    }

    // ── onboarding_maybe_scan ─────────────────────────────────────────────

    #[test]
    fn maybe_scan_declines_returns_people_unchanged() {
        // When the user answers 'n' to the scan prompt, the list is returned as-is.
        let temp = tempfile::tempdir().expect("tempdir");
        let people = vec![person_entry("Alice", "", "work", "")];
        let mut reader = std::io::Cursor::new(b"n\n");
        let result =
            onboarding_maybe_scan(temp.path(), people, "work", &mut reader).expect("must succeed");
        assert_eq!(result.len(), 1, "declining scan must preserve people list");
        assert_eq!(result[0].name, "Alice");
        // Pair assertion: no extra entries were added.
        assert!(
            result.len() <= 1,
            "no entities must have been added after declining"
        );
    }
}
