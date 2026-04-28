//! `mempalace onboard` — first-run interactive setup wizard.
//!
//! Asks the user about their world (people, projects, wings) and seeds the
//! entity registry and AAAK bootstrap markdown files. Mirrors Python's
//! `onboarding.py` module.

use std::collections::HashMap;
use std::io::Write as _;
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

    let mode = onboarding_ask_mode()?;
    let people = onboarding_ask_people(&mode)?;
    let projects = onboarding_ask_projects(&mode)?;
    let wings = onboarding_ask_wings(&mode)?;

    let all_people = onboarding_maybe_scan(directory, people, &mode)?;

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
fn onboarding_ask_mode() -> Result<String> {
    println!("  How are you using MemPalace?");
    println!("    1. Work — clients, projects, team");
    println!("    2. Personal — family, health, relationships");
    println!("    3. Combo — work and personal together");
    println!();

    loop {
        let input = onboarding_readline("Your choice [1/2/3]", None)?;
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
fn onboarding_ask_people(mode: &str) -> Result<Vec<PersonEntry>> {
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
        let entry = onboarding_ask_people_one(mode, people.len() + 1)?;
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
fn onboarding_ask_people_one(mode: &str, index: usize) -> Result<PersonEntry> {
    assert!(!mode.is_empty());
    assert!(index > 0);

    let name = onboarding_readline(&format!("  Person {index}"), None)?;
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
    )?;
    let relationship = onboarding_readline("  Relationship/role", Some(""))?;
    let context = match mode {
        "personal" => "personal".to_string(),
        "work" => "work".to_string(),
        _ => {
            let raw = onboarding_readline("  Context (p=personal / w=work)", Some("p"))?;
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
fn onboarding_ask_projects(mode: &str) -> Result<Vec<String>> {
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
        let name = onboarding_readline(&format!("  Project {}", projects.len() + 1), None)?;
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
fn onboarding_ask_wings(mode: &str) -> Result<Vec<String>> {
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
) -> Result<Vec<PersonEntry>> {
    assert!(!mode.is_empty());
    if !onboarding_readline_yn(
        "Scan your files for additional names we might have missed?",
        true,
    )? {
        return Ok(people);
    }
    let config = crate::config::MempalaceConfig::load().unwrap_or_default();
    let language_refs: Vec<&str> = config.entity_languages.iter().map(String::as_str).collect();
    let detected = onboarding_scan_directory(directory, &people, &language_refs);
    if detected.is_empty() {
        println!("  No additional candidates found.");
        return Ok(people);
    }
    onboarding_prompt_detected(people, detected, mode)
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
    if !onboarding_readline_yn("  Add any of these to your registry?", false)? {
        return Ok(people);
    }

    for entity in detected {
        if people.len() >= PEOPLE_LIMIT {
            break;
        }
        let choice = onboarding_readline(
            &format!("    {} — (p)erson, (s)kip?", entity.name),
            Some("s"),
        )?;
        if !choice.starts_with('p') {
            continue;
        }
        let relationship = onboarding_readline("    Relationship/role", Some(""))?;
        let context = match mode {
            "personal" => "personal".to_string(),
            "work" => "work".to_string(),
            _ => {
                let raw = onboarding_readline("    Context (p=personal / w=work)", Some("p"))?;
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
    add_to_known_entities(&by_category)
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

/// Print a prompt to stdout and read one trimmed line from stdin.
///
/// When `default` is `Some("")` the prompt shows nothing extra; when
/// `default` is `Some(val)` it shows `[val]` to guide the user.
fn onboarding_readline(prompt: &str, default: Option<&str>) -> Result<String> {
    assert!(!prompt.is_empty());
    let suffix = match default {
        Some(default_val) if !default_val.is_empty() => format!(" [{default_val}]"),
        _ => String::new(),
    };
    print!("  {prompt}{suffix}: ");
    std::io::stdout().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
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
fn onboarding_readline_yn(prompt: &str, default_yes: bool) -> Result<bool> {
    assert!(!prompt.is_empty());
    let hint = if default_yes { "Y/n" } else { "y/N" };
    print!("  {prompt} [{hint}]: ");
    std::io::stdout().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
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
}
