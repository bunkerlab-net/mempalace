//! Interactive entity confirmation for `mempalace init`.
//!
//! When `yes=false`, the user is prompted to accept or reject each detected
//! entity above the confidence threshold. When `yes=true` (non-interactive
//! or CI mode), all entities above the threshold are accepted automatically.
//!
//! Public API:
//! - [`ConfirmedEntities`] — the accepted people and project names
//! - [`confirm_entities`] — interactive or auto-accept confirmation gate

use std::io::Write as _;

use crate::palace::project_scanner::DetectedDict;

/// Minimum confidence required to include an entity in auto-accept output.
/// Entities below this threshold are skipped when `yes=true`.
const CONFIDENCE_THRESHOLD: f64 = 0.5;

// ===================== PUBLIC API =====================

/// Accepted entity names from the init confirmation step.
#[derive(Debug, Default)]
pub struct ConfirmedEntities {
    /// Accepted person names.
    pub people: Vec<String>,
    /// Accepted project names.
    pub projects: Vec<String>,
}

/// Accept or interactively confirm detected entities.
///
/// - `yes=true`: all entities with `confidence >= CONFIDENCE_THRESHOLD` are
///   accepted without prompting.
/// - `yes=false`: the user is prompted for each entity on stdin; pressing
///   enter or typing `y`/`yes` accepts; `n`/`no` skips.
///
/// Returns a [`ConfirmedEntities`] with only the accepted names.
pub fn confirm_entities(detected: &DetectedDict, yes: bool) -> ConfirmedEntities {
    let people_count = detected.people.len();
    let projects_count = detected.projects.len();

    let mut confirmed = ConfirmedEntities::default();

    if yes {
        confirm_entities_auto(detected, &mut confirmed);
    } else {
        confirm_entities_interactive(detected, &mut confirmed);
    }

    // Postconditions: confirmed lists cannot exceed the input lists.
    debug_assert!(confirmed.people.len() <= people_count);
    debug_assert!(confirmed.projects.len() <= projects_count);

    confirmed
}

// ===================== PRIVATE HELPERS =====================

/// Accept all entities above `CONFIDENCE_THRESHOLD` without prompting.
///
/// Called by [`confirm_entities`] when `yes=true`.
fn confirm_entities_auto(detected: &DetectedDict, confirmed: &mut ConfirmedEntities) {
    // Preconditions: confirmed lists must start empty (fresh ConfirmedEntities).
    debug_assert!(confirmed.people.is_empty());
    debug_assert!(confirmed.projects.is_empty());

    for entity in &detected.people {
        if entity.confidence >= CONFIDENCE_THRESHOLD && !entity.name.is_empty() {
            confirmed.people.push(entity.name.clone());
        }
    }
    for entity in &detected.projects {
        if entity.confidence >= CONFIDENCE_THRESHOLD && !entity.name.is_empty() {
            confirmed.projects.push(entity.name.clone());
        }
    }
}

/// Prompt the user interactively for each detected entity above threshold.
///
/// Called by [`confirm_entities`] when `yes=false`. Entities below
/// `CONFIDENCE_THRESHOLD` are skipped without prompting.
fn confirm_entities_interactive(detected: &DetectedDict, confirmed: &mut ConfirmedEntities) {
    // Preconditions: confirmed lists must start empty (fresh ConfirmedEntities).
    debug_assert!(confirmed.people.is_empty());
    debug_assert!(confirmed.projects.is_empty());

    if detected.people.is_empty() && detected.projects.is_empty() {
        return;
    }

    println!("\n  Confirm detected entities (enter/y = accept, n = skip):\n");

    for entity in &detected.people {
        // Mirror the guard in confirm_entities_auto: skip low-confidence and
        // empty names. Empty names would panic the assert in prompt_name.
        if entity.confidence < CONFIDENCE_THRESHOLD || entity.name.is_empty() {
            continue;
        }
        if confirm_entities_prompt_name(&entity.name, "person") {
            confirmed.people.push(entity.name.clone());
        }
    }

    for entity in &detected.projects {
        if entity.confidence < CONFIDENCE_THRESHOLD || entity.name.is_empty() {
            continue;
        }
        if confirm_entities_prompt_name(&entity.name, "project") {
            confirmed.projects.push(entity.name.clone());
        }
    }
}

/// Print a single `[person|project] "<name>" Accept? [Y/n]` prompt and read the response.
///
/// Returns `true` if the user accepts (blank line, `y`, or `yes`) or `false` if
/// the user declines (`n` or `no`). Any other input is treated as acceptance.
/// Called by [`confirm_entities_interactive`].
fn confirm_entities_prompt_name(name: &str, entity_type: &str) -> bool {
    assert!(!name.is_empty());
    assert!(!entity_type.is_empty());

    print!("    {entity_type} \"{name}\" — Accept? [Y/n] ");
    // stdout must be flushed before reading stdin or the prompt may not appear.
    let _ = std::io::stdout().flush();

    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() {
        // Read failure (e.g. stdin closed in tests): treat as acceptance.
        return true;
    }
    let response = line.trim().to_lowercase();
    // Negative space: stdin input must not contain null bytes after trimming.
    debug_assert!(
        !response.contains('\0'),
        "response must not contain null bytes"
    );
    response != "n" && response != "no"
}

// ===================== TESTS =====================

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::palace::entities::DetectedEntity;
    use crate::palace::project_scanner::DetectedDict;

    fn make_entity(name: &str, entity_type: &str, confidence: f64) -> DetectedEntity {
        DetectedEntity {
            name: name.to_string(),
            entity_type: entity_type.to_string(),
            confidence,
            frequency: 0,
            signals: vec![],
        }
    }

    fn make_dict(people: Vec<DetectedEntity>, projects: Vec<DetectedEntity>) -> DetectedDict {
        DetectedDict {
            people,
            projects,
            uncertain: vec![],
        }
    }

    // -- confirm_entities (yes=true, auto-accept) --

    #[test]
    fn auto_accept_includes_high_confidence_entities() {
        // Entities above threshold must be included in auto-accept output.
        let detected = make_dict(
            vec![
                make_entity("Alice", "person", 0.9),
                make_entity("Bob", "person", 0.8),
            ],
            vec![make_entity("myproject", "project", 0.95)],
        );
        let confirmed = confirm_entities(&detected, true);
        assert!(confirmed.people.contains(&"Alice".to_string()));
        assert!(confirmed.people.contains(&"Bob".to_string()));
        assert!(confirmed.projects.contains(&"myproject".to_string()));
        assert_eq!(confirmed.people.len(), 2);
    }

    #[test]
    fn auto_accept_excludes_low_confidence_entities() {
        // Entities below the confidence threshold must be excluded from auto-accept.
        let detected = make_dict(
            vec![make_entity("Uncertain Person", "person", 0.1)],
            vec![make_entity("unsure-proj", "project", 0.3)],
        );
        let confirmed = confirm_entities(&detected, true);
        assert!(
            confirmed.people.is_empty(),
            "low-confidence person must be excluded"
        );
        assert!(
            confirmed.projects.is_empty(),
            "low-confidence project must be excluded"
        );
    }

    #[test]
    fn auto_accept_empty_detected_returns_empty() {
        // Empty DetectedDict must yield empty ConfirmedEntities with yes=true.
        let detected = make_dict(vec![], vec![]);
        let confirmed = confirm_entities(&detected, true);
        assert!(confirmed.people.is_empty());
        assert!(confirmed.projects.is_empty());
    }

    #[test]
    fn auto_accept_count_bounded_by_input() {
        // Confirmed count must not exceed input count (postcondition check).
        let detected = make_dict(
            vec![
                make_entity("Alice", "person", 0.9),
                make_entity("Bob", "person", 0.2), // below threshold
                make_entity("Carol", "person", 0.8),
            ],
            vec![],
        );
        let confirmed = confirm_entities(&detected, true);
        assert!(
            confirmed.people.len() <= 3,
            "confirmed cannot exceed input count"
        );
        assert_eq!(
            confirmed.people.len(),
            2,
            "only Alice and Carol above threshold"
        );
    }

    // -- confirm_entities (yes=false, interactive) --

    #[test]
    fn interactive_mode_empty_detected_returns_empty_without_prompting() {
        // Empty people and projects must trigger the early-return path — no stdin read.
        let detected = make_dict(vec![], vec![]);
        let confirmed = confirm_entities(&detected, false);
        assert!(
            confirmed.people.is_empty(),
            "empty detected must yield empty people"
        );
        assert!(
            confirmed.projects.is_empty(),
            "empty detected must yield empty projects"
        );
    }

    #[test]
    fn interactive_mode_accepts_entities_when_stdin_at_eof() {
        // In non-TTY environments (CI), stdin is at EOF: read_line returns Ok(0), response
        // is an empty string, and the prompt function treats empty input as acceptance.
        // In interactive terminals this test returns early to avoid blocking.
        use std::io::IsTerminal as _;
        if std::io::stdin().is_terminal() {
            // Cannot run non-interactively in a live terminal — skip to avoid hanging.
            return;
        }
        let detected = make_dict(
            vec![
                make_entity("Alice", "person", 0.9),
                // Below threshold — must be skipped without prompting.
                make_entity("LowConf", "person", 0.1),
            ],
            vec![make_entity("mylib", "project", 0.85)],
        );
        let confirmed = confirm_entities(&detected, false);
        // EOF stdin → empty response → all above-threshold entities accepted.
        assert!(
            confirmed.people.contains(&"Alice".to_string()),
            "above-threshold person must be accepted on EOF stdin"
        );
        assert!(
            !confirmed.people.contains(&"LowConf".to_string()),
            "below-threshold person must be skipped"
        );
        assert!(
            confirmed.projects.contains(&"mylib".to_string()),
            "above-threshold project must be accepted on EOF stdin"
        );
    }

    #[test]
    fn confirm_entities_prompt_name_accepts_on_eof_stdin() {
        // Direct test of the prompt helper: EOF stdin → empty response → true (accept).
        use std::io::IsTerminal as _;
        if std::io::stdin().is_terminal() {
            return;
        }
        // EOF stdin (non-TTY): read_line returns Ok(0), line stays empty, response = "".
        let accepted = confirm_entities_prompt_name("Alice", "person");
        assert!(
            accepted,
            "empty response from EOF stdin must mean acceptance"
        );
    }
}
