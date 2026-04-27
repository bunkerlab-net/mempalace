//! Fact checker — detect entity confusion and KG contradictions in incoming text.
//!
//! Three classes of issue are detected:
//!
//! * `similar_name` — a registry name is mentioned while another name ≤ 2 edits
//!   away is registered but not mentioned (possible typo or mix-up).
//! * `relationship_mismatch` — text asserts "X is Y's Z" but the KG records a
//!   *different* current predicate for the same (X, Y) pair.
//! * `stale_fact` — text asserts a fact that the KG has closed (`valid_to` in
//!   the past).
//!
//! Purely offline. No network calls. Inputs: known-entities registry + KG DB.

use std::collections::HashSet;
use std::sync::LazyLock;

use regex::Regex;
use serde::Serialize;
use turso::Connection;

use crate::error::Result;
use crate::kg::query;
use crate::palace::known_entities;

// Compile-time literal — cannot fail at runtime.
#[allow(clippy::expect_used)]
// Parses "Bob is Alice's brother": subject=Bob, possessor=Alice, role=brother.
static CLAIM_PATTERN_SUBJECT_FIRST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b([A-Z][\w-]+)\s+is\s+([A-Z][\w-]+)'s\s+([a-z]{3,20})\b")
        .expect("CLAIM_PATTERN_SUBJECT_FIRST is a compile-time literal")
});

// Compile-time literal — cannot fail at runtime.
#[allow(clippy::expect_used)]
// Parses "Alice's brother is Bob": possessor=Alice, role=brother, subject=Bob.
static CLAIM_PATTERN_POSSESSOR_FIRST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b([A-Z][\w-]+)'s\s+([a-z]{3,20})\s+is\s+([A-Z][\w-]+)\b")
        .expect("CLAIM_PATTERN_POSSESSOR_FIRST is a compile-time literal")
});

/// Edit distance ≤ this threshold triggers a `similar_name` issue.
const EDIT_DISTANCE_THRESHOLD: usize = 2;

const _: () = assert!(EDIT_DISTANCE_THRESHOLD > 0);

/// An issue detected by the fact checker.
#[derive(Debug, Serialize)]
pub struct FactIssue {
    /// Issue category: `"similar_name"`, `"relationship_mismatch"`, or `"stale_fact"`.
    pub issue_type: String,
    /// Human-readable description.
    pub detail: String,
    /// The two names involved (`similar_name` only).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    /// Edit distance between the names (`similar_name` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<usize>,
    /// Subject entity (`relationship_mismatch`, `stale_fact`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    /// Date the KG fact was closed (`stale_fact` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<String>,
}

/// A structured relationship claim parsed from text.
struct Claim {
    subject: String,
    predicate: String,
    object: String,
    /// The original matched surface form for error messages.
    span: String,
}

/// Check `text` for entity confusion and KG contradictions.
///
/// Returns an empty vec when no issues are detected. The checker is deliberately
/// conservative: every issue is anchored to a specific KG fact or registry entry.
pub async fn check_text(text: &str, connection: &Connection) -> Result<Vec<FactIssue>> {
    assert!(!text.is_empty(), "check_text: text must not be empty");

    let mut issues = Vec::new();

    let registry_path = known_entities::registry_path();
    if let Ok(raw_json) = std::fs::read_to_string(&registry_path)
        && !raw_json.is_empty()
    {
        let entity_names = check_text_flatten_names(&raw_json);
        issues.extend(check_text_entity_confusion(text, &entity_names));
    }

    issues.extend(check_text_kg_contradictions(text, connection).await?);

    debug_assert!(issues.iter().all(|i| !i.issue_type.is_empty()));
    debug_assert!(issues.iter().all(|i| !i.detail.is_empty()));

    Ok(issues)
}

/// Called by `check_text` to flatten the registry JSON into a set of entity names.
///
/// Supports list-format (`["Alice", "Bob"]`) and dict-format (`{"Alice": null}`)
/// registry shapes used by both the Rust and Python implementations.
fn check_text_flatten_names(raw_json: &str) -> HashSet<String> {
    assert!(
        !raw_json.is_empty(),
        "check_text_flatten_names: raw_json must not be empty"
    );

    let mut names: HashSet<String> = HashSet::new();
    let Ok(root) = serde_json::from_str::<serde_json::Value>(raw_json) else {
        return names;
    };
    let Some(obj) = root.as_object() else {
        return names;
    };
    for value in obj.values() {
        match value {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    if let Some(name) = item.as_str().filter(|str| !str.is_empty()) {
                        names.insert(name.to_string());
                    }
                }
            }
            serde_json::Value::Object(dict) => {
                for key in dict.keys().filter(|k| !k.is_empty()) {
                    names.insert(key.clone());
                }
            }
            _ => {}
        }
    }
    names
}

/// Called by `check_text` to detect mentioned names close in edit distance to other registry names.
///
/// Only O(m × n) comparisons are made, where m is the number of registry names
/// actually mentioned in the text — not the full O(n²) pairwise scan.
fn check_text_entity_confusion(text: &str, all_names: &HashSet<String>) -> Vec<FactIssue> {
    assert!(
        !text.is_empty(),
        "check_text_entity_confusion: text must not be empty"
    );

    if all_names.is_empty() {
        return vec![];
    }

    let mentioned: Vec<&String> = all_names
        .iter()
        .filter(|name| {
            let escaped = regex::escape(name);
            Regex::new(&format!("(?i)\\b{escaped}\\b")).is_ok_and(|re| re.is_match(text))
        })
        .collect();

    if mentioned.is_empty() {
        return vec![];
    }

    let mut issues = Vec::new();
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();

    for name_a in &mentioned {
        let a_lower = name_a.to_lowercase();
        for name_b in all_names {
            if *name_a == name_b || mentioned.iter().any(|m| m == &name_b) {
                continue;
            }
            let b_lower = name_b.to_lowercase();
            let pair = check_text_entity_confusion_pair(&a_lower, &b_lower);
            if seen_pairs.contains(&pair) {
                continue;
            }
            let distance = edit_distance(&a_lower, &b_lower);
            if distance > 0 && distance <= EDIT_DISTANCE_THRESHOLD {
                issues.push(FactIssue {
                    issue_type: "similar_name".to_string(),
                    detail: format!(
                        "'{name_a}' mentioned — did you mean '{name_b}'? (edit distance {distance})"
                    ),
                    names: vec![(*name_a).clone(), name_b.clone()],
                    distance: Some(distance),
                    entity: None,
                    valid_to: None,
                });
                seen_pairs.insert(pair);
            } else {
                seen_pairs.insert(pair);
            }
        }
    }

    issues
}

/// Called by `check_text_entity_confusion` to build a canonical dedup key for a name pair.
///
/// Normalises order so (a, b) and (b, a) produce the same key, preventing double-reports.
fn check_text_entity_confusion_pair(a_lower: &str, b_lower: &str) -> (String, String) {
    assert!(
        !a_lower.is_empty(),
        "check_text_entity_confusion_pair: a_lower must not be empty"
    );
    assert!(
        !b_lower.is_empty(),
        "check_text_entity_confusion_pair: b_lower must not be empty"
    );

    if a_lower <= b_lower {
        (a_lower.to_string(), b_lower.to_string())
    } else {
        (b_lower.to_string(), a_lower.to_string())
    }
}

/// Called by `check_text` to parse relationship claims from `text` and compare them to the KG.
///
/// For each "(subject, predicate, object)" claim, queries outgoing KG facts for
/// the subject and dispatches to `check_text_check_claim` for the per-fact checks.
async fn check_text_kg_contradictions(
    text: &str,
    connection: &Connection,
) -> Result<Vec<FactIssue>> {
    assert!(
        !text.is_empty(),
        "check_text_kg_contradictions: text must not be empty"
    );

    let claims = check_text_extract_claims(text);
    if claims.is_empty() {
        return Ok(vec![]);
    }

    let mut issues = Vec::new();
    for claim in &claims {
        let facts = query::query_entity(connection, &claim.subject, None, "outgoing")
            .await
            .unwrap_or_default();
        if facts.is_empty() {
            continue;
        }
        issues.extend(check_text_check_claim(claim, &facts));
    }

    Ok(issues)
}

/// Called by `check_text_kg_contradictions` to evaluate one claim against retrieved KG facts.
///
/// Fires `relationship_mismatch` when the KG records a different predicate for the
/// same (subject, object) pair, and `stale_fact` when the exact triple is closed.
fn check_text_check_claim(claim: &Claim, facts: &[query::Fact]) -> Vec<FactIssue> {
    assert!(
        !claim.subject.is_empty(),
        "check_text_check_claim: subject must not be empty"
    );
    assert!(
        !facts.is_empty(),
        "check_text_check_claim: facts must not be empty"
    );

    let today = chrono::Utc::now().date_naive().to_string();
    let mut issues = Vec::new();

    for fact in facts {
        let objects_match = fact.object.trim().to_lowercase() == claim.object.trim().to_lowercase();
        if !objects_match {
            continue;
        }
        let kg_pred = fact.predicate.to_lowercase();

        if fact.current && !kg_pred.is_empty() && kg_pred != claim.predicate {
            issues.push(FactIssue {
                issue_type: "relationship_mismatch".to_string(),
                detail: format!(
                    "Text says '{}' but KG records {} {} {}",
                    claim.span, claim.subject, kg_pred, fact.object
                ),
                names: vec![],
                distance: None,
                entity: Some(claim.subject.clone()),
                valid_to: None,
            });
            continue;
        }

        if !fact.current
            && kg_pred == claim.predicate
            && let Some(valid_to) = &fact.valid_to
            && valid_to.as_str() < today.as_str()
        {
            issues.push(FactIssue {
                issue_type: "stale_fact".to_string(),
                detail: format!(
                    "Text says '{}' but KG marks this fact closed on {valid_to}",
                    claim.span
                ),
                names: vec![],
                distance: None,
                entity: Some(claim.subject.clone()),
                valid_to: Some(valid_to.clone()),
            });
        }
    }

    debug_assert!(issues.iter().all(|i| i.entity.is_some()));

    issues
}

/// Called by `check_text_kg_contradictions` to extract structured claims from text.
///
/// Supports two surface forms — `"X is Y's Z"` and `"X's Z is Y"` — both of which
/// resolve to the triple `(subject=X, predicate=Z, object=Y)`.
fn check_text_extract_claims(text: &str) -> Vec<Claim> {
    assert!(
        !text.is_empty(),
        "check_text_extract_claims: text must not be empty"
    );

    let mut claims = Vec::new();

    for mat in CLAIM_PATTERN_SUBJECT_FIRST.captures_iter(text) {
        claims.push(Claim {
            subject: mat[1].to_string(),
            predicate: mat[3].to_lowercase(),
            object: mat[2].to_string(),
            span: mat[0].to_string(),
        });
    }

    for mat in CLAIM_PATTERN_POSSESSOR_FIRST.captures_iter(text) {
        claims.push(Claim {
            subject: mat[3].to_string(),
            predicate: mat[2].to_lowercase(),
            object: mat[1].to_string(),
            span: mat[0].to_string(),
        });
    }

    claims
}

/// Levenshtein edit distance between `s1` and `s2`.
///
/// Time O(m × n), space O(min(m, n)). Operates on Unicode scalar values
/// so multi-byte characters count as one unit.
fn edit_distance(s1: &str, s2: &str) -> usize {
    let (s1, s2) = if s1.len() < s2.len() {
        (s2, s1)
    } else {
        (s1, s2)
    };
    let s2_chars: Vec<char> = s2.chars().collect();
    let s2_len = s2_chars.len();

    if s2_len == 0 {
        return s1.chars().count();
    }

    let mut prev: Vec<usize> = (0..=s2_len).collect();
    for (index_1, char_1) in s1.chars().enumerate() {
        let mut curr = vec![0usize; s2_len + 1];
        curr[0] = index_1 + 1;
        for (index_2, &char_2) in s2_chars.iter().enumerate() {
            let substitution_cost = usize::from(char_1 != char_2);
            curr[index_2 + 1] = (prev[index_2 + 1] + 1)
                .min(curr[index_2] + 1)
                .min(prev[index_2] + substitution_cost);
        }
        prev = curr;
    }

    let distance = prev[s2_len];
    debug_assert!(distance <= s1.chars().count());
    distance
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // ── edit_distance ────────────────────────────────────────────────

    #[test]
    fn edit_distance_identical_strings_returns_zero() {
        assert_eq!(edit_distance("alice", "alice"), 0);
    }

    #[test]
    fn edit_distance_one_substitution() {
        assert_eq!(edit_distance("alice", "alyce"), 1, "one vowel swap");
    }

    #[test]
    fn edit_distance_two_edits() {
        assert_eq!(edit_distance("alice", "alicx"), 1);
        assert_eq!(edit_distance("bob", "bab"), 1);
        assert_eq!(edit_distance("robert", "roberta"), 1, "one insertion");
    }

    #[test]
    fn edit_distance_empty_strings() {
        assert_eq!(edit_distance("", ""), 0);
        assert_eq!(edit_distance("abc", ""), 3);
        assert_eq!(edit_distance("", "abc"), 3);
    }

    // ── check_text_flatten_names ─────────────────────────────────────

    #[test]
    fn flatten_names_list_format() {
        let json = r#"{"people": ["Alice", "Bob"]}"#;
        let names = check_text_flatten_names(json);
        assert!(names.contains("Alice"), "Alice must be in names");
        assert!(names.contains("Bob"), "Bob must be in names");
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn flatten_names_dict_format() {
        let json = r#"{"people": {"Alice": null, "Bob": "person"}}"#;
        let names = check_text_flatten_names(json);
        assert!(
            names.contains("Alice"),
            "Alice must be in dict-format names"
        );
        assert!(names.contains("Bob"), "Bob must be in dict-format names");
    }

    #[test]
    fn flatten_names_invalid_json_returns_empty() {
        let names = check_text_flatten_names("not json!");
        assert!(names.is_empty(), "invalid JSON must return empty set");
    }

    // ── check_text_extract_claims ────────────────────────────────────

    #[test]
    fn extract_claims_subject_first_form() {
        let text = "Bob is Alice's brother.";
        let claims = check_text_extract_claims(text);
        assert_eq!(claims.len(), 1, "must extract exactly one claim");
        assert_eq!(claims[0].subject, "Bob");
        assert_eq!(claims[0].predicate, "brother");
        assert_eq!(claims[0].object, "Alice");
    }

    #[test]
    fn extract_claims_possessor_first_form() {
        let text = "Alice's brother is Bob.";
        let claims = check_text_extract_claims(text);
        assert_eq!(claims.len(), 1, "must extract exactly one claim");
        assert_eq!(claims[0].subject, "Bob");
        assert_eq!(claims[0].predicate, "brother");
        assert_eq!(claims[0].object, "Alice");
    }

    #[test]
    fn extract_claims_no_match_returns_empty() {
        let text = "There are no relationship claims here.";
        let claims = check_text_extract_claims(text);
        assert!(claims.is_empty(), "plain text must yield no claims");
    }

    // ── check_text_entity_confusion ──────────────────────────────────

    #[test]
    fn entity_confusion_detects_one_edit_apart() {
        let mut all_names = HashSet::new();
        all_names.insert("Alice".to_string());
        all_names.insert("Alyce".to_string());
        // Text mentions "Alice" but not "Alyce" — should flag confusion.
        let issues = check_text_entity_confusion("I saw Alice yesterday.", &all_names);
        assert_eq!(issues.len(), 1, "one similar_name issue expected");
        assert_eq!(issues[0].issue_type, "similar_name");
        assert!(
            issues[0].detail.contains("Alice"),
            "detail must name the mentioned entity"
        );
    }

    #[test]
    fn entity_confusion_no_issue_when_both_mentioned() {
        let mut all_names = HashSet::new();
        all_names.insert("Alice".to_string());
        all_names.insert("Alyce".to_string());
        // Both present in text — deliberate use, not confusion.
        let issues = check_text_entity_confusion("Alice and Alyce attended.", &all_names);
        assert!(issues.is_empty(), "no issue when both names appear");
    }

    #[test]
    fn entity_confusion_empty_registry_returns_empty() {
        let issues = check_text_entity_confusion("Alice attended.", &HashSet::new());
        assert!(issues.is_empty(), "empty registry must return no issues");
    }

    // ── check_text_check_claim ───────────────────────────────────────

    #[test]
    fn check_claim_mismatch_fires_for_different_predicate() {
        let claim = Claim {
            subject: "Bob".to_string(),
            predicate: "brother".to_string(),
            object: "Alice".to_string(),
            span: "Bob is Alice's brother".to_string(),
        };
        let fact = query::Fact {
            direction: "outgoing".to_string(),
            subject: "Bob".to_string(),
            predicate: "husband".to_string(),
            object: "Alice".to_string(),
            valid_from: None,
            valid_to: None,
            confidence: 1.0,
            current: true,
        };
        let issues = check_text_check_claim(&claim, &[fact]);
        assert_eq!(issues.len(), 1, "one relationship_mismatch expected");
        assert_eq!(issues[0].issue_type, "relationship_mismatch");
    }

    #[test]
    fn check_claim_stale_fact_fires_for_past_valid_to() {
        // When a KG fact is closed (current=false) with a valid_to in the past and
        // the predicate matches the claim, a stale_fact issue must be raised.
        let claim = Claim {
            subject: "Bob".to_string(),
            predicate: "brother".to_string(),
            object: "Alice".to_string(),
            span: "Bob is Alice's brother".to_string(),
        };
        let fact = query::Fact {
            direction: "outgoing".to_string(),
            subject: "Bob".to_string(),
            predicate: "brother".to_string(),
            object: "Alice".to_string(),
            valid_from: None,
            valid_to: Some("2000-01-01".to_string()),
            confidence: 1.0,
            current: false,
        };
        let issues = check_text_check_claim(&claim, &[fact]);
        assert_eq!(issues.len(), 1, "one stale_fact issue expected");
        assert_eq!(issues[0].issue_type, "stale_fact");
        assert_eq!(
            issues[0].valid_to.as_deref(),
            Some("2000-01-01"),
            "valid_to must be set on stale_fact"
        );
        assert!(
            issues[0].entity.is_some(),
            "entity must be set for stale_fact issue"
        );
    }

    #[test]
    fn flatten_names_skips_non_string_array_items() {
        // Non-string values in arrays (numbers, null, bool) must be silently ignored.
        let json = r#"{"people": ["Alice", 42, null, "Bob", true]}"#;
        let names = check_text_flatten_names(json);
        assert!(names.contains("Alice"), "Alice must be collected");
        assert!(names.contains("Bob"), "Bob must be collected");
        assert_eq!(names.len(), 2, "only string items must be collected");
    }

    #[test]
    fn entity_confusion_pair_orders_alphabetically() {
        // When a_lower > b_lower the pair must be (b, a) so the dedup set is symmetric.
        let pair = check_text_entity_confusion_pair("zach", "alice");
        assert_eq!(pair.0, "alice", "smaller string must be first in pair");
        assert_eq!(pair.1, "zach", "larger string must be second in pair");

        // When a_lower <= b_lower the pair is (a, b) — same result.
        let pair2 = check_text_entity_confusion_pair("alice", "zach");
        assert_eq!(pair2.0, "alice");
        assert_eq!(pair2.1, "zach");
    }

    #[test]
    fn check_claim_no_issue_when_predicate_matches() {
        let claim = Claim {
            subject: "Bob".to_string(),
            predicate: "brother".to_string(),
            object: "Alice".to_string(),
            span: "Bob is Alice's brother".to_string(),
        };
        let fact = query::Fact {
            direction: "outgoing".to_string(),
            subject: "Bob".to_string(),
            predicate: "brother".to_string(),
            object: "Alice".to_string(),
            valid_from: None,
            valid_to: None,
            confidence: 1.0,
            current: true,
        };
        let issues = check_text_check_claim(&claim, &[fact]);
        assert!(issues.is_empty(), "no issue when claim matches KG fact");
    }
}
