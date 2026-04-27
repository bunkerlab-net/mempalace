//! Locale data for `MemPalace` — entity-detection patterns and UI strings.
//!
//! All 14 locale JSON files are embedded at compile time via `include_str!`.
//! The primary entry point is `get_entity_patterns`, which merges patterns
//! from one or more language codes into a single `EntityPatterns` struct.
//!
//! Language codes are matched case-insensitively against the filename stem
//! (e.g., `"PT-BR"` resolves to `pt-br.json`).

use std::collections::HashSet;

use serde::Deserialize;

// ── Raw locale JSON bytes ────────────────────────────────────────────────────

// The English fallback is referenced by name in `get_entity_patterns`.
static LOCALE_EN: &str = include_str!("en.json");

/// All available locales as `(canonical_stem_lowercase, raw_json)` pairs.
/// Stems are lower-cased so `get_entity_patterns_resolve_locale` can match
/// BCP 47 codes case-insensitively without allocating.
static ALL_LOCALES: &[(&str, &str)] = &[
    ("be", include_str!("be.json")),
    ("de", include_str!("de.json")),
    ("en", LOCALE_EN),
    ("es", include_str!("es.json")),
    ("fr", include_str!("fr.json")),
    ("hi", include_str!("hi.json")),
    ("id", include_str!("id.json")),
    ("it", include_str!("it.json")),
    ("ja", include_str!("ja.json")),
    ("ko", include_str!("ko.json")),
    ("pt-br", include_str!("pt-br.json")),
    ("ru", include_str!("ru.json")),
    ("zh-cn", include_str!("zh-CN.json")),
    ("zh-tw", include_str!("zh-TW.json")),
];

// ── Deserialisation types ───────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct LocaleEntitySection {
    #[serde(default)]
    candidate_pattern: String,
    #[serde(default)]
    multi_word_pattern: String,
    #[serde(default)]
    person_verb_patterns: Vec<String>,
    #[serde(default)]
    pronoun_patterns: Vec<String>,
    #[serde(default)]
    dialogue_patterns: Vec<String>,
    #[serde(default)]
    direct_address_pattern: String,
    #[serde(default)]
    project_verb_patterns: Vec<String>,
    #[serde(default)]
    stopwords: Vec<String>,
    // `boundary_chars` from the JSON (Hindi, CJK) is intentionally omitted:
    // the Rust `regex` crate's Unicode mode handles combining-mark boundaries
    // correctly without lookaround substitution. serde ignores unknown fields.
}

#[derive(Deserialize, Default)]
struct LocaleFile {
    #[serde(default)]
    entity: LocaleEntitySection,
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Merged entity-detection patterns for one or more languages.
pub struct EntityPatterns {
    /// Ready-to-compile regex strings for single-word candidates (capture group included).
    pub candidate_patterns: Vec<String>,
    /// Ready-to-compile regex strings for multi-word candidates (capture group included).
    pub multi_word_patterns: Vec<String>,
    /// `{name}` templates for person-verb matching.
    pub person_verb_patterns: Vec<String>,
    /// Regex patterns for pronoun proximity scoring.
    pub pronoun_patterns: Vec<String>,
    /// `{name}` templates for dialogue detection.
    pub dialogue_patterns: Vec<String>,
    /// `{name}` templates for direct-address detection.
    pub direct_address_patterns: Vec<String>,
    /// `{name}` templates for project-verb matching.
    pub project_verb_patterns: Vec<String>,
    /// Merged lowercase stopwords from all requested locales.
    pub stopwords: HashSet<String>,
}

/// Return merged entity detection patterns for the requested language codes.
///
/// Language codes are matched case-insensitively (e.g., `"PT-BR"` or `"pt-br"`
/// both resolve to `pt-br.json`). Unknown codes are silently skipped. If no
/// requested language has entity data, English is used as a fallback so callers
/// always receive a working config.
///
/// Merge rules:
/// - List fields are concatenated in declaration order with duplicates removed.
/// - `stopwords` is the set union across all languages (lowercased).
/// - `candidate_patterns` and `multi_word_patterns` are wrapped with `\b...\b`
///   and a capture group and returned ready to compile.
pub fn get_entity_patterns(languages: &[&str]) -> EntityPatterns {
    assert!(!languages.is_empty(), "languages must not be empty");

    let mut acc = EntityPatternsAcc::default();
    let mut found_any = false;

    for &lang in languages {
        let Some(raw_json) = get_entity_patterns_resolve_locale(lang) else {
            continue;
        };
        let Ok(locale) = serde_json::from_str::<LocaleFile>(raw_json) else {
            continue;
        };
        get_entity_patterns_merge_section(&locale.entity, &mut acc);
        found_any = true;
    }

    if !found_any {
        // Fallback: English so callers always get a working config.
        if let Ok(locale) = serde_json::from_str::<LocaleFile>(LOCALE_EN) {
            get_entity_patterns_merge_section(&locale.entity, &mut acc);
        }
    }

    let patterns = EntityPatterns {
        candidate_patterns: acc.candidate_patterns,
        multi_word_patterns: acc.multi_word_patterns,
        person_verb_patterns: dedupe_vec(acc.person_verbs),
        pronoun_patterns: dedupe_vec(acc.pronouns),
        dialogue_patterns: dedupe_vec(acc.dialogue),
        direct_address_patterns: acc.direct_address,
        project_verb_patterns: dedupe_vec(acc.project_verbs),
        stopwords: acc.stopwords,
    };

    assert!(
        !patterns.stopwords.is_empty(),
        "merged entity patterns must have at least one stopword"
    );
    assert!(
        !patterns.candidate_patterns.is_empty(),
        "merged entity patterns must have at least one candidate pattern"
    );
    patterns
}

// ── Private helpers ─────────────────────────────────────────────────────────

/// Accumulator used while merging multiple locale sections.
#[derive(Default)]
struct EntityPatternsAcc {
    candidate_patterns: Vec<String>,
    multi_word_patterns: Vec<String>,
    person_verbs: Vec<String>,
    pronouns: Vec<String>,
    dialogue: Vec<String>,
    direct_address: Vec<String>,
    project_verbs: Vec<String>,
    stopwords: HashSet<String>,
}

/// Resolve a language code (case-insensitive) to its raw JSON string, if found.
fn get_entity_patterns_resolve_locale(lang: &str) -> Option<&'static str> {
    let lower = lang.to_lowercase();
    ALL_LOCALES
        .iter()
        .find(|(stem, _)| *stem == lower)
        .map(|(_, raw)| *raw)
}

/// Merge one locale's entity section into the running accumulator.
fn get_entity_patterns_merge_section(section: &LocaleEntitySection, acc: &mut EntityPatternsAcc) {
    if !section.candidate_pattern.is_empty() {
        acc.candidate_patterns
            .push(format!(r"\b({})\b", section.candidate_pattern));
    }
    if !section.multi_word_pattern.is_empty() {
        acc.multi_word_patterns
            .push(format!(r"\b({})\b", section.multi_word_pattern));
    }
    if !section.direct_address_pattern.is_empty() {
        acc.direct_address
            .push(section.direct_address_pattern.clone());
    }
    acc.person_verbs
        .extend(section.person_verb_patterns.iter().cloned());
    acc.pronouns
        .extend(section.pronoun_patterns.iter().cloned());
    acc.dialogue
        .extend(section.dialogue_patterns.iter().cloned());
    acc.project_verbs
        .extend(section.project_verb_patterns.iter().cloned());
    for word in &section.stopwords {
        acc.stopwords.insert(word.to_lowercase());
    }
}

/// Remove duplicates from a list while preserving first-occurrence order.
fn dedupe_vec(items: Vec<String>) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    items
        .into_iter()
        .filter(|item| seen.insert(item.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_entity_patterns_english_has_stopwords_and_patterns() {
        let patterns = get_entity_patterns(&["en"]);
        assert!(
            !patterns.stopwords.is_empty(),
            "English stopwords must be present"
        );
        assert!(
            !patterns.candidate_patterns.is_empty(),
            "English candidate pattern must be present"
        );
        assert!(
            !patterns.pronoun_patterns.is_empty(),
            "English pronoun patterns must be present"
        );
    }

    #[test]
    fn get_entity_patterns_unknown_language_falls_back_to_english() {
        let patterns = get_entity_patterns(&["zzz"]);
        // Fallback to English must provide stopwords.
        assert!(patterns.stopwords.contains("the"));
    }

    #[test]
    fn get_entity_patterns_multi_language_union_stopwords() {
        let en = get_entity_patterns(&["en"]);
        let de = get_entity_patterns(&["de"]);
        let both = get_entity_patterns(&["en", "de"]);
        // Union must be at least as large as either individual set.
        assert!(both.stopwords.len() >= en.stopwords.len());
        assert!(both.stopwords.len() >= de.stopwords.len());
    }

    #[test]
    fn get_entity_patterns_case_insensitive_language_resolution() {
        let lower = get_entity_patterns(&["pt-br"]);
        let upper = get_entity_patterns(&["PT-BR"]);
        // Same locale regardless of case — both must produce the same stopword count.
        assert_eq!(lower.stopwords.len(), upper.stopwords.len());
    }

    #[test]
    fn get_entity_patterns_candidate_pattern_is_wrapped() {
        let patterns = get_entity_patterns(&["en"]);
        let pat = &patterns.candidate_patterns[0];
        assert!(
            pat.starts_with(r"\b("),
            "candidate pattern must start with \\b("
        );
        assert!(
            pat.ends_with(r")\b"),
            "candidate pattern must end with )\\b"
        );
    }
}
