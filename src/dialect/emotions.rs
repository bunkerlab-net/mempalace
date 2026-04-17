//! Emotion and flag signal mappings for AAAK dialect detection.

use std::collections::HashMap;

/// Full emotion vocabulary: maps long-form names (e.g. `"vulnerability"`) to short codes (e.g. `"vul"`).
#[allow(dead_code)]
pub fn emotion_codes() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        ("vulnerability", "vul"),
        ("vulnerable", "vul"),
        ("joy", "joy"),
        ("joyful", "joy"),
        ("fear", "fear"),
        ("mild_fear", "fear"),
        ("trust", "trust"),
        ("trust_building", "trust"),
        ("grief", "grief"),
        ("raw_grief", "grief"),
        ("wonder", "wonder"),
        ("philosophical_wonder", "wonder"),
        ("rage", "rage"),
        ("anger", "rage"),
        ("love", "love"),
        ("devotion", "love"),
        ("hope", "hope"),
        ("despair", "despair"),
        ("hopelessness", "despair"),
        ("peace", "peace"),
        ("relief", "relief"),
        ("humor", "humor"),
        ("dark_humor", "humor"),
        ("tenderness", "tender"),
        ("raw_honesty", "raw"),
        ("brutal_honesty", "raw"),
        ("self_doubt", "doubt"),
        ("anxiety", "anx"),
        ("exhaustion", "exhaust"),
        ("conviction", "convict"),
        ("quiet_passion", "passion"),
        ("warmth", "warmth"),
        ("curiosity", "curious"),
        ("gratitude", "grat"),
        ("frustration", "frust"),
        ("confusion", "confuse"),
        ("satisfaction", "satis"),
        ("excitement", "excite"),
        ("determination", "determ"),
        ("surprise", "surprise"),
    ])
}

/// Keyword → emotion code signals for plain-text detection.
pub fn emotion_signals() -> &'static [(&'static str, &'static str)] {
    &[
        ("decided", "determ"),
        ("prefer", "convict"),
        ("worried", "anx"),
        ("excited", "excite"),
        ("frustrated", "frust"),
        ("confused", "confuse"),
        ("love", "love"),
        ("hate", "rage"),
        ("hope", "hope"),
        ("fear", "fear"),
        ("trust", "trust"),
        ("happy", "joy"),
        ("sad", "grief"),
        ("surprised", "surprise"),
        ("grateful", "grat"),
        ("curious", "curious"),
        ("wonder", "wonder"),
        ("anxious", "anx"),
        ("relieved", "relief"),
        ("satisf", "satis"),
        ("disappoint", "grief"),
        ("concern", "anx"),
    ]
}

/// Keyword → flag signals for plain-text detection.
pub fn flag_signals() -> &'static [(&'static str, &'static str)] {
    &[
        ("decided", "DECISION"),
        ("chose", "DECISION"),
        ("switched", "DECISION"),
        ("migrated", "DECISION"),
        ("replaced", "DECISION"),
        ("instead of", "DECISION"),
        ("because", "DECISION"),
        ("founded", "ORIGIN"),
        ("created", "ORIGIN"),
        ("started", "ORIGIN"),
        ("born", "ORIGIN"),
        ("launched", "ORIGIN"),
        ("first time", "ORIGIN"),
        ("core", "CORE"),
        ("fundamental", "CORE"),
        ("essential", "CORE"),
        ("principle", "CORE"),
        ("belief", "CORE"),
        ("always", "CORE"),
        ("never forget", "CORE"),
        ("turning point", "PIVOT"),
        ("changed everything", "PIVOT"),
        ("realized", "PIVOT"),
        ("breakthrough", "PIVOT"),
        ("epiphany", "PIVOT"),
        ("api", "TECHNICAL"),
        ("database", "TECHNICAL"),
        ("architecture", "TECHNICAL"),
        ("deploy", "TECHNICAL"),
        ("infrastructure", "TECHNICAL"),
        ("algorithm", "TECHNICAL"),
        ("framework", "TECHNICAL"),
        ("server", "TECHNICAL"),
        ("config", "TECHNICAL"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emotion_codes_is_non_empty_and_has_known_entries() {
        // emotion_codes() must return a non-empty map with expected long-form keys.
        let codes = emotion_codes();
        assert!(!codes.is_empty(), "emotion_codes must not be empty");
        // Positive space: specific well-known entries must be present.
        assert_eq!(codes.get("joy"), Some(&"joy"), "joy must map to 'joy'");
        assert_eq!(
            codes.get("vulnerability"),
            Some(&"vul"),
            "vulnerability must map to 'vul'"
        );
        assert_eq!(codes.get("rage"), Some(&"rage"), "rage must map to 'rage'");
        // Negative space: the short codes themselves are not keys.
        assert!(
            !codes.contains_key("vul"),
            "short code 'vul' must not be a key"
        );
    }

    #[test]
    fn emotion_signals_is_non_empty_with_expected_mappings() {
        // emotion_signals() must return a non-empty slice with key signal pairs.
        let signals = emotion_signals();
        assert!(!signals.is_empty(), "emotion_signals must not be empty");
        // At least one well-known keyword must be present.
        assert!(
            signals.iter().any(|(kw, _)| *kw == "love"),
            "emotion_signals must include 'love' keyword"
        );
        assert!(
            signals.iter().any(|(_, code)| *code == "joy"),
            "emotion_signals must include a signal mapping to 'joy'"
        );
    }

    #[test]
    fn flag_signals_is_non_empty_and_has_decision_entries() {
        // flag_signals() must return a non-empty slice including DECISION flags.
        let flags = flag_signals();
        assert!(!flags.is_empty(), "flag_signals must not be empty");
        // Positive space: DECISION and TECHNICAL categories must be present.
        let has_decision = flags.iter().any(|(_, flag)| *flag == "DECISION");
        let has_technical = flags.iter().any(|(_, flag)| *flag == "TECHNICAL");
        assert!(has_decision, "flag_signals must include DECISION flags");
        assert!(has_technical, "flag_signals must include TECHNICAL flags");
        // Negative space: no empty keywords.
        assert!(
            flags.iter().all(|(kw, _)| !kw.is_empty()),
            "all flag signal keywords must be non-empty"
        );
    }
}
