//! AAAK Dialect — compresses plain text into symbolic memory format (~30x compression).

pub mod emotions;
pub mod topics;

use std::collections::HashMap;
use std::path::Path;

use regex::Regex;

use emotions::{emotion_signals, flag_signals};
use topics::{extract_topics, stop_words};

/// AAAK Dialect encoder — compresses plain text into symbolic memory format.
pub struct Dialect {
    /// Known entity name → short code mappings.
    entity_codes: HashMap<String, String>,
    #[allow(dead_code)]
    skip_names: Vec<String>,
}

/// Optional metadata attached to the AAAK header line.
#[derive(Default)]
pub struct CompressMetadata<'a> {
    /// Path of the original source file.
    pub source_file: &'a str,
    /// Wing (project namespace).
    pub wing: &'a str,
    /// Room (category).
    pub room: &'a str,
    /// Date string (e.g. `"2024-01-15"`).
    pub date: &'a str,
}

/// Detect emotions from plain text using keyword signals.
fn detect_emotions(text: &str) -> Vec<String> {
    let text_lower = text.to_lowercase();
    let mut detected = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for &(keyword, code) in emotion_signals() {
        if text_lower.contains(keyword) && seen.insert(code) {
            detected.push(code.to_string());
        }
        if detected.len() >= 3 {
            break;
        }
    }
    detected
}

/// Detect importance flags from plain text using keyword signals.
fn detect_flags(text: &str) -> Vec<String> {
    let text_lower = text.to_lowercase();
    let mut detected = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for &(keyword, flag) in flag_signals() {
        if text_lower.contains(keyword) && seen.insert(flag) {
            detected.push(flag.to_string());
        }
        if detected.len() >= 3 {
            break;
        }
    }
    detected
}

/// Extract the most important sentence fragment from text.
fn extract_key_sentence(text: &str) -> String {
    let re = Regex::new(r"[.!?\n]+").expect("valid regex");
    let sentences: Vec<&str> = re
        .split(text)
        .map(str::trim)
        .filter(|s| s.len() > 10)
        .collect();

    if sentences.is_empty() {
        return String::new();
    }

    let decision_words = [
        "decided",
        "because",
        "instead",
        "prefer",
        "switched",
        "chose",
        "realized",
        "important",
        "key",
        "critical",
        "discovered",
        "learned",
        "conclusion",
        "solution",
        "reason",
        "why",
        "breakthrough",
        "insight",
    ];

    let mut scored: Vec<(i32, &str)> = sentences
        .into_iter()
        .map(|s| {
            let s_lower = s.to_lowercase();
            let mut score: i32 = 0;
            for w in &decision_words {
                if s_lower.contains(w) {
                    score += 2;
                }
            }
            if s.len() < 80 {
                score += 1;
            }
            if s.len() < 40 {
                score += 1;
            }
            if s.len() > 150 {
                score -= 2;
            }
            (score, s)
        })
        .collect();

    scored.sort_by(|a, b| b.0.cmp(&a.0));
    let best = scored[0].1;

    if best.len() > 55 {
        let mut end = 52;
        while end < best.len() && !best.is_char_boundary(end) {
            end += 1;
        }
        format!("{}...", &best[..end])
    } else {
        best.to_string()
    }
}

impl Dialect {
    pub fn new(entities: &HashMap<String, String>, skip_names: Vec<String>) -> Self {
        let mut entity_codes = HashMap::new();
        for (name, code) in entities {
            entity_codes.insert(name.clone(), code.clone());
            entity_codes.insert(name.to_lowercase(), code.clone());
        }
        Self {
            entity_codes,
            skip_names: skip_names.into_iter().map(|n| n.to_lowercase()).collect(),
        }
    }

    pub fn empty() -> Self {
        Self {
            entity_codes: HashMap::new(),
            skip_names: Vec::new(),
        }
    }

    /// Find known entities in text, or detect capitalized names.
    fn detect_entities(&self, text: &str) -> Vec<String> {
        let text_lower = text.to_lowercase();
        let mut found = Vec::new();

        // Check known entities
        for (name, code) in &self.entity_codes {
            if !name.chars().next().is_some_and(char::is_lowercase)
                && text_lower.contains(&name.to_lowercase())
                && !found.contains(code)
            {
                found.push(code.clone());
            }
        }
        if !found.is_empty() {
            return found;
        }

        // Fallback: capitalized words that look like names
        let stops = stop_words();
        let words: Vec<&str> = text.split_whitespace().collect();
        let clean_re = Regex::new(r"[^a-zA-Z]").expect("valid regex");

        for (i, w) in words.iter().enumerate() {
            let clean = clean_re.replace_all(w, "");
            if clean.len() >= 2
                && clean.chars().next().is_some_and(char::is_uppercase)
                && clean[1..].chars().all(char::is_lowercase)
                && i > 0
                && !stops.contains(clean.to_lowercase().as_str())
            {
                let code = clean[..3.min(clean.len())].to_uppercase();
                if !found.contains(&code) {
                    found.push(code);
                }
                if found.len() >= 3 {
                    break;
                }
            }
        }
        found
    }

    /// Compress plain text into AAAK Dialect format.
    pub fn compress(&self, text: &str, metadata: Option<&CompressMetadata>) -> String {
        let entities = self.detect_entities(text);
        let entity_str = if entities.is_empty() {
            "???".to_string()
        } else {
            entities[..3.min(entities.len())].join("+")
        };

        let topics = extract_topics(text, 3);
        let topic_str = if topics.is_empty() {
            "misc".to_string()
        } else {
            topics.join("_")
        };

        let quote = extract_key_sentence(text);
        let emotions = detect_emotions(text);
        let flags = detect_flags(text);

        let mut lines = Vec::new();

        // Header line (if metadata available)
        if let Some(meta) = metadata
            && (!meta.source_file.is_empty() || !meta.wing.is_empty())
        {
            let stem = if meta.source_file.is_empty() {
                "?"
            } else {
                Path::new(meta.source_file)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("?")
            };
            let header = format!(
                "{}|{}|{}|{}",
                if meta.wing.is_empty() { "?" } else { meta.wing },
                if meta.room.is_empty() { "?" } else { meta.room },
                if meta.date.is_empty() { "?" } else { meta.date },
                stem,
            );
            lines.push(header);
        }

        // Content line
        let mut parts = vec![format!("0:{entity_str}"), topic_str];
        if !quote.is_empty() {
            parts.push(format!("\"{quote}\""));
        }
        if !emotions.is_empty() {
            parts.push(emotions.join("+"));
        }
        if !flags.is_empty() {
            parts.push(flags.join("+"));
        }
        lines.push(parts.join("|"));

        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_basic() {
        let dialect = Dialect::empty();
        let result = dialect.compress(
            "We decided to use GraphQL instead of REST because it gives better flexibility",
            None,
        );
        assert!(result.contains("0:"));
        assert!(result.contains("DECISION"));
    }

    #[test]
    fn test_compress_with_metadata() {
        let dialect = Dialect::empty();
        let meta = CompressMetadata {
            source_file: "notes/meeting.txt",
            wing: "wing_project",
            room: "architecture",
            date: "2024-01-15",
        };
        let result = dialect.compress("Alice decided to switch from REST to GraphQL", Some(&meta));
        assert!(result.contains("wing_project|architecture|2024-01-15|meeting"));
    }

    #[test]
    fn test_detect_emotions() {
        let emotions = detect_emotions("I'm really excited but also worried about the deadline");
        assert!(emotions.contains(&"excite".to_string()));
        assert!(emotions.contains(&"anx".to_string()));
    }

    #[test]
    fn test_detect_flags() {
        let flags = detect_flags("We decided to switch because the old API was too slow");
        assert!(flags.contains(&"DECISION".to_string()));
        assert!(flags.contains(&"TECHNICAL".to_string()));
    }

    #[test]
    fn test_known_entities() {
        let mut entities = HashMap::new();
        entities.insert("Alice".to_string(), "ALC".to_string());
        entities.insert("Bob".to_string(), "BOB".to_string());
        let dialect = Dialect::new(&entities, vec![]);
        let found = dialect.detect_entities("Alice told Bob about the new architecture");
        assert!(found.contains(&"ALC".to_string()));
        assert!(found.contains(&"BOB".to_string()));
    }
}
