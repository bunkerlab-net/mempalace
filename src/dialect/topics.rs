//! Topic extraction for AAAK dialect compression.

use std::collections::{HashMap, HashSet};

use regex::Regex;

/// Common stop words to exclude from topic extraction.
pub fn stop_words() -> HashSet<&'static str> {
    HashSet::from([
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
        "do", "does", "did", "will", "would", "could", "should", "may", "might", "shall", "can",
        "to", "of", "in", "for", "on", "with", "at", "by", "from", "as", "into", "about",
        "between", "through", "during", "before", "after", "above", "below", "up", "down", "out",
        "off", "over", "under", "again", "further", "then", "once", "here", "there", "when",
        "where", "why", "how", "all", "each", "every", "both", "few", "more", "most", "other",
        "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very",
        "just", "don", "now", "and", "but", "or", "if", "while", "that", "this", "these", "those",
        "it", "its", "i", "we", "you", "he", "she", "they", "me", "him", "her", "us", "them", "my",
        "your", "his", "our", "their", "what", "which", "who", "whom", "also", "much", "many",
        "like", "because", "since", "get", "got", "use", "used", "using", "make", "made", "thing",
        "things", "way", "well", "really", "want", "need",
    ])
}

/// Extract key topic words from plain text by frequency + proper noun boost.
pub fn extract_topics(text: &str, max_topics: usize) -> Vec<String> {
    let stops = stop_words();
    let word_re = Regex::new(r"[a-zA-Z][a-zA-Z_-]{2,}").expect("valid regex");

    let words: Vec<&str> = word_re.find_iter(text).map(|m| m.as_str()).collect();

    // Count frequency, skip stop words
    let mut freq: HashMap<String, i32> = HashMap::new();
    for w in &words {
        let lower = w.to_lowercase();
        if stops.contains(lower.as_str()) || lower.len() < 3 {
            continue;
        }
        *freq.entry(lower).or_insert(0) += 1;
    }

    // Boost proper nouns and technical terms
    for w in &words {
        let lower = w.to_lowercase();
        if stops.contains(lower.as_str()) {
            continue;
        }
        if let Some(count) = freq.get_mut(&lower) {
            // Capitalized word (proper noun)
            if w.chars().next().is_some_and(char::is_uppercase) {
                *count += 2;
            }
            // CamelCase, underscore, or hyphen → technical term
            if w.contains('_') || w.contains('-') || w[1..].chars().any(char::is_uppercase) {
                *count += 2;
            }
        }
    }

    let mut ranked: Vec<_> = freq.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1));
    ranked
        .into_iter()
        .take(max_topics)
        .map(|(w, _)| w)
        .collect()
}
