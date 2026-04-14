//! Parser for Claude Code JSONL conversation exports.

use std::borrow::Cow;
use std::sync::LazyLock;

use regex::Regex;

use super::messages_to_transcript;

// ── Noise stripping ──────────────────────────────────────────────────────────
// Claude Code and other tools inject system tags, hook output, and UI chrome
// into transcripts. These waste drawer space and pollute search results.
//
// strip_noise is applied per message, never across message boundaries, so a
// stray unclosed tag in one message cannot eat content from adjacent messages.
// All patterns are line-anchored; user prose mentioning these strings inline
// is preserved verbatim.

const NOISE_TAG_NAMES: &[&str] = &[
    "system-reminder",
    "command-message",
    "command-name",
    "task-notification",
    "user-prompt-submit-hook",
    "hook_output",
];

const NOISE_LINE_PREFIXES: &[&str] = &[
    "CURRENT TIME:",
    "VERIFIED FACTS (do not contradict)",
    "AGENT SPECIALIZATION:",
    "Checking verified facts...",
    "Injecting timestamp...",
    "Starting background pipeline...",
    "Checking emotional weights...",
    "Auto-save reminder...",
    "Checking pipeline...",
    "MemPalace auto-save checkpoint.",
];

// Compiled once at startup. Panics are unreachable — patterns are compile-time
// literals. #[allow] covers the .expect() calls inside the LazyLock closures.
#[allow(clippy::expect_used)]
static NOISE_TAG_RES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    NOISE_TAG_NAMES
        .iter()
        .map(|name| {
            // (?sm): s = dot matches newline (spans tag body), m = ^ anchors to line start.
            // Lazy .*? stops at the first closing tag. The blockquote prefix (?:> )?
            // handles transcripts where lines are already prefixed with "> ".
            Regex::new(&format!(
                r"(?sm)^(?:> )?<{name}(?:\s[^>]*)?>.*?</{name}>[ \t]*\n?"
            ))
            .expect("noise tag regex is a compile-time literal and cannot fail")
        })
        .collect()
});

#[allow(clippy::expect_used)]
static NOISE_LINE_RES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    NOISE_LINE_PREFIXES
        .iter()
        .map(|prefix| {
            // Line-anchored, case-sensitive. regex::escape handles dots and parens.
            Regex::new(&format!(r"(?m)^(?:> )?{}.*\n?", regex::escape(prefix)))
                .expect("noise line regex is a compile-time literal and cannot fail")
        })
        .collect()
});

// "Ran 2 Stop hook", "Ran 1 PreCompact hook", etc. — Claude Code TUI chrome.
// Explicit hook names; prose like "our CI has a stop hook" stays intact.
#[allow(clippy::expect_used)]
static HOOK_LINE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?m)^(?:> )?Ran \d+ (?:Stop|PreCompact|PreToolUse|PostToolUse|UserPromptSubmit|Notification|SessionStart|SessionEnd) hooks?.*\n?",
    )
    .expect("hook line regex is a compile-time literal and cannot fail")
});

// "… +N lines" collapsed-output marker.
#[allow(clippy::expect_used)]
static COLLAPSED_LINES_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)^(?:> )?…\s*\+\d+ lines.*\n?")
        .expect("collapsed lines regex is a compile-time literal and cannot fail")
});

// "[N tokens] (ctrl+o to expand)" inline chrome.
// Narrow pattern — a bare "(ctrl+o to expand)" in user prose stays intact.
#[allow(clippy::expect_used)]
static TOKEN_MARKER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\s*\[\d+\s+tokens?\]\s*\(ctrl\+o to expand\)")
        .expect("token marker regex is a compile-time literal and cannot fail")
});

// Collapse runs of blank lines created by the removals.
#[allow(clippy::expect_used)]
static EXCESS_BLANK_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\n{4,}").expect("excess blank regex is a compile-time literal and cannot fail")
});

/// Remove system tags, hook output, and Claude Code UI chrome from a message.
///
/// Applied per message (never across boundaries) so a dangling open tag
/// in one message cannot consume text from the next.
pub fn strip_noise(text: &str) -> String {
    // Early return: empty input needs no processing.
    if text.is_empty() {
        return String::new();
    }

    // Use Cow to avoid allocating a new String per regex when nothing matches.
    // replace_all returns Cow::Borrowed (no copy) when the pattern is absent.
    let mut result: Cow<str> = Cow::Borrowed(text);
    for re in NOISE_TAG_RES.iter() {
        let replaced = re.replace_all(result.as_ref(), "");
        if let Cow::Owned(s) = replaced {
            result = Cow::Owned(s);
        }
    }
    for re in NOISE_LINE_RES.iter() {
        let replaced = re.replace_all(result.as_ref(), "");
        if let Cow::Owned(s) = replaced {
            result = Cow::Owned(s);
        }
    }
    let replaced = HOOK_LINE_RE.replace_all(result.as_ref(), "");
    if let Cow::Owned(s) = replaced {
        result = Cow::Owned(s);
    }
    let replaced = COLLAPSED_LINES_RE.replace_all(result.as_ref(), "");
    if let Cow::Owned(s) = replaced {
        result = Cow::Owned(s);
    }
    let replaced = TOKEN_MARKER_RE.replace_all(result.as_ref(), "");
    if let Cow::Owned(s) = replaced {
        result = Cow::Owned(s);
    }
    // Collapse runs of 4+ blank lines down to 3 (a visual paragraph break).
    let replaced = EXCESS_BLANK_RE.replace_all(result.as_ref(), "\n\n\n");
    if let Cow::Owned(s) = replaced {
        result = Cow::Owned(s);
    }

    // Postcondition: result is trimmed (no leading/trailing whitespace).
    result.trim().to_string()
}

/// Try to parse Claude Code JSONL format into transcript text.
///
/// Each line is a JSON object with `"type"` (`"human"` or `"user"` for the human
/// turn, `"assistant"` for the AI) and `"message"` containing a `"content"` field.
/// Both `"human"` and `"user"` are accepted as equivalent. Returns `None` if
/// fewer than 2 messages are found.
pub fn try_parse(content: &str) -> Option<String> {
    let mut messages: Vec<(String, String)> = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: serde_json::Value = serde_json::from_str(line).ok()?;
        let obj = entry.as_object()?;

        let msg_type = obj.get("type")?.as_str()?;
        let message = obj.get("message")?.as_object()?;
        let raw = extract_content(message.get("content")?);
        if raw.is_empty() {
            continue;
        }
        // Strip system-injected noise per message, never across boundaries.
        let text = strip_noise(&raw);
        if text.is_empty() {
            continue;
        }

        match msg_type {
            "human" | "user" => messages.push(("user".to_string(), text)),
            "assistant" => messages.push(("assistant".to_string(), text)),
            _ => {}
        }
    }

    if messages.len() >= 2 {
        let refs: Vec<(&str, &str)> = messages
            .iter()
            .map(|(r, t)| (r.as_str(), t.as_str()))
            .collect();
        Some(messages_to_transcript(&refs))
    } else {
        None
    }
}

fn extract_content(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.trim().to_string(),
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|item| {
                if let Some(s) = item.as_str() {
                    Some(s.to_string())
                } else if let Some(obj) = item.as_object() {
                    if obj.get("type").and_then(|t| t.as_str()) == Some("text") {
                        obj.get("text")
                            .and_then(|t| t.as_str())
                            .map(std::string::ToString::to_string)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string(),
        serde_json::Value::Object(obj) => obj
            .get("text")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_jsonl() {
        let jsonl = r#"{"type":"human","message":{"content":"hello world"}}
{"type":"assistant","message":{"content":"hi there"}}"#;
        let result = try_parse(jsonl);
        assert!(result.is_some());
        let text = result.expect("should parse");
        assert!(text.contains("> hello world"));
        assert!(text.contains("hi there"));
    }

    #[test]
    fn parse_single_message_returns_none() {
        let jsonl = r#"{"type":"human","message":{"content":"hello"}}"#;
        assert!(try_parse(jsonl).is_none());
    }

    #[test]
    fn parse_invalid_json_returns_none() {
        assert!(try_parse("not json at all").is_none());
    }

    #[test]
    fn parse_user_type_alias() {
        let jsonl = r#"{"type":"user","message":{"content":"hello world"}}
{"type":"assistant","message":{"content":"hi there"}}"#;
        let result = try_parse(jsonl);
        assert!(result.is_some());
        let text = result.expect("should parse");
        assert!(text.contains("> hello world"));
        assert!(text.contains("hi there"));
    }

    #[test]
    fn parse_array_content() {
        let jsonl = r#"{"type":"human","message":{"content":[{"type":"text","text":"array msg"}]}}
{"type":"assistant","message":{"content":"reply"}}"#;
        let result = try_parse(jsonl).expect("should parse");
        assert!(result.contains("> array msg"));
    }

    // ── strip_noise tests ────────────────────────────────────────────────────

    #[test]
    fn strip_noise_removes_system_reminder_tag() {
        let text = "hello\n<system-reminder>do not share</system-reminder>\nworld";
        let result = strip_noise(text);
        assert!(!result.contains("system-reminder"), "tag should be removed");
        assert!(
            !result.contains("do not share"),
            "tag body should be removed"
        );
        assert!(result.contains("hello"), "surrounding text preserved");
        assert!(result.contains("world"), "surrounding text preserved");
    }

    #[test]
    fn strip_noise_removes_hook_line() {
        let text = "some content\nRan 2 Stop hooks\nmore content";
        let result = strip_noise(text);
        assert!(!result.contains("Ran 2 Stop hooks"), "hook line removed");
        assert!(
            result.contains("some content"),
            "surrounding text preserved"
        );
        assert!(
            result.contains("more content"),
            "surrounding text preserved"
        );
    }

    #[test]
    fn strip_noise_removes_token_marker() {
        let text = "big response [512 tokens] (ctrl+o to expand) end";
        let result = strip_noise(text);
        assert!(!result.contains("ctrl+o to expand"), "token marker removed");
        assert!(
            result.contains("big response"),
            "surrounding text preserved"
        );
    }

    #[test]
    fn strip_noise_removes_noise_line_prefix() {
        let text = "useful text\nCURRENT TIME: 2026-04-14T12:00:00Z\nmore text";
        let result = strip_noise(text);
        assert!(!result.contains("CURRENT TIME:"), "noise prefix removed");
        assert!(result.contains("useful text"), "surrounding text preserved");
    }

    #[test]
    fn strip_noise_preserves_inline_mentions() {
        // "current time:" lowercase and mid-sentence should not be stripped.
        let text = "The current time: field is important in our API.";
        let result = strip_noise(text);
        assert_eq!(result, text.trim(), "inline lowercase mention preserved");
    }

    #[test]
    fn strip_noise_multiline_tag() {
        let text = "before\n<system-reminder>\nline one\nline two\n</system-reminder>\nafter";
        let result = strip_noise(text);
        assert!(!result.contains("line one"), "multiline tag body removed");
        assert!(!result.contains("line two"), "multiline tag body removed");
        assert!(result.contains("before"), "surrounding text preserved");
        assert!(result.contains("after"), "surrounding text preserved");
    }

    #[test]
    fn strip_noise_empty_after_stripping_returns_empty() {
        // A message that is purely a system-reminder should survive as empty.
        let text = "<system-reminder>noise only</system-reminder>";
        let result = strip_noise(text);
        assert!(result.is_empty(), "pure noise becomes empty");
    }

    #[test]
    fn strip_noise_removes_collapsed_lines_marker() {
        let text = "output\n… +42 lines\nmore";
        let result = strip_noise(text);
        assert!(
            !result.contains("42 lines"),
            "collapsed lines marker removed"
        );
    }

    #[test]
    fn jsonl_messages_with_noise_are_stripped() {
        // Verify strip_noise is called during try_parse — noise in a JSONL message
        // should not appear in the parsed transcript.
        let jsonl = "{\"type\":\"human\",\"message\":{\"content\":\"hello world\"}}\n\
            {\"type\":\"assistant\",\"message\":{\"content\":\"reply\\n<system-reminder>internal</system-reminder>\"}}";
        let result = try_parse(jsonl).expect("should parse");
        assert!(result.contains("> hello world"), "user turn preserved");
        assert!(result.contains("reply"), "assistant content preserved");
        assert!(
            !result.contains("system-reminder"),
            "noise tag stripped from assistant turn"
        );
        assert!(
            !result.contains("internal"),
            "noise tag body stripped from assistant turn"
        );
    }
}
