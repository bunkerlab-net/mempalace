// Integration test — .expect() is acceptable with a descriptive message.
#![allow(clippy::expect_used)]

use std::fs;

use mempalace::normalize::normalize;

/// Write a valid Claude Code JSONL file, normalize it, and verify
/// the output contains transcript markers ("> " for user messages).
#[test]
fn normalize_claude_code_jsonl() {
    let directory = tempfile::tempdir().expect("tempdir should be created");
    let path = directory.path().join("conversation.jsonl");

    // Two messages minimum required for Claude Code parser to produce output.
    let jsonl = r#"{"type":"human","message":{"content":"What is Rust?"}}
{"type":"assistant","message":{"content":"Rust is a systems programming language focused on safety."}}"#;
    fs::write(&path, jsonl).expect("write JSONL file should succeed");

    let result = normalize(&path).expect("normalize should succeed for valid JSONL");
    assert!(
        result.contains("> What is Rust?"),
        "user message should be prefixed with '> '"
    );
    assert!(
        result.contains("Rust is a systems programming language"),
        "assistant response should be present in transcript"
    );
}

/// Plain text files should pass through normalization with content preserved.
#[test]
fn normalize_plain_text_passthrough() {
    let directory = tempfile::tempdir().expect("tempdir should be created");
    let path = directory.path().join("notes.txt");

    let content = "These are plain text notes about project architecture and design decisions.";
    fs::write(&path, content).expect("write plain text file should succeed");

    let result = normalize(&path).expect("normalize should succeed for plain text");
    assert!(
        result.contains("plain text notes"),
        "plain text content should be preserved"
    );
    assert!(
        result.contains("architecture"),
        "full content should be preserved in passthrough"
    );
}

/// An empty file should normalize to an empty string without error.
#[test]
fn normalize_empty_file_returns_empty() {
    let directory = tempfile::tempdir().expect("tempdir should be created");
    let path = directory.path().join("empty.txt");

    fs::write(&path, "").expect("write empty file should succeed");

    let result = normalize(&path).expect("normalize should succeed for empty file");
    assert!(
        result.is_empty(),
        "empty file should normalize to empty string"
    );
}
