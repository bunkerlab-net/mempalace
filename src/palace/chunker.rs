const CHUNK_SIZE: usize = 800;
const CHUNK_OVERLAP: usize = 100;
const CHUNK_SIZE_MIN: usize = 50;
/// UTF-8 code points are at most 4 bytes, so a char-boundary snap never takes more than 3 steps.
const CHAR_BOUNDARY_SNAP_MAX: usize = 4;

// Compile-time invariants.
const _: () = assert!(CHUNK_OVERLAP < CHUNK_SIZE);
const _: () = assert!(CHUNK_SIZE_MIN < CHUNK_SIZE);

/// A single text chunk produced by [`chunk_text`].
pub struct Chunk {
    /// The chunk's text content.
    pub content: String,
    /// Zero-based position of this chunk within the source document.
    pub chunk_index: usize,
}

/// Snap a byte offset to the nearest char boundary (forward).
fn snap_forward(text: &str, mut pos: usize) -> usize {
    assert!(
        pos <= text.len(),
        "snap_forward: pos {pos} exceeds string length {}",
        text.len()
    );
    let mut snap_steps: usize = 0;
    while pos < text.len() && !text.is_char_boundary(pos) {
        snap_steps += 1;
        assert!(
            snap_steps < CHAR_BOUNDARY_SNAP_MAX,
            "snap_forward: exceeded CHAR_BOUNDARY_SNAP_MAX ({CHAR_BOUNDARY_SNAP_MAX}) steps"
        );
        pos += 1;
    }
    debug_assert!(text.is_char_boundary(pos));
    pos
}

/// Snap a byte offset to the nearest char boundary (backward).
fn snap_backward(text: &str, mut pos: usize) -> usize {
    assert!(
        pos <= text.len(),
        "snap_backward: pos {pos} exceeds string length {}",
        text.len()
    );
    let mut snap_steps: usize = 0;
    while pos > 0 && !text.is_char_boundary(pos) {
        snap_steps += 1;
        assert!(
            snap_steps < CHAR_BOUNDARY_SNAP_MAX,
            "snap_backward: exceeded CHAR_BOUNDARY_SNAP_MAX ({CHAR_BOUNDARY_SNAP_MAX}) steps"
        );
        pos -= 1;
    }
    debug_assert!(text.is_char_boundary(pos));
    pos
}

/// Split content into drawer-sized chunks, breaking at paragraph/line boundaries.
pub fn chunk_text(content: &str) -> Vec<Chunk> {
    let content = content.trim();
    if content.is_empty() {
        return vec![];
    }

    // Each chunk covers at least CHUNK_SIZE_MIN bytes, so iterations are bounded by the input.
    let chunks_max = content.len() / CHUNK_SIZE_MIN + 1;

    let mut chunks = Vec::new();
    let mut start = 0;
    let mut chunk_index = 0;
    let mut chunk_count: usize = 0;

    while start < content.len() {
        assert!(
            chunk_count < chunks_max,
            "chunk_text: exceeded chunks_max ({chunks_max}) iterations"
        );
        chunk_count += 1;
        let mut end = snap_backward(content, (start + CHUNK_SIZE).min(content.len()));

        // Try to break at paragraph boundary, then line boundary.
        if end < content.len() {
            if let Some(pos) = content[start..end].rfind("\n\n") {
                let abs_pos = start + pos;
                if abs_pos > start + CHUNK_SIZE / 2 {
                    end = abs_pos;
                }
            } else if let Some(pos) = content[start..end].rfind('\n') {
                let abs_pos = start + pos;
                if abs_pos > start + CHUNK_SIZE / 2 {
                    end = abs_pos;
                }
            }
        }

        let chunk = content[start..end].trim();
        if chunk.len() >= CHUNK_SIZE_MIN {
            chunks.push(Chunk {
                content: chunk.to_string(),
                chunk_index,
            });
            chunk_index += 1;
        }

        if end >= content.len() {
            break;
        }
        start = snap_forward(content, end.saturating_sub(CHUNK_OVERLAP));
    }

    // Postcondition: chunk indices are sequential.
    debug_assert!(chunks.iter().enumerate().all(|(i, c)| c.chunk_index == i));
    // Postcondition: all chunks have content above minimum size.
    debug_assert!(chunks.iter().all(|c| c.content.len() >= CHUNK_SIZE_MIN));

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_returns_no_chunks() {
        assert!(chunk_text("").is_empty());
        assert!(chunk_text("   ").is_empty());
    }

    #[test]
    fn short_string_below_min_size_returns_no_chunks() {
        // Below CHUNK_SIZE_MIN (50).
        assert!(chunk_text("too short").is_empty());
    }

    #[test]
    fn string_below_chunk_size_returns_single_chunk() {
        let text = "a".repeat(100);
        let chunks = chunk_text(&text);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].content, text);
    }

    #[test]
    fn breaks_at_paragraph_boundary() {
        // Single paragraph break within CHUNK_SIZE window (second half).
        let before_break = "x".repeat(600);
        let after_break = "y".repeat(500);
        let text = format!("{before_break}\n\n{after_break}");
        let chunks = chunk_text(&text);
        assert!(chunks.len() >= 2);
        // First chunk should end at the paragraph boundary (all x's).
        assert!(
            chunks[0].content.ends_with('x'),
            "first chunk should end with 'x' but was: ...{}",
            &chunks[0].content[chunks[0].content.len().saturating_sub(20)..]
        );
    }

    #[test]
    fn breaks_at_line_boundary() {
        let first_half = "x".repeat(500);
        let second_half = "y".repeat(200);
        let remainder = "z".repeat(200);
        let text = format!("{first_half}\n{second_half}\n{remainder}");
        let chunks = chunk_text(&text);
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn chunk_indexes_are_sequential() {
        let text = "word ".repeat(500); // well over CHUNK_SIZE
        let chunks = chunk_text(&text);
        assert!(chunks.len() >= 2);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i);
        }
    }

    #[test]
    fn multibyte_utf8_does_not_panic() {
        // Mix of ASCII, emoji, and CJK to stress char boundary snapping.
        let text = "Hello \u{1F600} world! ".repeat(100) + &"\u{4E16}\u{754C}".repeat(200);
        let chunks = chunk_text(&text);
        assert!(!chunks.is_empty());
        // Verify all chunks are valid UTF-8 (implicit — String guarantees this).
        for chunk in &chunks {
            assert!(chunk.content.len() >= CHUNK_SIZE_MIN);
        }
    }

    #[test]
    fn snap_forward_on_boundary_is_identity() {
        let text = "hello";
        assert_eq!(snap_forward(text, 0), 0);
        assert_eq!(snap_forward(text, 3), 3);
    }

    #[test]
    fn snap_backward_on_boundary_is_identity() {
        let text = "hello";
        assert_eq!(snap_backward(text, 0), 0);
        assert_eq!(snap_backward(text, 3), 3);
    }

    #[test]
    fn snap_forward_finds_next_char_boundary() {
        let text = "\u{1F600}end"; // 4-byte emoji then ASCII
        // Byte offset 1 is mid-emoji, should snap to 4.
        assert_eq!(snap_forward(text, 1), 4);
    }

    #[test]
    fn snap_backward_finds_prev_char_boundary() {
        let text = "\u{1F600}end"; // 4-byte emoji then ASCII
        // Byte offset 3 is mid-emoji, should snap to 0.
        assert_eq!(snap_backward(text, 3), 0);
    }

    #[test]
    fn large_file_exceeding_old_fixed_limit_does_not_panic() {
        // Regression: files that produced > 5_000 chunks panicked under the old fixed CHUNKS_MAX.
        // 5_001 chunks × CHUNK_SIZE bytes ensures we exceed the former constant.
        let text = "word ".repeat(5_001 * CHUNK_SIZE / "word ".len());
        let chunks = chunk_text(&text);
        assert!(chunks.len() > 5_000);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i);
            assert!(chunk.content.len() >= CHUNK_SIZE_MIN);
        }
    }
}
