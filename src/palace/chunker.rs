const CHUNK_SIZE: usize = 800;
const CHUNK_OVERLAP: usize = 100;
const MIN_CHUNK_SIZE: usize = 50;

/// A single text chunk produced by [`chunk_text`].
pub struct Chunk {
    /// The chunk's text content.
    pub content: String,
    /// Zero-based position of this chunk within the source document.
    pub chunk_index: usize,
}

/// Snap a byte offset to the nearest char boundary (forward).
fn snap_forward(s: &str, mut pos: usize) -> usize {
    while pos < s.len() && !s.is_char_boundary(pos) {
        pos += 1;
    }
    pos
}

/// Snap a byte offset to the nearest char boundary (backward).
fn snap_backward(s: &str, mut pos: usize) -> usize {
    while pos > 0 && !s.is_char_boundary(pos) {
        pos -= 1;
    }
    pos
}

/// Split content into drawer-sized chunks, breaking at paragraph/line boundaries.
pub fn chunk_text(content: &str) -> Vec<Chunk> {
    let content = content.trim();
    if content.is_empty() {
        return vec![];
    }

    let mut chunks = Vec::new();
    let mut start = 0;
    let mut chunk_index = 0;

    while start < content.len() {
        let mut end = snap_backward(content, (start + CHUNK_SIZE).min(content.len()));

        // Try to break at paragraph boundary, then line boundary
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
        if chunk.len() >= MIN_CHUNK_SIZE {
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
        // Below MIN_CHUNK_SIZE (50)
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
        // Single paragraph break within CHUNK_SIZE window (second half)
        let before_break = "x".repeat(600);
        let after_break = "y".repeat(500);
        let text = format!("{before_break}\n\n{after_break}");
        let chunks = chunk_text(&text);
        assert!(chunks.len() >= 2);
        // First chunk should end at the paragraph boundary (all x's)
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
        // Mix of ASCII, emoji, and CJK to stress char boundary snapping
        let text = "Hello \u{1F600} world! ".repeat(100) + &"\u{4E16}\u{754C}".repeat(200);
        let chunks = chunk_text(&text);
        assert!(!chunks.is_empty());
        // Verify all chunks are valid UTF-8 (implicit — String guarantees this)
        for chunk in &chunks {
            assert!(chunk.content.len() >= MIN_CHUNK_SIZE);
        }
    }

    #[test]
    fn snap_forward_on_boundary_is_identity() {
        let s = "hello";
        assert_eq!(snap_forward(s, 0), 0);
        assert_eq!(snap_forward(s, 3), 3);
    }

    #[test]
    fn snap_backward_on_boundary_is_identity() {
        let s = "hello";
        assert_eq!(snap_backward(s, 0), 0);
        assert_eq!(snap_backward(s, 3), 3);
    }

    #[test]
    fn snap_forward_finds_next_char_boundary() {
        let s = "\u{1F600}end"; // 4-byte emoji then ASCII
        // Byte offset 1 is mid-emoji, should snap to 4
        assert_eq!(snap_forward(s, 1), 4);
    }

    #[test]
    fn snap_backward_finds_prev_char_boundary() {
        let s = "\u{1F600}end"; // 4-byte emoji then ASCII
        // Byte offset 3 is mid-emoji, should snap to 0
        assert_eq!(snap_backward(s, 3), 0);
    }
}
