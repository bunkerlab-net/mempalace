//! Print packaged skill instructions for a named `MemPalace` command.

use crate::error::{Error, Result};

// Instruction files are embedded at compile time so the binary is self-contained.
const INSTRUCTIONS_HELP: &str = include_str!("../instructions/help.md");
const INSTRUCTIONS_INIT: &str = include_str!("../instructions/init.md");
const INSTRUCTIONS_MINE: &str = include_str!("../instructions/mine.md");
const INSTRUCTIONS_SEARCH: &str = include_str!("../instructions/search.md");
const INSTRUCTIONS_STATUS: &str = include_str!("../instructions/status.md");

const AVAILABLE: [&str; 5] = ["help", "init", "mine", "search", "status"];

/// Print the instruction Markdown for `name` to stdout.
///
/// Returns `Err` when `name` is not one of the available instruction names.
pub fn run(name: &str) -> Result<()> {
    assert!(!name.is_empty(), "instruction name must not be empty");

    let text = match name {
        "help" => INSTRUCTIONS_HELP,
        "init" => INSTRUCTIONS_INIT,
        "mine" => INSTRUCTIONS_MINE,
        "search" => INSTRUCTIONS_SEARCH,
        "status" => INSTRUCTIONS_STATUS,
        other => {
            return Err(Error::Other(format!(
                "unknown instructions: {other}\nAvailable: {}",
                AVAILABLE.join(", ")
            )));
        }
    };

    assert!(
        !text.is_empty(),
        "embedded instruction file must not be empty"
    );
    print!("{text}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_known_name_prints_without_error() {
        // Every defined instruction name must succeed without returning Err.
        for name in AVAILABLE {
            let result = run(name);
            assert!(result.is_ok(), "run({name}) must return Ok");
        }
    }

    #[test]
    fn run_unknown_name_returns_error() {
        let result = run("nonexistent");
        assert!(result.is_err(), "unknown instruction name must return Err");
        assert!(
            result
                .err()
                .is_some_and(|e| e.to_string().contains("nonexistent")),
            "error must name the unknown instruction"
        );
    }

    #[test]
    fn all_instruction_files_are_non_empty() {
        // Pair assertion: every embedded file must have content.
        assert!(!INSTRUCTIONS_HELP.is_empty(), "help.md must not be empty");
        assert!(!INSTRUCTIONS_INIT.is_empty(), "init.md must not be empty");
        assert!(!INSTRUCTIONS_MINE.is_empty(), "mine.md must not be empty");
        assert!(
            !INSTRUCTIONS_SEARCH.is_empty(),
            "search.md must not be empty"
        );
        assert!(
            !INSTRUCTIONS_STATUS.is_empty(),
            "status.md must not be empty"
        );
    }
}
