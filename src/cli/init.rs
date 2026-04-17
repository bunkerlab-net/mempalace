use std::io::Write as _;
use std::path::Path;

use crate::config::ProjectConfig;
use crate::error::Result;
use crate::palace::room_detect::detect_rooms_from_folders;

pub fn run(directory: &Path, yes: bool, no_gitignore: bool) -> Result<()> {
    let directory = directory.canonicalize().map_err(|e| {
        crate::error::Error::Other(format!("directory not found: {}: {e}", directory.display()))
    })?;

    let project_name = directory
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase()
        .replace([' ', '-'], "_");

    let rooms = detect_rooms_from_folders(&directory);

    // Count files for display; honour the same gitignore flag used by `mine`.
    let file_count = crate::palace::miner::scan_project_with_opts(&directory, !no_gitignore).len();

    println!("\n=======================================================");
    println!("  MemPalace Init");
    println!("=======================================================");
    println!("\n  WING: {project_name}");
    println!("  ({file_count} files found, rooms detected from folder structure)\n");

    for room in &rooms {
        println!("    ROOM: {}", room.name);
        println!("          {}", room.description);
    }
    println!("\n-------------------------------------------------------");

    if !yes {
        print!("\n  Proceed? [Y/n] ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let trimmed = input.trim().to_lowercase();
        if trimmed == "n" || trimmed == "no" {
            println!("  Aborted.");
            return Ok(());
        }
    }

    // Save config
    let config = ProjectConfig {
        wing: project_name.clone(),
        rooms,
    };

    let config_path = directory.join("mempalace.yaml");
    let yaml = serde_yaml::to_string(&config).map_err(crate::error::Error::Yaml)?;
    std::fs::write(&config_path, &yaml)?;

    println!("\n  Config saved: {}", config_path.display());
    println!("\n  Next step:");
    println!("    mempalace mine {}", directory.display());
    println!("\n=======================================================\n");

    Ok(())
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn init_run_creates_mempalace_yaml_in_directory() {
        // init::run with yes=true must write a mempalace.yaml to the target directory.
        let temp_directory =
            tempfile::tempdir().expect("failed to create temporary directory for init test");
        run(temp_directory.path(), true, false)
            .expect("init::run should succeed for a valid directory with yes=true");

        let config_path = temp_directory.path().join("mempalace.yaml");
        assert!(config_path.exists(), "mempalace.yaml must be created");
        // Pair assertion: the file must contain valid YAML with a wing key.
        let contents = std::fs::read_to_string(&config_path)
            .expect("mempalace.yaml must be readable after init");
        assert!(
            contents.contains("wing:"),
            "config must contain a wing field"
        );
        assert!(!contents.is_empty(), "config file must not be empty");
    }

    #[test]
    fn init_run_nonexistent_directory_returns_error() {
        // Passing a path that does not exist must return Err.
        let path = std::path::Path::new("/nonexistent/path/that/does/not/exist");
        let result = run(path, true, false);
        assert!(result.is_err(), "nonexistent directory must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| !error.to_string().is_empty()),
            "error message must not be empty"
        );
    }

    #[test]
    fn init_run_no_gitignore_flag_counts_files() {
        // no_gitignore=true must still produce a valid config (just without .gitignore filtering).
        let temp_directory = tempfile::tempdir()
            .expect("failed to create temporary directory for no-gitignore init test");
        std::fs::write(temp_directory.path().join("test.rs"), "fn main() {}")
            .expect("failed to write test source file");

        run(temp_directory.path(), true, true)
            .expect("init::run should succeed with no_gitignore=true");

        let config_path = temp_directory.path().join("mempalace.yaml");
        assert!(
            config_path.exists(),
            "mempalace.yaml must be created with no_gitignore=true"
        );
        let contents = std::fs::read_to_string(&config_path)
            .expect("mempalace.yaml must be readable after init with no_gitignore");
        assert!(contents.contains("wing:"), "config must contain wing field");
    }
}
