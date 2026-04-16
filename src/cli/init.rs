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
