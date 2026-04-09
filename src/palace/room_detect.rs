use std::collections::HashMap;
use std::path::Path;

use crate::config::RoomConfig;

/// Maps folder name keywords to room names.
const FOLDER_ROOM_MAP: &[(&str, &str)] = &[
    ("frontend", "frontend"),
    ("front_end", "frontend"),
    ("client", "frontend"),
    ("ui", "frontend"),
    ("views", "frontend"),
    ("components", "frontend"),
    ("pages", "frontend"),
    ("backend", "backend"),
    ("back_end", "backend"),
    ("server", "backend"),
    ("api", "backend"),
    ("routes", "backend"),
    ("services", "backend"),
    ("controllers", "backend"),
    ("models", "backend"),
    ("database", "backend"),
    ("db", "backend"),
    ("docs", "documentation"),
    ("doc", "documentation"),
    ("documentation", "documentation"),
    ("wiki", "documentation"),
    ("readme", "documentation"),
    ("notes", "documentation"),
    ("design", "design"),
    ("designs", "design"),
    ("mockups", "design"),
    ("wireframes", "design"),
    ("assets", "design"),
    ("storyboard", "design"),
    ("costs", "costs"),
    ("cost", "costs"),
    ("budget", "costs"),
    ("finance", "costs"),
    ("financial", "costs"),
    ("pricing", "costs"),
    ("invoices", "costs"),
    ("accounting", "costs"),
    ("meetings", "meetings"),
    ("meeting", "meetings"),
    ("calls", "meetings"),
    ("meeting_notes", "meetings"),
    ("standup", "meetings"),
    ("minutes", "meetings"),
    ("team", "team"),
    ("staff", "team"),
    ("hr", "team"),
    ("hiring", "team"),
    ("employees", "team"),
    ("people", "team"),
    ("research", "research"),
    ("references", "research"),
    ("reading", "research"),
    ("papers", "research"),
    ("planning", "planning"),
    ("roadmap", "planning"),
    ("strategy", "planning"),
    ("specs", "planning"),
    ("requirements", "planning"),
    ("tests", "testing"),
    ("test", "testing"),
    ("testing", "testing"),
    ("qa", "testing"),
    ("scripts", "scripts"),
    ("tools", "scripts"),
    ("utils", "scripts"),
    ("config", "configuration"),
    ("configs", "configuration"),
    ("settings", "configuration"),
    ("infrastructure", "configuration"),
    ("infra", "configuration"),
    ("deploy", "configuration"),
];

const SKIP_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "dist",
    "build",
    ".next",
    "coverage",
    ".mempalace",
    "target",
    ".ruff_cache",
    ".mypy_cache",
    ".pytest_cache",
    ".cache",
    ".tox",
    ".nox",
    ".idea",
    ".vscode",
    ".ipynb_checkpoints",
    ".eggs",
    "htmlcov",
];

fn normalize_name(name: &str) -> String {
    name.to_lowercase().replace('-', "_")
}

fn folder_map() -> HashMap<&'static str, &'static str> {
    FOLDER_ROOM_MAP.iter().copied().collect()
}

/// Detect rooms from the folder structure of a project directory.
pub fn detect_rooms_from_folders(project_dir: &Path) -> Vec<RoomConfig> {
    let map = folder_map();
    let mut found: HashMap<String, String> = HashMap::new(); // room_name -> original_folder

    let Ok(entries) = std::fs::read_dir(project_dir) else {
        return vec![general_room()];
    };

    // Top-level directories
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if SKIP_DIRS.contains(&name.as_str()) {
            continue;
        }

        let normalized = normalize_name(&name);
        if let Some(&room_name) = map.get(normalized.as_str()) {
            found.entry(room_name.to_string()).or_insert(name.clone());
        } else if name.len() > 2 && name.chars().next().is_some_and(char::is_alphabetic) {
            let clean = normalize_name(&name);
            found.entry(clean).or_insert(name.clone());
        }

        // One level deeper
        if let Ok(sub_entries) = std::fs::read_dir(&path) {
            for sub_entry in sub_entries.flatten() {
                if !sub_entry.path().is_dir() {
                    continue;
                }
                let sub_name = sub_entry.file_name().to_string_lossy().to_string();
                if SKIP_DIRS.contains(&sub_name.as_str()) {
                    continue;
                }
                let sub_normalized = normalize_name(&sub_name);
                if let Some(&room_name) = map.get(sub_normalized.as_str()) {
                    found.entry(room_name.to_string()).or_insert(sub_name);
                }
            }
        }
    }

    let mut rooms: Vec<RoomConfig> = found
        .into_iter()
        .map(|(room_name, original)| RoomConfig {
            description: format!("Files from {original}/"),
            keywords: vec![room_name.clone(), original.to_lowercase()],
            name: room_name,
        })
        .collect();

    rooms.sort_by(|a, b| a.name.cmp(&b.name));

    if !rooms.iter().any(|r| r.name == "general") {
        rooms.push(general_room());
    }

    rooms
}

fn general_room() -> RoomConfig {
    RoomConfig {
        name: "general".to_string(),
        description: "Files that don't fit other rooms".to_string(),
        keywords: vec![],
    }
}

/// Route a file to the appropriate room based on path, filename, and content.
pub fn detect_room(
    filepath: &Path,
    content: &str,
    rooms: &[RoomConfig],
    project_path: &Path,
) -> String {
    let relative = filepath
        .strip_prefix(project_path)
        .unwrap_or(filepath)
        .to_string_lossy()
        .to_lowercase();
    let filename = filepath
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase();

    // Priority 1: folder path contains room name
    let path_parts: Vec<&str> = relative.split(['/', '\\']).collect();
    for part in &path_parts[..path_parts.len().saturating_sub(1)] {
        for room in rooms {
            let rn = room.name.to_lowercase();
            if rn.contains(part) || part.contains(&rn) {
                return room.name.clone();
            }
        }
    }

    // Priority 2: filename matches room name
    for room in rooms {
        let rn = room.name.to_lowercase();
        if rn.contains(&filename) || filename.contains(&rn) {
            return room.name.clone();
        }
    }

    // Priority 3: keyword scoring
    let content_lower = content
        .chars()
        .take(2000)
        .collect::<String>()
        .to_lowercase();
    let mut best_room = "general".to_string();
    let mut best_score = 0usize;

    for room in rooms {
        let mut score = 0usize;
        let mut keywords: Vec<String> = room.keywords.clone();
        keywords.push(room.name.clone());
        for kw in &keywords {
            score += content_lower.matches(&kw.to_lowercase()).count();
        }
        if score > best_score {
            best_score = score;
            best_room.clone_from(&room.name);
        }
    }

    if best_score > 0 {
        return best_room;
    }

    "general".to_string()
}

pub fn is_skip_dir(name: &str) -> bool {
    SKIP_DIRS.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile;

    #[test]
    fn is_skip_dir_matches_known_dirs() {
        assert!(is_skip_dir(".git"));
        assert!(is_skip_dir("node_modules"));
        assert!(is_skip_dir("target"));
        assert!(is_skip_dir("__pycache__"));
        assert!(is_skip_dir(".mypy_cache"));
        assert!(is_skip_dir(".ruff_cache"));
        assert!(is_skip_dir(".idea"));
        assert!(is_skip_dir("htmlcov"));
    }

    #[test]
    fn is_skip_dir_rejects_normal_dirs() {
        assert!(!is_skip_dir("src"));
        assert!(!is_skip_dir("lib"));
        assert!(!is_skip_dir("app"));
    }

    #[test]
    fn normalize_name_lowercases_and_replaces_dashes() {
        assert_eq!(normalize_name("Front-End"), "front_end");
        assert_eq!(normalize_name("MY-PROJECT"), "my_project");
        assert_eq!(normalize_name("already_lower"), "already_lower");
    }

    #[test]
    fn detect_room_priority1_folder_path() {
        let rooms = vec![
            RoomConfig {
                name: "backend".to_string(),
                description: String::new(),
                keywords: vec!["backend".to_string()],
            },
            RoomConfig {
                name: "frontend".to_string(),
                description: String::new(),
                keywords: vec!["frontend".to_string()],
            },
        ];
        let project = PathBuf::from("/project");
        let filepath = PathBuf::from("/project/backend/server.rs");
        assert_eq!(detect_room(&filepath, "", &rooms, &project), "backend");
    }

    #[test]
    fn detect_room_priority2_filename() {
        let rooms = vec![RoomConfig {
            name: "testing".to_string(),
            description: String::new(),
            keywords: vec!["testing".to_string()],
        }];
        let project = PathBuf::from("/project");
        // File at root (no folder match), but filename contains room name
        let filepath = PathBuf::from("/project/testing_utils.rs");
        assert_eq!(detect_room(&filepath, "", &rooms, &project), "testing");
    }

    #[test]
    fn detect_room_priority3_keyword_scoring() {
        let rooms = vec![RoomConfig {
            name: "backend".to_string(),
            description: String::new(),
            keywords: vec!["database".to_string(), "server".to_string()],
        }];
        let project = PathBuf::from("/project");
        let filepath = PathBuf::from("/project/misc/stuff.txt");
        let content =
            "The database server handles requests and the database pool manages connections";
        assert_eq!(detect_room(&filepath, content, &rooms, &project), "backend");
    }

    #[test]
    fn detect_room_keyword_scoring_handles_utf8_without_panicking() {
        let rooms = vec![RoomConfig {
            name: "architecture".to_string(),
            description: String::new(),
            keywords: vec!["architecture".to_string(), "module".to_string()],
        }];
        let project = PathBuf::from("/project");
        let filepath = PathBuf::from("/project/misc/notes.txt");
        let content =
            &"🔥 architecture review of the payment module with São Paulo notes. ".repeat(80);
        assert_eq!(
            detect_room(&filepath, content, &rooms, &project),
            "architecture"
        );
    }

    #[test]
    fn detect_room_fallback_to_general() {
        let rooms = vec![RoomConfig {
            name: "backend".to_string(),
            description: String::new(),
            keywords: vec!["server".to_string()],
        }];
        let project = PathBuf::from("/project");
        let filepath = PathBuf::from("/project/random/file.txt");
        assert_eq!(
            detect_room(&filepath, "nothing relevant here", &rooms, &project),
            "general"
        );
    }

    #[test]
    fn detect_rooms_from_folders_creates_rooms() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let dir_path = dir.path();
        std::fs::create_dir_all(dir_path.join("frontend")).expect("create frontend");
        std::fs::create_dir_all(dir_path.join("backend")).expect("create backend");
        std::fs::create_dir_all(dir_path.join("docs")).expect("create docs");

        let rooms = detect_rooms_from_folders(dir_path);
        let names: Vec<&str> = rooms.iter().map(|r| r.name.as_str()).collect();

        assert!(names.contains(&"frontend"));
        assert!(names.contains(&"backend"));
        assert!(names.contains(&"documentation"));
        assert!(names.contains(&"general"));
        // TempDir auto-cleans up when dropped
    }
}
