# MemPalace

AI memory system. Store everything, find anything. Local, free, no API key.

---

## Slash Commands

| Command              | Description                    |
|----------------------|--------------------------------|
| /mempalace:init      | Install and set up MemPalace   |
| /mempalace:search    | Search your memories           |
| /mempalace:mine      | Mine projects and conversations|
| /mempalace:status    | Palace overview and stats      |
| /mempalace:help      | This help message              |

---

## MCP Tools (29)

### Palace (read)

- mempalace_status -- Palace status and stats
- mempalace_list_wings -- List all wings
- mempalace_list_rooms -- List rooms in a wing
- mempalace_get_taxonomy -- Get the full taxonomy tree
- mempalace_search -- Search memories by query
- mempalace_check_duplicate -- Check if a memory already exists
- mempalace_get_aaak_spec -- Get the AAAK specification

### Palace (write)

- mempalace_add_drawer -- Add a new memory (drawer)
- mempalace_update_drawer -- Update an existing drawer
- mempalace_delete_drawer -- Delete a memory (drawer)
- mempalace_get_drawer -- Fetch one drawer with metadata
- mempalace_list_drawers -- Paginated drawer listing

### Knowledge Graph

- mempalace_kg_query -- Query the knowledge graph
- mempalace_kg_add -- Add a knowledge graph entry
- mempalace_kg_invalidate -- Invalidate a knowledge graph entry
- mempalace_kg_timeline -- View knowledge graph timeline
- mempalace_kg_stats -- Knowledge graph statistics

### Navigation

- mempalace_traverse -- Traverse the palace structure
- mempalace_find_tunnels -- Find cross-wing connections
- mempalace_graph_stats -- Graph connectivity statistics
- mempalace_create_tunnel -- Create an explicit cross-wing tunnel
- mempalace_list_tunnels -- List explicit tunnels
- mempalace_delete_tunnel -- Delete an explicit tunnel
- mempalace_follow_tunnels -- Follow tunnels from a wing/room

### Agent Diary

- mempalace_diary_write -- Write a diary entry
- mempalace_diary_read -- Read diary entries

### Hooks

- mempalace_hook_settings -- Read or write hook configuration
- mempalace_memories_filed_away -- Show recently filed memories
- mempalace_reconnect -- Reconnect the palace (recovery tool)

---

## CLI Commands

    mempalace init <dir>                  Initialize a new palace
    mempalace mine <dir>                  Mine a project (default mode)
    mempalace mine <dir> --mode convos    Mine conversation exports
    mempalace search "query"              Search your memories
    mempalace split <dir>                 Split large transcript files
    mempalace wakeup                      Load palace into context
    mempalace compress                    Compress palace storage
    mempalace status                      Show palace status
    mempalace repair                      Rebuild inverted index
    mempalace mcp                         Run the MCP server
    mempalace hook run                    Run hook logic (for harness integration)
    mempalace instructions <name>         Output skill instructions

---

## Auto-Save Hooks

- Stop hook -- Automatically saves memories every 15 messages. Counts human
  messages in the session transcript (skipping command-messages). When the
  threshold is reached, saves a diary checkpoint silently (systemMessage) or
  blocks the AI with a save instruction. Uses the hook_state directory to
  track save points per session.

- PreCompact hook -- Emergency save before context compaction. Mines the
  transcript synchronously so memories land before compaction proceeds.

Hooks read JSON from stdin and output JSON to stdout. They can be invoked via:

    echo '{"session_id":"abc","stop_hook_active":false,"transcript_path":"..."}' | mempalace hook run --hook stop --harness claude-code

---

## Architecture

    Wings (projects/people)
      +-- Rooms (topics)
            +-- Drawers (verbatim memories)

    Tunnels connect rooms across wings.

The palace is stored locally using SQLite for both the inverted keyword index
and all metadata. No cloud services or API keys required.

---

## Getting Started

1. /mempalace:init -- Set up your palace
2. /mempalace:mine -- Mine a project or conversation
3. /mempalace:search -- Find what you stored
