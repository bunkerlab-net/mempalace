# MemPalace

MemPalace is a local-first AI memory system. It stores conversation context,
project knowledge, and code insights in an embedded SQLite database accessible
via MCP tools or the `mempalace` CLI. No cloud services or API keys required.

## Architecture

    Wings (top-level categories, e.g. projects, people, domains)
      +-- Rooms (sub-topics within a wing)
            +-- Drawers (verbatim memory chunks)

    Tunnels connect related rooms across different wings.

## When to Use MemPalace

- **Remember context** across sessions: call `mempalace_search` when the user
  references something that may have been discussed before.
- **Wake up** at the start of a session: check `mempalace_status` and the
  L0/L1 context from `mempalace wakeup` to orient yourself.
- **File important facts**: use `mempalace_add_drawer` to store decisions,
  preferences, milestones, and key facts the user wants to be remembered.
- **Mine projects**: run `mempalace mine <dir>` to ingest a project directory
  or conversation exports into the palace.

## MCP Tools Quick Reference

### Search and recall

    mempalace_search(query, wing?, room?)     -- keyword search with BM25 ranking
    mempalace_list_wings()                    -- list all top-level wings
    mempalace_list_rooms(wing)                -- list rooms within a wing
    mempalace_get_taxonomy()                  -- full wing/room/drawer tree
    mempalace_traverse(room, wing?)           -- graph traversal from a room
    mempalace_find_tunnels(wing1, wing2)      -- cross-wing connections
    mempalace_follow_tunnels(wing, room?)     -- follow tunnels from a location

### Write

    mempalace_add_drawer(wing, room, content, source_file?)   -- store a memory
    mempalace_update_drawer(id, content)                      -- update a memory
    mempalace_delete_drawer(id)                               -- remove a memory
    mempalace_get_drawer(id)                                  -- fetch one drawer
    mempalace_list_drawers(wing?, room?, limit?, offset?)     -- paginated listing

### Knowledge Graph

    mempalace_kg_query(entity?, predicate?, object?)    -- query KG triples
    mempalace_kg_add(subject, predicate, object, ...)   -- assert a KG fact
    mempalace_kg_invalidate(id)                         -- retract a KG fact
    mempalace_kg_timeline(entity)                       -- entity history
    mempalace_kg_stats()                                -- graph statistics

### Diary

    mempalace_diary_write(content, date?)    -- write a diary entry
    mempalace_diary_read(date?, wing?)       -- read diary entries

### Palace topology

    mempalace_graph_stats()    -- room/tunnel/wing-edge counts and connectivity
                                  (distinct from `mempalace_kg_stats` above,
                                   which counts knowledge-graph triples)

## Slash Commands

| Command             | What it does                              |
|---------------------|-------------------------------------------|
| /mempalace:init     | First-time setup wizard                   |
| /mempalace:mine     | Guide through mining a directory          |
| /mempalace:search   | Guide through searching memories          |
| /mempalace:status   | Show palace overview                      |
| /mempalace:help     | Show all commands and MCP tools           |

## CLI Commands (fallback when MCP is unavailable)

    mempalace init <dir>                 Scan project, detect rooms, write config
    mempalace mine <dir>                 Mine project files
    mempalace mine <dir> --mode convos   Mine conversation exports
    mempalace search "query"             Keyword search
    mempalace wakeup                     Print L0+L1 wake-up context
    mempalace status                     Palace stats overview
    mempalace compress                   AAAK dialect compression
    mempalace split <dir>                Split large transcript files
    mempalace repair                     Rebuild inverted index
    mempalace mcp                        Run MCP server (stdio)

## Auto-Save Hooks

Two hooks auto-file memories during Claude Code sessions:

- **Stop** (`mempalace hook --hook stop --harness claude-code`): runs after
  every N exchanges, filing a diary checkpoint and optionally mining the
  current transcript directory.
- **PreCompact** (`mempalace hook --hook precompact --harness claude-code`):
  runs before context compaction, mining the transcript so memories land
  before the window closes.

Hooks are pre-configured by the plugin's `hooks.json`. Manual install
instructions are in `hooks/mempal_save_hook.sh` and
`hooks/mempal_precompact_hook.sh`.
