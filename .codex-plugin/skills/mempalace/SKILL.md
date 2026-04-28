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
- **Wake up** at the start of a session: there is no `mempalace_wakeup` MCP
  tool — call `mempalace_status` for the palace overview and use
  `mempalace_search` to recall the specific context you need. The
  `mempalace wake-up` CLI command (with a hyphen) prints the same L0/L1
  layers for manual operator use.
- **File important facts**: use `mempalace_add_drawer` to store decisions,
  preferences, milestones, and key facts the user wants to be remembered.
- **Mine projects**: run `mempalace mine <dir>` to ingest a project directory
  or conversation exports into the palace.

## MCP Tools Quick Reference

Signatures match the canonical schemas in `src/mcp/protocol.rs`. Required
parameters are listed first; optional parameters are suffixed with `?`.

### Search and recall

    mempalace_search(query, limit?, wing?, room?, context?)   -- keyword search with BM25 ranking
    mempalace_check_duplicate(content)                        -- check if content already filed
    mempalace_list_wings()                                    -- list all wings with drawer counts
    mempalace_list_rooms(wing?)                               -- list rooms (all wings if omitted)
    mempalace_get_taxonomy()                                  -- full wing → room → count tree
    mempalace_get_aaak_spec()                                 -- AAAK dialect specification
    mempalace_traverse(start_room, max_hops?)                 -- BFS from a room (max_hops default 2)
    mempalace_find_tunnels(wing_a?, wing_b?)                  -- rooms that bridge two wings
    mempalace_follow_tunnels(wing, room)                      -- follow explicit tunnels from a room
    mempalace_graph_stats()                                   -- rooms, tunnels, wing edges

### Write

    mempalace_add_drawer(wing, room, content, source_file?, added_by?)   -- file a memory
    mempalace_update_drawer(drawer_id, content?, wing?, room?)           -- update content/location
    mempalace_delete_drawer(drawer_id)                                   -- remove a memory
    mempalace_get_drawer(drawer_id)                                      -- fetch one drawer
    mempalace_list_drawers(wing?, room?, limit?, offset?)                -- paginated listing

### Knowledge Graph

    mempalace_kg_query(entity, as_of?, direction?)                       -- query an entity's facts
    mempalace_kg_add(subject, predicate, object, valid_from?, source_closet?)  -- assert a fact
    mempalace_kg_invalidate(subject, predicate, object, ended?)          -- retract a fact
    mempalace_kg_timeline(entity?)                                       -- chronological fact list
    mempalace_kg_stats()                                                 -- graph statistics

### Tunnels (explicit cross-wing links)

    mempalace_create_tunnel(source_wing, source_room, target_wing, target_room, label?, source_drawer_id?, target_drawer_id?)
    mempalace_list_tunnels(wing?)                                        -- list explicit tunnels
    mempalace_delete_tunnel(tunnel_id)                                   -- delete a tunnel by ID

### Diary

    mempalace_diary_write(agent_name, entry, topic?, wing?)              -- write a diary entry
    mempalace_diary_read(agent_name, last_n?, wing?)                     -- read recent entries

`mempalace_diary_read` always filters by the requesting `agent_name`, both
when a `wing` is supplied and on the cross-wing path; it never returns
diary entries authored by a different agent. Callers that need another
agent's entries must look elsewhere — the diary is intentionally
agent-private to keep `mempalace_diary_write` a safe place for per-agent
notes.

## CLI Commands (fallback when MCP is unavailable)

    mempalace init <dir>                 Scan project, detect rooms, write config
    mempalace mine <dir>                 Mine project files
    mempalace mine <dir> --mode convos   Mine conversation exports
    mempalace search "query"             Keyword search
    mempalace wake-up                    Print L0+L1 wake-up context
    mempalace status                     Palace stats overview
    mempalace compress                   AAAK dialect compression
    mempalace split <dir>                Split large transcript files
    mempalace sweep <target>             Tandem miner: catch missed messages
    mempalace dedup                      Detect/remove near-duplicate drawers
    mempalace export --output <dir>      Export drawers to markdown
    mempalace instructions <name>        Print packaged skill instructions
    mempalace onboard                    First-run interactive setup wizard
    mempalace diary-ingest <dir>         Ingest YYYY-MM-DD*.md diary files
    mempalace closet-llm --llm           Regenerate closets via LLM
    mempalace repair                     Rebuild inverted index
    mempalace mcp                        Run MCP server (stdio)

## Auto-Save Hooks

Two hooks auto-file memories during Codex CLI sessions:

- **Stop** (`mempalace hook --hook stop --harness codex`): runs after every N
  exchanges, filing a diary checkpoint and mining the current transcript.
- **PreCompact** (`mempalace hook --hook precompact --harness codex`): runs
  before context compaction so memories land before the window closes.

Hooks are pre-configured by the plugin's `hooks.json`. Manual install
instructions are in `hooks/mempal_save_hook.sh` and
`hooks/mempal_precompact_hook.sh`.
