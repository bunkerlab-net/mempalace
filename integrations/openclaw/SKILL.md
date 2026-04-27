# MemPalace — OpenClaw Integration

MemPalace is a local-first AI memory system. This skill teaches an
OpenClaw agent how to use MemPalace MCP tools and CLI commands.

## Architecture

    Wings (top-level categories, e.g. projects, people, domains)
      +-- Rooms (sub-topics within a wing)
            +-- Drawers (verbatim memory chunks)

    Tunnels connect related rooms across different wings.

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

## CLI Commands (fallback when MCP is unavailable)

    mempalace search "query"             Keyword search
    mempalace wakeup                     Print L0+L1 wake-up context
    mempalace status                     Palace stats overview
    mempalace mine <dir>                 Mine project files
    mempalace mcp                        Run MCP server (stdio)

## Auto-Save Hooks

MemPalace can auto-file memories at session end.  Run the MCP server
with:

    mempalace mcp

and configure your OpenClaw agent to connect to it via the stdio
transport.
