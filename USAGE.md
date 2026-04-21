# Usage Guide

## Concepts

The palace is organized as a hierarchy:

```text
WING  (person or project)
  └── ROOM  (subtopic — auth, billing, deploy, etc.)
        └── DRAWER  (verbatim text chunk, 800 chars with 100-char overlap)
```

**Tunnels** connect rooms across wings — automatically when the same room name appears
in multiple wings, or explicitly via MCP tools. **AAAK** is a lossy compression dialect
for efficient LLM context loading. The **knowledge graph** stores temporal facts:
`Maya → assigned_to → auth-migration, valid 2026-01-15 to 2026-02-01`.

---

## Initial Setup

### 1. Identity file

Write `$XDG_DATA_HOME/mempalace/identity.txt` (default: `~/.local/share/mempalace/identity.txt`)
— this is L0, loaded every session (~100 tokens). Never auto-generated; write it yourself.

```text
I am Atlas, assistant to Alice.
People: Alice (engineer, creator), Jordan (Alice's partner), Riley (18, athlete).
Projects: mempalace, homelab.
Traits: direct, memory-first, no summaries.
```

### 2. Global config

`$XDG_DATA_HOME/mempalace/config.json` (default: `~/.local/share/mempalace/config.json`) is
created automatically on first run.

```json
{
  "palace_path": "~/.local/share/mempalace/palace.db",
  "collection_name": "mempalace_drawers",
  "people_map": {}
}
```

| Field             | Purpose                                                                |
| ----------------- | ---------------------------------------------------------------------- |
| `palace_path`     | Path to the SQLite database file                                       |
| `collection_name` | Legacy field (unused; kept for config compatibility with mempalace-py) |
| `people_map`      | Optional name → code mappings for AAAK compression                     |

Override the data directory or just the DB path without editing the file:

```bash
export MEMPALACE_DIR=/path/to/mempalace           # overrides the entire data directory
export MEMPALACE_PALACE_PATH=/path/to/palace.db   # overrides only the database path
```

**Legacy migration:** On first run after upgrading from an older install, the binary
automatically moves `~/.mempalace/` to the XDG location — `config.json`,
`identity.txt`, `palace.db` (plus WAL files), and `wal/`. If `palace_path` in
`config.json` still points to the old default it is patched in place. The legacy
directory is removed if empty after migration. Migration is skipped when
`MEMPALACE_DIR` is set.

---

## Typical Workflow

```bash
# 1. Initialise a project — detects rooms, writes mempalace.yaml
mempalace init ~/my-project

# 2. Mine project source files
mempalace mine ~/my-project

# 3. Mine conversation transcripts
mempalace mine ~/.claude/projects/ --mode convos

# 4. Connect to Claude Code as an MCP server
claude mcp add mempalace -- /path/to/mempalace mcp

# 5. Check the palace
mempalace status

# 6. Generate wake-up context for a local model or manual session
mempalace wake-up
```

After the MCP server is connected, the AI calls tools automatically — you don't run
`mempalace search` by hand. The AI calls `mempalace_status` on first use and learns
the memory protocol from the response.

---

## CLI Reference

### `mempalace init <dir>`

Scans a project directory, detects rooms from the folder structure, and writes `mempalace.yaml`.

```bash
mempalace init ~/my-project
mempalace init ~/my-project --yes        # non-interactive / CI mode
mempalace init ~/my-project --no-gitignore  # include gitignored files
```

`mempalace.yaml` controls the wing name and room taxonomy used during mining.
Edit it before running `mine` if the auto-detected rooms need adjustment.

---

### `mempalace mine <dir>`

Ingests files from a directory into the palace.

```text
mempalace mine <dir> [OPTIONS]

Options:
  --mode <mode>          projects | convos  (default: projects)
  --extract-mode <mode>  exchange | general (default: exchange, convos only)
  --wing <name>          Override wing name (default: from mempalace.yaml or dir name)
  --agent <name>         Agent name recorded on each drawer (default: mempalace)
  --limit <n>            Maximum files to process; 0 = no limit (default: 0)
  --dry-run              Preview what would be filed without writing
  --no-gitignore         Disable .gitignore filtering (include all files)
```

**Projects mode** (`--mode projects`): Reads source files (`.py`, `.rs`, `.ts`, `.go`,
`.md`, etc.), chunks at 800-character boundaries with 100-character overlap, routes each
chunk to a room via folder/filename/keyword heuristics. Respects `.gitignore` by default
(same engine as ripgrep); pass `--no-gitignore` to include all files.

**Convos mode** (`--mode convos`): Reads conversation exports in any of these formats:

- Claude Code JSONL (`~/.claude/projects/`)
- OpenAI Codex CLI JSONL (`~/.codex/sessions/*/rollout-*.jsonl`)
- Claude.ai JSON (standard export and privacy export)
- ChatGPT JSON
- Slack JSON export
- Plain text with `>` quote markers

All formats are normalised to `> prompt\nresponse\n\n` before chunking. Chunks are one
exchange pair (user turn + AI response) each. Room detection assigns topics from content
keywords: `technical`, `architecture`, `planning`, `decisions`, `problems`, `general`.

Files already in the palace (matched by path) are skipped automatically. Move or rename
a file to re-mine it.

---

### `mempalace split <dir>`

Splits plain-text terminal capture files that contain multiple concatenated sessions
into individual per-session files. Useful when you've recorded your terminal with
`script` or a similar tool and ended up with one large `.txt` file spanning many sessions.

**Not needed for Claude Code's native JSONL format** — each session is already stored
as its own UUID-named file in `~/.claude/projects/<project>/`. Use
`mine --mode convos` directly on that directory.

```bash
mempalace split ~/transcripts
mempalace split ~/transcripts --output-dir ~/sessions
mempalace split ~/transcripts --dry-run
mempalace split ~/transcripts --min-sessions 3
mempalace split ~/transcripts --no-gitignore
```

Detects true session starts from `Claude Code v` headers, filtering out context-restore
continuations. Original files are renamed to `.mega_backup`.

---

### `mempalace sweep <target>`

Sweeps a single `.jsonl` Claude Code transcript file, or every `.jsonl` in a directory tree,
inserting one drawer per user/assistant message. Idempotent: re-running the same target is a
safe no-op — already-present messages are detected by UUID and counted but not re-inserted.

```bash
mempalace sweep ~/.claude/projects/my-project/session.jsonl
mempalace sweep ~/.claude/projects/my-project/
mempalace sweep ~/.claude/projects/ --wing conversations
```

```text
Options:
  --wing <name>  Wing to file drawers under (default: conversations)
```

Each drawer contains one raw user or assistant message. The message UUID from the JSONL record
is embedded in the drawer ID (`sweep_{session_id}_{uuid}`), so repeated runs never create
duplicates.

**Difference from `mine --mode convos`:** `mine --mode convos` normalises Claude Code JSONL into
exchange pairs (user turn + AI response) and chunks them at 800-character boundaries. `sweep`
inserts raw individual messages with no chunking or pairing — useful when you want message-level
granularity or when the exchange-pair format loses context you care about.

---

### `mempalace search "<query>"`

Keyword search using the inverted index.

```bash
mempalace search "chromadb locking"
mempalace search "riley" --wing wing_family
mempalace search "api design" --room architecture --results 20
```

Results are ranked by total word-hit count across matched drawers.
Output includes wing, room, source file, hit count, and verbatim drawer content.

Search is keyword-only — no fuzzy or semantic matching. Use specific nouns for best results.

---

### `mempalace wake-up`

Prints L0 + L1 context for loading at the start of a session (~600–900 tokens total).

```bash
mempalace wake-up
mempalace wake-up --wing wing_myproject
```

- **L0 (identity):** Contents of `$XDG_DATA_HOME/mempalace/identity.txt` (~100 tokens).
- **L1 (essential story):** The 15 most-recent drawers grouped by room, capped at 3200 characters.

Paste the output into a local model's system prompt, or let the MCP server handle it automatically.

---

### `mempalace compress`

Compresses drawers into AAAK dialect format and stores them in the `compressed` table.

```bash
mempalace compress
mempalace compress --wing wing_code
mempalace compress --dry-run
mempalace compress --config entities.json
```

AAAK is a structured symbolic notation readable by any LLM without a decoder. It is
**lossy** — the original text cannot be reconstructed from AAAK output. Compression ratio
depends on how many repeated named entities appear in your content; it saves tokens at
scale, not for short individual texts.

The optional `--config` JSON file maps full names to their codes:

```json
{
  "entities": {
    "Alice": "ALC",
    "Jordan": "JOR"
  }
}
```

Entity codes can also be set globally in `config.json` via `people_map`.

---

### `mempalace status`

Prints a palace overview: total drawers, per-wing and per-room counts, knowledge graph stats.

---

### `mempalace repair`

Backs up the palace database and rebuilds the inverted word index from scratch.
Use this if search results seem wrong after an interrupted mine or a manual DB edit.

```bash
mempalace repair
# Creates palace.db.bak, then re-indexes all drawers
```

---

### `mempalace mcp`

Runs the MCP server over stdio (JSON-RPC 2.0). This is the mode used by Claude Code after
`claude mcp add`.

---

## Project Config (`mempalace.yaml`)

Generated by `mempalace init`. Controls the wing name and room taxonomy for a project.

```yaml
wing: my_project
rooms:
  - name: backend
    description: Server-side code
    keywords: [api, routes, database, server]
  - name: frontend
    description: Client code
    keywords: [ui, components, views, client]
  - name: general
    description: Catch-all
    keywords: []
```

Room detection priority during mining:

1. Folder path contains the room name
2. Filename matches the room name
3. Content keyword scoring (first 2000 chars, most keyword hits wins)
4. Fallback: `general`

---

## MCP Tools (26)

All tools communicate over JSON-RPC 2.0. After `claude mcp add`, the AI invokes these
automatically. Call `mempalace_status` first in a new session — the response contains the
full memory protocol and AAAK spec.

### Palace / Drawers

| Tool                        | Parameters                                             | What it does                                        |
| --------------------------- | ------------------------------------------------------ | --------------------------------------------------- |
| `mempalace_status`          | —                                                      | Overview + memory protocol + AAAK spec              |
| `mempalace_list_wings`      | —                                                      | Wing names with drawer counts                       |
| `mempalace_list_rooms`      | `wing?`                                                | Room names with counts (all wings or one)           |
| `mempalace_get_taxonomy`    | —                                                      | Full `wing → room → count` hierarchy                |
| `mempalace_get_aaak_spec`   | —                                                      | AAAK dialect specification                          |
| `mempalace_search`          | `query`, `limit?`, `wing?`, `room?`, `context?`        | Keyword search; sanitizes contaminated queries      |
| `mempalace_check_duplicate` | `content`                                              | True if highly similar content already exists       |
| `mempalace_add_drawer`      | `wing`, `room`, `content`, `source_file?`, `added_by?` | File a memory; blocks on duplicates                 |
| `mempalace_delete_drawer`   | `drawer_id`                                            | Permanently delete a drawer and its index entries   |
| `mempalace_get_drawer`      | `drawer_id`                                            | Fetch full content and metadata for a single drawer |
| `mempalace_list_drawers`    | `wing?`, `room?`, `limit?` (max 100), `offset?`        | Paginated drawer listing with content previews      |
| `mempalace_update_drawer`   | `drawer_id`, `content?`, `wing?`, `room?`              | Update an existing drawer's content and/or location |

`mempalace_add_drawer` performs a duplicate check before inserting. If a highly similar
drawer already exists it returns `{"success": false, "reason": "duplicate", "matches": [...]}`
without writing.

### Knowledge Graph

Facts are temporal: every triple can have `valid_from` and `valid_to` dates. Querying
with `as_of` returns only facts that were true at that moment. Invalidation preserves
the old fact with a `valid_to` date rather than deleting it.

| Tool                      | Parameters                                                        | What it does                                      |
| ------------------------- | ----------------------------------------------------------------- | ------------------------------------------------- |
| `mempalace_kg_query`      | `entity`, `as_of?`, `direction?`                                  | Facts about an entity (optionally at a past date) |
| `mempalace_kg_add`        | `subject`, `predicate`, `object`, `valid_from?`, `source_closet?` | Assert a fact                                     |
| `mempalace_kg_invalidate` | `subject`, `predicate`, `object`, `ended?`                        | Mark a fact as no longer true                     |
| `mempalace_kg_timeline`   | `entity?`                                                         | Chronological fact history                        |
| `mempalace_kg_stats`      | —                                                                 | Entity/triple counts, relationship types          |

### Palace Graph

Auto-tunnels are rooms that appear in more than one wing — discovered automatically,
no configuration needed.

| Tool                     | Parameters                | What it does                                 |
| ------------------------ | ------------------------- | -------------------------------------------- |
| `mempalace_traverse`     | `start_room`, `max_hops?` | BFS from a room, discovering connected ideas |
| `mempalace_find_tunnels` | `wing_a?`, `wing_b?`      | Rooms that bridge two wings                  |
| `mempalace_graph_stats`  | —                         | Total rooms, tunnel count, edges             |

### Explicit Tunnels

Use explicit tunnels when content in one project relates to another (e.g. an API design
in `project_api` connects to a schema in `project_database`). Tunnels are symmetric —
A→B and B→A share the same ID.

| Tool                       | Parameters                                                                                                     | What it does                                           |
| -------------------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| `mempalace_create_tunnel`  | `source_wing`, `source_room`, `target_wing`, `target_room`, `label?`, `source_drawer_id?`, `target_drawer_id?` | Create a named cross-wing link between two rooms       |
| `mempalace_list_tunnels`   | `wing?`                                                                                                        | List all explicit tunnels, optionally filtered by wing |
| `mempalace_delete_tunnel`  | `tunnel_id`                                                                                                    | Delete an explicit tunnel by its ID                    |
| `mempalace_follow_tunnels` | `wing`, `room`                                                                                                 | See what a room connects to via explicit tunnels       |

### Agent Diary

Diary entries live in `wing_{agent_name}/diary`. Use AAAK format for compact entries.

| Tool                    | Parameters                      | What it does                       |
| ----------------------- | ------------------------------- | ---------------------------------- |
| `mempalace_diary_write` | `agent_name`, `entry`, `topic?` | Write a diary entry for an agent   |
| `mempalace_diary_read`  | `agent_name`, `last_n?`         | Read the most recent diary entries |

---

## Database Schema

Single SQLite file at `$XDG_DATA_HOME/mempalace/palace.db` (default: `~/.local/share/mempalace/palace.db`):

| Table              | Purpose                                                                                                             |
| ------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `drawers`          | Content chunks: wing, room, content, source_file, chunk_index, added_by, ingest_mode, filed_at                      |
| `drawer_words`     | Inverted index: word → drawer_id → count                                                                            |
| `entities`         | Knowledge graph nodes: name, type, properties (JSON)                                                                |
| `triples`          | Knowledge graph edges: subject, predicate, object, valid_from, valid_to, confidence, source_drawer_id, adapter_name |
| `compressed`       | AAAK-compressed drawer versions                                                                                     |
| `explicit_tunnels` | Agent-created cross-wing links: source/target wing+room, label, canonical SHA256 tunnel_id                          |
