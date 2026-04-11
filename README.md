# Mempalace

A local-first memory palace for AI assistants. Single static binary backed by embedded SQLite (turso).
No Python, no ChromaDB, no API keys.

**Drop-in replacement for [milla-jovovich/mempalace](https://github.com/milla-jovovich/mempalace) with a ~13MB binary instead of a ~100MB Python environment.**

---

## Why

The Python version used ChromaDB + SQLite. Under multiple simultaneous MCP clients,
SQLite locking caused dropped writes. ChromaDB also carried a large dependency footprint
and required Python to be installed.

This reimplementation:

- Ships as a single self-contained binary
- Replaces ChromaDB semantic search with a keyword inverted index (BM25-style scoring via `drawer_words`)
- Fixes the concurrency problem at the turso layer
- Keeps all 19 MCP tools and all CLI commands fully compatible

**Trade-off:** Keyword search instead of embedding-based semantic search.
Semantic search is deferred until an embedded model is available without network dependencies.

---

## Installation

```bash
git clone https://github.com/bunkerlab-net/mempalace.git
cd mempalace
cargo build --release
# binary is at: target/release/mempalace
```

Optionally copy to a location on your PATH:

```bash
cp target/release/mempalace ~/.local/bin/mempalace
```

---

## MCP Setup (Claude Code)

```bash
claude mcp add mempalace -- /path/to/mempalace mcp
```

The MCP server runs as a JSON-RPC 2.0 process over stdio. All 19 tools are available immediately
after the server starts.

On first use, call `mempalace_status` — it returns the full memory protocol and AAAK dialect spec
in the response, so the AI learns how to use the palace during wake-up.

---

## Quick Start

```bash
# 1. Initialise a project (creates mempalace.yaml)
mempalace init ~/my-project

# 2. Mine project files into the palace
mempalace mine ~/my-project

# 3. Mine conversation transcripts
mempalace mine ~/my-transcripts --mode convos

# 4. Search
mempalace search "chromadb locking"

# 5. Generate wake-up context (L0 identity + L1 essential story)
mempalace wake-up
```

---

## CLI Reference

### `mempalace init <dir>`

Scans a project directory, detects rooms from the folder structure, and writes `mempalace.yaml`.

```bash
mempalace init ~/my-project
mempalace init ~/my-project --yes        # non-interactive / CI mode
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

**Projects mode** (`--mode projects`): Reads source files (`.py`, `.rs`, `.ts`, `.go`, `.md`, etc.),
chunks at 800-character boundaries with 100-character overlap, routes each chunk to a room
via folder/filename/keyword heuristics. Respects `.gitignore` rules by default (same engine as
ripgrep); pass `--no-gitignore` to include all files regardless.

**Convos mode** (`--mode convos`): Reads conversation exports in any of these formats:

- Claude Code JSONL
- OpenAI Codex CLI JSONL (`~/.codex/sessions/*/rollout-*.jsonl`)
- Claude.ai JSON (standard export and privacy export)
- ChatGPT JSON
- Slack JSON export
- Plain text with `>` quote markers

All formats are normalised to `> prompt\nresponse\n\n` before chunking.
Chunks are one exchange pair (user turn + AI response) each.
Room detection uses content keywords to assign topics
(`technical`, `architecture`, `planning`, `decisions`, `problems`, `general`).

Files already in the palace (matched by path) are skipped automatically. Move or rename a file
to re-mine it.

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

---

### `mempalace wake-up`

Prints L0 + L1 context for loading at the start of a session (~600–900 tokens total).

```bash
mempalace wake-up
mempalace wake-up --wing wing_myproject
```

- **L0 (identity):** Contents of `~/.mempalace/identity.txt` (~100 tokens).
  Write this file yourself to describe the AI's role, the people it knows, and the projects it works on.
- **L1 (essential story):** The 15 most-recent drawers grouped by room, capped at 3200 characters.

---

### `mempalace compress`

Compresses drawers into AAAK dialect format and stores them in the `compressed` table.

```bash
mempalace compress
mempalace compress --wing wing_code
mempalace compress --dry-run
mempalace compress --config entities.json
```

AAAK is a structured symbolic notation readable by any LLM without a decoder.
Compression ratio depends heavily on how many repeated named entities appear in your content;
it is **lossy** (original text cannot be reconstructed from AAAK output).
Entity names are abbreviated to 3-letter codes (ALC, JOR, RIL, etc.).
Emotions and flags are extracted from content.

The optional `--config` JSON file maps full names to their codes:

```json
{
  "entities": {
    "Alice": "ALC",
    "Jordan": "JOR"
  }
}
```

---

### `mempalace split <dir>`

Splits concatenated transcript mega-files into per-session files.

```bash
mempalace split ~/transcripts
mempalace split ~/transcripts --output-dir ~/sessions
mempalace split ~/transcripts --dry-run
mempalace split ~/transcripts --min-sessions 3
```

Detects true session starts from `Claude Code v` headers, filtering out context-restore continuations.
Original files are renamed to `.mega_backup`. Run this before `mine --mode convos`.

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

## Configuration

### `~/.mempalace/config.json`

Created automatically on first run. Rarely needs manual editing.

```json
{
  "palace_path": "~/.mempalace/palace.db",
  "collection_name": "mempalace_drawers",
  "people_map": {}
}
```

| Field             | Purpose                                                                |
| ----------------- | ---------------------------------------------------------------------- |
| `palace_path`     | Path to the SQLite database file                                       |
| `collection_name` | Legacy field (unused; kept for config compatibility with mempalace-py) |
| `people_map`      | Optional name → code mappings for AAAK compression                     |

Override the database path without editing the file:

```bash
export MEMPALACE_PALACE_PATH=/path/to/palace.db
```

---

### `<project>/mempalace.yaml`

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

### `~/.mempalace/identity.txt`

Plain text describing the AI's identity, loaded as L0 during `wake-up`. Write this yourself;
it is never auto-generated.

```text
I am Atlas, assistant to Alice.
People: Alice (engineer, creator), Jordan (Alice's partner), Riley (18, athlete), Max (11, chess).
Projects: mempalace, homelab.
Traits: direct, memory-first, no summaries.
```

---

## MCP Tools (19)

All tools communicate over JSON-RPC 2.0. Invoke them from the AI side via the MCP protocol.

### Palace / Drawers

| Tool                        | Parameters                                             | What it does                                                                |
| --------------------------- | ------------------------------------------------------ | --------------------------------------------------------------------------- |
| `mempalace_status`          | —                                                      | Overview + memory protocol + AAAK spec                                      |
| `mempalace_list_wings`      | —                                                      | Wing names with drawer counts                                               |
| `mempalace_list_rooms`      | `wing?`                                                | Room names with counts (all wings or one)                                   |
| `mempalace_get_taxonomy`    | —                                                      | Full `wing → room → count` hierarchy                                        |
| `mempalace_get_aaak_spec`   | —                                                      | AAAK dialect specification                                                  |
| `mempalace_search`          | `query`, `limit?`, `wing?`, `room?`, `context?`        | Keyword search, returns `similarity` scores; sanitizes contaminated queries |
| `mempalace_check_duplicate` | `content`                                              | True if highly similar content already exists                               |
| `mempalace_add_drawer`      | `wing`, `room`, `content`, `source_file?`, `added_by?` | File a memory; blocks on duplicates                                         |
| `mempalace_delete_drawer`   | `drawer_id`                                            | Permanently delete a drawer and its index entries                           |

`mempalace_add_drawer` performs a duplicate check before inserting.
If a highly similar drawer already exists it returns
`{"success": false, "reason": "duplicate", "matches": [...]}` without writing.

### Knowledge Graph

| Tool                      | Parameters                                                        | What it does                                      |
| ------------------------- | ----------------------------------------------------------------- | ------------------------------------------------- |
| `mempalace_kg_query`      | `entity`, `as_of?`, `direction?`                                  | Facts about an entity (optionally at a past date) |
| `mempalace_kg_add`        | `subject`, `predicate`, `object`, `valid_from?`, `source_closet?` | Assert a fact                                     |
| `mempalace_kg_invalidate` | `subject`, `predicate`, `object`, `ended?`                        | Mark a fact as no longer true                     |
| `mempalace_kg_timeline`   | `entity?`                                                         | Chronological fact history                        |
| `mempalace_kg_stats`      | —                                                                 | Entity/triple counts, relationship types          |

Facts are temporal: every triple can have `valid_from` and `valid_to` dates.
Querying with `as_of` returns only facts that were true at that moment.
Invalidation preserves the old fact with a `valid_to` date rather than deleting it.

### Palace Graph

| Tool                     | Parameters                | What it does                                 |
| ------------------------ | ------------------------- | -------------------------------------------- |
| `mempalace_traverse`     | `start_room`, `max_hops?` | BFS from a room, discovering connected ideas |
| `mempalace_find_tunnels` | `wing_a?`, `wing_b?`      | Rooms that bridge two wings                  |
| `mempalace_graph_stats`  | —                         | Total rooms, tunnel count, edges             |

Tunnels are rooms that appear in more than one wing — they are automatic cross-wing connections,
no configuration needed.

### Agent Diary

| Tool                    | Parameters                      | What it does                       |
| ----------------------- | ------------------------------- | ---------------------------------- |
| `mempalace_diary_write` | `agent_name`, `entry`, `topic?` | Write a diary entry for an agent   |
| `mempalace_diary_read`  | `agent_name`, `last_n?`         | Read the most recent diary entries |

Diary entries live in `wing_{agent_name}/diary`. Use AAAK format for compact entries.

---

## Database Schema

Single SQLite file at `~/.mempalace/palace.db`:

| Table          | Purpose                                                                                        |
| -------------- | ---------------------------------------------------------------------------------------------- |
| `drawers`      | Content chunks: wing, room, content, source_file, chunk_index, added_by, ingest_mode, filed_at |
| `drawer_words` | Inverted index: word → drawer_id → count                                                       |
| `entities`     | Knowledge graph nodes: name, type, properties (JSON)                                           |
| `triples`      | Knowledge graph edges: subject, predicate, object, valid_from, valid_to, confidence            |
| `compressed`   | AAAK-compressed drawer versions                                                                |

---

## Architecture

```text
src/
  main.rs              Entry point: clap dispatch → open_palace() → handler
  db.rs                open_db(), query_all() helpers over turso::Connection
  schema.rs            DDL: 5 tables + indexes, ensure_schema()
  config.rs            MempalaceConfig (~/.mempalace/config.json) + ProjectConfig (mempalace.yaml)
  error.rs             thiserror Error enum

  cli/                 One file per subcommand
    init.rs            Room detection → write mempalace.yaml (--yes skips prompt)
    search.rs          CLI search output
    wakeup.rs          L0 + L1 assembly and print
    compress.rs        AAAK batch compression
    split.rs           Mega-file session splitter
    status.rs          Palace stats display
    repair.rs          Backup + rebuild inverted index

  palace/
    miner.rs           Project file scanner + chunker + drawer writer; MineParams struct
    convo_miner.rs     Conversation file scanner + normaliser + drawer writer
    drawer.rs          add_drawer(), file_already_mined(), inverted index maintenance
    chunker.rs         chunk_text(): 800-char chunks with 100-char overlap
    search.rs          search_memories(): inverted index query with relevance scoring
    room_detect.rs     70+ folder-to-room mappings, detect_room(), detect_rooms_from_folders()
    query_sanitizer.rs 4-step sanitizer: strip system-prompt contamination from search queries
    entity_detect.rs   Person vs project heuristic classifier
    layers.rs          L0 identity + L1 essential story assembly
    graph.rs           BFS traversal, tunnel detection

  kg/
    mod.rs             Entity + triple CRUD
    query.rs           query_entity(), kg_timeline()

  normalize/           Chat export parsers → canonical transcript text
    claude_code.rs     JSONL (Claude Code); accepts both "human" and "user" types
    claude_ai.rs       JSON array (Claude.ai) + privacy export format
    codex.rs           JSONL (OpenAI Codex CLI)
    chatgpt.rs         ChatGPT export JSON
    slack.rs           Slack export JSON

  dialect/             AAAK compression
    mod.rs             compress(): header + content line assembly
    emotions.rs        38 emotion codes, keyword → code mapping
    topics.rs          Topic extraction with proper-noun frequency boost

  extract/             Memory type classifier (used in general extraction mode)
    mod.rs             5-type classifier: decision, preference, milestone, problem, emotional
    markers.rs         ~80 regex patterns

  mcp/
    mod.rs             Async stdio JSON-RPC 2.0 event loop
    protocol.rs        PALACE_PROTOCOL, AAAK_SPEC, 19 tool schemas
    tools.rs           Tool dispatch + all 19 handler implementations
```

---

## Differences from mempalace-py

| Area                           | Python                        | Rust                                |
| ------------------------------ | ----------------------------- | ----------------------------------- |
| Search                         | ChromaDB semantic / embedding | Keyword inverted index (BM25-style) |
| `mempalace_search` score field | `similarity` (0–1 cosine)     | `similarity` (word hit count)       |
| Storage                        | ChromaDB + SQLite             | Single turso (SQLite) file          |
| Binary size                    | ~100MB Python env             | ~13MB binary                        |
| Concurrency                    | SQLite locking issues         | WAL mode; resolved at turso layer   |
| Duplicate detection            | 0.9 cosine threshold          | Keyword overlap threshold           |
| Entity registry                | Wikipedia lookups             | Heuristic only (deferred)           |
| Onboarding wizard              | Interactive                   | Not implemented (deferred)          |
| ChromaDB import                | N/A                           | Not implemented (deferred)          |
| Gitignore support              | Full (projects)               | Full (`ignore` crate)               |
| Repair command                 | Yes                           | Yes (`mempalace repair`)            |
| Conversation formats           | Limited                       | Extended (+ Codex CLI)              |
| MCP error responses            | Generic                       | Generic                             |
| Query sanitizer                | Yes (issue #333)              | Yes (ported from mempalace-py)      |

---

## Lint Configuration

```toml
[lints.rust]
unsafe_code = "forbid"
warnings = "deny"

[lints.clippy]
enum_glob_use = "deny"
pedantic = { level = "deny", priority = -1 }
unwrap_used = "deny"
```

All clippy suppressions have an inline comment explaining why the lint cannot be resolved without harming correctness
or readability.

---

## Tests

```bash
cargo test
```
