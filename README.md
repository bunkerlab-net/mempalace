# Mempalace

A local-first memory palace for AI assistants. Single static binary backed by embedded SQLite (turso).
No Python, no ChromaDB, no API keys.

**Drop-in replacement for [MemPalace/mempalace](https://github.com/MemPalace/mempalace) with a ~13MB binary instead of a ~100MB Python environment.**

[![codecov](https://codecov.io/gh/bunkerlab-net/mempalace/graph/badge.svg)](https://codecov.io/gh/bunkerlab-net/mempalace)

---

## Why

The Python version used ChromaDB + SQLite. Under multiple simultaneous MCP clients,
SQLite locking caused dropped writes. ChromaDB also carried a large dependency footprint
and required Python to be installed.

This reimplementation:

- Ships as a single self-contained binary
- Replaces ChromaDB semantic search with a keyword inverted index (BM25-style scoring via `drawer_words`)
- Fixes the concurrency problem at the turso layer
- Keeps all MCP tools and all CLI commands fully compatible

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

The MCP server runs as a JSON-RPC 2.0 process over stdio. All 26 tools are available immediately
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
mempalace mine ~/.claude/projects/ --mode convos

# 4. Search
mempalace search "chromadb locking"

# 5. Generate wake-up context (L0 identity + L1 essential story)
mempalace wake-up
```

See [USAGE.md](USAGE.md) for the full CLI reference, configuration options, and MCP tool descriptions.

---

## Architecture

```text
src/
  main.rs              Entry point: clap dispatch → open_palace() → handler
  db.rs                open_db(), query_all() helpers over turso::Connection
  schema.rs            DDL: 6 tables + indexes, ensure_schema()
  config.rs            MempalaceConfig ($XDG_DATA_HOME/mempalace/config.json) + ProjectConfig (mempalace.yaml)
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
    graph.rs           BFS traversal, auto-tunnel detection, explicit tunnel CRUD

  kg/
    mod.rs             Entity + triple CRUD
    query.rs           query_entity(), kg_timeline()

  normalize/           Chat export parsers → canonical transcript text
    claude_code.rs     JSONL (Claude Code); strip_noise() removes UI chrome / system-reminder tags
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
    protocol.rs        PALACE_PROTOCOL, AAAK_SPEC, 26 tool schemas
    tools.rs           Tool dispatch + all 26 handler implementations
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

## Code Style

See [STYLEGUIDE.md](STYLEGUIDE.md) for the full coding conventions: assertions,
loop bounds, no-abbreviation naming, function length limits, clippy configuration,
and the "always say why" comment rule.

---

## Tests

```bash
cargo nextest run
```
