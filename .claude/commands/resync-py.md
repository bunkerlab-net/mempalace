# Resync mempalace-rs with mempalace-py

Analyse all commits in `./mempalace-py` since the last recorded sync commit and port the
applicable changes to this Rust codebase.

## How to use

```bash
/resync-py
```

You can optionally pass a base commit to diff from:

```bash
/resync-py [base-commit]
```

If no commit is given, look it up from the git log of this repo — the most recent commit
whose message references a `mempalace-py` commit hash or contains "resync"/"parity" is a
good heuristic.

## Instructions

### Phase 1 — Discover what changed in Python

1. Run `git log --oneline <base-commit>..HEAD` inside `./mempalace-py` to list all new commits.
2. Run `git diff <base-commit>..HEAD` inside `./mempalace-py` to see the full diff.
3. Group changes by theme: security, features, bug fixes, documentation, tests.

### Phase 2 — Determine what's applicable to Rust

Not every Python change has a Rust equivalent. Skip:

- Python build system changes (`pyproject.toml`, `requirements.txt`, `uv.lock`)
- Python-specific idioms with no Rust equivalent (e.g. `usedforsecurity=False` on md5)
- Pure documentation changes that describe Python-only behaviour
- Python test framework changes

**Do port:**

- New or updated MCP tool behaviour
- New conversation format parsers
- Security hardening (input validation, error sanitization, WAL mode, query limits)
- New CLI commands or flags
- Changes to skip-dir lists, file filters, or mining heuristics
- Bug fixes to logic that exists in both codebases

### Phase 3 — Plan

For each applicable change, identify:

- Which Rust file(s) need to change
- Whether the change is a new file, a modification, or a constant update
- Any new crate dependencies required

Present the plan grouped into work units before writing any code.

### Phase 4 — Implement

Work through the plan unit by unit. After each unit, verify with `cargo build`.
Run `cargo test` and `cargo clippy` after all units are complete.

### Phase 5 — Update documentation

1. Update doc comments on all modified files to reflect new behaviour.
2. Update `README.md`:
   - CLI reference (new commands/flags)
   - Conversation format list
   - Architecture tree (new files)
   - Differences table
3. Record the new sync commit in a comment or commit message so the next
   `/resync-py` knows where to start.

### Phase 6 — Commit

Commit with a message like:

```text
Resync with mempalace-py @ <short-hash>

Ports: <bullet list of what was ported>
```

## Key file mappings (Python → Rust)

| Python file                        | Rust equivalent                                |
| ---------------------------------- | ---------------------------------------------- |
| `mempalace/mcp_server.py`          | `src/mcp/mod.rs`, `src/mcp/tools.rs`           |
| `mempalace/normalize.py`           | `src/normalize/mod.rs` + per-format files      |
| `mempalace/miner.py`               | `src/palace/miner.rs`                          |
| `mempalace/convo_miner.py`         | `src/palace/convo_miner.rs`                    |
| `mempalace/knowledge_graph.py`     | `src/kg/mod.rs`, `src/kg/query.rs`             |
| `mempalace/room_detector_local.py` | `src/palace/room_detect.rs`, `src/cli/init.rs` |
| `mempalace/searcher.py`            | `src/palace/search.rs`                         |
| `mempalace/dialect.py`             | `src/dialect/mod.rs`                           |
| `mempalace/cli.py`                 | `src/cli/mod.rs`, `src/main.rs`                |
| `hooks/mempal_save_hook.sh`        | N/A (hook scripts are not part of this repo)   |

## Common patterns

- Python `sys.exit(1)` in library code → Rust `Result::Err` (already correct in Rust)
- Python `except Exception` → Rust already uses typed errors, no change needed
- Python `logger.exception()` → Rust `eprintln!` to stderr (MCP servers must not pollute stdout)
- Python `chromadb.get(limit=10000)` unbounded query guards → Rust SQL `LIMIT` clauses
- Python `hashlib.md5(usedforsecurity=False)` → Rust `uuid::Uuid::new_v4()` (no change needed)
