# Resync mempalace-rs with mempalace-py

Analyse all commits in `./mempalace-py` since the last recorded sync commit and port the applicable changes to this
Rust codebase.

## How to use

```bash
/resync-py
```

You can optionally pass a base commit to diff from:

```bash
/resync-py [base-commit]
```

If no commit is given, look it up from the git log of this repo — the most recent commit whose message references a
`mempalace-py` commit hash or contains "resync"/"parity" is a good heuristic.

## Instructions

### Phase 1 — Discover what changed in Python

`mempalace-py` lives at `./mempalace-py` relative to this repo's root. Use `git -C ./mempalace-py` to run git commands
inside it without changing directory.

1. Pull the latest `main` so you are diffing against current upstream:

   ```bash
   git -C ./mempalace-py fetch origin
   git -C ./mempalace-py checkout main
   git -C ./mempalace-py pull --ff-only
   ```

2. Record the HEAD commit — this becomes the **target hash** in `.claude/local/sync-<date>.md`:

   ```bash
   git -C ./mempalace-py rev-parse HEAD
   ```

3. Run `git -C ./mempalace-py log --oneline <base-commit>..HEAD` to list all new commits.
4. Run `git -C ./mempalace-py diff <base-commit>..HEAD --stat -- mempalace/` first (the full diff can exceed 30 KB).
   Then diff each interesting file individually.
5. Group changes by theme: security, features, bug fixes, documentation, tests.

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
- Python `hashlib.md5(usedforsecurity=False)` in the **miner** (source_file+chunk_index hash) → Rust `uuid::Uuid::new_v4()`
  (no change needed)
- Python `hashlib.md5(content.encode())` for **deterministic/idempotent MCP IDs** → add the `md5` crate and use
  `md5::compute`. This is a distinct case from the miner pattern above.

## Turso API gotchas

- `row.get(idx)` returns `Result<T, Error>`, **not** `Option<T>`. Use `.ok()` for nullable columns: `row.get(0).ok()`.
- `Option<T>` can be passed directly in `turso::params![]`; `None` becomes SQL `NULL`.
- Comparing OS mtimes as `f64` triggers `clippy::float_cmp` (pedantic). The comparison is correct because both values
  originate from the same OS syscall — suppress with `#[allow(clippy::float_cmp)]` and a comment.

## Schema migration pattern

When adding a nullable column to an existing table, do **both**:

1. Add the column to the `CREATE TABLE IF NOT EXISTS` DDL (for new databases).
2. In `ensure_schema`, call `ALTER TABLE … ADD COLUMN` and discard the error — idempotent for existing databases (column
   already present) and new ones (DDL already added it).

```rust
let _ = conn.execute("ALTER TABLE drawers ADD COLUMN new_col REAL", ()).await;
```
