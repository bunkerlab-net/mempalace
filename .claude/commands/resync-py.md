# Resync mempalace-rs with mempalace-py

Analyse all commits in `./mempalace-py` since the last pinned submodule commit and port the applicable changes to this
Rust codebase.

## How to use

```bash
/resync-py
```

## Instructions

### Phase 0 — Branch and commit the submodule update

Do this before any analysis so the submodule pointer is captured on a dedicated branch
regardless of whether Rust changes follow.

1. Record the current (old) submodule commit:

   ```bash
   git submodule status mempalace-py
   ```

2. Advance the submodule:

   ```bash
   git submodule update --init --remote mempalace-py
   ```

3. Identify the new commit hash:

   ```bash
   git diff mempalace-py
   ```

   The `+Subproject commit <new>` line gives the full hash. Use the first 7 characters as `<short>`.

4. Create a branch named after the new commit and commit the pointer:

   ```bash
   git checkout -b resync/mempalace-py/<short>
   git add mempalace-py
   git commit -m "Update mempalace-py submodule to <short>"
   ```

### Phase 1 — Discover what changed in Python

Use `<old>` (from Phase 0 step 1) and `<new>` (from Phase 0 step 3) to diff the Python changes.

1. List all new commits:

   ```bash
   git -C ./mempalace-py log --oneline <old>..<new>
   ```

2. Diff the interesting directory (check stat first; full diff can exceed 30 KB):

   ```bash
   git -C ./mempalace-py diff <old>..<new> --stat -- mempalace/
   ```

   Then diff each interesting file individually.

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
Run `cargo nextest run` and `cargo clippy --all-targets --all-features` after all units are complete.

### Phase 5 — Update documentation

1. Update doc comments on all modified files to reflect new behaviour.
2. Update `README.md`:
   - CLI reference (new commands/flags)
   - Conversation format list
   - Architecture tree (new files)
   - Differences table

### Phase 6 — Commit

The submodule pointer is already committed from Phase 0. Stage only the Rust changes and commit:

```text
Resync with mempalace-py @ <short-hash>

Ports: <bullet list of what was ported>
```

If there are no Rust changes to port, no additional commit is needed — Phase 0 is the only commit.

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
- Python `hashlib.sha256(...)` for **deterministic/idempotent MCP IDs** (add_drawer, diary entries) → use the `sha2`
  crate: `sha2::Sha256::digest(input.as_bytes())`, format the output bytes as lowercase hex via `fold`/`write!`.
  The `md5` crate has been removed; do not re-introduce it.

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
