# Style Guide

Adapted from [TigerBeetle's TIGER_STYLE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
for Rust. Design goals are **safety**, **performance**, and **developer experience**
— in that order.

## Core Philosophy

- Simplicity is not the first attempt but the hardest revision. Spend mental energy
  upfront.
- **Zero technical debt policy**: do it right the first time. The second time may not
  come.
- An hour of design is worth weeks in production.
- **Always say why.** Explain the rationale for every decision — in comments, commit
  messages, and code.

---

## Safety

### Assertions

Assert all function arguments and return values, preconditions, postconditions, and
invariants. The assertion density of the code must average a minimum of **two
assertions per function**.

Use `assert!` for cheap invariants that indicate logic bugs. Use `debug_assert!` for
expensive postconditions (e.g. verifying sort order of a result set). Both are
appropriate — `assert!` stays in release builds, `debug_assert!` is stripped.

```rust
// Preconditions: assert arguments.
pub fn add_drawer(connection: &Connection, params: &DrawerParams) -> Result<bool> {
    assert!(!params.id.is_empty());
    assert!(!params.wing.is_empty());
    assert!(!params.room.is_empty());
    assert!(!params.content.is_empty());

    let rows_affected = execute_insert(connection, params).await?;

    // Postcondition: INSERT OR IGNORE affects at most one row.
    assert!(rows_affected <= 1);

    Ok(rows_affected == 1)
}
```

**Pair assertions**: for every property to enforce, find at least two code paths for
assertions. For example, assert validity before writing to the database, then assert
again after reading back:

```rust
// Before write:
assert!(triple_id.starts_with("t_"));
execute_insert(connection, &triple).await?;

// After write (pair assertion):
let count = query_count(connection, "SELECT count(*) FROM triples WHERE id = ?", &triple_id).await?;
assert!(count == 1, "pair assertion: triple must exist after insert");
```

Split compound assertions — prefer `assert!(a); assert!(b);` over `assert!(a && b)`.

Assert **positive space** (what you expect) AND **negative space** (what you don't
expect):

```rust
// Positive space: result is non-empty and trimmed.
assert!(!name.is_empty());
assert!(name == name.trim());

// Negative space: result does not contain path traversal characters.
assert!(!name.contains(".."));
assert!(!name.contains('/'));
assert!(!name.contains('\\'));
assert!(!name.contains('\0'));
```

Use single-line `if` for implications: `if a { assert!(b); }`

Assert compile-time constant relationships to document and enforce invariants:

```rust
const CHUNK_SIZE: usize = 800;
const CHUNK_OVERLAP: usize = 100;
const _: () = assert!(CHUNK_OVERLAP < CHUNK_SIZE);
```

### Control Flow

- Use only simple, explicit control flow. **No recursion** for bounded executions.
  Use iterative approaches with explicit stacks and depth limits.
- Use **only a minimum of excellent abstractions** — every abstraction has a cost and
  leak risk.
- **Put a limit on everything**: all loops and queues must have a fixed upper bound.
- Split compound conditions into simple nested `if/else` branches.
- State invariants positively. Prefer `if index < length` over `if index >= length`.
- Consider whether every `if` also needs a matching `else` to handle negative space.

```rust
// Iterative directory walk with depth limit, replacing recursion.
const DEPTH_LIMIT: usize = 64;
let mut stack: Vec<(PathBuf, usize)> = vec![(root, 0)];

while let Some((dir, depth)) = stack.pop() {
    assert!(depth <= DEPTH_LIMIT);
    if depth >= DEPTH_LIMIT {
        continue;
    }
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.is_dir() {
            stack.push((path, depth + 1));
        } else {
            files.push(path);
        }
    }
}
```

Loop bounds — every loop must have a documented upper bound:

```rust
const REQUEST_LIMIT: usize = 1_000_000;
let mut request_count: usize = 0;

while let Some(line) = reader.next_line().await? {
    assert!(request_count < REQUEST_LIMIT, "request limit exceeded");
    request_count += 1;
    // ...
}
```

### Memory and Variables

Rust's ownership system provides strong memory safety guarantees. Lean into it:

- Declare variables at the **smallest possible scope**.
- **Minimize the number of variables in scope** to reduce misuse probability.
- Don't duplicate variables or take aliases — reduces probability of state getting
  out of sync.
- Calculate or check variables close to where/when they are used. Avoid POCPOU bugs.
- Pass large arguments by `&` (shared reference) when they should not be copied.
- Prefer stack allocation and pre-sized `Vec::with_capacity` over repeated dynamic
  growth.

### Functions

**Hard limit: 70 lines per function.**

- Good function shape: few parameters, simple return type, meaty logic.
- Centralize control flow in parent functions; move non-branchy logic to helpers.
- ["Push `if`s up and `for`s down."](https://matklad.github.io/2023/11/15/push-ifs-up-and-fors-down.html)
- Keep leaf functions pure; let parents manage state.
- Prefix helper names with the calling function name:
  `mine()` and `mine_resolve_wing()`, `mine_process_file()`.

**All errors must be handled.** Use `Result<T>` and the `?` operator. Mark return
types with `#[must_use]` where callers must not silently discard the value.

**Explicitly pass options to library functions** at the call site — never rely on
defaults.

Ensure functions run to completion without suspending so precondition assertions stay
valid. In async Rust, if a function holds local invariants, do not `.await` in the
middle of a section protected by those invariants.

**Data-only exemptions**: Functions that are purely declarative data (JSON schema
definitions, stop-word lists) may exceed 70 lines with an explicit exemption comment:

```rust
// TigerStyle exemption: declarative data, not logic.
#[allow(clippy::too_many_lines)]
fn tool_definitions() -> Value { ... }
```

### Types and Sizes

- Use explicitly-sized types (`u32`, `i64`, `f64`) rather than architecture-dependent
  types at serialization boundaries (database columns, wire protocols).
- `usize` is appropriate for Rust-internal indexing and collection sizes.
- Document truncation casts with `#[allow(clippy::cast_possible_truncation)]` and a
  "why" comment.
- Appreciate all **compiler warnings at strictest settings**: `clippy::pedantic`,
  `warnings = "deny"`.

### Off-By-One Errors

- Treat `index`, `count`, and `size` as distinct concepts with clear conversion
  rules: `index` is 0-based, `count` is 1-based, `size = count * unit`.
- Add units or qualifiers to variable names, qualifiers last:
  `latency_ms_max`, not `max_latency_ms`.
- Show intent with division — use explicit functions or annotate whether you expect
  exact, floor, or ceiling division.

### Buffer Safety

Guard against **buffer bleeds**: padding not zeroed correctly can leak sensitive
information or violate deterministic guarantees.

### Error Handling in Rust

Use the project's `Result<T>` type alias with the custom `Error` enum. Use `?` for
propagation. Use `thiserror` for deriving `Display` on error variants.

Do not use `.unwrap()` or `.expect()` in production code — the project denies
`clippy::unwrap_used`. Use `.unwrap_or()`, `.unwrap_or_default()`, or propagate
with `?`.

Assertions (`assert!`) are distinct from error handling — they catch **programmer
errors** (bugs), not **operating errors** (expected failures). A failed assertion
means the code is wrong; a failed `Result` means the environment is hostile.

---

## Performance

- Think about performance **from the design phase** — this is where 1000x wins are
  found.
- **Back-of-the-envelope sketches** across the four resources: network, disk, memory,
  CPU; and their two characteristics: bandwidth, latency.
- Optimize for slowest resources first: network → disk → memory → CPU (after
  adjusting for frequency of use).
- **Amortize costs by batching** network, disk, memory, and CPU accesses.
- Distinguish control plane from data plane — batching enables high assertion safety
  without losing performance.
- Be predictable. Don't force the CPU to zig-zag. Give it large chunks of work.
- Be explicit. Minimize dependence on the compiler to do the right thing.

---

## Developer Experience

### Naming

- **Get the nouns and verbs just right.** Great names capture what a thing is or
  does.
- Use `snake_case` for functions, variables, and file names.
- **Do not abbreviate** variable names (except primitive integers in sort/matrix
  code). No `conn` — write `connection`. No `vf` — write `valid_from_value`.
- Use proper capitalization for acronyms: `MCPServer`, not `McpServer`.
- Add **units or qualifiers last**, sorted by descending significance:
  `latency_ms_max`.
- Infuse names with meaning — good names inform the reader of semantics and
  ownership.
- When choosing related names, prefer equal character counts so variables line up:
  `source`/`target` over `src`/`dest`.
- Prefix helper/callback names with the calling function:
  `mine()` → `mine_resolve_wing()`, `mine_process_file()`.
- Callbacks go last in parameter lists.
- **Order matters**: put important things near the top of files. `main` goes first.
  For structs: fields → types → methods.
- Don't overload names with multiple context-dependent meanings.
- Prefer nouns over adjectives/participles for descriptors — nouns compose better.

**Project-specific acronym exceptions**: `MCP` (Model Context Protocol) is an
industry-standard protocol acronym, like HTTP. It may be used as a module name.

### Comments and Commits

- **Don't forget to say why.** Code is not documentation.
- **Don't forget to say how.** Explain methodology, especially in tests.
- Comments are sentences: space after `//`, capital letter, full stop (or colon
  before related code). End-of-line comments can be phrases without punctuation.
- **Write descriptive commit messages** — commit messages are read; PR descriptions
  are not stored in git and are invisible in `git blame`.

### Formatting

Run `cargo fmt`. This is the authoritative formatter for the project. Do not
enforce a manual column limit — defer to rustfmt's defaults.

### Dependencies

**Minimal dependencies.** If you only need a specific feature or function from a
dependency, prefer reimplementing it over pulling in the entire package. Every
dependency introduces supply chain risk, safety/performance risk, and maintenance
burden.

**Standardize your toolbox.** A small, consistent set of tools is simpler to operate
than an array of specialized instruments.

> "The right tool for the job is often the tool you are already using—adding new
> tools has a higher cost than many people appreciate." — John Carmack

### Clippy Configuration

The project enforces strict clippy lints:

```toml
[lints.rust]
unsafe_code = "deny"
warnings = "deny"

[lints.clippy]
pedantic = { level = "deny", priority = -1 }
enum_glob_use = "deny"
unwrap_used = "deny"
```

All code must pass `cargo clippy` with zero warnings before commit.

**Lint suppression requires justification.** Any time a clippy lint is suppressed with
`#[allow(...)]`, an inline comment must be added on the line immediately above (or on
the same line) explaining *why* the suppression is necessary:

```rust
// Byte lengths for display-only ratio; precision loss negligible for practical sizes.
#[allow(clippy::cast_precision_loss)]
let ratio = original_len as f64 / compressed_len as f64;

// Large static stopword list — line count reflects data volume, not code complexity.
#[allow(clippy::too_many_lines)]
fn stopwords() -> HashSet<&'static str> { ... }
```

Suppressions without a justification comment are not acceptable.
