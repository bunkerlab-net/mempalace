# MemPalace Hooks

Two shell hooks ship with MemPalace to auto-save memories during AI
assistant sessions.

## `mempal_save_hook.sh` — Stop hook

Runs at the end of every AI turn.  Counts exchanges, writes a diary
checkpoint when the threshold is reached, and mines the current
transcript into the palace.

**Claude Code** — add to `.claude/settings.local.json`:

    "hooks": {
      "Stop": [{
        "matcher": "*",
        "hooks": [{
          "type": "command",
          "command": "/absolute/path/to/hooks/mempal_save_hook.sh",
          "timeout": 30
        }]
      }]
    }

**Codex CLI** — add to `.codex/hooks.json`:

    "Stop": [{
      "type": "command",
      "command": "/absolute/path/to/hooks/mempal_save_hook.sh",
      "timeout": 30
    }]

## `mempal_precompact_hook.sh` — PreCompact hook

Runs before the context window is compacted so memories are filed
before the transcript is truncated.

**Claude Code** — add to `.claude/settings.local.json`:

    "hooks": {
      "PreCompact": [{
        "hooks": [{
          "type": "command",
          "command": "/absolute/path/to/hooks/mempal_precompact_hook.sh",
          "timeout": 120
        }]
      }]
    }

**Codex CLI** — add to `.codex/hooks.json`:

    "PreCompact": [{
      "type": "command",
      "command": "/absolute/path/to/hooks/mempal_precompact_hook.sh",
      "timeout": 120
    }]

## Plugin-managed installation

If you installed MemPalace via the `.claude-plugin/` or
`.codex-plugin/` manifests the hooks are pre-configured automatically
— no manual edits to settings files are required.

## Configuration

`MEMPALACE_DIR` — override the palace data directory.
`MEMPAL_DIR` — override the directory mined during auto-ingest.

See `mempalace hook --help` for all available options.
