#!/bin/bash
# MEMPALACE PRE-COMPACT HOOK — Synchronous mine before context compression
#
# PreCompact hook. Delegates to
# `mempalace hook --hook precompact --harness "${MEMPAL_HARNESS:-claude-code}"`,
# letting the harness identifier flow from the `MEMPAL_HARNESS` env var (defaulting
# to `claude-code` for legacy installs and `codex` for Codex CLI — see the example
# block below).
# The Rust binary mines the transcript directory synchronously so memories land
# before the compaction window closes, then outputs `{}` to allow compaction.
#
# === INSTALL ===
# Add to .claude/settings.local.json:
#
#   "hooks": {
#     "PreCompact": [{
#       "hooks": [{
#         "type": "command",
#         "command": "/absolute/path/to/hooks/mempal_precompact_hook.sh",
#         "timeout": 120
#       }]
#     }]
#   }
#
# For Codex CLI, add to .codex/hooks.json. The MEMPAL_HARNESS=codex env var is
# required so src/cli/hook.rs sees the correct harness — without it the script
# defaults to "claude-code" and the harness-scoped logic mislabels Codex
# sessions as Claude Code:
#
#   "PreCompact": [{
#     "type": "command",
#     "command": "env MEMPAL_HARNESS=codex /absolute/path/to/hooks/mempal_precompact_hook.sh",
#     "timeout": 120
#   }]
#
# === CONFIGURATION ===
#
# MEMPAL_DIR  — override the directory mined during pre-compact (optional).
#               Defaults to the parent of the transcript_path from stdin.
#
# MEMPALACE_DIR — override the palace data directory (optional).
#                 Defaults to $XDG_DATA_HOME/mempalace or ~/.local/share/mempalace.
#
# MEMPAL_HARNESS — override the harness identifier passed to the hook command (optional).
#                  Defaults to "claude-code". Set to "codex" for Codex CLI installs.
#
# A longer timeout (120s) is appropriate here because the mine runs synchronously.

exec mempalace hook --hook precompact --harness "${MEMPAL_HARNESS:-claude-code}"
