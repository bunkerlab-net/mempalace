#!/bin/bash
# MEMPALACE PRE-COMPACT HOOK — Synchronous mine before context compression
#
# Claude Code "PreCompact" hook. Delegates to `mempalace hook run --hook precompact`.
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
# For Codex CLI, add to .codex/hooks.json:
#
#   "PreCompact": [{
#     "type": "command",
#     "command": "/absolute/path/to/hooks/mempal_precompact_hook.sh",
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
# A longer timeout (120s) is appropriate here because the mine runs synchronously.

exec mempalace hook --hook precompact --harness claude-code
