#!/bin/bash
# MEMPALACE SAVE HOOK — Auto-save every N exchanges
#
# Claude Code "Stop" hook. Delegates to `mempalace hook --hook stop --harness claude-code`.
# The Rust binary counts messages, manages save state, writes diary entries,
# and emits the correct JSON response on stdout.
#
# === INSTALL ===
# Add to .claude/settings.local.json:
#
#   "hooks": {
#     "Stop": [{
#       "matcher": "*",
#       "hooks": [{
#         "type": "command",
#         "command": "/absolute/path/to/hooks/mempal_save_hook.sh",
#         "timeout": 30
#       }]
#     }]
#   }
#
# For Codex CLI, add to .codex/hooks.json. The MEMPAL_HARNESS=codex env var is
# required so src/cli/hook.rs sees the correct harness — without it the script
# defaults to "claude-code" and the harness-scoped logic mislabels Codex
# sessions as Claude Code:
#
#   "Stop": [{
#     "type": "command",
#     "command": "env MEMPAL_HARNESS=codex /absolute/path/to/hooks/mempal_save_hook.sh",
#     "timeout": 30
#   }]
#
# === CONFIGURATION ===
#
# MEMPAL_DIR  — override the directory mined during auto-ingest (optional).
#               Defaults to the parent of the transcript_path from stdin.
#
# MEMPALACE_DIR — override the palace data directory (optional).
#                 Defaults to $XDG_DATA_HOME/mempalace or ~/.local/share/mempalace.
#
# MEMPAL_HARNESS — override the harness identifier passed to the hook command (optional).
#                  Defaults to "claude-code". Set to "codex" for Codex CLI installs.
#
# hook_silent_save — set via `mempalace_hook_settings` MCP tool.
#                    true (default) = write diary directly, emit systemMessage.
#                    false = return {"decision":"block"} and ask Claude to save.

exec mempalace hook --hook stop --harness "${MEMPAL_HARNESS:-claude-code}"
