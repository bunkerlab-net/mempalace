# MemPalace Init

Guide the user through a complete MemPalace setup. Follow each step in order,
stopping to report errors and attempt remediation before proceeding.

## Step 1: Verify the binary

Run `mempalace --version` to confirm the binary is available and show the
version. If the command is not found, tell the user to install mempalace and
stop.

## Step 2: Check for an existing palace

Run `mempalace status` to see if a palace already exists. If one is found,
report the current stats and skip to Step 5.

## Step 3: Ask for project directory

Ask the user which project directory they want to initialize with MemPalace.
Offer the current working directory as the default. Wait for their response
before continuing.

## Step 4: Initialize the palace

Run `mempalace init --yes <dir>` where `<dir>` is the directory from Step 3.

If this fails, report the error and stop.

## Step 5: Configure MCP server

Register the MemPalace MCP server with Claude Code:

    claude mcp add mempalace -- mempalace mcp

If this fails, report the error but continue to the next step (MCP
configuration can be done manually later).

## Step 6: Verify installation

Run `mempalace status` and confirm the output shows a healthy palace.

If the command fails or reports errors, walk the user through troubleshooting
based on the output.

## Step 7: Show next steps

Tell the user setup is complete and suggest these next actions:

- Use /mempalace:mine to start adding data to their palace
- Use /mempalace:search to query their palace and retrieve stored knowledge
