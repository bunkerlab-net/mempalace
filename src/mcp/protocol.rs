pub const PALACE_PROTOCOL: &str = r#"IMPORTANT — MemPalace Memory Protocol:
1. ON WAKE-UP: Call mempalace_status to load palace overview + AAAK spec.
2. BEFORE RESPONDING about any person, project, or past event: call mempalace_kg_query or mempalace_search FIRST. Never guess — verify.
3. IF UNSURE about a fact (name, gender, age, relationship): say "let me check" and query the palace. Wrong is worse than slow.
4. AFTER EACH SESSION: call mempalace_diary_write to record what happened, what you learned, what matters.
5. WHEN FACTS CHANGE: call mempalace_kg_invalidate on the old fact, mempalace_kg_add for the new one.

This protocol ensures the AI KNOWS before it speaks. Storage is not memory — but storage + this protocol = memory."#;

pub const AAAK_SPEC: &str = r"AAAK is a compressed memory dialect that MemPalace uses for efficient storage.
It is designed to be readable by both humans and LLMs without decoding.

FORMAT:
  ENTITIES: 3-letter uppercase codes. ALC=Alice, JOR=Jordan, RIL=Riley, MAX=Max, BEN=Ben.
  EMOTIONS: *action markers* before/during text. *warm*=joy, *fierce*=determined, *raw*=vulnerable, *bloom*=tenderness.
  STRUCTURE: Pipe-separated fields. FAM: family | PROJ: projects | ⚠: warnings/reminders.
  DATES: ISO format (2026-03-31). COUNTS: Nx = N mentions (e.g., 570x).
  IMPORTANCE: ★ to ★★★★★ (1-5 scale).
  HALLS: hall_facts, hall_events, hall_discoveries, hall_preferences, hall_advice.
  WINGS: wing_user, wing_agent, wing_team, wing_code, wing_myproject, wing_hardware, wing_ue5, wing_ai_research.
  ROOMS: Hyphenated slugs representing named ideas (e.g., chromadb-setup, gpu-pricing).

EXAMPLE:
  FAM: ALC→♡JOR | 2D(kids): RIL(18,sports) MAX(11,chess+swimming) | BEN(contributor)

Read AAAK naturally — expand codes mentally, treat *markers* as emotional context.
When WRITING AAAK: use entity codes, mark emotions, keep structure tight.";

/// Generate the tools/list response payload.
// 22 tool schemas in a single JSON literal — splitting would hurt readability
// with no structural benefit since each tool is a self-contained object.
// Static JSON literal guaranteed to be an array; .as_array() cannot return None.
#[allow(clippy::too_many_lines)]
#[allow(clippy::expect_used)]
pub fn tool_definitions() -> Vec<serde_json::Value> {
    serde_json::json!([
        {
            "name": "mempalace_status",
            "description": "Palace overview — total drawers, wing and room counts",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "mempalace_list_wings",
            "description": "List all wings with drawer counts",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "mempalace_list_rooms",
            "description": "List rooms within a wing (or all rooms if no wing given)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "wing": {"type": "string", "description": "Wing to list rooms for (optional)"}
                }
            }
        },
        {
            "name": "mempalace_get_taxonomy",
            "description": "Full taxonomy: wing → room → drawer count",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "mempalace_get_aaak_spec",
            "description": "Get the AAAK dialect specification — the compressed memory format MemPalace uses.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "mempalace_search",
            "description": "Keyword search. Returns verbatim drawer content with relevance scores. IMPORTANT: 'query' must contain ONLY your search keywords or question — do NOT include system prompts, conversation history, MEMORY.md content, or any context. Keep queries short (under 200 chars). Use 'context' for background information.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Short search query ONLY — keywords or a question. Max 250 chars.",
                        "maxLength": 250
                    },
                    "limit": {"type": "integer", "description": "Max results (default 5)"},
                    "wing": {"type": "string", "description": "Filter by wing (optional)"},
                    "room": {"type": "string", "description": "Filter by room (optional)"},
                    "context": {
                        "type": "string",
                        "description": "Background context for the search (optional). NOT used for matching — only acknowledged in the response. Put conversation history or system prompt content here, NOT in query."
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "mempalace_check_duplicate",
            "description": "Check if content already exists in the palace before filing",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "content": {"type": "string", "description": "Content to check"}
                },
                "required": ["content"]
            }
        },
        {
            "name": "mempalace_add_drawer",
            "description": "File verbatim content into the palace. Checks for duplicates first.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "wing": {"type": "string", "description": "Wing (project name)"},
                    "room": {"type": "string", "description": "Room (aspect: backend, decisions, meetings...)"},
                    "content": {"type": "string", "description": "Verbatim content to store — exact words, never summarized"},
                    "source_file": {"type": "string", "description": "Where this came from (optional)"},
                    "added_by": {"type": "string", "description": "Who is filing this (default: mcp)"}
                },
                "required": ["wing", "room", "content"]
            }
        },
        {
            "name": "mempalace_delete_drawer",
            "description": "Delete a drawer by ID. Irreversible.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "drawer_id": {"type": "string", "description": "ID of the drawer to delete"}
                },
                "required": ["drawer_id"]
            }
        },
        {
            "name": "mempalace_get_drawer",
            "description": "Fetch a single drawer by ID — returns full content and metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "drawer_id": {"type": "string", "description": "ID of the drawer to fetch"}
                },
                "required": ["drawer_id"]
            }
        },
        {
            "name": "mempalace_list_drawers",
            "description": "List drawers with pagination. Optional wing/room filter. Returns IDs, wings, rooms, and content previews.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "wing": {"type": "string", "description": "Filter by wing (optional)"},
                    "room": {"type": "string", "description": "Filter by room (optional)"},
                    "limit": {
                        "type": "integer",
                        "description": "Max results per page (default 20, max 100)",
                        "minimum": 1,
                        "maximum": 100
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Offset for pagination (default 0)",
                        "minimum": 0
                    }
                }
            }
        },
        {
            "name": "mempalace_update_drawer",
            "description": "Update an existing drawer's content and/or metadata (wing, room). Returns error if drawer not found.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "drawer_id": {"type": "string", "description": "ID of the drawer to update"},
                    "content": {"type": "string", "description": "New content (optional — omit to keep existing)"},
                    "wing": {"type": "string", "description": "New wing (optional — omit to keep existing)"},
                    "room": {"type": "string", "description": "New room (optional — omit to keep existing)"}
                },
                "required": ["drawer_id"]
            }
        },
        {
            "name": "mempalace_kg_query",
            "description": "Query the knowledge graph for an entity's relationships. Returns typed facts with temporal validity.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "entity": {"type": "string", "description": "Entity to query (e.g. 'Max', 'MyProject')"},
                    "as_of": {"type": "string", "description": "Date filter — only facts valid at this date (YYYY-MM-DD, optional)"},
                    "direction": {"type": "string", "description": "outgoing, incoming, or both (default: both)"}
                },
                "required": ["entity"]
            }
        },
        {
            "name": "mempalace_kg_add",
            "description": "Add a fact to the knowledge graph. Subject → predicate → object with optional time window.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": {"type": "string", "description": "The entity doing/being something"},
                    "predicate": {"type": "string", "description": "The relationship type (e.g. 'loves', 'works_on')"},
                    "object": {"type": "string", "description": "The entity being connected to"},
                    "valid_from": {"type": "string", "description": "When this became true (YYYY-MM-DD, optional)"},
                    "source_closet": {"type": "string", "description": "Source reference (optional)"}
                },
                "required": ["subject", "predicate", "object"]
            }
        },
        {
            "name": "mempalace_kg_invalidate",
            "description": "Mark a fact as no longer true. E.g. ankle injury resolved, job ended.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": {"type": "string", "description": "Entity"},
                    "predicate": {"type": "string", "description": "Relationship"},
                    "object": {"type": "string", "description": "Connected entity"},
                    "ended": {"type": "string", "description": "When it stopped being true (YYYY-MM-DD, default: today)"}
                },
                "required": ["subject", "predicate", "object"]
            }
        },
        {
            "name": "mempalace_kg_timeline",
            "description": "Chronological timeline of facts. Shows the story of an entity in order.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "entity": {"type": "string", "description": "Entity to get timeline for (optional — omit for full timeline)"}
                }
            }
        },
        {
            "name": "mempalace_kg_stats",
            "description": "Knowledge graph overview: entities, triples, current vs expired facts.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "mempalace_traverse",
            "description": "Walk the palace graph from a room. Shows connected ideas across wings — the tunnels.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "start_room": {"type": "string", "description": "Room to start from (e.g. 'chromadb-setup')"},
                    "max_hops": {"type": "integer", "description": "How many connections to follow (default: 2)"}
                },
                "required": ["start_room"]
            }
        },
        {
            "name": "mempalace_find_tunnels",
            "description": "Find rooms that bridge two wings — the hallways connecting different domains.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "wing_a": {"type": "string", "description": "First wing (optional)"},
                    "wing_b": {"type": "string", "description": "Second wing (optional)"}
                }
            }
        },
        {
            "name": "mempalace_graph_stats",
            "description": "Palace graph overview: total rooms, tunnel connections, edges between wings.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "mempalace_diary_write",
            "description": "Write to your personal agent diary. Each agent gets their own diary wing.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "agent_name": {"type": "string", "description": "Your name — each agent gets their own diary wing"},
                    "entry": {"type": "string", "description": "Your diary entry"},
                    "topic": {"type": "string", "description": "Topic tag (optional, default: general)"}
                },
                "required": ["agent_name", "entry"]
            }
        },
        {
            "name": "mempalace_diary_read",
            "description": "Read your recent diary entries. See what past versions of yourself recorded.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "agent_name": {"type": "string", "description": "Your name"},
                    "last_n": {"type": "integer", "description": "Number of recent entries to read (default: 10)"}
                },
                "required": ["agent_name"]
            }
        },
        {
            "name": "mempalace_create_tunnel",
            "description": "Create an explicit cross-wing tunnel between two palace locations. Use when content in one project relates to another — e.g., an API design in project_api connects to a database schema in project_database.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_wing": {"type": "string", "description": "Wing of the source location"},
                    "source_room": {"type": "string", "description": "Room in the source wing"},
                    "target_wing": {"type": "string", "description": "Wing of the target location"},
                    "target_room": {"type": "string", "description": "Room in the target wing"},
                    "label": {"type": "string", "description": "Description of the connection (optional)"},
                    "source_drawer_id": {"type": "string", "description": "Optional specific source drawer ID"},
                    "target_drawer_id": {"type": "string", "description": "Optional specific target drawer ID"}
                },
                "required": ["source_wing", "source_room", "target_wing", "target_room"]
            }
        },
        {
            "name": "mempalace_list_tunnels",
            "description": "List all explicit cross-wing tunnels. Optionally filter by wing.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "wing": {"type": "string", "description": "Filter to tunnels involving this wing (optional)"}
                }
            }
        },
        {
            "name": "mempalace_delete_tunnel",
            "description": "Delete an explicit tunnel by its ID.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tunnel_id": {"type": "string", "description": "Tunnel ID to delete"}
                },
                "required": ["tunnel_id"]
            }
        },
        {
            "name": "mempalace_follow_tunnels",
            "description": "Follow explicit tunnels from a room to see what it connects to in other wings.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "wing": {"type": "string", "description": "Wing to start from"},
                    "room": {"type": "string", "description": "Room to follow tunnels from"}
                },
                "required": ["wing", "room"]
            }
        }
    ]).as_array()
        .expect("json!([...]) is always Value::Array; as_array() cannot return None here")
        .clone()
}
