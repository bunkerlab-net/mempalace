use std::collections::{HashMap, HashSet, VecDeque};

use chrono::Utc;
use serde::Serialize;
use sha2::Digest as _;
use turso::Connection;

use crate::config::normalize_wing_name;
use crate::db::query_all;
use crate::error::Result;

/// A room node in the palace graph.
#[derive(Debug, Clone, Serialize)]
pub struct RoomNode {
    /// Room name.
    pub room: String,
    /// Wings that contain this room.
    pub wings: Vec<String>,
    /// Total drawer count across all wings.
    pub count: usize,
}

/// A tunnel edge: a room that spans multiple wings, connecting them.
#[derive(Debug, Clone, Serialize)]
pub struct TunnelEdge {
    /// The shared room name.
    pub room: String,
    /// First wing in the pair.
    pub wing_a: String,
    /// Second wing in the pair.
    pub wing_b: String,
    /// Total drawer count in this room.
    pub count: usize,
}

/// A single entry from a BFS traversal of the palace graph.
#[derive(Debug, Clone, Serialize)]
pub struct TraversalResult {
    /// Room name.
    pub room: String,
    /// Wings containing this room.
    pub wings: Vec<String>,
    /// Drawer count.
    pub count: usize,
    /// Number of hops from the start room (0 = start).
    pub hop: usize,
    /// Wings shared with the previous hop that caused this connection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_via: Option<Vec<String>>,
}

/// Summary statistics about the palace graph.
#[derive(Debug, Clone, Serialize)]
pub struct GraphStats {
    /// Total distinct rooms (excluding "general").
    pub rooms_total: usize,
    /// Rooms that span two or more wings.
    pub tunnel_rooms: usize,
    /// Total tunnel edges (wing-pair connections).
    pub edges_total: usize,
    /// Room count per wing.
    pub rooms_per_wing: HashMap<String, usize>,
    /// Top rooms by number of wings spanned.
    pub top_tunnels: Vec<RoomNode>,
}

/// Normalize a wing slug: trim whitespace, then apply the canonical slug rule.
///
/// Callers that filter by wing name must normalize before querying so that
/// "mempalace-rs" and "`mempalace_rs`" resolve to the same wing. Wraps
/// `config::normalize_wing_name` after trimming.
fn normalize_wing(wing: &str) -> String {
    let trimmed = wing.trim();
    assert!(
        !trimmed.is_empty(),
        "normalize_wing: wing must not be empty after trim"
    );
    normalize_wing_name(trimmed)
}

/// Build the palace graph from drawer metadata.
/// Returns (nodes, edges) where nodes are rooms and edges are tunnels.
pub async fn build_graph(
    connection: &Connection,
) -> Result<(HashMap<String, RoomNode>, Vec<TunnelEdge>)> {
    // "general" is the catch-all room assigned when no specific room matches.
    // It appears in every wing and would create spurious tunnel edges between
    // all wings if included, making the graph useless for navigation.
    let rows = query_all(
        connection,
        "SELECT room, wing, COUNT(*) as cnt FROM drawers WHERE room != 'general' AND room != '' GROUP BY room, wing",
        (),
    )
    .await?;

    // Aggregate room data across wings.
    let mut room_data: HashMap<String, (HashSet<String>, usize)> = HashMap::new();
    for row in &rows {
        let room: String = row.get(0)?;
        let wing: String = row.get(1)?;
        let count: i64 = row.get(2)?;
        let entry = room_data.entry(room).or_insert_with(|| (HashSet::new(), 0));
        entry.0.insert(wing);
        entry.1 += usize::try_from(count).unwrap_or(0);
    }

    // Build nodes.
    let mut nodes = HashMap::new();
    for (room, (wings, count)) in &room_data {
        let mut wing_list: Vec<String> = wings.iter().cloned().collect();
        wing_list.sort();
        nodes.insert(
            room.clone(),
            RoomNode {
                room: room.clone(),
                wings: wing_list,
                count: *count,
            },
        );
    }

    // Build edges from rooms spanning multiple wings.
    let mut edges = Vec::new();
    for (room, (wings, count)) in &room_data {
        let mut wing_list: Vec<&String> = wings.iter().collect();
        wing_list.sort();
        if wing_list.len() >= 2 {
            for (i, wa) in wing_list.iter().enumerate() {
                for wb in &wing_list[i + 1..] {
                    edges.push(TunnelEdge {
                        room: room.clone(),
                        wing_a: (*wa).clone(),
                        wing_b: (*wb).clone(),
                        count: *count,
                    });
                }
            }
        }
    }

    Ok((nodes, edges))
}

/// Maximum results returned by `traverse` and `find_tunnels` to keep MCP
/// responses within a reasonable token budget.
const GRAPH_RESULT_CAP: usize = 50;

/// BFS traversal from a starting room. Find connected rooms through shared wings.
///
/// Returns `(results, truncated)` where `truncated` is `true` when the full
/// result set exceeded `GRAPH_RESULT_CAP` and was capped.
pub async fn traverse(
    connection: &Connection,
    start_room: &str,
    hops_max: usize,
) -> Result<(Vec<TraversalResult>, bool)> {
    assert!(hops_max > 0, "hops_max must be positive");
    assert!(!start_room.is_empty(), "start_room must not be empty");

    let (nodes, _) = build_graph(connection).await?;

    let start = match nodes.get(start_room) {
        Some(node) => node.clone(),
        None => return Ok((Vec::new(), false)),
    };

    let mut visited = HashSet::new();
    visited.insert(start_room.to_string());

    let mut results = vec![TraversalResult {
        room: start.room.clone(),
        wings: start.wings.clone(),
        count: start.count,
        hop: 0,
        connected_via: None,
    }];

    let mut frontier: VecDeque<(String, usize)> = VecDeque::new();
    frontier.push_back((start_room.to_string(), 0));

    // Upper bound: each room enters `visited` before being pushed to `frontier`,
    // so the frontier empties after at most nodes.len() iterations.
    while let Some((room_current, depth)) = frontier.pop_front() {
        assert!(
            visited.len() <= nodes.len(),
            "visited set cannot exceed node count — frontier invariant is broken"
        );
        if depth >= hops_max {
            continue;
        }
        traverse_expand_frontier(
            &room_current,
            depth,
            hops_max,
            &nodes,
            &mut visited,
            &mut frontier,
            &mut results,
        );
    }

    // Sort by hop first so callers see the closest rooms first; break ties by
    // drawer count so the most active rooms surface before sparse ones.
    results.sort_by(|a, b| a.hop.cmp(&b.hop).then_with(|| b.count.cmp(&a.count)));
    let truncated = results.len() > GRAPH_RESULT_CAP;
    results.truncate(GRAPH_RESULT_CAP);

    // Postcondition: result count bounded by hard limit.
    debug_assert!(results.len() <= GRAPH_RESULT_CAP);

    Ok((results, truncated))
}

/// Called by `traverse` to keep that function within the 70-line limit.
///
/// For one BFS frontier node at `room_current`/`depth`, find all unvisited rooms
/// that share at least one wing with it, record them in `results`, mark them
/// `visited`, and push them onto `frontier` if they are below `hops_max`.
fn traverse_expand_frontier(
    room_current: &str,
    depth: usize,
    hops_max: usize,
    nodes: &HashMap<String, RoomNode>,
    visited: &mut HashSet<String>,
    frontier: &mut VecDeque<(String, usize)>,
    results: &mut Vec<TraversalResult>,
) {
    assert!(!room_current.is_empty(), "room_current must not be empty");
    assert!(depth < hops_max, "depth must be below hops_max on entry");

    let wings_current: HashSet<String> = nodes
        .get(room_current)
        .map(|n| n.wings.iter().cloned().collect())
        .unwrap_or_default();

    for (room, node) in nodes {
        if visited.contains(room) {
            continue;
        }
        let node_wings: HashSet<String> = node.wings.iter().cloned().collect();
        let shared: Vec<String> = wings_current.intersection(&node_wings).cloned().collect();
        if !shared.is_empty() {
            visited.insert(room.clone());
            let mut sorted_shared = shared;
            sorted_shared.sort();
            results.push(TraversalResult {
                room: room.clone(),
                wings: node.wings.clone(),
                count: node.count,
                hop: depth + 1,
                connected_via: Some(sorted_shared),
            });
            if depth + 1 < hops_max {
                frontier.push_back((room.clone(), depth + 1));
            }
        }
    }
}

/// Find rooms that connect two wings (tunnels).
///
/// Returns `(tunnels, truncated)` where `truncated` is `true` when the full
/// result set exceeded `GRAPH_RESULT_CAP` and was capped.
// `wing_a_norm`/`wing_b_norm` are intentionally parallel: `a` and `b` are
// the canonical endpoint labels for a tunnel; suppressing similar_names here.
#[allow(clippy::similar_names)]
pub async fn find_tunnels(
    connection: &Connection,
    wing_a: Option<&str>,
    wing_b: Option<&str>,
) -> Result<(Vec<RoomNode>, bool)> {
    let (nodes, _) = build_graph(connection).await?;

    // Normalize filters so "mempalace-rs" and "mempalace_rs" resolve identically.
    let wing_a_norm = wing_a.map(normalize_wing);
    let wing_b_norm = wing_b.map(normalize_wing);

    let mut tunnels: Vec<RoomNode> = nodes
        .into_values()
        .filter(|node| {
            if node.wings.len() < 2 {
                return false;
            }
            if let Some(ref wa) = wing_a_norm
                && !node.wings.contains(wa)
            {
                return false;
            }
            if let Some(ref wb) = wing_b_norm
                && !node.wings.contains(wb)
            {
                return false;
            }
            true
        })
        .collect();

    // Surface the busiest shared rooms first — they are the most useful bridges.
    tunnels.sort_by_key(|b| std::cmp::Reverse(b.count));
    let truncated = tunnels.len() > GRAPH_RESULT_CAP;
    tunnels.truncate(GRAPH_RESULT_CAP);

    // Postcondition: all returned nodes span at least 2 wings.
    debug_assert!(tunnels.iter().all(|t| t.wings.len() >= 2));

    Ok((tunnels, truncated))
}

/// Summary statistics about the palace graph.
pub async fn graph_stats(connection: &Connection) -> Result<GraphStats> {
    let (nodes, edges) = build_graph(connection).await?;

    let tunnel_rooms = nodes.values().filter(|n| n.wings.len() >= 2).count();

    let mut wing_counts: HashMap<String, usize> = HashMap::new();
    for node in nodes.values() {
        for w in &node.wings {
            *wing_counts.entry(w.clone()).or_insert(0) += 1;
        }
    }

    let mut top_tunnels: Vec<RoomNode> = nodes
        .values()
        .filter(|n| n.wings.len() >= 2)
        .cloned()
        .collect();
    top_tunnels.sort_by_key(|b| std::cmp::Reverse(b.wings.len()));
    top_tunnels.truncate(10);

    Ok(GraphStats {
        rooms_total: nodes.len(),
        tunnel_rooms,
        edges_total: edges.len(),
        rooms_per_wing: wing_counts,
        top_tunnels,
    })
}

// =============================================================================
// EXPLICIT TUNNELS — agent-created cross-wing links
// =============================================================================
// Passive tunnels are discovered from shared room names across wings.
// Explicit tunnels are created by agents when they notice a connection
// between two specific rooms in different wings/projects.
//
// Tunnels are symmetric (undirected): create_tunnel(A, B) and
// create_tunnel(B, A) produce the same canonical ID via a sorted hash,
// so a second call with flipped endpoints updates rather than duplicates.

/// An explicit tunnel linking two palace locations.
#[derive(Debug, Clone, Serialize)]
pub struct ExplicitTunnel {
    /// Canonical tunnel ID — SHA256 of sorted endpoints.
    pub id: String,
    /// Source wing.
    pub source_wing: String,
    /// Source room.
    pub source_room: String,
    /// Target wing.
    pub target_wing: String,
    /// Target room.
    pub target_room: String,
    /// Optional specific source drawer ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_drawer_id: Option<String>,
    /// Optional specific target drawer ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_drawer_id: Option<String>,
    /// Human-readable description of the connection.
    pub label: String,
    /// Tunnel category: `"explicit"` (user-created) or `"topic"` (auto-generated).
    pub kind: String,
    /// ISO timestamp when the tunnel was created.
    pub created_at: String,
    /// ISO timestamp when the tunnel was last updated (if it has been).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// A connection returned by `follow_tunnels`, relative to the queried location.
#[derive(Debug, Clone, Serialize)]
pub struct TunnelConnection {
    /// `"outgoing"` if the queried location is the source, `"incoming"` if target.
    pub direction: String,
    /// Wing of the connected room.
    pub connected_wing: String,
    /// Room of the connected room.
    pub connected_room: String,
    /// Human-readable description.
    pub label: String,
    /// Tunnel ID.
    pub tunnel_id: String,
    /// Optional drawer ID at the connected end.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<String>,
    /// Short preview of the connected drawer content (if collection supplied).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drawer_preview: Option<String>,
}

/// Compute the canonical tunnel ID from two endpoints.
///
/// Tunnels are undirected — sort the two endpoint strings before hashing so
/// `canonical_tunnel_id(A, B) == canonical_tunnel_id(B, A)`.
fn canonical_tunnel_id(
    source_wing: &str,
    source_room: &str,
    target_wing: &str,
    target_room: &str,
) -> String {
    assert!(!source_wing.is_empty(), "source_wing must not be empty");
    assert!(!source_room.is_empty(), "source_room must not be empty");
    assert!(!target_wing.is_empty(), "target_wing must not be empty");
    assert!(!target_room.is_empty(), "target_room must not be empty");

    let source_path = format!("{source_wing}/{source_room}");
    let target_path = format!("{target_wing}/{target_room}");
    let (first_endpoint, second_endpoint) = if source_path <= target_path {
        (source_path.as_str(), target_path.as_str())
    } else {
        (target_path.as_str(), source_path.as_str())
    };
    // ↔ (U+2194) separates the two endpoints. A bare `/` would be ambiguous
    // because wing and room strings can themselves contain slashes in principle;
    // a non-ASCII multi-byte separator makes accidental collisions impossible.
    let input = format!("{first_endpoint}\u{2194}{second_endpoint}");
    let hash = sha2::Sha256::digest(input.as_bytes());
    let hex: String = hash.iter().fold(String::new(), |mut hex_string, byte| {
        use std::fmt::Write as _;
        let _ = write!(hex_string, "{byte:02x}");
        hex_string
    });
    // Postcondition: SHA256 hex is always 64 chars; we take the first 16.
    assert_eq!(hex.len(), 64, "SHA256 hex output must be 64 characters");
    hex[..16].to_string()
}

/// Parameters for creating or updating an explicit tunnel.
pub struct CreateTunnelParams<'a> {
    /// Wing of the source location.
    pub source_wing: &'a str,
    /// Room in the source wing.
    pub source_room: &'a str,
    /// Wing of the target location.
    pub target_wing: &'a str,
    /// Room in the target wing.
    pub target_room: &'a str,
    /// Human-readable description of the connection.
    pub label: &'a str,
    /// Tunnel category: `"explicit"` for user-created, `"topic"` for auto-generated.
    pub kind: &'a str,
    /// Optional specific source drawer ID.
    pub source_drawer_id: Option<&'a str>,
    /// Optional specific target drawer ID.
    pub target_drawer_id: Option<&'a str>,
}

/// Create (or update) an explicit tunnel between two palace locations.
///
/// Tunnels are symmetric: calling with (A, B) and (B, A) both resolve to the
/// same canonical ID.  A second call with the same endpoints updates the label
/// and optional drawer IDs rather than creating a duplicate.
pub async fn create_tunnel(
    connection: &Connection,
    params: &CreateTunnelParams<'_>,
) -> Result<ExplicitTunnel> {
    assert!(
        !params.source_wing.is_empty(),
        "source_wing must not be empty"
    );
    assert!(
        !params.source_room.is_empty(),
        "source_room must not be empty"
    );
    assert!(
        !params.target_wing.is_empty(),
        "target_wing must not be empty"
    );
    assert!(
        !params.target_room.is_empty(),
        "target_room must not be empty"
    );

    // Reject any tunnel kind outside the closed taxonomy. The schema column
    // `explicit_tunnels.kind` is consumed by the MCP `find_tunnels` filter and
    // by downstream UI labelling, so an unexpected value would leak into both
    // — fail fast at the write boundary instead of persisting bad data.
    if !matches!(params.kind, "explicit" | "topic") {
        return Err(crate::error::Error::Other(format!(
            "create_tunnel: kind must be \"explicit\" or \"topic\", got {:?}",
            params.kind
        )));
    }

    // Normalize wing slugs so "my-project" and "my_project" resolve identically.
    let source_wing_norm = normalize_wing(params.source_wing);
    let target_wing_norm = normalize_wing(params.target_wing);
    assert!(!source_wing_norm.is_empty());
    assert!(!target_wing_norm.is_empty());

    let norm_params = CreateTunnelParams {
        source_wing: &source_wing_norm,
        target_wing: &target_wing_norm,
        source_room: params.source_room,
        target_room: params.target_room,
        label: params.label,
        kind: params.kind,
        source_drawer_id: params.source_drawer_id,
        target_drawer_id: params.target_drawer_id,
    };

    let tunnel_id = canonical_tunnel_id(
        norm_params.source_wing,
        norm_params.source_room,
        norm_params.target_wing,
        norm_params.target_room,
    );
    let now = Utc::now().to_rfc3339();

    create_tunnel_upsert(connection, &tunnel_id, &norm_params, &now).await?;
    create_tunnel_read_back(connection, &tunnel_id).await
}

/// UPDATE the tunnel if it exists, INSERT if it does not.
///
/// The UPDATE-then-INSERT pattern (rather than INSERT OR REPLACE) preserves
/// `created_at` on repeated calls — REPLACE would delete and re-insert the row,
/// resetting the creation timestamp.
async fn create_tunnel_upsert(
    connection: &Connection,
    tunnel_id: &str,
    params: &CreateTunnelParams<'_>,
    now: &str,
) -> Result<()> {
    assert!(!tunnel_id.is_empty(), "tunnel_id must not be empty");
    assert!(!now.is_empty(), "now must not be empty");

    let rows_updated = connection
        .execute(
            "UPDATE explicit_tunnels SET label = ?1, source_drawer_id = ?2, target_drawer_id = ?3, kind = ?4, updated_at = ?5 WHERE id = ?6",
            turso::params![
                params.label,
                params.source_drawer_id,
                params.target_drawer_id,
                params.kind,
                now,
                tunnel_id,
            ],
        )
        .await?;

    // Postcondition: at most one row updated (tunnel_id is the primary key).
    assert!(
        rows_updated <= 1,
        "tunnel ID is a primary key — at most one row updated"
    );

    if rows_updated == 0 {
        // No existing row — insert the new tunnel.
        connection
            .execute(
                "INSERT INTO explicit_tunnels (id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label, kind, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                turso::params![
                    tunnel_id,
                    params.source_wing,
                    params.source_room,
                    params.target_wing,
                    params.target_room,
                    params.source_drawer_id,
                    params.target_drawer_id,
                    params.label,
                    params.kind,
                    now
                ],
            )
            .await?;
    }

    Ok(())
}

/// Read the tunnel row back after upsert — the pair assertion half of the write.
async fn create_tunnel_read_back(
    connection: &Connection,
    tunnel_id: &str,
) -> Result<ExplicitTunnel> {
    assert!(!tunnel_id.is_empty(), "tunnel_id must not be empty");

    let rows = query_all(
        connection,
        "SELECT id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label, kind, created_at, updated_at FROM explicit_tunnels WHERE id = ?1",
        [tunnel_id],
    )
    .await?;

    // Pair assertion: the row must exist immediately after create_tunnel_upsert.
    assert!(
        !rows.is_empty(),
        "pair assertion: tunnel must exist after upsert"
    );

    let row = &rows[0];
    Ok(ExplicitTunnel {
        id: row.get(0).unwrap_or_default(),
        source_wing: row.get(1).unwrap_or_default(),
        source_room: row.get(2).unwrap_or_default(),
        target_wing: row.get(3).unwrap_or_default(),
        target_room: row.get(4).unwrap_or_default(),
        source_drawer_id: row.get(5).ok(),
        target_drawer_id: row.get(6).ok(),
        label: row.get(7).unwrap_or_default(),
        kind: row.get(8).unwrap_or_else(|_| "explicit".to_string()),
        created_at: row.get(9).unwrap_or_default(),
        updated_at: row.get(10).ok(),
    })
}

/// List explicit tunnels, optionally filtered to those involving a given wing.
pub async fn list_tunnels(
    connection: &Connection,
    wing: Option<&str>,
) -> Result<Vec<ExplicitTunnel>> {
    // Normalize the filter slug so hyphenated and underscored names match stored values.
    let wing_norm = wing.map(normalize_wing);
    if let Some(ref w) = wing_norm {
        assert!(!w.is_empty(), "wing filter must not be an empty string");
    }

    // Two separate queries rather than one with a `?1 IS NULL OR ...` guard,
    // so SQLite can use the wing column index when a filter is present instead
    // of falling back to a full table scan.
    let rows = if let Some(ref w) = wing_norm {
        query_all(
            connection,
            "SELECT id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label, kind, created_at, updated_at FROM explicit_tunnels WHERE source_wing = ?1 OR target_wing = ?1 ORDER BY created_at DESC",
            [w.as_str()],
        )
        .await?
    } else {
        query_all(
            connection,
            "SELECT id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label, kind, created_at, updated_at FROM explicit_tunnels ORDER BY created_at DESC",
            (),
        )
        .await?
    };

    let tunnels: Vec<ExplicitTunnel> = rows
        .iter()
        .map(|row| ExplicitTunnel {
            id: row.get(0).unwrap_or_default(),
            source_wing: row.get(1).unwrap_or_default(),
            source_room: row.get(2).unwrap_or_default(),
            target_wing: row.get(3).unwrap_or_default(),
            target_room: row.get(4).unwrap_or_default(),
            source_drawer_id: row.get(5).ok(),
            target_drawer_id: row.get(6).ok(),
            label: row.get(7).unwrap_or_default(),
            kind: row.get(8).unwrap_or_else(|_| "explicit".to_string()),
            created_at: row.get(9).unwrap_or_default(),
            updated_at: row.get(10).ok(),
        })
        .collect();

    // Postcondition: every returned tunnel was assigned an ID at insert time.
    debug_assert!(tunnels.iter().all(|t| !t.id.is_empty()));

    Ok(tunnels)
}

/// Delete an explicit tunnel by ID.  Returns `true` if a row was deleted.
pub async fn delete_tunnel(connection: &Connection, tunnel_id: &str) -> Result<bool> {
    assert!(!tunnel_id.is_empty(), "tunnel_id must not be empty");
    let rows_affected = connection
        .execute("DELETE FROM explicit_tunnels WHERE id = ?1", [tunnel_id])
        .await?;

    // Postcondition: at most one row deleted (ID is the primary key).
    assert!(
        rows_affected <= 1,
        "tunnel ID is a primary key — at most one row deleted"
    );

    Ok(rows_affected == 1)
}

/// Follow explicit tunnels from a room — returns connections to linked rooms.
///
/// Optionally fetches a short preview of the drawer at the connected end
/// when `drawer_ids` happen to be stored on the tunnel.
pub async fn follow_tunnels(
    connection: &Connection,
    wing: &str,
    room: &str,
) -> Result<Vec<TunnelConnection>> {
    assert!(!wing.is_empty(), "wing must not be empty");
    assert!(!room.is_empty(), "room must not be empty");

    // Normalize so "my-project" and "my_project" resolve to the same wing.
    let wing = normalize_wing(wing);
    assert!(!wing.is_empty());

    let rows = query_all(
        connection,
        "SELECT id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label FROM explicit_tunnels WHERE (source_wing = ?1 AND source_room = ?2) OR (target_wing = ?1 AND target_room = ?2)",
        [wing.as_str(), room],
    )
    .await?;

    let mut connections = Vec::new();
    for row in &rows {
        let tunnel_id: String = row.get(0).unwrap_or_default();
        let source_wing: String = row.get(1).unwrap_or_default();
        let source_room: String = row.get(2).unwrap_or_default();
        let target_wing: String = row.get(3).unwrap_or_default();
        let target_room: String = row.get(4).unwrap_or_default();
        let source_drawer_id: Option<String> = row.get(5).ok();
        let target_drawer_id: Option<String> = row.get(6).ok();
        let label: String = row.get(7).unwrap_or_default();

        // Direction is relative to the queried location: if we ARE the source
        // the link points away from us (outgoing); if we are the target, it
        // points at us (incoming).
        if source_wing == wing && source_room == room {
            connections.push(TunnelConnection {
                direction: "outgoing".to_string(),
                connected_wing: target_wing,
                connected_room: target_room,
                label,
                tunnel_id,
                drawer_id: target_drawer_id,
                drawer_preview: None,
            });
        } else {
            connections.push(TunnelConnection {
                direction: "incoming".to_string(),
                connected_wing: source_wing,
                connected_room: source_room,
                label,
                tunnel_id,
                drawer_id: source_drawer_id,
                drawer_preview: None,
            });
        }
    }

    // Postcondition: every connection has a recognised direction value.
    debug_assert!(
        connections
            .iter()
            .all(|c| c.direction == "outgoing" || c.direction == "incoming")
    );

    Ok(connections)
}

// =============================================================================
// TOPIC TUNNELS — auto-link wings that share confirmed TOPIC labels
// =============================================================================

/// Prefix for synthetic topic-tunnel room identifiers.
///
/// Namespaces topic rooms away from literal folder-derived rooms so a wing
/// with both an "Angular" folder room and a "shared topic: Angular" tunnel
/// remains distinguishable in `follow_tunnels` / `list_tunnels` output.
pub const TOPIC_ROOM_PREFIX: &str = "topic:";

/// Normalize a topic name for case-insensitive overlap detection.
fn topic_normalize(name: &str) -> String {
    assert!(!name.is_empty(), "topic_normalize: name must not be empty");
    name.trim().to_lowercase()
}

/// Return the synthetic room identifier for a topic tunnel.
///
/// The `topic:` prefix avoids collisions with literal folder-derived rooms
/// of the same name and signals auto-generated rooms to human and LLM readers.
pub fn topic_room(name: &str) -> String {
    assert!(!name.is_empty(), "topic_room: name must not be empty");
    format!("{TOPIC_ROOM_PREFIX}{name}")
}

/// Create tunnels for every pair of wings that share `>= min_count` topics.
///
/// Topics are compared case-insensitively; the first-observed casing (from
/// whichever wing sorts lexicographically first) is used for the room name.
/// Wings with no topics and the empty-map case are no-ops. `min_count` is
/// Build a normalized-topic → first-seen-casing map per wing from the raw topic lists.
///
/// Called by `compute_topic_tunnels` before intersecting pairs. Empty wing names
/// and empty topic names are silently skipped so the caller only sees clean data.
///
/// Wing keys are normalized via [`normalize_wing`] (the canonical slug rule used
/// elsewhere in the graph) so different raw spellings of the same wing — e.g.
/// `my-proj` and `my_proj` — collapse into one bucket here rather than colliding
/// later inside `create_tunnel`. Topics from each variant merge into the shared
/// bucket with first-observed casing winning per topic key.
fn compute_topic_tunnels_build_wing_map(
    topics_by_wing: &std::collections::BTreeMap<String, Vec<String>>,
) -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>> {
    let mut wing_topics: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, String>,
    > = std::collections::BTreeMap::new();
    for (wing, names) in topics_by_wing {
        let wing_trimmed = wing.trim();
        if wing_trimmed.is_empty() {
            continue;
        }
        let wing_key = normalize_wing_name(wing_trimmed);
        if wing_key.is_empty() {
            continue;
        }
        let bucket = wing_topics.entry(wing_key).or_default();
        for name in names {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                continue;
            }
            let key = topic_normalize(trimmed);
            // setdefault: keep the first-observed casing across every variant
            // of this wing key (e.g. both `my-proj` and `my_proj` contribute
            // into the same bucket; whichever entry we see first wins).
            bucket.entry(key).or_insert_with(|| trimmed.to_string());
        }
    }
    // Drop wings that contributed only empty topic names — keeps the downstream
    // pair-intersection loop free of placeholder buckets.
    wing_topics.retain(|_, bucket| !bucket.is_empty());
    wing_topics
}

/// clamped to `max(1, min_count)` so a value of 0 still requires one match.
/// Returns the number of tunnels created or refreshed.
pub async fn compute_topic_tunnels(
    connection: &Connection,
    topics_by_wing: &std::collections::BTreeMap<String, Vec<String>>,
    min_count: usize,
    label_prefix: &str,
) -> Result<usize> {
    assert!(
        !label_prefix.is_empty(),
        "compute_topic_tunnels: label_prefix must not be empty"
    );

    if topics_by_wing.is_empty() {
        return Ok(0);
    }

    let effective_min = min_count.max(1);
    let wing_topics = compute_topic_tunnels_build_wing_map(topics_by_wing);

    let wings: Vec<&str> = wing_topics.keys().map(String::as_str).collect();
    let wing_count = wings.len();
    assert!(
        wing_count <= 10_000,
        "compute_topic_tunnels: wing count must be bounded"
    );

    let mut created: usize = 0;
    for i in 0..wing_count {
        let wing_a = wings[i];
        let topics_a = &wing_topics[wing_a];
        for wing_b in wings.iter().skip(i + 1) {
            let topics_b = &wing_topics[*wing_b];
            let shared: Vec<String> = topics_a
                .keys()
                .filter(|key| topics_b.contains_key(*key))
                .cloned()
                .collect();

            if shared.len() < effective_min {
                continue;
            }

            for key in &shared {
                let topic_name = topics_a[key].as_str();
                let room = topic_room(topic_name);
                create_tunnel(
                    connection,
                    &CreateTunnelParams {
                        source_wing: wing_a,
                        source_room: &room,
                        target_wing: wing_b,
                        target_room: &room,
                        label: &format!("{label_prefix}: {topic_name}"),
                        kind: "topic",
                        source_drawer_id: None,
                        target_drawer_id: None,
                    },
                )
                .await?;
                created += 1;
            }
        }
    }

    Ok(created)
}

/// Compute topic tunnels involving a single wing only.
///
/// Used by the miner to incrementally update tunnels for the wing that just
/// finished mining without recomputing all pairs. Returns the number of
/// tunnels created or refreshed.
pub async fn topic_tunnels_for_wing(
    connection: &Connection,
    wing: &str,
    topics_by_wing: &std::collections::BTreeMap<String, Vec<String>>,
    min_count: usize,
    label_prefix: &str,
) -> Result<usize> {
    assert!(
        !wing.is_empty(),
        "topic_tunnels_for_wing: wing must not be empty"
    );
    assert!(
        !label_prefix.is_empty(),
        "topic_tunnels_for_wing: label_prefix must not be empty"
    );

    if topics_by_wing.is_empty() {
        return Ok(0);
    }

    // Callers may pass an unnormalised wing spelling (e.g. `my-proj`) while
    // `topics_by_wing` is keyed by the canonical slug (`my_proj`) — produced by
    // `compute_topic_tunnels_build_wing_map` for the same reason. Normalise
    // here so the lookup hits the right bucket and the slice we hand to
    // `compute_topic_tunnels` keeps the slug invariant on both keys.
    let normalized_wing = normalize_wing(wing);
    assert!(
        !normalized_wing.is_empty(),
        "topic_tunnels_for_wing: normalized wing must not be empty"
    );

    let own = match topics_by_wing.get(&normalized_wing) {
        Some(names) if !names.is_empty() => names,
        _ => return Ok(0),
    };

    // Build two-wing slices for each (wing, other) pair and reuse
    // compute_topic_tunnels to keep threshold and casing logic in one place.
    let mut total: usize = 0;
    for (other, other_topics) in topics_by_wing {
        if other == &normalized_wing || other_topics.is_empty() {
            continue;
        }
        let mut slice: std::collections::BTreeMap<String, Vec<String>> =
            std::collections::BTreeMap::new();
        slice.insert(normalized_wing.clone(), own.clone());
        slice.insert(other.clone(), other_topics.clone());
        total += compute_topic_tunnels(connection, &slice, min_count, label_prefix).await?;
    }

    Ok(total)
}

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    async fn seed_graph(connection: &Connection) {
        // Create drawers across wings and rooms to build a graph.
        for (id, wing, room) in [
            ("g1", "proj_a", "backend"),
            ("g2", "proj_a", "frontend"),
            ("g3", "proj_b", "backend"), // "backend" spans both wings — tunnel
            ("g4", "proj_b", "database"),
        ] {
            connection
                .execute(
                    "INSERT INTO drawers (id, wing, room, content) VALUES (?1, ?2, ?3, 'content')",
                    turso::params![id, wing, room],
                )
                .await
                .expect("seed drawer");
        }
    }

    #[tokio::test]
    async fn build_graph_creates_nodes_and_edges() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_graph(&connection).await;
        let (nodes, edges) = build_graph(&connection).await.expect("build_graph");
        // "backend" spans 2 wings, "frontend" in 1, "database" in 1
        assert!(nodes.contains_key("backend"));
        assert!(nodes.contains_key("frontend"));
        assert!(nodes.contains_key("database"));
        // "backend" creates a tunnel edge between proj_a and proj_b
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].room, "backend");
    }

    #[tokio::test]
    async fn traverse_reaches_connected_rooms() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_graph(&connection).await;
        let (results, truncated) = traverse(&connection, "frontend", 2)
            .await
            .expect("traverse");
        assert!(!truncated);
        // frontend (hop 0) → backend (hop 1, shared proj_a) → database (hop 2, shared proj_b).
        assert!(!results.is_empty());
        assert_eq!(results[0].room, "frontend");
        assert_eq!(results[0].hop, 0);

        // Verify hop 1: backend reached via shared proj_a wing.
        let hop1 = results
            .iter()
            .find(|r| r.room == "backend" && r.hop == 1)
            .expect("backend at hop 1");
        assert!(hop1.wings.contains(&"proj_a".to_string()));
        assert!(hop1.wings.contains(&"proj_b".to_string()));
        assert_eq!(hop1.connected_via, Some(vec!["proj_a".to_string()]));

        // Verify hop 2: database reached via shared proj_b wing.
        let hop2 = results
            .iter()
            .find(|r| r.room == "database" && r.hop == 2)
            .expect("database at hop 2");
        assert!(hop2.wings.contains(&"proj_b".to_string()));
        assert_eq!(hop2.connected_via, Some(vec!["proj_b".to_string()]));
    }

    #[tokio::test]
    async fn find_tunnels_returns_multi_wing_rooms() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_graph(&connection).await;
        let (tunnels, truncated) = find_tunnels(&connection, None, None)
            .await
            .expect("find_tunnels");
        assert!(!truncated);
        assert_eq!(tunnels.len(), 1);
        assert_eq!(tunnels[0].room, "backend");
        assert_eq!(tunnels[0].wings.len(), 2);
    }

    // ── explicit tunnel tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn create_tunnel_round_trip() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let tunnel = create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "wing_api",
                source_room: "schemas",
                target_wing: "wing_db",
                target_room: "migrations",
                label: "API schema drives DB migration",
                kind: "explicit",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("create_tunnel should succeed");

        assert!(!tunnel.id.is_empty(), "tunnel ID must be assigned");
        assert_eq!(tunnel.source_wing, "wing_api");
        assert_eq!(tunnel.source_room, "schemas");
        assert_eq!(tunnel.target_wing, "wing_db");
        assert_eq!(tunnel.target_room, "migrations");
        assert_eq!(tunnel.label, "API schema drives DB migration");
        assert!(tunnel.updated_at.is_none(), "new tunnel has no updated_at");
    }

    #[tokio::test]
    async fn create_tunnel_idempotent_update() {
        // A second call with the same endpoints must update, not duplicate.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let first = create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "wA",
                source_room: "rA",
                target_wing: "wB",
                target_room: "rB",
                label: "first label",
                kind: "explicit",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("first create_tunnel");

        let second = create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "wA",
                source_room: "rA",
                target_wing: "wB",
                target_room: "rB",
                label: "updated label",
                kind: "explicit",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("second create_tunnel");

        assert_eq!(first.id, second.id, "same endpoints → same canonical ID");
        assert_eq!(second.label, "updated label", "label must be updated");
        assert!(second.updated_at.is_some(), "repeated call sets updated_at");

        let tunnels = list_tunnels(&connection, None).await.expect("list_tunnels");
        assert_eq!(tunnels.len(), 1, "must remain exactly one tunnel");
    }

    #[tokio::test]
    async fn create_tunnel_symmetric_id() {
        // (A→B) and (B→A) must produce the same canonical ID.
        let id_ab = canonical_tunnel_id("wing_a", "room_a", "wing_b", "room_b");
        let id_ba = canonical_tunnel_id("wing_b", "room_b", "wing_a", "room_a");
        assert_eq!(id_ab, id_ba, "tunnel ID must be symmetric");
    }

    #[tokio::test]
    async fn list_tunnels_wing_filter() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "wA",
                source_room: "rA",
                target_wing: "wB",
                target_room: "rB",
                label: "",
                kind: "explicit",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("create AB");
        create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "wC",
                source_room: "rC",
                target_wing: "wD",
                target_room: "rD",
                label: "",
                kind: "explicit",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("create CD");

        // Wing names are normalized to lowercase on insert, so "wA" → "wa".
        // The filter is also normalized so querying with "wA" finds the stored "wa" row.
        let tunnels = list_tunnels(&connection, Some("wA"))
            .await
            .expect("list by wA");
        assert_eq!(tunnels.len(), 1, "filter by wA should return 1 tunnel");
        assert!(
            tunnels[0].source_wing == "wa" || tunnels[0].target_wing == "wa",
            "returned tunnel must involve normalized wA (stored as 'wa')"
        );
    }

    #[tokio::test]
    async fn delete_tunnel_removes_row() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let tunnel = create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "wx",
                source_room: "rx",
                target_wing: "wy",
                target_room: "ry",
                label: "",
                kind: "explicit",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("create tunnel");

        let deleted = delete_tunnel(&connection, &tunnel.id)
            .await
            .expect("delete_tunnel");
        assert!(deleted, "delete must return true for existing tunnel");

        let tunnels = list_tunnels(&connection, None)
            .await
            .expect("list after delete");
        assert!(tunnels.is_empty(), "tunnel list must be empty after delete");
    }

    #[tokio::test]
    async fn delete_tunnel_nonexistent_returns_false() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let deleted = delete_tunnel(&connection, "nonexistent_id_000000")
            .await
            .expect("delete_tunnel on missing ID should not error");
        assert!(!deleted, "delete of nonexistent tunnel must return false");
    }

    #[tokio::test]
    async fn follow_tunnels_returns_connections() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "wing_api",
                source_room: "design",
                target_wing: "wing_db",
                target_room: "schema",
                label: "api design → db schema",
                kind: "explicit",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("create tunnel");

        let connections = follow_tunnels(&connection, "wing_api", "design")
            .await
            .expect("follow_tunnels");
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].direction, "outgoing");
        assert_eq!(connections[0].connected_wing, "wing_db");
        assert_eq!(connections[0].connected_room, "schema");

        // Pair assertion: follow from the other end returns incoming.
        let reverse = follow_tunnels(&connection, "wing_db", "schema")
            .await
            .expect("follow_tunnels reverse");
        assert_eq!(reverse.len(), 1);
        assert_eq!(reverse[0].direction, "incoming");
        assert_eq!(reverse[0].connected_wing, "wing_api");
    }

    #[test]
    fn topic_room_prefix_is_correct() {
        let room = topic_room("Rust");
        assert_eq!(room, "topic:Rust", "topic_room must prefix with 'topic:'");
        assert!(
            room.starts_with(TOPIC_ROOM_PREFIX),
            "topic_room must start with TOPIC_ROOM_PREFIX"
        );
    }

    #[tokio::test]
    async fn compute_topic_tunnels_creates_shared_topic_tunnels() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let mut topics_by_wing = std::collections::BTreeMap::new();
        topics_by_wing.insert(
            "wing_alpha".to_string(),
            vec!["Rust".to_string(), "WebAssembly".to_string()],
        );
        topics_by_wing.insert(
            "wing_beta".to_string(),
            vec!["rust".to_string(), "Python".to_string()],
        );

        let count = compute_topic_tunnels(&connection, &topics_by_wing, 1, "shared topic")
            .await
            .expect("compute_topic_tunnels must succeed");

        // "rust" overlaps (case-insensitive) → 1 tunnel between wing_alpha and wing_beta.
        assert!(count >= 1, "at least one topic tunnel must be created");

        let tunnels = list_tunnels(&connection, None)
            .await
            .expect("list_tunnels must succeed");
        assert!(!tunnels.is_empty(), "tunnels must exist after compute");
        assert!(
            tunnels.iter().any(|t| t.kind == "topic"),
            "at least one tunnel must have kind='topic'"
        );
        assert!(
            tunnels
                .iter()
                .any(|t| t.source_room.starts_with(TOPIC_ROOM_PREFIX)),
            "topic tunnel room must use topic: prefix"
        );
    }

    #[tokio::test]
    async fn compute_topic_tunnels_min_count_filters_pairs() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let mut topics_by_wing = std::collections::BTreeMap::new();
        topics_by_wing.insert("wing_a".to_string(), vec!["Rust".to_string()]);
        topics_by_wing.insert("wing_b".to_string(), vec!["rust".to_string()]);

        // min_count=2 requires 2 shared topics; only 1 shared → no tunnels.
        let count = compute_topic_tunnels(&connection, &topics_by_wing, 2, "shared topic")
            .await
            .expect("compute_topic_tunnels with min_count=2");
        assert_eq!(
            count, 0,
            "min_count=2 with 1 shared topic must create 0 tunnels"
        );

        let tunnels = list_tunnels(&connection, None).await.expect("list_tunnels");
        assert!(
            tunnels.is_empty(),
            "no tunnels must exist when threshold not met"
        );
    }

    #[tokio::test]
    async fn compute_topic_tunnels_collapses_wing_key_variants() {
        // Regression: `my-proj` and `my_proj` must normalise to a single wing key
        // so the loop that pairs wings cannot accidentally create a self-tunnel
        // between two spellings of the same underlying wing.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let mut topics_by_wing = std::collections::BTreeMap::new();
        topics_by_wing.insert("my-proj".to_string(), vec!["Rust".to_string()]);
        topics_by_wing.insert("my_proj".to_string(), vec!["rust".to_string()]);
        topics_by_wing.insert("other_proj".to_string(), vec!["Rust".to_string()]);

        // After normalisation the inputs reduce to two distinct wings: `my_proj`
        // and `other_proj` — exactly one pair, so exactly one tunnel.
        let count = compute_topic_tunnels(&connection, &topics_by_wing, 1, "shared topic")
            .await
            .expect("compute_topic_tunnels must succeed");
        assert_eq!(
            count, 1,
            "wing-key variants must collapse: exactly one pair → one tunnel"
        );

        let tunnels = list_tunnels(&connection, None)
            .await
            .expect("list_tunnels must succeed");
        // Pair assertion: no tunnel should ever have source == target wing.
        assert!(
            tunnels.iter().all(|t| t.source_wing != t.target_wing),
            "wing-key variants must not produce a self-tunnel: {tunnels:?}"
        );
    }

    #[tokio::test]
    async fn topic_tunnels_for_wing_pairs_only_with_wing() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        let mut topics_by_wing = std::collections::BTreeMap::new();
        topics_by_wing.insert("wing_x".to_string(), vec!["Angular".to_string()]);
        topics_by_wing.insert("wing_y".to_string(), vec!["angular".to_string()]);
        topics_by_wing.insert("wing_z".to_string(), vec!["Vue".to_string()]);

        // wing_x shares "angular" with wing_y but not wing_z.
        let count =
            topic_tunnels_for_wing(&connection, "wing_x", &topics_by_wing, 1, "shared topic")
                .await
                .expect("topic_tunnels_for_wing must succeed");
        assert_eq!(
            count, 1,
            "exactly one topic tunnel must be created for wing_x"
        );

        let tunnels = list_tunnels(&connection, Some("wing_x"))
            .await
            .expect("list_tunnels for wing_x");
        assert_eq!(tunnels.len(), 1, "one tunnel must involve wing_x");
        assert_eq!(tunnels[0].kind, "topic", "tunnel kind must be 'topic'");
    }

    #[tokio::test]
    async fn topic_tunnels_for_wing_normalizes_caller_wing() {
        // Regression: callers may pass an unnormalised wing slug ("my-proj")
        // while the registry was populated with the canonical slug
        // ("my_proj"). The lookup must still hit the right bucket so the
        // incremental update from the miner does not silently no-op when the
        // wing was stored under its hyphen form.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let mut topics_by_wing = std::collections::BTreeMap::new();
        topics_by_wing.insert("my_proj".to_string(), vec!["Rust".to_string()]);
        topics_by_wing.insert("other_proj".to_string(), vec!["rust".to_string()]);

        // Hyphenated alias must resolve to the canonical bucket above.
        let count =
            topic_tunnels_for_wing(&connection, "my-proj", &topics_by_wing, 1, "shared topic")
                .await
                .expect("topic_tunnels_for_wing must succeed for hyphen alias");
        assert_eq!(
            count, 1,
            "hyphenated alias must resolve to canonical wing bucket and create one tunnel"
        );
        let tunnels = list_tunnels(&connection, Some("my_proj"))
            .await
            .expect("list_tunnels for my_proj");
        assert_eq!(
            tunnels.len(),
            1,
            "exactly one tunnel must reference the canonical slug"
        );
    }

    #[tokio::test]
    async fn kind_column_preserved_on_explicit_tunnel() {
        // Explicit tunnels written via MCP or API must carry kind="explicit".
        let (_db, connection) = crate::test_helpers::test_db().await;
        let tunnel = create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "alpha",
                source_room: "code",
                target_wing: "beta",
                target_room: "code",
                label: "linked code rooms",
                kind: "explicit",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("create explicit tunnel");
        assert_eq!(
            tunnel.kind, "explicit",
            "explicit tunnel must carry kind='explicit'"
        );

        let tunnels = list_tunnels(&connection, None).await.expect("list_tunnels");
        assert_eq!(tunnels.len(), 1, "exactly one tunnel");
        assert_eq!(
            tunnels[0].kind, "explicit",
            "listed tunnel must carry kind='explicit'"
        );
    }

    #[tokio::test]
    async fn create_tunnel_rejects_invalid_kind() {
        // Negative space: any kind outside the closed {"explicit","topic"}
        // taxonomy must be refused before the row is persisted, otherwise the
        // value would leak through `find_tunnels`/`list_tunnels` filters.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let result = create_tunnel(
            &connection,
            &CreateTunnelParams {
                source_wing: "alpha",
                source_room: "code",
                target_wing: "beta",
                target_room: "code",
                label: "bad kind",
                kind: "bogus",
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await;
        let error = result.expect_err("invalid kind must error");
        let message = error.to_string();
        assert!(
            message.contains("kind must be"),
            "error must explain valid kinds, got {message:?}"
        );
        // Pair: nothing was written even though the error fired late on the
        // path — list_tunnels must report zero rows.
        let tunnels = list_tunnels(&connection, None)
            .await
            .expect("list_tunnels must succeed");
        assert!(
            tunnels.is_empty(),
            "no row may be persisted for an invalid kind"
        );
    }
}
