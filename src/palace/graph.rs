use std::collections::{HashMap, HashSet, VecDeque};

use chrono::Utc;
use serde::Serialize;
use sha2::Digest as _;
use turso::Connection;

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
    pub total_rooms: usize,
    /// Rooms that span two or more wings.
    pub tunnel_rooms: usize,
    /// Total tunnel edges (wing-pair connections).
    pub total_edges: usize,
    /// Room count per wing.
    pub rooms_per_wing: HashMap<String, usize>,
    /// Top rooms by number of wings spanned.
    pub top_tunnels: Vec<RoomNode>,
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

    // Aggregate room data across wings
    let mut room_data: HashMap<String, (HashSet<String>, usize)> = HashMap::new();
    for row in &rows {
        let room: String = row.get(0)?;
        let wing: String = row.get(1)?;
        let count: i64 = row.get(2)?;
        let entry = room_data.entry(room).or_insert_with(|| (HashSet::new(), 0));
        entry.0.insert(wing);
        entry.1 += usize::try_from(count).unwrap_or(0);
    }

    // Build nodes
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

    // Build edges from rooms spanning multiple wings
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
    max_hops: usize,
) -> Result<(Vec<TraversalResult>, bool)> {
    assert!(max_hops > 0, "max_hops must be positive");
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
    while let Some((current_room, depth)) = frontier.pop_front() {
        assert!(
            visited.len() <= nodes.len(),
            "visited set cannot exceed node count — frontier invariant is broken"
        );
        if depth >= max_hops {
            continue;
        }

        let current_wings: HashSet<String> = nodes
            .get(&current_room)
            .map(|n| n.wings.iter().cloned().collect())
            .unwrap_or_default();

        for (room, node) in &nodes {
            if visited.contains(room) {
                continue;
            }
            let node_wings: HashSet<String> = node.wings.iter().cloned().collect();
            let shared: Vec<String> = current_wings.intersection(&node_wings).cloned().collect();
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
                if depth + 1 < max_hops {
                    frontier.push_back((room.clone(), depth + 1));
                }
            }
        }
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

/// Find rooms that connect two wings (tunnels).
///
/// Returns `(tunnels, truncated)` where `truncated` is `true` when the full
/// result set exceeded `GRAPH_RESULT_CAP` and was capped.
pub async fn find_tunnels(
    connection: &Connection,
    wing_a: Option<&str>,
    wing_b: Option<&str>,
) -> Result<(Vec<RoomNode>, bool)> {
    let (nodes, _) = build_graph(connection).await?;

    let mut tunnels: Vec<RoomNode> = nodes
        .into_values()
        .filter(|node| {
            if node.wings.len() < 2 {
                return false;
            }
            if let Some(wa) = wing_a
                && !node.wings.contains(&wa.to_string())
            {
                return false;
            }
            if let Some(wb) = wing_b
                && !node.wings.contains(&wb.to_string())
            {
                return false;
            }
            true
        })
        .collect();

    // Surface the busiest shared rooms first — they are the most useful bridges.
    tunnels.sort_by(|a, b| b.count.cmp(&a.count));
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
    top_tunnels.sort_by(|a, b| b.wings.len().cmp(&a.wings.len()));
    top_tunnels.truncate(10);

    Ok(GraphStats {
        total_rooms: nodes.len(),
        tunnel_rooms,
        total_edges: edges.len(),
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

    let src = format!("{source_wing}/{source_room}");
    let tgt = format!("{target_wing}/{target_room}");
    let (a, b) = if src <= tgt {
        (src.as_str(), tgt.as_str())
    } else {
        (tgt.as_str(), src.as_str())
    };
    // ↔ (U+2194) separates the two endpoints. A bare `/` would be ambiguous
    // because wing and room strings can themselves contain slashes in principle;
    // a non-ASCII multi-byte separator makes accidental collisions impossible.
    let input = format!("{a}\u{2194}{b}");
    let hash = sha2::Sha256::digest(input.as_bytes());
    let hex: String = hash.iter().fold(String::new(), |mut s, byte| {
        use std::fmt::Write as _;
        let _ = write!(s, "{byte:02x}");
        s
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

    let tunnel_id = canonical_tunnel_id(
        params.source_wing,
        params.source_room,
        params.target_wing,
        params.target_room,
    );
    let now = Utc::now().to_rfc3339();

    create_tunnel_upsert(connection, &tunnel_id, params, &now).await?;
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
            "UPDATE explicit_tunnels SET label = ?1, source_drawer_id = ?2, target_drawer_id = ?3, updated_at = ?4 WHERE id = ?5",
            turso::params![params.label, params.source_drawer_id, params.target_drawer_id, now, tunnel_id],
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
                "INSERT INTO explicit_tunnels (id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                turso::params![
                    tunnel_id,
                    params.source_wing,
                    params.source_room,
                    params.target_wing,
                    params.target_room,
                    params.source_drawer_id,
                    params.target_drawer_id,
                    params.label,
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
        "SELECT id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label, created_at, updated_at FROM explicit_tunnels WHERE id = ?1",
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
        created_at: row.get(8).unwrap_or_default(),
        updated_at: row.get(9).ok(),
    })
}

/// List explicit tunnels, optionally filtered to those involving a given wing.
pub async fn list_tunnels(
    connection: &Connection,
    wing: Option<&str>,
) -> Result<Vec<ExplicitTunnel>> {
    if let Some(w) = wing {
        assert!(!w.is_empty(), "wing filter must not be an empty string");
    }

    // Two separate queries rather than one with a `?1 IS NULL OR ...` guard,
    // so SQLite can use the wing column index when a filter is present instead
    // of falling back to a full table scan.
    let rows = if let Some(w) = wing {
        query_all(
            connection,
            "SELECT id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label, created_at, updated_at FROM explicit_tunnels WHERE source_wing = ?1 OR target_wing = ?1 ORDER BY created_at DESC",
            [w],
        )
        .await?
    } else {
        query_all(
            connection,
            "SELECT id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label, created_at, updated_at FROM explicit_tunnels ORDER BY created_at DESC",
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
            created_at: row.get(8).unwrap_or_default(),
            updated_at: row.get(9).ok(),
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

    let rows = query_all(
        connection,
        "SELECT id, source_wing, source_room, target_wing, target_room, source_drawer_id, target_drawer_id, label FROM explicit_tunnels WHERE (source_wing = ?1 AND source_room = ?2) OR (target_wing = ?1 AND target_room = ?2)",
        [wing, room],
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

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    async fn seed_graph(connection: &Connection) {
        // Create drawers across wings and rooms to build a graph
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
        // frontend (hop 0) → backend (hop 1, shared proj_a) → database (hop 2, shared proj_b)
        assert!(!results.is_empty());
        assert_eq!(results[0].room, "frontend");
        assert_eq!(results[0].hop, 0);

        // Verify hop 1: backend reached via shared proj_a wing
        let hop1 = results
            .iter()
            .find(|r| r.room == "backend" && r.hop == 1)
            .expect("backend at hop 1");
        assert!(hop1.wings.contains(&"proj_a".to_string()));
        assert!(hop1.wings.contains(&"proj_b".to_string()));
        assert_eq!(hop1.connected_via, Some(vec!["proj_a".to_string()]));

        // Verify hop 2: database reached via shared proj_b wing
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
                source_drawer_id: None,
                target_drawer_id: None,
            },
        )
        .await
        .expect("create CD");

        let tunnels = list_tunnels(&connection, Some("wA"))
            .await
            .expect("list by wA");
        assert_eq!(tunnels.len(), 1, "filter by wA should return 1 tunnel");
        assert!(
            tunnels[0].source_wing == "wA" || tunnels[0].target_wing == "wA",
            "returned tunnel must involve wA"
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
}
