//! MCP server — JSON-RPC 2.0 over stdio exposing palace tools.
//!
//! Error handling policy: tool errors are logged to stderr with limited/truncated
//! detail (first 100 chars) and the client receives only a generic `"Internal tool error"`
//! message for unstructured errors, so that internal paths and database details are
//! never leaked over the protocol.

pub mod protocol;
pub mod tools;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use turso::Connection;

use crate::error::Result;

const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Hard cap on a single newline-delimited request frame. A client-controlled
/// line is buffered entirely before JSON parsing, so this bound prevents OOM
/// before any validation runs. 1 MiB comfortably fits any real tool payload.
const MAX_REQUEST_BYTES: usize = 1024 * 1024; // 1 MiB

/// Run the MCP server: read JSON-RPC 2.0 requests from stdin, write responses to stdout.
pub async fn run(connection: &Connection) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    // Intentional server loop: runs until stdin closes (bytes_read == 0 signals EOF).
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            break; // EOF — client disconnected.
        }

        if line.len() > MAX_REQUEST_BYTES {
            let err_response = json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32700, "message": "Request exceeds maximum frame size"}
            });
            let out = serde_json::to_string(&err_response).unwrap_or_default();
            stdout.write_all(out.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let request: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                let err_response = json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {"code": -32700, "message": format!("Parse error: {e}")}
                });
                let out = serde_json::to_string(&err_response).unwrap_or_default();
                stdout.write_all(out.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
                continue;
            }
        };

        let response = handle_request(connection, &request).await;

        if let Some(resp) = response {
            let out = serde_json::to_string(&resp).unwrap_or_default();
            stdout.write_all(out.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

async fn handle_request(connection: &Connection, request: &Value) -> Option<Value> {
    // Malformed (non-object) requests get a JSON-RPC error rather than a panic;
    // the MCP server must stay alive for subsequent well-formed requests.
    if !request.is_object() {
        return Some(json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": {"code": -32600, "message": "Invalid Request: expected JSON object"}
        }));
    }

    // A missing or non-string "method" field is also an Invalid Request per JSON-RPC 2.0.
    let Some(method) = request.get("method").and_then(|m| m.as_str()) else {
        return Some(json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": {"code": -32600, "message": "Invalid Request: expected string 'method'"}
        }));
    };
    let params = request.get("params").cloned().unwrap_or(json!({}));
    let req_id = request.get("id").cloned().unwrap_or(Value::Null);

    match method {
        "initialize" => {
            let client_version = params.get("protocolVersion").and_then(|v| v.as_str());
            let negotiated = if let Some(cv) = client_version {
                if SUPPORTED_PROTOCOL_VERSIONS.contains(&cv) {
                    cv
                } else {
                    SUPPORTED_PROTOCOL_VERSIONS[0]
                }
            } else {
                SUPPORTED_PROTOCOL_VERSIONS[0]
            };
            Some(json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": negotiated,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "mempalace", "version": env!("CARGO_PKG_VERSION")},
                }
            }))
        }

        "ping" => Some(json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {}
        })),

        m if m.starts_with("notifications/") => None,

        "tools/list" => {
            let tools = protocol::tool_definitions();
            Some(json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": tools}
            }))
        }

        "tools/call" => Some(handle_request_tools_call(connection, &params, req_id).await),

        _ => Some(json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": format!("Unknown method: {method}")}
        })),
    }
}

/// Dispatch a `tools/call` request and return a sanitized JSON-RPC response.
async fn handle_request_tools_call(
    connection: &Connection,
    params: &Value,
    req_id: Value,
) -> Value {
    let tool_name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
    let tool_args = params
        .get("arguments")
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or(json!({}));

    let result = tools::dispatch(connection, tool_name, &tool_args).await;

    // Sanitize: only expose errors that tools explicitly mark as public.
    // All other errors are masked so internal paths and database details
    // are never leaked over the protocol.
    let sanitized = if let Some(error_val) = result.get("error") {
        let is_public = result
            .get("public")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        // Extract the full error string before deciding what to expose.
        // Truncate only for logging so we don't shorten public error messages.
        let full_error = error_val.as_str().unwrap_or("unknown");
        let truncated: String = full_error.chars().take(100).collect();
        // Sanitize log fields: replace control characters so a hostile client
        // cannot inject newlines or terminal escape sequences into stderr.
        let tool_name_safe: String = tool_name
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        let error_safe: String = truncated
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        eprintln!("tool error: tool={tool_name_safe} error={error_safe}");
        if is_public {
            json!({"error": full_error})
        } else {
            json!({"error": "Internal tool error"})
        }
    } else {
        result
    };

    let text = serde_json::to_string_pretty(&sanitized).unwrap_or_default();

    json!({
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "content": [{"type": "text", "text": text}]
        }
    })
}
