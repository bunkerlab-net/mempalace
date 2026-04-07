//! MCP server — JSON-RPC 2.0 over stdio exposing palace tools.

pub mod protocol;
pub mod tools;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use turso::Connection;

use crate::error::Result;

/// Run the MCP server: read JSON-RPC 2.0 requests from stdin, write responses to stdout.
pub async fn run(conn: &Connection) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            break; // EOF
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

        let response = handle_request(conn, &request).await;

        if let Some(resp) = response {
            let out = serde_json::to_string(&resp).unwrap_or_default();
            stdout.write_all(out.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

async fn handle_request(conn: &Connection, request: &Value) -> Option<Value> {
    let method = request.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = request.get("params").cloned().unwrap_or(json!({}));
    let req_id = request.get("id").cloned().unwrap_or(Value::Null);

    match method {
        "initialize" => Some(json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "mempalace", "version": "2.0.0-rs"},
            }
        })),

        "notifications/initialized" => None,

        "tools/list" => {
            let tools = protocol::tool_definitions();
            Some(json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": tools}
            }))
        }

        "tools/call" => {
            let tool_name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let tool_args = params.get("arguments").cloned().unwrap_or(json!({}));

            let result = tools::dispatch(conn, tool_name, &tool_args).await;
            let text = serde_json::to_string_pretty(&result).unwrap_or_default();

            Some(json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": text}]
                }
            }))
        }

        _ => Some(json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": format!("Unknown method: {method}")}
        })),
    }
}
