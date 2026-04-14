//! MCP server — JSON-RPC 2.0 over stdio exposing palace tools.
//!
//! Error handling policy: tool errors are logged to stderr with limited/truncated
//! detail (first 100 chars) and the client receives only a generic `"Internal tool error"`
//! message for unstructured errors, so that internal paths and database details are
//! never leaked over the protocol.

pub mod protocol;
pub mod tools;

use std::pin::Pin;

use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use turso::Connection;

use crate::error::Result;

const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Hard cap on a single newline-delimited request frame. A client-controlled
/// line is buffered entirely before JSON parsing, so this bound prevents OOM
/// before any validation runs. 1 MiB comfortably fits any real tool payload.
const MAX_REQUEST_BYTES: usize = 1024 * 1024; // 1 MiB

/// Outcome of reading a single newline-delimited frame from the buffered reader.
enum LineRead {
    /// A complete line was read (trailing newline stripped).
    Line(String),
    /// The line exceeded the byte limit — stream resynced past the next newline.
    Overflow,
    /// End-of-stream — stdin closed.
    Eof,
}

/// Run the MCP server: read JSON-RPC 2.0 requests from stdin, write responses to stdout.
pub async fn run(connection: &Connection) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    // Reusable buffer for line reading — allocated once, cleared each iteration.
    let mut line_buffer: Vec<u8> = Vec::with_capacity(4096);

    // Intentional server loop: runs until stdin closes (Eof signals EOF).
    loop {
        let line = match run_read_line(&mut reader, &mut line_buffer, MAX_REQUEST_BYTES).await {
            Ok(LineRead::Eof) => break, // Client disconnected.
            Ok(LineRead::Overflow) => {
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
            Err(e) => return Err(e.into()),
            Ok(LineRead::Line(line)) => line,
        };

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

/// Read one newline-delimited line from stdin, enforcing a hard byte limit.
///
/// Prevents OOM by never buffering more than `limit` bytes from a client-controlled
/// line. On overflow, drains forward to the next newline so the stream resyncs
/// for subsequent reads (same resync behavior as `LinesCodec`).
async fn run_read_line(
    reader: &mut BufReader<tokio::io::Stdin>,
    buffer: &mut Vec<u8>,
    limit: usize,
) -> std::io::Result<LineRead> {
    assert!(limit > 0);
    buffer.clear();

    loop {
        let available = reader.fill_buf().await?;

        // EOF: return accumulated buffer or signal end-of-stream.
        if available.is_empty() {
            if buffer.is_empty() {
                return Ok(LineRead::Eof);
            }
            debug_assert!(buffer.len() <= limit);
            let line = String::from_utf8_lossy(buffer).into_owned();
            return Ok(LineRead::Line(line));
        }

        let chunk_length = available.len();

        if let Some(newline_index) = available.iter().position(|&b| b == b'\n') {
            let within_limit = buffer.len() + newline_index <= limit;
            if within_limit {
                buffer.extend_from_slice(&available[..newline_index]);
            }
            // Last use of `available` — immutable borrow on reader ends here.
            Pin::new(&mut *reader).consume(newline_index + 1);

            if !within_limit {
                return Ok(LineRead::Overflow);
            }

            // Strip trailing \r for \r\n line endings.
            if buffer.last() == Some(&b'\r') {
                buffer.pop();
            }
            debug_assert!(buffer.len() <= limit);
            let line = String::from_utf8_lossy(buffer).into_owned();
            return Ok(LineRead::Line(line));
        }

        // No newline in this chunk — accumulate if within limit.
        let within_limit = buffer.len() + chunk_length <= limit;
        if within_limit {
            buffer.extend_from_slice(available);
        }
        // Last use of `available` — immutable borrow on reader ends here.
        Pin::new(&mut *reader).consume(chunk_length);

        if !within_limit {
            // Overlong line with no newline yet — drain to resync.
            return run_read_line_drain(reader).await;
        }
    }
}

/// Drain bytes from the reader until the next newline or EOF.
/// Called after detecting an overlong line to resync the stream.
async fn run_read_line_drain(
    reader: &mut BufReader<tokio::io::Stdin>,
) -> std::io::Result<LineRead> {
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            // EOF during drain — still report overflow so the error response is sent.
            return Ok(LineRead::Overflow);
        }
        let chunk_length = available.len();
        assert!(chunk_length > 0); // Progress guarantee: non-empty after EOF check.
        let newline_position = available.iter().position(|&b| b == b'\n');
        let consume_count = match newline_position {
            Some(index) => index + 1,
            None => chunk_length,
        };
        assert!(consume_count > 0); // Forward progress: always consume at least one byte.
        // Last use of `available` — immutable borrow on reader ends here.
        Pin::new(&mut *reader).consume(consume_count);

        if newline_position.is_some() {
            return Ok(LineRead::Overflow);
        }
    }
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
    // Validate params shape before dispatch — reject non-object params, a
    // missing or non-string name, and non-object arguments with Invalid Params
    // (-32602) rather than letting malformed input reach tools::dispatch.
    if !params.is_object() {
        return json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32602, "message": "Invalid params: expected object"}
        });
    }
    let Some(tool_name) = params
        .get("name")
        .and_then(|name_value| name_value.as_str())
    else {
        return json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32602, "message": "Invalid params: missing or non-string 'name'"}
        });
    };
    let arguments_value = params.get("arguments");
    if arguments_value.is_some_and(|arguments| !arguments.is_null() && !arguments.is_object()) {
        return json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32602, "message": "Invalid params: 'arguments' must be an object or null"}
        });
    }
    let tool_args = arguments_value
        .filter(|arguments| !arguments.is_null())
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
