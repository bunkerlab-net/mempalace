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
const REQUEST_BYTES_MAX: usize = 1024 * 1024; // 1 MiB

/// Outcome of reading a single newline-delimited frame from the buffered reader.
enum LineRead {
    /// A complete line was read (trailing newline stripped).
    Line(String),
    /// The line exceeded the byte limit — stream resynced past the next newline.
    Overflow,
    /// The line contained invalid UTF-8 and cannot be parsed as JSON.
    Invalid,
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

    // Intentional: server loop runs until stdin closes.
    loop {
        let line = match run_read_line(&mut reader, &mut line_buffer, REQUEST_BYTES_MAX).await {
            Ok(LineRead::Eof) => break, // Client disconnected.
            Ok(LineRead::Overflow) => {
                let err_response = json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {"code": -32700, "message": "Request exceeds maximum frame size"}
                });
                run_write_response(&mut stdout, &err_response).await?;
                continue;
            }
            Ok(LineRead::Invalid) => {
                let err_response = json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {"code": -32700, "message": "Request contains invalid UTF-8"}
                });
                run_write_response(&mut stdout, &err_response).await?;
                continue;
            }
            Err(e) => return Err(e.into()),
            Ok(LineRead::Line(line)) => line,
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let request = match run_parse_request(trimmed) {
            Ok(parsed) => parsed,
            Err(err_response) => {
                run_write_response(&mut stdout, &err_response).await?;
                continue;
            }
        };

        let response = handle_request(connection, &request).await;

        if let Some(response_body) = response {
            run_write_response(&mut stdout, &response_body).await?;
        }
    }

    Ok(())
}

/// Parse a JSON-RPC request from a trimmed line of text.
///
/// Returns `Ok(Value)` on success, or `Err(Value)` containing a JSON-RPC
/// parse-error response ready to send to the client.
fn run_parse_request(trimmed: &str) -> std::result::Result<Value, Value> {
    assert!(!trimmed.is_empty());
    serde_json::from_str(trimmed).map_err(|e| {
        json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": {"code": -32700, "message": format!("Parse error: {e}")}
        })
    })
}

/// Serialize `response` and write it as a newline-terminated frame to `stdout`.
async fn run_write_response(
    stdout: &mut tokio::io::Stdout,
    response: &Value,
) -> std::io::Result<()> {
    let serialized_response = serde_json::to_string(response)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    stdout.write_all(serialized_response.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
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
    run_read_line_impl(reader, buffer, limit).await
}

/// Generic core of `run_read_line` — accepts any `AsyncBufRead` so tests can
/// drive the algorithm with a `Cursor` rather than a real stdin handle.
async fn run_read_line_impl<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
    limit: usize,
) -> std::io::Result<LineRead> {
    assert!(limit > 0);
    buffer.clear();

    let mut read_iterations: usize = 0;
    loop {
        read_iterations += 1;
        // debug_assert: iteration count is a proxy for bytes read; a 1-byte-per-chunk
        // client could legitimately reach REQUEST_BYTES_MAX+1 iterations without being
        // a logic error, so this fires only in debug builds as a sanity ceiling.
        debug_assert!(
            read_iterations <= REQUEST_BYTES_MAX,
            "run_read_line_impl: exceeded REQUEST_BYTES_MAX ({REQUEST_BYTES_MAX}) read iterations"
        );
        let available = reader.fill_buf().await?;

        // EOF: return accumulated buffer or signal end-of-stream.
        if available.is_empty() {
            if buffer.is_empty() {
                return Ok(LineRead::Eof);
            }
            debug_assert!(buffer.len() <= limit);
            return match String::from_utf8(buffer.clone()) {
                Ok(line) => Ok(LineRead::Line(line)),
                Err(_) => Ok(LineRead::Invalid),
            };
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
            return match String::from_utf8(buffer.clone()) {
                Ok(line) => Ok(LineRead::Line(line)),
                Err(_) => Ok(LineRead::Invalid),
            };
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
async fn run_read_line_drain<R: AsyncBufRead + Unpin>(reader: &mut R) -> std::io::Result<LineRead> {
    let mut drain_iterations: usize = 0;
    loop {
        drain_iterations += 1;
        // debug_assert: same reasoning as run_read_line_impl — iteration count is not
        // a byte count; a slow client could legitimately exceed this in production.
        debug_assert!(
            drain_iterations <= REQUEST_BYTES_MAX,
            "run_read_line_drain: exceeded REQUEST_BYTES_MAX ({REQUEST_BYTES_MAX}) drain iterations"
        );
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

/// Validate request shape and dispatch to `handle_request_dispatch`.
///
/// Returns `None` for notifications (no response required per JSON-RPC 2.0).
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

    handle_request_dispatch(connection, method, &params, req_id).await
}

/// Dispatch a validated JSON-RPC method to the appropriate handler.
///
/// Called after `handle_request` has confirmed the request is a well-formed object
/// with a string `method` field.
async fn handle_request_dispatch(
    connection: &Connection,
    method: &str,
    params: &Value,
    req_id: Value,
) -> Option<Value> {
    match method {
        "initialize" => Some(handle_request_initialize(params, &req_id)),

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

        "tools/call" => Some(handle_request_tools_call(connection, params, req_id).await),

        _ => Some(json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": format!("Unknown method: {method}")}
        })),
    }
}

/// Handle the `initialize` method: negotiate protocol version and return server info.
fn handle_request_initialize(params: &Value, req_id: &Value) -> Value {
    let client_version = params
        .get("protocolVersion")
        .and_then(|proto_val| proto_val.as_str());
    let negotiated = if let Some(cv) = client_version {
        if SUPPORTED_PROTOCOL_VERSIONS.contains(&cv) {
            cv
        } else {
            SUPPORTED_PROTOCOL_VERSIONS[0]
        }
    } else {
        SUPPORTED_PROTOCOL_VERSIONS[0]
    };
    json!({
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "protocolVersion": negotiated,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mempalace", "version": env!("CARGO_PKG_VERSION")},
        }
    })
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
    let (tool_name, tool_args) = match handle_request_tools_call_validate(params, &req_id) {
        Ok(validated) => validated,
        Err(err_response) => return err_response,
    };

    let result = tools::dispatch(connection, tool_name, &tool_args).await;

    handle_request_tools_call_respond(tool_name, result, &req_id)
}

/// Validate `tools/call` params and extract `(tool_name, tool_args)`.
///
/// Returns `Ok((name, args))` when the params are well-formed, or `Err(response)`
/// with a ready-to-send JSON-RPC Invalid Params error when they are not.
fn handle_request_tools_call_validate<'a>(
    params: &'a Value,
    req_id: &Value,
) -> std::result::Result<(&'a str, Value), Value> {
    if !params.is_object() {
        return Err(json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32602, "message": "Invalid params: expected object"}
        }));
    }
    let Some(tool_name) = params
        .get("name")
        .and_then(|name_value| name_value.as_str())
    else {
        return Err(json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32602, "message": "Invalid params: missing or non-string 'name'"}
        }));
    };
    let arguments_value = params.get("arguments");
    if arguments_value.is_some_and(|arguments| !arguments.is_null() && !arguments.is_object()) {
        return Err(json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32602, "message": "Invalid params: 'arguments' must be an object or null"}
        }));
    }
    let tool_args = arguments_value
        .filter(|arguments| !arguments.is_null())
        .cloned()
        .unwrap_or(json!({}));
    Ok((tool_name, tool_args))
}

/// Build a sanitized JSON-RPC response from a raw tool dispatch result.
///
/// Sanitize: only expose errors that tools explicitly mark as public.
/// All other errors are masked so internal paths and database details
/// are never leaked over the protocol.
fn handle_request_tools_call_respond(tool_name: &str, result: Value, req_id: &Value) -> Value {
    let sanitized = if let Some(error_val) = result.get("error") {
        let is_public = result
            .get("public")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        // Extract the full error string before deciding what to expose.
        // Owned String so the borrow on `result` via `error_val` ends here,
        // allowing `result` to be moved in the is_public arm below.
        let full_error: String = error_val.as_str().unwrap_or("unknown").to_owned();
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
            // Return the full result so sibling fields (e.g. "existing_drawer_id")
            // are preserved alongside the "error" key.
            result
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

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // -- handle_request tests ------------------------------------------------

    #[tokio::test]
    async fn handle_request_initialize_supported_version() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "initialize",
            "params": {"protocolVersion": "2024-11-05"},
            "id": 1
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        let result = &response["result"];
        assert_eq!(result["protocolVersion"], "2024-11-05");
        assert_eq!(result["serverInfo"]["name"], "mempalace");
    }

    #[tokio::test]
    async fn handle_request_initialize_unsupported_falls_back() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "initialize",
            "params": {"protocolVersion": "9999-01-01"},
            "id": 1
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        let result = &response["result"];
        // Falls back to the latest supported version.
        assert_eq!(result["protocolVersion"], SUPPORTED_PROTOCOL_VERSIONS[0]);
        assert_eq!(result["serverInfo"]["name"], "mempalace");
    }

    #[tokio::test]
    async fn handle_request_initialize_no_version() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "initialize",
            "params": {},
            "id": 1
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        let result = &response["result"];
        // Missing protocolVersion falls back to the latest supported version.
        assert_eq!(result["protocolVersion"], SUPPORTED_PROTOCOL_VERSIONS[0]);
        assert_eq!(result["serverInfo"]["name"], "mempalace");
    }

    #[tokio::test]
    async fn handle_request_ping() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({"method": "ping", "id": 2});
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        assert_eq!(response["result"], json!({}));
        assert_eq!(response["id"], 2);
    }

    #[tokio::test]
    async fn handle_request_notification_returns_none() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({"method": "notifications/initialized"});
        let response = handle_request(&connection, &request).await;
        assert!(response.is_none(), "notifications must return None");
        // Also verify a different notification prefix.
        let request_notification = json!({"method": "notifications/cancelled"});
        let response_notification = handle_request(&connection, &request_notification).await;
        assert!(
            response_notification.is_none(),
            "all notifications/ must return None"
        );
    }

    #[tokio::test]
    async fn handle_request_tools_list() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({"method": "tools/list", "id": 3});
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        let tools = response["result"]["tools"]
            .as_array()
            .expect("tools should be an array");
        assert!(!tools.is_empty(), "tools list must not be empty");
        // Every entry must be an object with the required MCP tool keys.
        for tool in tools {
            assert!(tool.is_object(), "each tool must be a JSON object");
            assert!(tool.get("name").is_some(), "each tool must have a 'name'");
            assert!(
                tool.get("description").is_some(),
                "each tool must have a 'description'"
            );
            assert!(
                tool.get("inputSchema").is_some(),
                "each tool must have an 'inputSchema'"
            );
        }
        // At least one well-known tool must be present.
        assert!(
            tools.iter().any(|tool| tool["name"] == "mempalace_status"),
            "tools list must include 'mempalace_status'"
        );
    }

    #[tokio::test]
    async fn handle_request_unknown_method() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({"method": "bogus", "id": 4});
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        assert_eq!(response["error"]["code"], -32601);
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message should be a string")
                .contains("bogus"),
            "error message should mention the unknown method"
        );
    }

    #[tokio::test]
    async fn handle_request_non_object() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!("string");
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        assert_eq!(response["error"]["code"], -32600);
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message should be a string")
                .contains("Invalid Request"),
        );
    }

    #[tokio::test]
    async fn handle_request_missing_method() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({"id": 1});
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        assert_eq!(response["error"]["code"], -32600);
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message should be a string")
                .contains("method"),
            "error message should mention missing method"
        );
    }

    #[tokio::test]
    async fn handle_request_tools_call_valid() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "tools/call",
            "params": {"name": "mempalace_status", "arguments": {}},
            "id": 5
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        let content = response["result"]["content"]
            .as_array()
            .expect("content should be an array");
        assert!(!content.is_empty(), "content array must not be empty");
        assert_eq!(content[0]["type"], "text");
    }

    #[tokio::test]
    async fn handle_request_tools_call_missing_name() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "tools/call",
            "params": {"arguments": {}},
            "id": 6
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message should be a string")
                .contains("name"),
            "error message should mention missing name"
        );
    }

    #[tokio::test]
    async fn handle_request_tools_call_invalid_arguments() {
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "tools/call",
            "params": {"name": "mempalace_status", "arguments": "string"},
            "id": 7
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message should be a string")
                .contains("arguments"),
            "error message should mention invalid arguments"
        );
    }

    // -- run_parse_request ---------------------------------------------------

    #[test]
    fn run_parse_request_valid_json_returns_ok() {
        // A well-formed JSON object must be returned as a parsed Value.
        let result = run_parse_request(r#"{"method":"ping","id":1}"#);
        assert!(result.is_ok(), "valid JSON must return Ok");
        let value = result.expect("valid JSON must parse");
        assert_eq!(value["method"], "ping");
        assert_eq!(value["id"], 1);
    }

    #[test]
    fn run_parse_request_invalid_json_returns_error_response() {
        // Invalid JSON must produce a JSON-RPC -32700 parse-error response.
        let result = run_parse_request("not_valid{{{json");
        assert!(result.is_err(), "invalid JSON must return Err");
        let error_response = result.expect_err("invalid JSON must produce error");
        assert_eq!(
            error_response["error"]["code"], -32700,
            "error code must be -32700 for a parse error"
        );
        assert!(
            error_response["error"]["message"]
                .as_str()
                .expect("message must be a string")
                .contains("Parse error"),
            "error message must contain 'Parse error'"
        );
        // The id field must be null for parse errors (no id extracted).
        assert_eq!(
            error_response["id"],
            serde_json::Value::Null,
            "id must be null when the request cannot be parsed"
        );
    }

    // -- run_read_line tests (via run_read_line_impl) ------------------------

    #[tokio::test]
    async fn read_line_normal() {
        let cursor = Cursor::new(b"hello\n".to_vec());
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, 1024)
            .await
            .expect("read should succeed");
        let LineRead::Line(line) = result else {
            panic!("expected LineRead::Line, got Overflow or Eof");
        };
        assert_eq!(line, "hello");
        // Verify buffer was used for accumulation.
        assert_eq!(buffer.len(), 5, "buffer should contain 'hello' (5 bytes)");
    }

    #[tokio::test]
    async fn read_line_eof() {
        let cursor = Cursor::new(Vec::new());
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, 1024)
            .await
            .expect("read should succeed");
        assert!(
            matches!(result, LineRead::Eof),
            "expected Eof on empty input"
        );
        assert!(buffer.is_empty(), "buffer should remain empty on EOF");
    }

    #[tokio::test]
    async fn read_line_overflow() {
        // Oversized line followed by a valid short line. After the Overflow the
        // reader must resync past the newline so the next frame is recovered.
        let limit = 10;
        let input = "a".repeat(limit + 5) + "\n" + "ok\n";
        let cursor = Cursor::new(input.into_bytes());
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, limit)
            .await
            .expect("read should succeed");
        assert!(
            matches!(result, LineRead::Overflow),
            "expected Overflow for line exceeding limit"
        );
        // After overflow drain, the reader must resync and return the next line.
        buffer.clear();
        let next = run_read_line_impl(&mut reader, &mut buffer, limit)
            .await
            .expect("second read should succeed");
        let LineRead::Line(recovered) = next else {
            panic!("expected LineRead::Line after overflow resync, got Eof or Overflow");
        };
        assert_eq!(
            recovered, "ok",
            "valid line after overflow must be recovered"
        );
    }

    #[tokio::test]
    async fn read_line_invalid_utf8() {
        // A line containing an invalid UTF-8 sequence must yield Invalid, not a
        // silently repaired string that could produce unexpected JSON parse results.
        let mut input = b"valid prefix \xFF\xFE invalid suffix\n".to_vec();
        // Pair assertion: the bytes are definitely not valid UTF-8.
        assert!(String::from_utf8(input.clone()).is_err());
        let cursor = Cursor::new(std::mem::take(&mut input));
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, 1024)
            .await
            .expect("read should succeed");
        assert!(
            matches!(result, LineRead::Invalid),
            "expected Invalid for line with non-UTF-8 bytes"
        );
    }

    #[tokio::test]
    async fn read_line_crlf_stripped() {
        let cursor = Cursor::new(b"hello\r\n".to_vec());
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, 1024)
            .await
            .expect("read should succeed");
        let LineRead::Line(line) = result else {
            panic!("expected LineRead::Line, got Overflow or Eof");
        };
        assert_eq!(line, "hello", "\\r\\n should be stripped to just 'hello'");
        // Buffer should have 'hello' without the \r.
        assert_eq!(buffer.len(), 5, "buffer should be 5 bytes after \\r strip");
    }

    #[tokio::test]
    async fn read_line_eof_with_partial_line_no_newline() {
        // A stream that ends without a trailing newline must return the accumulated bytes as a line.
        let cursor = Cursor::new(b"no newline at end".to_vec());
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, 1024)
            .await
            .expect("read should succeed");
        let LineRead::Line(line) = result else {
            panic!("expected LineRead::Line for partial line at EOF");
        };
        assert_eq!(
            line, "no newline at end",
            "partial line at EOF must be returned"
        );
        assert_eq!(buffer.len(), 17, "buffer must contain all bytes");
    }

    #[tokio::test]
    async fn read_line_overflow_no_newline_drains_to_eof() {
        // An oversized line with no trailing newline must drain to EOF and still report Overflow.
        let limit = 5;
        let input = "a".repeat(limit + 10); // No newline — entire input is one oversized line.
        let cursor = Cursor::new(input.into_bytes());
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, limit)
            .await
            .expect("read should succeed");
        assert!(
            matches!(result, LineRead::Overflow),
            "oversized line without newline must report Overflow"
        );
        // After drain the stream is at EOF; next read must return Eof.
        buffer.clear();
        let next = run_read_line_impl(&mut reader, &mut buffer, limit)
            .await
            .expect("second read should succeed");
        assert!(
            matches!(next, LineRead::Eof),
            "after overflow drain to EOF the next read must return Eof"
        );
    }

    // -- handle_request_tools_call edge cases --------------------------------

    #[tokio::test]
    async fn handle_request_tools_call_non_object_params_returns_error() {
        // When 'params' is not a JSON object (e.g. a string), tools/call must
        // return Invalid Params (-32602) rather than panicking.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "tools/call",
            "params": "not_an_object",
            "id": 8
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("handle_request should return Some for tools/call with non-object params");
        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("error message must be a string")
                .contains("params"),
            "error must mention invalid params"
        );
    }

    #[tokio::test]
    async fn handle_request_tools_call_null_arguments_succeeds() {
        // null arguments must be treated as an empty object — the call must succeed.
        let (_db, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "tools/call",
            "params": {"name": "mempalace_status", "arguments": null},
            "id": 9
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("handle_request should return Some for tools/call with null arguments");
        // Must produce a result, not an error.
        let content = response["result"]["content"]
            .as_array()
            .expect("result content must be a JSON array");
        assert!(
            !content.is_empty(),
            "null arguments must produce a valid tool result"
        );
        assert_eq!(content[0]["type"], "text", "content type must be 'text'");
    }

    #[tokio::test]
    async fn handle_request_tools_call_unknown_tool_returns_internal_error() {
        // Calling an unknown tool name must return a sanitized error (not expose internals).
        let (_db, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "tools/call",
            "params": {"name": "nonexistent_tool", "arguments": {}},
            "id": 10
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("handle_request should return Some for unknown tool name");
        // The result content must contain an error field.
        let content = response["result"]["content"]
            .as_array()
            .expect("result content must be a JSON array");
        assert!(
            !content.is_empty(),
            "unknown tool must return content with error"
        );
        let text = content[0]["text"]
            .as_str()
            .expect("text field must be a string");
        assert!(!text.is_empty(), "error text must not be empty");
    }

    // -- handle_request_tools_call_respond: public vs private error sanitization --

    #[test]
    fn tools_call_respond_public_error_is_exposed() {
        // When the tool result carries `"public": true` the full error must be
        // forwarded to the client (e.g. "Unknown tool: foo").
        let result = json!({"error": "Unknown tool: foo", "public": true});
        let req_id = json!(42);
        let response = handle_request_tools_call_respond("foo", result, &req_id);

        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text must be a string");
        assert!(
            text.contains("Unknown tool"),
            "public error text must contain the original error"
        );
        // The response must be a proper JSON-RPC result, not a top-level error.
        assert!(
            response.get("error").is_none(),
            "public tool error must not produce a top-level JSON-RPC error"
        );
        assert_eq!(response["id"], 42);
    }

    #[test]
    fn tools_call_respond_private_error_is_masked() {
        // When the tool result does NOT carry `"public": true` the error must be
        // replaced with a generic "Internal tool error" message so internals are
        // never exposed over the protocol.
        let result = json!({"error": "database path /secret/db leaked", "public": false});
        let req_id = json!(99);
        let response = handle_request_tools_call_respond("some_tool", result, &req_id);

        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text must be a string");
        assert!(
            text.contains("Internal tool error"),
            "private error must be replaced with 'Internal tool error'"
        );
        assert!(
            !text.contains("secret"),
            "internal path must not be leaked in the response"
        );
        assert_eq!(response["id"], 99);
    }

    #[test]
    fn tools_call_respond_no_error_field_passes_through() {
        // When the tool result has no "error" key the full result must be forwarded.
        let result = json!({"total_drawers": 7, "wings": {}});
        let req_id = json!(5);
        let response = handle_request_tools_call_respond("mempalace_status", result, &req_id);

        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text must be a string");
        assert!(
            text.contains("total_drawers"),
            "successful result must contain the tool output"
        );
        assert_eq!(response["id"], 5);
    }

    // -- handle_request_tools_call_validate edge cases --

    #[test]
    fn tools_call_validate_null_name_returns_error() {
        // A null "name" field must return Invalid Params (-32602).
        let params = json!({"name": null, "arguments": {}});
        let req_id = json!(11);
        let result = handle_request_tools_call_validate(&params, &req_id);
        assert!(result.is_err(), "null name must return Err");
        let error_response = result.expect_err("null name must be invalid");
        assert_eq!(
            error_response["error"]["code"], -32602,
            "null name must produce -32602"
        );
    }

    #[test]
    fn tools_call_validate_absent_arguments_defaults_to_empty_object() {
        // When "arguments" is absent from params it must default to an empty JSON object.
        let params = json!({"name": "mempalace_status"});
        let req_id = json!(12);
        let result = handle_request_tools_call_validate(&params, &req_id);
        assert!(result.is_ok(), "absent arguments must be Ok");
        let (name, args) = result.expect("absent arguments must produce Ok");
        assert_eq!(name, "mempalace_status");
        assert_eq!(
            args,
            json!({}),
            "absent arguments must default to empty object"
        );
    }

    // -- run_read_line: edge cases --

    #[tokio::test]
    async fn read_line_empty_line_before_valid_line() {
        // An empty line (lone newline) must return LineRead::Line("") — the server
        // loop then skips it via the `trimmed.is_empty()` check, but the reader
        // itself must return Line, not Eof.
        let cursor = Cursor::new(b"\nhello\n".to_vec());
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, 1024)
            .await
            .expect("read should succeed");
        let LineRead::Line(line) = result else {
            panic!("expected LineRead::Line for empty line, got Eof or Overflow");
        };
        assert_eq!(line, "", "lone newline must produce empty string");
        // Pair: next read must return the following line.
        buffer.clear();
        let next = run_read_line_impl(&mut reader, &mut buffer, 1024)
            .await
            .expect("second read must succeed");
        let LineRead::Line(next_line) = next else {
            panic!("expected LineRead::Line for 'hello'");
        };
        assert_eq!(next_line, "hello");
    }

    #[tokio::test]
    async fn read_line_eof_with_partial_invalid_utf8_no_newline() {
        // A stream that ends mid-line with invalid UTF-8 and no trailing newline
        // must return Invalid, not Eof or a silently repaired string.
        let raw: Vec<u8> = vec![b'o', b'k', 0xFF, 0xFE]; // no newline
        // Pair assertion: raw bytes are definitely not valid UTF-8.
        assert!(String::from_utf8(raw.clone()).is_err());
        let cursor = Cursor::new(raw);
        let mut reader = BufReader::new(cursor);
        let mut buffer = Vec::new();
        let result = run_read_line_impl(&mut reader, &mut buffer, 1024)
            .await
            .expect("read should succeed");
        assert!(
            matches!(result, LineRead::Invalid),
            "EOF partial line with invalid UTF-8 must return Invalid"
        );
    }

    // -- run_parse_request: additional edge cases --

    #[test]
    fn run_parse_request_json_array_returns_ok() {
        // A JSON array is valid JSON and must parse as Ok, even though it
        // will later be rejected by handle_request's is_object() check.
        let result = run_parse_request("[1,2,3]");
        assert!(
            result.is_ok(),
            "JSON array must parse as Ok (validation happens later)"
        );
        let value = result.expect("array must parse");
        assert!(value.is_array(), "parsed value must be a JSON array");
    }

    // -- handle_request_initialize: non-string protocolVersion falls back --

    #[tokio::test]
    async fn handle_request_initialize_numeric_version_falls_back() {
        // A numeric protocolVersion (which fails as_str()) must fall back to the
        // latest supported version rather than panicking.
        let (_database, connection) = crate::test_helpers::test_db().await;
        let request = json!({
            "method": "initialize",
            "params": {"protocolVersion": 20_241_105},
            "id": 1
        });
        let response = handle_request(&connection, &request)
            .await
            .expect("should return Some");
        let result = &response["result"];
        assert_eq!(
            result["protocolVersion"], SUPPORTED_PROTOCOL_VERSIONS[0],
            "numeric protocolVersion must fall back to latest supported version"
        );
        assert_eq!(result["serverInfo"]["name"], "mempalace");
    }
}
