//! MCP server — JSON-RPC 2.0 over stdio exposing palace tools.
//!
//! Error handling policy: tool errors are logged to stderr with limited/truncated
//! detail (first 100 chars) and the client receives only a generic `"Internal tool error"`
//! message for unstructured errors, so that internal paths and database details are
//! never leaked over the protocol.

pub mod protocol;
pub mod tools;

use serde_json::{Value, json};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use turso::Connection;

use crate::error::{Error, Result};

const MAX_MCP_BODY_LEN: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportMode {
    LineDelimitedJson,
    HeaderFramed,
}

impl TransportMode {
    fn uses_headers(self) -> bool {
        matches!(self, Self::HeaderFramed)
    }
}

const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Run the MCP server: read JSON-RPC 2.0 requests from stdin, write responses to stdout.
pub async fn run(conn: &Connection) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut stdout = tokio::io::stdout();
    let mut transport_mode = None;

    loop {
        let Some(request_text) = read_request(&mut reader, &mut transport_mode).await? else {
            break; // EOF
        };

        let request: Value = match serde_json::from_str(&request_text) {
            Ok(v) => v,
            Err(e) => {
                let err_response = json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {"code": -32700, "message": format!("Parse error: {e}")}
                });
                write_response(
                    &mut stdout,
                    transport_mode.unwrap_or(TransportMode::LineDelimitedJson),
                    &err_response,
                )
                .await?;
                continue;
            }
        };

        let response = handle_request(conn, &request).await;

        if let Some(resp) = response {
            write_response(
                &mut stdout,
                transport_mode.unwrap_or(TransportMode::LineDelimitedJson),
                &resp,
            )
            .await?;
        }
    }

    Ok(())
}

async fn read_request<R>(
    reader: &mut R,
    transport_mode: &mut Option<TransportMode>,
) -> Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    match transport_mode {
        Some(TransportMode::LineDelimitedJson) => read_line_request(reader).await,
        Some(TransportMode::HeaderFramed) => read_framed_request(reader, None).await,
        None => {
            let Some(first_line) = read_nonempty_line(reader).await? else {
                return Ok(None);
            };

            if looks_like_json(&first_line) || !looks_like_mcp_header(&first_line) {
                *transport_mode = Some(TransportMode::LineDelimitedJson);
                Ok(Some(first_line))
            } else {
                *transport_mode = Some(TransportMode::HeaderFramed);
                read_framed_request(reader, Some(first_line)).await
            }
        }
    }
}

async fn read_line_request<R>(reader: &mut R) -> Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    read_nonempty_line(reader).await
}

async fn read_nonempty_line<R>(reader: &mut R) -> Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            return Ok(None);
        }

        let trimmed = trim_line_endings(&line);
        if trimmed.is_empty() {
            continue;
        }

        return Ok(Some(trimmed.to_string()));
    }
}

async fn read_framed_request<R>(
    reader: &mut R,
    first_line: Option<String>,
) -> Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    let mut content_length = None;

    if let Some(line) = first_line {
        parse_header_line(&line, &mut content_length)?;
    }

    loop {
        let mut header_line = String::new();
        let bytes_read = reader.read_line(&mut header_line).await?;
        if bytes_read == 0 {
            if content_length.is_none() {
                return Ok(None);
            }
            return Err(Error::Other(
                "unexpected EOF while reading MCP headers".to_string(),
            ));
        }

        let trimmed = trim_line_endings(&header_line);
        if trimmed.is_empty() {
            break;
        }

        parse_header_line(trimmed, &mut content_length)?;
    }

    let body_len = content_length
        .ok_or_else(|| Error::Other("missing Content-Length header in MCP request".to_string()))?;
    if body_len > MAX_MCP_BODY_LEN {
        return Err(Error::Other(format!(
            "MCP request body exceeds {MAX_MCP_BODY_LEN} bytes"
        )));
    }

    let mut body = vec![0_u8; body_len];
    reader.read_exact(&mut body).await?;

    String::from_utf8(body)
        .map(Some)
        .map_err(|e| Error::Other(format!("invalid UTF-8 body in MCP request: {e}")))
}

fn parse_header_line(line: &str, content_length: &mut Option<usize>) -> Result<()> {
    let (name, value) = line
        .split_once(':')
        .ok_or_else(|| Error::Other(format!("invalid MCP header line: {line}")))?;

    if name.eq_ignore_ascii_case("Content-Length") {
        let parsed = value.trim().parse::<usize>().map_err(|e| {
            Error::Other(format!(
                "invalid Content-Length header value '{}': {e}",
                value.trim()
            ))
        })?;
        *content_length = Some(parsed);
    }

    Ok(())
}

async fn write_response<W>(writer: &mut W, mode: TransportMode, response: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let body = serde_json::to_string(response).unwrap_or_default();

    if mode.uses_headers() {
        let header = format!("Content-Length: {}\r\n\r\n", body.len());
        writer.write_all(header.as_bytes()).await?;
        writer.write_all(body.as_bytes()).await?;
    } else {
        writer.write_all(body.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }

    writer.flush().await?;
    Ok(())
}

fn trim_line_endings(line: &str) -> &str {
    line.trim_end_matches(['\r', '\n'])
}

fn looks_like_json(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with('{') || trimmed.starts_with('[')
}

fn looks_like_mcp_header(line: &str) -> bool {
    match line.split_once(':') {
        Some((name, _)) => name.eq_ignore_ascii_case("Content-Length"),
        None => false,
    }
}

async fn handle_request(conn: &Connection, request: &Value) -> Option<Value> {
    let method = request.get("method").and_then(|m| m.as_str()).unwrap_or("");
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

        "tools/call" => {
            let tool_name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let tool_args = params
                .get("arguments")
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or(json!({}));

            let result = tools::dispatch(conn, tool_name, &tool_args).await;

            // Sanitize: only expose errors that tools explicitly mark as public.
            // All other errors are masked so internal paths and database details
            // are never leaked over the protocol.
            let sanitized = if let Some(error_val) = result.get("error") {
                let is_public = result
                    .get("public")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let error_msg: String = error_val
                    .as_str()
                    .unwrap_or("unknown")
                    .chars()
                    .take(100)
                    .collect();
                eprintln!("tool error: tool={tool_name} error={error_msg}");
                if is_public {
                    json!({"error": error_msg})
                } else {
                    json!({"error": "Internal tool error"})
                }
            } else {
                result
            };

            let text = serde_json::to_string_pretty(&sanitized).unwrap_or_default();

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::test_db;

    #[tokio::test]
    async fn reads_legacy_line_delimited_json() {
        let input = br#"{"jsonrpc":"2.0","id":1,"method":"initialize"}
"#;
        let mut reader = BufReader::new(&input[..]);
        let mut mode = None;

        let request = read_request(&mut reader, &mut mode)
            .await
            .expect("read request")
            .expect("request body");

        assert_eq!(mode, Some(TransportMode::LineDelimitedJson));
        assert_eq!(request, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#);
    }

    #[tokio::test]
    async fn reads_content_length_framed_json() {
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;
        let input = format!("Content-Length: {}\r\n\r\n{body}", body.len());
        let mut reader = BufReader::new(input.as_bytes());
        let mut mode = None;

        let request = read_request(&mut reader, &mut mode)
            .await
            .expect("read request")
            .expect("request body");

        assert_eq!(mode, Some(TransportMode::HeaderFramed));
        assert_eq!(request, body);
    }

    #[tokio::test]
    async fn malformed_non_header_input_stays_in_line_mode() {
        let input = b"oops\n";
        let mut reader = BufReader::new(&input[..]);
        let mut mode = None;

        let request = read_request(&mut reader, &mut mode)
            .await
            .expect("read request")
            .expect("request body");

        assert_eq!(mode, Some(TransportMode::LineDelimitedJson));
        assert_eq!(request, "oops");
    }

    #[tokio::test]
    async fn rejects_oversized_framed_requests() {
        let input = format!("Content-Length: {}\r\n\r\n", MAX_MCP_BODY_LEN + 1);
        let mut reader = BufReader::new(input.as_bytes());

        let error = read_framed_request(&mut reader, None)
            .await
            .expect_err("oversized frame should fail");

        match error {
            Error::Other(message) => assert!(message.contains("exceeds")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn writes_content_length_framed_responses() {
        let mut output = Vec::new();
        let response = json!({"jsonrpc": "2.0", "id": 1, "result": {"ok": true}});

        write_response(&mut output, TransportMode::HeaderFramed, &response)
            .await
            .expect("write response");

        let output = String::from_utf8(output).expect("utf8");
        let body = serde_json::to_string(&response).expect("serialize");
        assert_eq!(
            output,
            format!("Content-Length: {}\r\n\r\n{body}", body.len())
        );
    }

    #[tokio::test]
    async fn initialize_returns_server_info() {
        let (_db, conn) = test_db().await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1.0"}
            }
        });

        let response = handle_request(&conn, &request).await.expect("response");

        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(response["result"]["serverInfo"]["name"], "mempalace");
    }

    #[tokio::test]
    async fn tools_list_returns_known_tools() {
        let (_db, conn) = test_db().await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        });

        let response = handle_request(&conn, &request).await.expect("response");
        let tools = response["result"]["tools"].as_array().expect("tool array");

        assert!(!tools.is_empty());
        assert_eq!(tools[0]["name"], "mempalace_status");
    }

    #[tokio::test]
    async fn tools_call_returns_status_payload() {
        let (_db, conn) = test_db().await;
        let request = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "mempalace_status",
                "arguments": {}
            }
        });

        let response = handle_request(&conn, &request).await.expect("response");
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text content");

        assert!(text.contains("\"total_drawers\": 0"));
        assert!(text.contains("\"wings\": {}"));
    }
}
