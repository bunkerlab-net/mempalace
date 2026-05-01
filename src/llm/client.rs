//! LLM provider implementations for entity refinement.
//!
//! All providers implement the [`LlmProvider`] trait with a `classify` method
//! (system + user prompt → structured text) and a `check_available` probe
//! (fast reachability check). HTTP is handled via `ureq` (sync, no tokio
//! dependency needed for the sequential init pipeline).
//!
//! Providers:
//! - [`OllamaProvider`] — local Ollama server (default, fully offline)
//! - [`OpenAICompatProvider`] — any `/v1/chat/completions` endpoint
//! - [`AnthropicProvider`] — Anthropic Messages API
//!
//! Public API:
//! - [`LlmProvider`] trait
//! - [`LlmResponse`] struct
//! - [`get_provider`] factory

use std::time::Duration;

use serde_json::{Value, json};

use crate::error::{Error, Result};

// Default endpoints — overridable via --llm-endpoint.
const OLLAMA_DEFAULT_ENDPOINT: &str = "http://localhost:11434";
const ANTHROPIC_DEFAULT_ENDPOINT: &str = "https://api.anthropic.com";

// Anthropic API version header value.
const ANTHROPIC_API_VERSION: &str = "2023-06-01";

// Low temperature reduces hallucination in classification tasks.
const CLASSIFY_TEMPERATURE: f64 = 0.1;

// Timeout for check_available probes — fast, must not block the init flow.
const CHECK_TIMEOUT_SECS: u64 = 5;

const _: () = assert!(CHECK_TIMEOUT_SECS > 0);

// ===================== PUBLIC TYPES =====================

/// Structured response from an LLM classify call.
pub struct LlmResponse {
    /// The raw text returned by the model (typically JSON for entity refinement).
    pub text: String,
}

/// Provenance of a provider's API key.
///
/// Used by `mempalace init` to decide whether to prompt for consent when the
/// provider's endpoint is external: a key loaded from the environment without
/// an explicit `--llm-api-key` flag may be a stray credential, so we ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeySource {
    /// Key was supplied via the explicit `--llm-api-key` CLI flag.
    Flag,
    /// Key was resolved from an environment variable as a fallback.
    Env,
}

/// Common interface for all LLM providers.
pub trait LlmProvider: Send + Sync {
    /// Send a system + user prompt and return the model's text response.
    ///
    /// `json_mode=true` signals to the provider that JSON output is expected;
    /// providers that support a native JSON mode will enable it.
    fn classify(&self, system: &str, user: &str, json_mode: bool) -> Result<LlmResponse>;

    /// Fast reachability probe. Returns `(ok, message)`.
    ///
    /// `ok=false` means the provider is unreachable or misconfigured. The
    /// message is human-readable and should be shown to the user.
    fn check_available(&self) -> (bool, String);

    /// Short identifier string (e.g. `"ollama"`, `"openai-compat"`, `"anthropic"`).
    fn name(&self) -> &'static str;

    /// The configured endpoint URL for this provider.
    ///
    /// Used by the default `is_external_service` implementation.
    fn endpoint(&self) -> &str;

    /// How the provider's API key was obtained, or `None` when no key is in play.
    ///
    /// Used by `mempalace init` consent gate: `Some(Env)` + external endpoint
    /// triggers the interactive `[y/N]` prompt unless `--accept-external-llm` is set.
    fn api_key_source(&self) -> Option<ApiKeySource>;

    /// Return `true` when this provider's endpoint will send user content off
    /// the local machine or private network.
    ///
    /// URL-based heuristic shared by all three in-tree providers. `cmd_init`
    /// uses the result to print a privacy warning before the first classify
    /// call (issue #24).
    fn is_external_service(&self) -> bool {
        !endpoint_is_local(self.endpoint())
    }
}

// ===================== OLLAMA =====================

/// Local Ollama server provider (default, no API key required).
pub struct OllamaProvider {
    model: String,
    endpoint: String,
    timeout_secs: u64,
}

impl OllamaProvider {
    /// Build an `OllamaProvider`, resolving the endpoint to its default if absent.
    ///
    /// Called by [`get_provider`] when `name == "ollama"`.
    pub fn new(model: String, endpoint: Option<String>, timeout_secs: u64) -> Self {
        assert!(!model.is_empty());
        assert!(timeout_secs > 0);
        let resolved_endpoint = endpoint
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| OLLAMA_DEFAULT_ENDPOINT.to_string());
        assert!(!resolved_endpoint.is_empty());
        Self {
            model,
            endpoint: resolved_endpoint,
            timeout_secs,
        }
    }
}

impl LlmProvider for OllamaProvider {
    fn name(&self) -> &'static str {
        "ollama"
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    // Ollama never uses an API key.
    fn api_key_source(&self) -> Option<ApiKeySource> {
        None
    }

    fn check_available(&self) -> (bool, String) {
        assert!(!self.endpoint.is_empty());
        assert!(!self.model.is_empty());

        let url = format!("{}/api/tags", self.endpoint);
        let Ok(data) = http_get(&url, CHECK_TIMEOUT_SECS) else {
            return (false, format!("Cannot reach Ollama at {}", self.endpoint));
        };

        // Accept model name with or without the `:latest` tag.
        let names: std::collections::HashSet<String> = data
            .get("models")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(|model| model.get("name").and_then(Value::as_str))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();

        let model_found =
            names.contains(&self.model) || names.contains(&format!("{}:latest", self.model));
        if !model_found {
            return (
                false,
                format!(
                    "Model '{}' not in Ollama. Run: ollama pull {}",
                    self.model, self.model
                ),
            );
        }
        (true, "ok".to_string())
    }

    fn classify(&self, system: &str, user: &str, json_mode: bool) -> Result<LlmResponse> {
        assert!(!system.is_empty());
        assert!(!user.is_empty());

        let mut body = json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": false,
            "options": {"temperature": CLASSIFY_TEMPERATURE},
        });
        if json_mode {
            body["format"] = json!("json");
        }

        let url = format!("{}/api/chat", self.endpoint);
        let data = http_post_json(&url, &body, &[], self.timeout_secs)?;
        let text = data
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_default();

        if text.is_empty() {
            return Err(Error::Llm(format!(
                "Empty response from Ollama (model={})",
                self.model
            )));
        }
        Ok(LlmResponse { text })
    }
}

// ===================== OPENAI-COMPAT =====================

/// Any OpenAI-compatible `/v1/chat/completions` endpoint.
pub struct OpenAICompatProvider {
    model: String,
    endpoint: String,
    api_key: Option<String>,
    api_key_source: Option<ApiKeySource>,
    timeout_secs: u64,
}

impl OpenAICompatProvider {
    /// Build an `OpenAICompatProvider`, resolving the API key from the environment if absent.
    ///
    /// Tracks key provenance: explicit `api_key` → `Flag`, env fallback → `Env`,
    /// no key → `None`. Called by [`get_provider`] when `name == "openai-compat"`.
    pub fn new(
        model: String,
        endpoint: Option<String>,
        api_key: Option<String>,
        timeout_secs: u64,
    ) -> Self {
        assert!(!model.is_empty());
        assert!(timeout_secs > 0);
        let (resolved_key, source) = if let Some(key) = api_key.filter(|k| !k.is_empty()) {
            (Some(key), Some(ApiKeySource::Flag))
        } else {
            let env_key = std::env::var("OPENAI_API_KEY").ok();
            let source = if env_key.is_some() {
                Some(ApiKeySource::Env)
            } else {
                None
            };
            (env_key, source)
        };
        let resolved_endpoint = endpoint.unwrap_or_default();
        Self {
            model,
            endpoint: resolved_endpoint,
            api_key: resolved_key,
            api_key_source: source,
            timeout_secs,
        }
    }

    /// Resolve the `/v1/chat/completions` URL from the raw endpoint string.
    ///
    /// Handles endpoints supplied with or without the `/v1` suffix and with or
    /// without the full path. Called by [`LlmProvider::classify`] and
    /// [`LlmProvider::check_available`] on `OpenAICompatProvider`.
    fn resolve_url(&self) -> Result<String> {
        // model invariant: model must always be set (construction assert enforces this).
        assert!(!self.model.is_empty());
        if self.endpoint.is_empty() {
            return Err(Error::Llm(
                "openai-compat provider requires --llm-endpoint".to_string(),
            ));
        }
        let base = self.endpoint.trim_end_matches('/');
        let url = if base.ends_with("/chat/completions") {
            base.to_string()
        } else if base.ends_with("/v1") {
            format!("{base}/chat/completions")
        } else {
            format!("{base}/v1/chat/completions")
        };
        assert!(!url.is_empty());
        Ok(url)
    }
}

impl LlmProvider for OpenAICompatProvider {
    fn name(&self) -> &'static str {
        "openai-compat"
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn api_key_source(&self) -> Option<ApiKeySource> {
        self.api_key_source
    }

    fn check_available(&self) -> (bool, String) {
        assert!(!self.model.is_empty());
        if self.endpoint.is_empty() {
            return (false, "no --llm-endpoint configured".to_string());
        }
        let base = self.endpoint.trim_end_matches('/');
        // Strip suffixes progressively: each unwrap_or falls back to the result
        // of the prior strip, not back to `base`, so "…/v1/chat/completions"
        // reduces to "…" in two steps rather than bouncing back to the original.
        let stripped = base.strip_suffix("/chat/completions").unwrap_or(base);
        let stripped = stripped.strip_suffix("/v1").unwrap_or(stripped);
        let url = format!("{stripped}/v1/models");
        let mut headers: Vec<(&str, String)> = Vec::new();
        if let Some(key) = &self.api_key {
            headers.push(("Authorization", format!("Bearer {key}")));
        }
        let header_refs: Vec<(&str, &str)> =
            headers.iter().map(|(k, val)| (*k, val.as_str())).collect();
        match http_get_with_headers(&url, &header_refs, CHECK_TIMEOUT_SECS) {
            Ok(_) => (true, "ok".to_string()),
            Err(_) => (false, format!("Cannot reach {}", self.endpoint)),
        }
    }

    fn classify(&self, system: &str, user: &str, json_mode: bool) -> Result<LlmResponse> {
        assert!(!system.is_empty());
        assert!(!user.is_empty());

        let mut body = json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": CLASSIFY_TEMPERATURE,
        });
        if json_mode {
            body["response_format"] = json!({"type": "json_object"});
        }

        let mut extra_headers: Vec<(&str, String)> = Vec::new();
        if let Some(key) = &self.api_key {
            extra_headers.push(("Authorization", format!("Bearer {key}")));
        }
        let header_refs: Vec<(&str, &str)> = extra_headers
            .iter()
            .map(|(k, val)| (*k, val.as_str()))
            .collect();

        let url = self.resolve_url()?;
        let data = http_post_json(&url, &body, &header_refs, self.timeout_secs)?;
        let text = data
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_default();

        if text.is_empty() {
            return Err(Error::Llm(format!(
                "Empty response from {} (model={})",
                self.name(),
                self.model
            )));
        }
        Ok(LlmResponse { text })
    }
}

// ===================== ANTHROPIC =====================

/// Anthropic Messages API provider (requires API key).
pub struct AnthropicProvider {
    model: String,
    endpoint: String,
    api_key: Option<String>,
    api_key_source: Option<ApiKeySource>,
    timeout_secs: u64,
}

impl AnthropicProvider {
    /// Build an `AnthropicProvider`, resolving the API key from the environment if absent.
    ///
    /// Tracks key provenance: explicit `api_key` → `Flag`, env fallback → `Env`,
    /// no key → `None`. Called by [`get_provider`] when `name == "anthropic"`.
    pub fn new(
        model: String,
        endpoint: Option<String>,
        api_key: Option<String>,
        timeout_secs: u64,
    ) -> Self {
        assert!(!model.is_empty());
        assert!(timeout_secs > 0);
        let (resolved_key, source) = if let Some(key) = api_key.filter(|k| !k.is_empty()) {
            (Some(key), Some(ApiKeySource::Flag))
        } else {
            let env_key = std::env::var("ANTHROPIC_API_KEY").ok();
            let source = if env_key.is_some() {
                Some(ApiKeySource::Env)
            } else {
                None
            };
            (env_key, source)
        };
        let resolved_endpoint = endpoint
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| ANTHROPIC_DEFAULT_ENDPOINT.to_string());
        assert!(!resolved_endpoint.is_empty());
        Self {
            model,
            endpoint: resolved_endpoint,
            api_key: resolved_key,
            api_key_source: source,
            timeout_secs,
        }
    }
}

impl LlmProvider for AnthropicProvider {
    fn name(&self) -> &'static str {
        "anthropic"
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn api_key_source(&self) -> Option<ApiKeySource> {
        self.api_key_source
    }

    fn check_available(&self) -> (bool, String) {
        assert!(!self.model.is_empty());
        assert!(!self.endpoint.is_empty());
        // Anthropic: don't probe the network — a live request costs money.
        // Surface auth errors on the first actual classify call instead.
        if self.api_key.is_none() {
            return (
                false,
                "ANTHROPIC_API_KEY not set (use --llm-api-key or set the env var)".to_string(),
            );
        }
        (true, "ok".to_string())
    }

    fn classify(&self, system: &str, user: &str, json_mode: bool) -> Result<LlmResponse> {
        assert!(!system.is_empty());
        assert!(!user.is_empty());

        let Some(api_key) = &self.api_key else {
            return Err(Error::Llm(
                "Anthropic provider requires ANTHROPIC_API_KEY or --llm-api-key".to_string(),
            ));
        };

        let sys_prompt = if json_mode {
            format!("{system}\n\nRespond with valid JSON only, no prose.")
        } else {
            system.to_string()
        };

        let body = json!({
            "model": self.model,
            "max_tokens": 2048,
            "temperature": CLASSIFY_TEMPERATURE,
            "system": sys_prompt,
            "messages": [{"role": "user", "content": user}],
        });

        let extra_headers = [
            ("X-API-Key", api_key.as_str()),
            ("anthropic-version", ANTHROPIC_API_VERSION),
        ];
        let url = format!("{}/v1/messages", self.endpoint);
        let data = http_post_json(&url, &body, &extra_headers, self.timeout_secs)?;

        let text = anthropic_extract_text(&data);
        if text.is_empty() {
            return Err(Error::Llm(format!(
                "Empty response from Anthropic (model={})",
                self.model
            )));
        }
        Ok(LlmResponse { text })
    }
}

/// Extract concatenated text from an Anthropic Messages API response.
///
/// The response `content` array may contain multiple text blocks. This function
/// joins them in order. Called by [`AnthropicProvider::classify`].
fn anthropic_extract_text(data: &Value) -> String {
    // Pair assertion: Anthropic always returns an object at the top level.
    debug_assert!(data.is_object(), "Anthropic response must be a JSON object");
    let Some(content) = data.get("content").and_then(Value::as_array) else {
        return String::new();
    };
    // Content blocks are bounded in practice; a realistic limit is 100.
    debug_assert!(content.len() <= 100, "content block count must be bounded");
    content
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("")
}

// ===================== FACTORY =====================

/// Build an [`LlmProvider`] by name.
///
/// Valid provider names: `"ollama"`, `"openai-compat"`, `"anthropic"`. Returns
/// `Err(Error::Llm)` for unknown names.
pub fn get_provider(
    name: &str,
    model: &str,
    endpoint: Option<String>,
    api_key: Option<String>,
    timeout_secs: u64,
) -> Result<Box<dyn LlmProvider>> {
    assert!(!name.is_empty());
    assert!(!model.is_empty());
    assert!(timeout_secs > 0);

    let provider: Box<dyn LlmProvider> = match name {
        "ollama" => Box::new(OllamaProvider::new(
            model.to_string(),
            endpoint,
            timeout_secs,
        )),
        "openai-compat" => Box::new(OpenAICompatProvider::new(
            model.to_string(),
            endpoint,
            api_key,
            timeout_secs,
        )),
        "anthropic" => Box::new(AnthropicProvider::new(
            model.to_string(),
            endpoint,
            api_key,
            timeout_secs,
        )),
        _ => {
            return Err(Error::Llm(format!(
                "Unknown provider '{name}'. Choices: anthropic, ollama, openai-compat"
            )));
        }
    };
    Ok(provider)
}

// ===================== HTTP HELPERS =====================

/// POST JSON to `url` and return the parsed response body as a [`Value`].
///
/// Sets `Content-Type: application/json` and any `extra_headers`. Maps ureq
/// transport/HTTP errors to [`Error::Http`] and JSON parse errors to
/// [`Error::Json`]. Called by all three provider `classify` implementations.
fn http_post_json(
    url: &str,
    payload: &Value,
    extra_headers: &[(&str, &str)],
    timeout_secs: u64,
) -> Result<Value> {
    assert!(!url.is_empty());
    assert!(timeout_secs > 0);

    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs)))
        .build()
        .new_agent();

    // Fold extra headers onto the request builder so ownership moves cleanly.
    let request = extra_headers.iter().fold(
        agent.post(url).header("Content-Type", "application/json"),
        |req, (key, value)| req.header(*key, *value),
    );
    let response = request
        .send_json(payload)
        .map_err(|error| Error::Http(error.to_string()))?;
    let text = response
        .into_body()
        .read_to_string()
        .map_err(|error| Error::Http(error.to_string()))?;

    // Pair assertion: a non-empty body must arrive before JSON parsing.
    debug_assert!(
        !text.is_empty(),
        "HTTP POST response body must not be empty"
    );
    let value: Value = serde_json::from_str(&text)?;
    Ok(value)
}

/// GET `url` and return the parsed response body as a [`Value`].
///
/// Used by availability probes that only need to reach a URL, not send a body.
/// Called by [`OllamaProvider::check_available`].
fn http_get(url: &str, timeout_secs: u64) -> Result<Value> {
    assert!(!url.is_empty());
    assert!(timeout_secs > 0);

    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs)))
        .build()
        .new_agent();
    let response = agent
        .get(url)
        .call()
        .map_err(|error| Error::Http(error.to_string()))?;
    let text = response
        .into_body()
        .read_to_string()
        .map_err(|error| Error::Http(error.to_string()))?;

    // Pair assertion: a non-empty body must arrive before JSON parsing.
    debug_assert!(!text.is_empty(), "HTTP GET response body must not be empty");
    let value: Value = serde_json::from_str(&text)?;
    Ok(value)
}

/// GET `url` with additional `headers` and return the parsed response body.
///
/// Used by [`OpenAICompatProvider::check_available`] which needs to send an
/// `Authorization` header. Called by `check_available` on `OpenAICompatProvider`.
fn http_get_with_headers(url: &str, headers: &[(&str, &str)], timeout_secs: u64) -> Result<Value> {
    assert!(!url.is_empty());
    assert!(timeout_secs > 0);

    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs)))
        .build()
        .new_agent();
    let request = headers
        .iter()
        .fold(agent.get(url), |req, (key, value)| req.header(*key, *value));
    let response = request
        .call()
        .map_err(|error| Error::Http(error.to_string()))?;
    let text = response
        .into_body()
        .read_to_string()
        .map_err(|error| Error::Http(error.to_string()))?;

    // Pair assertion: a non-empty body must arrive before JSON parsing.
    debug_assert!(!text.is_empty(), "HTTP GET response body must not be empty");
    let value: Value = serde_json::from_str(&text)?;
    Ok(value)
}

// ===================== LOCAL-ENDPOINT DETECTION =====================

/// Return `true` when `url`'s hostname is on the user's machine or private network.
///
/// Local includes: `localhost`, `127.0.0.1`, `::1`, `.local` hostnames (mDNS),
/// RFC 1918 (`10/8`, `172.16–31/12`, `192.168/16`), Tailscale CGNAT
/// (`100.64.0.0/10` — octet 100, second octet 64–127), and IPv6 ULA (`fc../fd..`).
///
/// Empty or unparseable URLs return `true` (defensive — no endpoint means no
/// external request can happen yet). Called by the default
/// `LlmProvider::is_external_service` implementation.
// Host is already lowercased by extract_endpoint_hostname, so the .local
// comparison is effectively case-insensitive despite the clippy warning.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub fn endpoint_is_local(url: &str) -> bool {
    if url.is_empty() {
        return true;
    }
    let host = extract_endpoint_hostname(url);
    if host.is_empty() {
        return true;
    }
    if host == "localhost" || host == "127.0.0.1" || host == "::1" {
        return true;
    }
    if host.ends_with(".local") {
        return true;
    }
    // RFC 1918: 10.0.0.0/8
    if host.starts_with("10.") {
        return true;
    }
    // RFC 1918: 192.168.0.0/16
    if host.starts_with("192.168.") {
        return true;
    }
    // RFC 1918: 172.16.0.0/12 — second octet 16..=31
    if let Some(rest) = host.strip_prefix("172.") {
        let second: u8 = rest
            .split('.')
            .next()
            .and_then(|octet_str| octet_str.parse().ok())
            .unwrap_or(0);
        if (16..=31).contains(&second) {
            return true;
        }
    }
    // Tailscale CGNAT: 100.64.0.0/10 — first octet 100, second octet 64..=127.
    // 100.x.x.x outside this range remains regular allocated public space (external).
    if let Some(rest) = host.strip_prefix("100.") {
        let second: u8 = rest
            .split('.')
            .next()
            .and_then(|octet_str| octet_str.parse().ok())
            .unwrap_or(0);
        if (64..=127).contains(&second) {
            return true;
        }
    }
    // IPv6 unique-local: fc00::/7 — addresses starting with fc or fd.
    if host.starts_with("fc") || host.starts_with("fd") {
        return true;
    }
    false
}

/// Extract the lowercase hostname from a URL string.
///
/// Strips scheme, port, and path. Handles IPv6 bracketed addresses (`[::1]`).
/// Returns an empty string when the URL is malformed. Called by [`endpoint_is_local`].
fn extract_endpoint_hostname(url: &str) -> String {
    assert!(!url.is_empty());
    // Strip scheme: "https://host:port/path" → "host:port/path"
    let after_scheme = url.find("://").map_or(url, |position| &url[position + 3..]);

    let host = if after_scheme.starts_with('[') {
        // IPv6 bracketed: "[::1]:port/path" → "::1"
        after_scheme
            .strip_prefix('[')
            .and_then(|bracketed| bracketed.split(']').next())
            .unwrap_or("")
    } else {
        // Strip port and path: "host:port/path" or "host/path" → "host"
        after_scheme.split(&[':', '/'][..]).next().unwrap_or("")
    };
    host.to_lowercase()
}

// ===================== TESTS =====================

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // -- get_provider factory --

    #[test]
    fn get_provider_returns_ollama_by_name() {
        // Factory must return an OllamaProvider when called with "ollama".
        let provider =
            get_provider("ollama", "gemma3:4b", None, None, 60).expect("get_provider must succeed");
        assert_eq!(provider.name(), "ollama");
        assert!(!provider.name().is_empty());
    }

    #[test]
    fn get_provider_returns_openai_compat_by_name() {
        // Factory must return an OpenAICompatProvider when called with "openai-compat".
        let provider = get_provider(
            "openai-compat",
            "gpt-4o",
            Some("http://localhost:8080".to_string()),
            None,
            60,
        )
        .expect("get_provider must succeed");
        assert_eq!(provider.name(), "openai-compat");
        assert!(!provider.name().is_empty());
    }

    #[test]
    fn get_provider_returns_anthropic_by_name() {
        // Factory must return an AnthropicProvider when called with "anthropic".
        let provider = get_provider(
            "anthropic",
            "claude-haiku-4-5-20251001",
            None,
            Some("sk-ant-test".to_string()),
            60,
        )
        .expect("get_provider must succeed");
        assert_eq!(provider.name(), "anthropic");
        assert!(!provider.name().is_empty());
    }

    #[test]
    fn get_provider_returns_error_for_unknown_provider() {
        // Unknown provider names must return Err containing the provider name.
        let result = get_provider("unknown-provider", "some-model", None, None, 60);
        assert!(result.is_err(), "unknown provider must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("unknown-provider")),
            "error must mention the unknown name"
        );
    }

    // -- OllamaProvider URL resolution --

    #[test]
    fn ollama_provider_uses_default_endpoint_when_none_given() {
        // OllamaProvider must fall back to the default localhost endpoint.
        let provider = OllamaProvider::new("gemma3:4b".to_string(), None, 60);
        assert!(
            provider.endpoint.contains("localhost"),
            "default endpoint must contain localhost"
        );
        assert!(
            provider.endpoint.contains("11434"),
            "default port must be 11434"
        );
    }

    #[test]
    fn ollama_provider_uses_custom_endpoint_when_given() {
        // A custom endpoint must override the default.
        let provider = OllamaProvider::new(
            "gemma3:4b".to_string(),
            Some("http://custom-host:11434".to_string()),
            60,
        );
        assert!(
            provider.endpoint.contains("custom-host"),
            "custom endpoint must be used"
        );
        assert!(!provider.endpoint.is_empty(), "endpoint must not be empty");
    }

    // -- OpenAICompatProvider URL resolution --

    #[test]
    fn openai_compat_resolve_url_appends_v1_path() {
        // A bare host:port must have /v1/chat/completions appended.
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some("http://localhost:8080".to_string()),
            None,
            60,
        );
        let url = provider.resolve_url().expect("must resolve");
        assert!(
            url.ends_with("/v1/chat/completions"),
            "must append full path"
        );
        assert!(!url.is_empty(), "url must not be empty");
    }

    #[test]
    fn openai_compat_resolve_url_accepts_full_path() {
        // An endpoint that already ends with /chat/completions must be used as-is.
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some("http://localhost:8080/v1/chat/completions".to_string()),
            None,
            60,
        );
        let url = provider.resolve_url().expect("must resolve");
        assert_eq!(url, "http://localhost:8080/v1/chat/completions");
        assert!(!url.is_empty(), "url must not be empty");
    }

    #[test]
    fn openai_compat_resolve_url_returns_error_for_empty_endpoint() {
        // An empty endpoint must return Err — the caller must supply one.
        let provider = OpenAICompatProvider::new("gpt-4o".to_string(), None, None, 60);
        let result = provider.resolve_url();
        assert!(result.is_err(), "empty endpoint must return Err");
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("--llm-endpoint")),
            "error must mention the required flag"
        );
    }

    // -- AnthropicProvider API key handling --

    #[test]
    fn anthropic_check_available_fails_without_api_key() {
        // AnthropicProvider must report unavailable when no API key is set.
        temp_env::with_var("ANTHROPIC_API_KEY", None::<&str>, || {
            let provider =
                AnthropicProvider::new("claude-haiku-4-5-20251001".to_string(), None, None, 60);
            let (ok, message) = provider.check_available();
            assert!(!ok, "must be unavailable without API key");
            assert!(!message.is_empty(), "must explain why");
        });
    }

    #[test]
    fn anthropic_check_available_succeeds_with_api_key() {
        // AnthropicProvider must report available when an API key is set.
        let provider = AnthropicProvider::new(
            "claude-haiku-4-5-20251001".to_string(),
            None,
            Some("sk-ant-test".to_string()),
            60,
        );
        let (ok, message) = provider.check_available();
        assert!(ok, "must be available with API key");
        assert_eq!(message, "ok");
    }

    // -- Mock HTTP server helpers --

    /// Bind a random port, accept exactly one connection, and respond with `body` as JSON.
    ///
    /// Returns the port so callers can build an endpoint URL. The spawned thread exits
    /// after serving one request. Used to test provider code without a real LLM server.
    fn serve_once(body: &str) -> u16 {
        use std::io::{Read, Write};
        use std::net::{Shutdown, TcpListener};

        let listener = TcpListener::bind("127.0.0.1:0").expect("must bind to random port");
        let port = listener.local_addr().expect("must get local addr").port();
        // `Connection: close` tells ureq to not attempt keep-alive — without
        // it, an instrumented build (e.g. `cargo llvm-cov`) sometimes raced
        // ureq's reuse logic against the server's socket close and surfaced
        // EINVAL from the read side.
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body,
        );
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                // Drain the request so ureq does not get a broken pipe on write.
                // Vec allocation avoids the large_stack_arrays lint from a fixed array.
                let mut buf = vec![0u8; 65536];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(response.as_bytes());
                // Shutdown the write side so ureq sees a clean EOF rather than
                // waiting for the OS to close the socket when the thread exits.
                let _ = stream.shutdown(Shutdown::Write);
            }
        });
        port
    }

    /// Bind a random port then immediately drop it, leaving the port unreachable.
    ///
    /// Used to produce a predictable "connection refused" in network error path tests.
    fn unreachable_port() -> u16 {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").expect("must bind to random port");
        let port = listener.local_addr().expect("must get local addr").port();
        drop(listener);
        port
    }

    // -- anthropic_extract_text (private, tested here) --

    #[test]
    fn anthropic_extract_text_single_block() {
        // A single text block must return its content unchanged.
        let data = serde_json::json!({"content": [{"type": "text", "text": "Hello world"}]});
        let result = anthropic_extract_text(&data);
        assert_eq!(result, "Hello world");
        assert!(!result.is_empty());
    }

    #[test]
    fn anthropic_extract_text_multiple_blocks_joined() {
        // Multiple text blocks must be joined in order with no separator.
        let data = serde_json::json!({
            "content": [
                {"type": "text", "text": "Hello "},
                {"type": "text", "text": "world"},
            ]
        });
        let result = anthropic_extract_text(&data);
        assert_eq!(result, "Hello world");
        assert!(!result.is_empty());
    }

    #[test]
    fn anthropic_extract_text_non_text_blocks_ignored() {
        // Non-text blocks (e.g. tool_use) must not appear in the output.
        let data = serde_json::json!({
            "content": [
                {"type": "tool_use", "id": "tu_1", "name": "search", "input": {}},
                {"type": "text", "text": "Only this"},
            ]
        });
        let result = anthropic_extract_text(&data);
        assert_eq!(result, "Only this");
        assert!(!result.is_empty());
    }

    #[test]
    fn anthropic_extract_text_empty_content_array() {
        // An empty content array must produce an empty string.
        let data = serde_json::json!({"content": []});
        let result = anthropic_extract_text(&data);
        assert!(result.is_empty(), "empty content must produce empty string");
    }

    #[test]
    fn anthropic_extract_text_missing_content_key() {
        // A response without a "content" key must return an empty string.
        let data = serde_json::json!({"type": "message", "role": "assistant"});
        let result = anthropic_extract_text(&data);
        assert!(
            result.is_empty(),
            "missing content key must produce empty string"
        );
    }

    // -- OllamaProvider::check_available --

    #[test]
    fn ollama_check_available_when_unreachable() {
        // OllamaProvider must report unavailable when the endpoint refuses connections.
        let port = unreachable_port();
        let provider = OllamaProvider::new(
            "gemma3:4b".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            5,
        );
        let (ok, message) = provider.check_available();
        assert!(!ok, "unreachable endpoint must report unavailable");
        assert!(!message.is_empty(), "must provide a reason");
    }

    #[test]
    fn ollama_check_available_model_not_in_list() {
        // OllamaProvider must report unavailable when the model is absent from the list.
        let port = serve_once(r#"{"models":[]}"#);
        let provider = OllamaProvider::new(
            "gemma3:4b".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            5,
        );
        let (ok, message) = provider.check_available();
        assert!(!ok, "absent model must report unavailable");
        assert!(
            message.contains("gemma3:4b"),
            "message must name the missing model"
        );
    }

    #[test]
    fn ollama_check_available_model_in_list() {
        // OllamaProvider must report available when the model is present in the list.
        let port = serve_once(r#"{"models":[{"name":"gemma3:4b"}]}"#);
        let provider = OllamaProvider::new(
            "gemma3:4b".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            5,
        );
        let (ok, message) = provider.check_available();
        assert!(ok, "present model must report available");
        assert_eq!(message, "ok");
    }

    #[test]
    fn ollama_check_available_model_matched_with_latest_tag() {
        // A model without a tag (e.g. "gemma3") must match "gemma3:latest" in the list.
        let port = serve_once(r#"{"models":[{"name":"gemma3:latest"}]}"#);
        let provider = OllamaProvider::new(
            "gemma3".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            5,
        );
        let (ok, _message) = provider.check_available();
        assert!(
            ok,
            "bare model name must match the :latest variant in the server list"
        );
    }

    // -- OllamaProvider::classify --

    #[test]
    fn ollama_classify_when_unreachable_returns_error() {
        // classify must return Err when the endpoint refuses connections.
        let port = unreachable_port();
        let provider = OllamaProvider::new(
            "gemma3:4b".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            5,
        );
        let result = provider.classify("system prompt", "user prompt", false);
        assert!(result.is_err(), "unreachable endpoint must return Err");
    }

    #[test]
    fn ollama_classify_extracts_message_content() {
        // classify must return the message.content field from a valid Ollama response.
        let port = serve_once(r#"{"message":{"content":"classified!"}}"#);
        let provider = OllamaProvider::new(
            "gemma3:4b".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            5,
        );
        let result = provider
            .classify("system prompt", "user prompt", false)
            .expect("must succeed with mock server");
        assert_eq!(result.text, "classified!");
    }

    #[test]
    fn ollama_classify_with_json_mode_sets_format_field() {
        // json_mode=true must be accepted and the response must still be extracted.
        let port = serve_once(r#"{"message":{"content":"{\"decisions\":{}}"}}"#);
        let provider = OllamaProvider::new(
            "gemma3:4b".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            5,
        );
        let result = provider
            .classify("system prompt", "user prompt", true)
            .expect("json_mode must succeed with mock server");
        assert!(!result.text.is_empty());
    }

    // -- OpenAICompatProvider::check_available --

    #[test]
    fn openai_check_available_empty_endpoint_reports_unavailable() {
        // An empty endpoint must report unavailable without making any HTTP call.
        let provider = OpenAICompatProvider::new("gpt-4o".to_string(), None, None, 5);
        let (ok, message) = provider.check_available();
        assert!(!ok, "empty endpoint must report unavailable");
        assert!(
            message.contains("--llm-endpoint"),
            "message must mention the required flag"
        );
    }

    #[test]
    fn openai_check_available_when_unreachable() {
        // OpenAICompatProvider must report unavailable when the endpoint refuses connections.
        let port = unreachable_port();
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            None,
            5,
        );
        let (ok, message) = provider.check_available();
        assert!(!ok, "unreachable endpoint must report unavailable");
        assert!(!message.is_empty(), "must provide a reason");
    }

    #[test]
    fn openai_check_available_when_reachable() {
        // OpenAICompatProvider must report available when the models endpoint responds.
        let port = serve_once(r#"{"data":[]}"#);
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            None,
            5,
        );
        let (ok, message) = provider.check_available();
        assert!(ok, "reachable endpoint must report available");
        assert_eq!(message, "ok");
    }

    // -- OpenAICompatProvider::classify --

    #[test]
    fn openai_classify_when_unreachable_returns_error() {
        // classify must return Err when the endpoint refuses connections.
        let port = unreachable_port();
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            None,
            5,
        );
        let result = provider.classify("system prompt", "user prompt", false);
        assert!(result.is_err(), "unreachable endpoint must return Err");
    }

    #[test]
    fn openai_classify_extracts_choices_content() {
        // classify must extract choices[0].message.content from a valid response.
        let port = serve_once(r#"{"choices":[{"message":{"content":"classified!"}}]}"#);
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            None,
            5,
        );
        let result = provider
            .classify("system prompt", "user prompt", false)
            .expect("must succeed with mock server");
        assert_eq!(result.text, "classified!");
    }

    #[test]
    fn openai_classify_with_json_mode_sends_response_format() {
        // json_mode=true must be accepted and the response must still be extracted.
        let port = serve_once(r#"{"choices":[{"message":{"content":"{\"decisions\":{}}"}}]}"#);
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            None,
            5,
        );
        let result = provider
            .classify("system prompt", "user prompt", true)
            .expect("json_mode must succeed with mock server");
        assert!(!result.text.is_empty());
    }

    // -- AnthropicProvider::classify --

    #[test]
    fn anthropic_classify_without_api_key_returns_error() {
        // classify must return Err(Llm) immediately when no API key is configured.
        temp_env::with_var("ANTHROPIC_API_KEY", None::<&str>, || {
            let provider =
                AnthropicProvider::new("claude-haiku-4-5-20251001".to_string(), None, None, 5);
            let result = provider.classify("system prompt", "user prompt", false);
            assert!(result.is_err(), "missing API key must return Err");
            let message = result.err().map(|e| e.to_string()).unwrap_or_default();
            assert!(
                message.contains("ANTHROPIC_API_KEY"),
                "error must mention the env var"
            );
        });
    }

    #[test]
    fn anthropic_classify_with_key_when_unreachable_returns_error() {
        // classify must return Err(Http) when the endpoint refuses connections.
        let port = unreachable_port();
        let provider = AnthropicProvider::new(
            "claude-haiku-4-5-20251001".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            Some("sk-ant-test".to_string()),
            5,
        );
        let result = provider.classify("system prompt", "user prompt", false);
        assert!(result.is_err(), "unreachable endpoint must return Err");
    }

    #[test]
    fn anthropic_classify_extracts_content_blocks() {
        // classify must extract text from the Anthropic Messages API response format.
        let port = serve_once(r#"{"content":[{"type":"text","text":"classified!"}]}"#);
        let provider = AnthropicProvider::new(
            "claude-haiku-4-5-20251001".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            Some("sk-ant-test".to_string()),
            5,
        );
        let result = provider
            .classify("system prompt", "user prompt", false)
            .expect("must succeed with mock server");
        assert_eq!(result.text, "classified!");
    }

    #[test]
    fn anthropic_classify_with_json_mode_appends_instruction() {
        // json_mode=true must add a JSON instruction to the system prompt; response still extracts.
        let port = serve_once(r#"{"content":[{"type":"text","text":"{\"decisions\":{}}"}]}"#);
        let provider = AnthropicProvider::new(
            "claude-haiku-4-5-20251001".to_string(),
            Some(format!("http://127.0.0.1:{port}")),
            Some("sk-ant-test".to_string()),
            5,
        );
        let result = provider
            .classify("system prompt", "user prompt", true)
            .expect("json_mode must succeed with mock server");
        assert!(!result.text.is_empty());
    }

    // -- endpoint_is_local --

    #[test]
    fn endpoint_is_local_empty_url_is_local() {
        // Empty URL means no external request possible — treat as local.
        assert!(endpoint_is_local(""), "empty url must be local");
    }

    #[test]
    fn endpoint_is_local_localhost_variants() {
        // All three loopback forms must be local.
        assert!(
            endpoint_is_local("http://localhost:11434"),
            "localhost must be local"
        );
        assert!(
            endpoint_is_local("http://127.0.0.1:8080"),
            "127.0.0.1 must be local"
        );
        assert!(endpoint_is_local("http://[::1]:8080"), "::1 must be local");
    }

    #[test]
    fn endpoint_is_local_mdns_suffix() {
        // Hostnames ending in .local (mDNS/Bonjour) are on the local network.
        assert!(
            endpoint_is_local("http://my-server.local:11434"),
            ".local hostname must be local"
        );
        // Pair assertion: a non-.local public hostname must not be local.
        assert!(
            !endpoint_is_local("http://my-server.example.com:11434"),
            ".example.com hostname must be external"
        );
    }

    #[test]
    fn endpoint_is_local_rfc1918_ranges() {
        // RFC 1918 private ranges must all be local.
        assert!(endpoint_is_local("http://10.0.0.1"), "10/8 must be local");
        assert!(
            endpoint_is_local("http://10.255.255.255"),
            "10/8 top must be local"
        );
        assert!(
            endpoint_is_local("http://192.168.1.1"),
            "192.168/16 must be local"
        );
        assert!(
            endpoint_is_local("http://172.16.0.1"),
            "172.16/12 start must be local"
        );
        assert!(
            endpoint_is_local("http://172.31.255.255"),
            "172.16/12 end must be local"
        );
        // Outside 172.16-31 range — external.
        assert!(
            !endpoint_is_local("http://172.15.0.1"),
            "172.15 is not RFC1918"
        );
        assert!(
            !endpoint_is_local("http://172.32.0.1"),
            "172.32 is not RFC1918"
        );
    }

    #[test]
    fn endpoint_is_local_tailscale_cgnat() {
        // Tailscale CGNAT: 100.64.0.0/10 — second octet 64..=127 is local.
        assert!(
            endpoint_is_local("http://100.64.0.1"),
            "100.64.x is CGNAT local"
        );
        assert!(
            endpoint_is_local("http://100.127.255.255"),
            "100.127.x is CGNAT local"
        );
        // Boundary: 100.63.x and 100.128.x are outside the CGNAT range — external.
        assert!(
            !endpoint_is_local("http://100.63.255.255"),
            "100.63.x is not CGNAT"
        );
        assert!(
            !endpoint_is_local("http://100.128.0.0"),
            "100.128.x is not CGNAT"
        );
    }

    #[test]
    fn endpoint_is_local_ipv6_ula() {
        // IPv6 ULA (fc00::/7) — fc and fd prefixes are local.
        assert!(
            endpoint_is_local("http://[fd12:3456:789a::1]"),
            "fd.. IPv6 ULA must be local"
        );
        assert!(
            endpoint_is_local("http://[fc00::1]"),
            "fc00 IPv6 ULA must be local"
        );
    }

    #[test]
    fn endpoint_is_local_public_endpoints_are_external() {
        // Public SaaS endpoints must not be treated as local.
        assert!(
            !endpoint_is_local("https://api.anthropic.com"),
            "Anthropic API must be external"
        );
        assert!(
            !endpoint_is_local("https://api.openai.com"),
            "OpenAI API must be external"
        );
    }

    // -- ApiKeySource provenance tracking --

    #[test]
    fn openai_compat_api_key_source_flag_when_explicit() {
        // Explicit api_key must set source to Flag.
        temp_env::with_var("OPENAI_API_KEY", None::<&str>, || {
            let provider = OpenAICompatProvider::new(
                "gpt-4o".to_string(),
                None,
                Some("sk-explicit".to_string()),
                60,
            );
            assert_eq!(provider.api_key_source(), Some(ApiKeySource::Flag));
            assert!(provider.api_key.is_some());
        });
    }

    #[test]
    fn openai_compat_api_key_source_env_when_env_fallback() {
        // When no explicit key is passed but the env var is set, source must be Env.
        temp_env::with_var("OPENAI_API_KEY", Some("sk-from-env"), || {
            let provider = OpenAICompatProvider::new("gpt-4o".to_string(), None, None, 60);
            assert_eq!(provider.api_key_source(), Some(ApiKeySource::Env));
            assert!(provider.api_key.is_some());
        });
    }

    #[test]
    fn openai_compat_api_key_source_none_when_no_key() {
        // When neither explicit key nor env var, source must be None.
        temp_env::with_var("OPENAI_API_KEY", None::<&str>, || {
            let provider = OpenAICompatProvider::new("gpt-4o".to_string(), None, None, 60);
            assert_eq!(provider.api_key_source(), None);
            assert!(provider.api_key.is_none());
        });
    }

    #[test]
    fn anthropic_api_key_source_flag_when_explicit() {
        // Explicit api_key must set source to Flag.
        temp_env::with_var("ANTHROPIC_API_KEY", None::<&str>, || {
            let provider = AnthropicProvider::new(
                "claude-haiku-4-5-20251001".to_string(),
                None,
                Some("sk-ant-explicit".to_string()),
                60,
            );
            assert_eq!(provider.api_key_source(), Some(ApiKeySource::Flag));
        });
    }

    #[test]
    fn anthropic_api_key_source_env_when_env_fallback() {
        // When no explicit key is passed but the env var is set, source must be Env.
        temp_env::with_var("ANTHROPIC_API_KEY", Some("sk-ant-from-env"), || {
            let provider =
                AnthropicProvider::new("claude-haiku-4-5-20251001".to_string(), None, None, 60);
            assert_eq!(provider.api_key_source(), Some(ApiKeySource::Env));
        });
    }

    // -- is_external_service --

    #[test]
    fn ollama_is_not_external_with_default_endpoint() {
        // OllamaProvider uses localhost by default — must not be external.
        let provider = OllamaProvider::new("gemma3:4b".to_string(), None, 60);
        assert!(
            !provider.is_external_service(),
            "Ollama default endpoint is localhost"
        );
    }

    #[test]
    fn anthropic_is_external_with_default_endpoint() {
        // AnthropicProvider's default endpoint is api.anthropic.com — external.
        temp_env::with_var("ANTHROPIC_API_KEY", None::<&str>, || {
            let provider =
                AnthropicProvider::new("claude-haiku-4-5-20251001".to_string(), None, None, 60);
            assert!(
                provider.is_external_service(),
                "Anthropic default endpoint is external"
            );
        });
    }

    #[test]
    fn openai_compat_is_not_external_with_local_endpoint() {
        // An OpenAI-compat provider pointed at localhost must not be external.
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some("http://localhost:8080".to_string()),
            None,
            60,
        );
        assert!(
            !provider.is_external_service(),
            "localhost endpoint is not external"
        );
    }

    #[test]
    fn openai_compat_is_external_with_public_endpoint() {
        // An OpenAI-compat provider pointed at a public URL must be external.
        let provider = OpenAICompatProvider::new(
            "gpt-4o".to_string(),
            Some("https://api.openai.com".to_string()),
            None,
            60,
        );
        assert!(
            provider.is_external_service(),
            "public endpoint is external"
        );
    }
}
