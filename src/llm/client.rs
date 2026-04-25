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
    /// Model identifier reported by or passed to the provider.
    // Read by callers that log or display the model used; currently only `text` is consumed.
    #[allow(dead_code)]
    pub model: String,
    /// Short provider name (e.g. `"ollama"`, `"anthropic"`).
    // Read by callers that log or display the provider used; currently only `text` is consumed.
    #[allow(dead_code)]
    pub provider: String,
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
        Ok(LlmResponse {
            text,
            model: self.model.clone(),
            provider: self.name().to_string(),
        })
    }
}

// ===================== OPENAI-COMPAT =====================

/// Any OpenAI-compatible `/v1/chat/completions` endpoint.
pub struct OpenAICompatProvider {
    model: String,
    endpoint: String,
    api_key: Option<String>,
    timeout_secs: u64,
}

impl OpenAICompatProvider {
    /// Build an `OpenAICompatProvider`, resolving the API key from the environment if absent.
    ///
    /// Called by [`get_provider`] when `name == "openai-compat"`.
    pub fn new(
        model: String,
        endpoint: Option<String>,
        api_key: Option<String>,
        timeout_secs: u64,
    ) -> Self {
        assert!(!model.is_empty());
        assert!(timeout_secs > 0);
        let resolved_key = api_key
            .filter(|k| !k.is_empty())
            .or_else(|| std::env::var("OPENAI_API_KEY").ok());
        let resolved_endpoint = endpoint.unwrap_or_default();
        Self {
            model,
            endpoint: resolved_endpoint,
            api_key: resolved_key,
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

    fn check_available(&self) -> (bool, String) {
        assert!(!self.model.is_empty());
        if self.endpoint.is_empty() {
            return (false, "no --llm-endpoint configured".to_string());
        }
        let base = self.endpoint.trim_end_matches('/');
        let stripped = base
            .strip_suffix("/chat/completions")
            .unwrap_or(base)
            .strip_suffix("/v1")
            .unwrap_or(base);
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
        Ok(LlmResponse {
            text,
            model: self.model.clone(),
            provider: self.name().to_string(),
        })
    }
}

// ===================== ANTHROPIC =====================

/// Anthropic Messages API provider (requires API key).
pub struct AnthropicProvider {
    model: String,
    endpoint: String,
    api_key: Option<String>,
    timeout_secs: u64,
}

impl AnthropicProvider {
    /// Build an `AnthropicProvider`, resolving the API key from the environment if absent.
    ///
    /// Called by [`get_provider`] when `name == "anthropic"`.
    pub fn new(
        model: String,
        endpoint: Option<String>,
        api_key: Option<String>,
        timeout_secs: u64,
    ) -> Self {
        assert!(!model.is_empty());
        assert!(timeout_secs > 0);
        let resolved_key = api_key
            .filter(|k| !k.is_empty())
            .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok());
        let resolved_endpoint = endpoint
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| ANTHROPIC_DEFAULT_ENDPOINT.to_string());
        assert!(!resolved_endpoint.is_empty());
        Self {
            model,
            endpoint: resolved_endpoint,
            api_key: resolved_key,
            timeout_secs,
        }
    }
}

impl LlmProvider for AnthropicProvider {
    fn name(&self) -> &'static str {
        "anthropic"
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
        Ok(LlmResponse {
            text,
            model: self.model.clone(),
            provider: self.name().to_string(),
        })
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
}
