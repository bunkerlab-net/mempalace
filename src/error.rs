//! Error types and `Result` alias for the mempalace crate.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] turso::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// TOML parse error from manifest files (Cargo.toml, pyproject.toml).
    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),

    /// LLM provider error — failed call, malformed response, or missing model.
    #[error("llm error: {0}")]
    Llm(String),

    /// HTTP transport error — connection refused, timeout, non-2xx status.
    #[error("http error: {0}")]
    Http(String),

    #[error("config not found: {0}")]
    ConfigNotFound(PathBuf),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
// Test code — .expect() is acceptable with a descriptive message.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn llm_error_displays_correctly() {
        // Llm variant must prefix the message with "llm error:" for easy diagnosis.
        let error = Error::Llm("model not loaded".to_string());
        let display = error.to_string();
        assert!(display.starts_with("llm error:"), "must use variant prefix");
        assert!(display.contains("model not loaded"), "must include message");
    }

    #[test]
    fn http_error_displays_correctly() {
        // Http variant must prefix the message with "http error:" for easy diagnosis.
        let error = Error::Http("connection refused".to_string());
        let display = error.to_string();
        assert!(
            display.starts_with("http error:"),
            "must use variant prefix"
        );
        assert!(
            display.contains("connection refused"),
            "must include message"
        );
    }

    #[test]
    fn toml_error_displays_correctly() {
        // Verify Toml variant wraps toml::de::Error and displays the parse message.
        // Parse intentionally invalid TOML to obtain a `toml::de::Error`.
        let result: std::result::Result<toml::Value, _> = toml::from_str("invalid = [[[");
        let error = Error::Toml(result.expect_err("must fail to parse invalid TOML"));
        let display = error.to_string();
        assert!(
            display.starts_with("toml parse error:"),
            "must use variant prefix"
        );
        assert!(!display.is_empty(), "display must not be empty");
    }
}
