//! Error types and `Result` alias for the mempalace crate.

use std::path::PathBuf;

/// Fine-grained error codes for RFC-002 source adapters.
///
/// These wrap into [`Error::SourceAdapter`] so adapter errors propagate through
/// the standard `Result<T>` chain without losing their structural type.
///
/// Variants are constructed by third-party adapter implementations, not by
/// the CLI binary itself, so the `dead_code` lint is suppressed at the enum
/// level rather than variant-by-variant.
#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub enum SourceAdapterError {
    /// A `DrawerRecord` field failed schema validation (RFC-002 §5.2).
    #[error("schema conformance error: {0}")]
    SchemaConformance(String),
    /// A declared content transformation produced an invalid result.
    #[error("transformation violation: {0}")]
    TransformationViolation(String),
    /// The adapter requires credentials that were not provided.
    #[error("authentication required: {0}")]
    AuthRequired(String),
    /// The adapter was called after `close()` was invoked.
    #[error("adapter closed")]
    AdapterClosed,
    /// All other adapter-originated failures.
    #[error("adapter error: {0}")]
    Other(String),
}

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

    /// A source adapter returned a structured failure (RFC-002).
    #[error("source adapter error: {0}")]
    SourceAdapter(#[from] SourceAdapterError),

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
    fn source_adapter_error_schema_conformance_displays_correctly() {
        // SchemaConformance must prefix the message to aid diagnosis.
        let error = SourceAdapterError::SchemaConformance("field 'author' missing".to_string());
        let display = error.to_string();
        assert!(
            display.starts_with("schema conformance error:"),
            "must use variant prefix"
        );
        assert!(
            display.contains("field 'author' missing"),
            "must include message"
        );
    }

    #[test]
    fn source_adapter_error_converts_to_main_error() {
        // SourceAdapterError must convert into Error via the From impl.
        let source_error = SourceAdapterError::AuthRequired("token expired".to_string());
        let main_error: Error = source_error.into();
        let display = main_error.to_string();
        assert!(
            display.starts_with("source adapter error:"),
            "must use Error variant prefix"
        );
        assert!(
            display.contains("token expired"),
            "must propagate inner message"
        );
    }

    #[test]
    fn source_adapter_error_adapter_closed_has_fixed_message() {
        // AdapterClosed has no payload so its message is always the same.
        let error = SourceAdapterError::AdapterClosed;
        let display = error.to_string();
        assert_eq!(
            display, "adapter closed",
            "AdapterClosed must produce a fixed message"
        );
        assert!(
            !display.contains('{'),
            "AdapterClosed must not have a format placeholder"
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
