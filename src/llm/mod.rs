//! LLM provider abstraction for optional entity refinement in `mempalace init`.
//!
//! Three providers cover the useful space:
//! - **`ollama`** (default) — local models, fully offline.
//! - **`openai-compat`** — any `/v1/chat/completions` endpoint (`OpenRouter`,
//!   `LM Studio`, `vLLM`, `Groq`, etc.).
//! - **`anthropic`** — the official Messages API with `X-API-Key` auth.

pub mod client;
pub mod refine;

pub use client::{LlmProvider, get_provider};
pub use refine::{collect_corpus_text, refine_entities};
