use turso::Connection;

use crate::db::query_all;
use crate::dialect::{CompressMetadata, Dialect};
use crate::error::Result;

/// Load dialect from an optional config file, or return an empty dialect.
fn run_load_dialect(config_path: Option<&str>) -> Result<Dialect> {
    let Some(path) = config_path else {
        return Ok(Dialect::empty());
    };
    let content = std::fs::read_to_string(path)?;
    let config: serde_json::Value = serde_json::from_str(&content)?;
    let entities = config
        .get("entities")
        .and_then(|e| e.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let skip = config
        .get("skip_names")
        .and_then(|s| s.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    Ok(Dialect::new(&entities, skip))
}

/// Compress one drawer row and persist or preview it. Returns `(original_len, compressed_len)`.
async fn run_compress_row(
    connection: &Connection,
    row: &turso::Row,
    dialect: &Dialect,
    dry_run: bool,
    count: usize,
) -> Result<(usize, usize)> {
    let id: String = row.get(0)?;
    let content: String = row.get(1)?;
    let wing_val: String = row.get(2)?;
    let room: String = row.get(3)?;
    let source: String = row.get::<String>(4).unwrap_or_default();
    let date: String = row.get::<String>(5).unwrap_or_default();

    let meta = CompressMetadata {
        source_file: &source,
        wing: &wing_val,
        room: &room,
        date: &date,
    };

    let compressed = dialect.compress(&content, Some(&meta));
    let original_len = content.len();
    let compressed_len = compressed.len();
    // Byte lengths for display-only ratio; precision loss negligible for practical sizes.
    #[allow(clippy::cast_precision_loss)]
    let ratio = if compressed_len > 0 {
        original_len as f64 / compressed_len as f64
    } else {
        0.0
    };

    if dry_run {
        if count <= 3 {
            println!("--- Drawer {} ---", &id[..8.min(id.len())]);
            println!("{compressed}");
            println!("  ({original_len} → {compressed_len} bytes, {ratio:.1}x)\n");
        }
    } else {
        connection.execute(
            "INSERT OR REPLACE INTO compressed (id, content, compression_ratio, wing, room) VALUES (?, ?, ?, ?, ?)",
            (id, compressed, ratio, wing_val, room),
        )
        .await?;
    }

    Ok((original_len, compressed_len))
}

/// Run the compress command: compress drawers into AAAK dialect format.
pub async fn run(
    connection: &Connection,
    wing: Option<&str>,
    dry_run: bool,
    config_path: Option<&str>,
) -> Result<()> {
    let dialect = run_load_dialect(config_path)?;

    let rows = if let Some(w) = wing {
        query_all(
            connection,
            "SELECT id, content, wing, room, source_file, filed_at FROM drawers WHERE wing = ? ORDER BY filed_at",
            [w.to_string()],
        ).await?
    } else {
        query_all(
            connection,
            "SELECT id, content, wing, room, source_file, filed_at FROM drawers ORDER BY filed_at",
            (),
        )
        .await?
    };

    if rows.is_empty() {
        println!("No drawers to compress.");
        return Ok(());
    }

    let mut total_original = 0usize;
    let mut total_compressed = 0usize;

    for (count, row) in rows.iter().enumerate() {
        let (orig, comp) = run_compress_row(connection, row, &dialect, dry_run, count + 1).await?;
        total_original += orig;
        total_compressed += comp;
    }

    let count = rows.len();
    // Byte lengths for display-only ratio; precision loss negligible for practical sizes.
    #[allow(clippy::cast_precision_loss)]
    let overall_ratio = if total_compressed > 0 {
        total_original as f64 / total_compressed as f64
    } else {
        0.0
    };

    if dry_run {
        println!("Dry run: {count} drawers would be compressed");
    } else {
        println!("Compressed {count} drawers into AAAK dialect");
    }
    println!(
        "  Total: {total_original} → {total_compressed} bytes ({overall_ratio:.1}x compression)"
    );

    Ok(())
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn run_load_dialect_none_returns_empty() {
        let dialect = run_load_dialect(None).expect("None config must return Ok");
        // Dialect::empty() has zero entity codes — compress with it and verify
        // it produces non-empty output (proves the dialect is usable).
        let output = dialect.compress("hello world test content", None);
        assert!(
            !output.is_empty(),
            "empty dialect must still produce output"
        );
        // AAAK content line always starts with "0:" entity prefix.
        assert!(
            output.contains("0:"),
            "compressed output must contain entity prefix"
        );
    }

    #[test]
    fn run_load_dialect_valid_file() {
        let dir = tempfile::tempdir().expect("must create temp dir");
        let config_path = dir.path().join("dialect.json");
        std::fs::write(
            &config_path,
            r#"{"entities": {"Rust": "RS", "Python": "PY"}, "skip_names": ["test"]}"#,
        )
        .expect("must write config file");

        let path_str = config_path.to_str().expect("path must be valid utf-8");
        let dialect = run_load_dialect(Some(path_str)).expect("valid config must return Ok");
        // Verify the dialect loaded entities by compressing text that mentions them.
        let output = dialect.compress("Rust and Python are languages for programming", None);
        assert!(
            !output.is_empty(),
            "dialect with entities must produce output"
        );
        // AAAK content line always starts with "0:" entity prefix.
        assert!(
            output.contains("0:"),
            "compressed output must contain entity prefix"
        );
        // Verify the loaded entity mappings were applied: "Rust" maps to "RS"
        // and "Python" maps to "PY", so at least one code must appear.
        assert!(
            output.contains("RS") || output.contains("PY"),
            "compressed output must contain entity codes from loaded dialect"
        );
    }
}

#[cfg(test)]
// Acceptable in tests: .expect() produces immediate, clear failures.
#[allow(clippy::expect_used)]
mod async_tests {
    use super::*;

    async fn seed_drawer(connection: &Connection) {
        crate::palace::drawer::add_drawer(
            connection,
            &crate::palace::drawer::DrawerParams {
                id: "compress-test-1",
                wing: "test_wing",
                room: "test_room",
                content: "This is test content for compression with enough words to be meaningful",
                source_file: "test.txt",
                chunk_index: 0,
                added_by: "test",
                ingest_mode: "projects",
                source_mtime: None,
            },
        )
        .await
        .expect("seeding drawer for compress test must succeed");
    }

    #[tokio::test]
    async fn run_compress_row_returns_sizes() {
        let (_db, connection) = crate::test_helpers::test_db().await;
        seed_drawer(&connection).await;

        let rows = crate::db::query_all(
            &connection,
            "SELECT id, content, wing, room, source_file, filed_at FROM drawers LIMIT 1",
            (),
        )
        .await
        .expect("query must succeed");
        let row = rows.first().expect("must have at least one row");

        let dialect = Dialect::empty();
        let (orig, comp) = run_compress_row(&connection, row, &dialect, true, 1)
            .await
            .expect("compress row must succeed");
        assert!(orig > 0, "original size must be positive");
        assert!(comp > 0, "compressed size must be positive");
    }
}
