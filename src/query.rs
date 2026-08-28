//! Query side: DataFusion session setup, remote table registration, result
//! printing, and the bytes-transferred stats line.

use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::{array::RecordBatch, csv, json, util::pretty};
use datafusion::common::TableReference;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use iroh::EndpointId;
use tracing::warn;
use url::Url;

use crate::{client::IrohObjectStore, proto::Entry};

/// Output format for `popfusion query`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    Table,
    Csv,
    Json,
}

/// Build a DataFusion session with every remote `*.parquet` registered as a
/// table named by its relative path, and each all-Parquet directory
/// registered as one listing table named after the directory.
///
/// Returns the session and the total remote dataset size in bytes.
pub async fn create_session(
    store: Arc<IrohObjectStore>,
    node: EndpointId,
) -> Result<(SessionContext, u64)> {
    let entries = store.list_all("").await.context("listing remote root")?;
    let total: u64 = entries.iter().map(|e| e.size).sum();

    let ctx = SessionContext::new();
    let base = format!("iroh://{node}");
    let url = Url::parse(&base).context("building iroh object store URL")?;
    ctx.register_object_store(&url, store);

    for entry in entries.iter().filter(|e| e.path.ends_with(".parquet")) {
        let table_path = format!("{base}/{}", entry.path);
        // AIDEV-NOTE: TableReference::bare keeps "wfi_catalog.parquet" as ONE quoted
        // identifier; a plain &str would be parsed as schema "wfi_catalog", table "parquet".
        if let Err(err) = ctx
            .register_parquet(
                TableReference::bare(entry.path.clone()),
                &table_path,
                ParquetReadOptions::default(),
            )
            .await
        {
            warn!(table = %entry.path, %err, "skipping unregisterable parquet file");
        }
    }

    for dir in homogeneous_parquet_dirs(&entries) {
        let table_path = format!("{base}/{dir}/");
        if let Err(err) = ctx
            .register_parquet(
                TableReference::bare(dir.clone()),
                &table_path,
                ParquetReadOptions::default(),
            )
            .await
        {
            warn!(table = %dir, %err, "skipping directory listing table");
        }
    }

    Ok((ctx, total))
}

/// Directories (at any depth) whose recursive contents are exclusively
/// `.parquet` files. These become single listing tables named after the
/// directory's relative path.
pub fn homogeneous_parquet_dirs(entries: &[Entry]) -> Vec<String> {
    use std::collections::BTreeMap;
    // dir -> (parquet file count, other file count), counting every ancestor.
    let mut stats: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for entry in entries {
        let is_parquet = entry.path.ends_with(".parquet");
        let mut parent = entry.path.rsplit_once('/').map(|(dir, _)| dir);
        while let Some(dir) = parent {
            let counts = stats.entry(dir.to_string()).or_default();
            if is_parquet {
                counts.0 += 1;
            } else {
                counts.1 += 1;
            }
            parent = dir.rsplit_once('/').map(|(up, _)| up);
        }
    }
    stats
        .into_iter()
        .filter(|(_, (parquet, other))| *parquet > 0 && *other == 0)
        .map(|(dir, _)| dir)
        .collect()
}

/// Run one SQL statement and print results (and optionally stats) to stdout.
pub async fn run(
    store: Arc<IrohObjectStore>,
    node: EndpointId,
    sql: &str,
    format: OutputFormat,
    stats: bool,
) -> Result<()> {
    let (ctx, total) = create_session(store.clone(), node).await?;
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    print_batches(&batches, format)?;
    if stats {
        println!(
            "transferred: {} of {} remote data",
            human_bytes(store.bytes_transferred()),
            human_bytes(total)
        );
    }
    Ok(())
}

fn print_batches(batches: &[RecordBatch], format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            println!("{}", pretty::pretty_format_batches(batches)?);
        }
        OutputFormat::Csv => {
            let mut writer = csv::WriterBuilder::new()
                .with_header(true)
                .build(std::io::stdout());
            for batch in batches {
                writer.write(batch)?;
            }
        }
        OutputFormat::Json => {
            let mut writer = json::ArrayWriter::new(std::io::stdout());
            writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
            writer.finish()?;
            println!();
        }
    }
    Ok(())
}

/// Decimal-unit humanization matching the README examples ("3.2 MB", "41.7 GB").
pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, size: u64) -> Entry {
        Entry {
            path: path.into(),
            size,
        }
    }

    #[test]
    fn homogeneous_dirs_detected() {
        let entries = vec![
            entry("solo.parquet", 1),
            entry("clean/a.parquet", 1),
            entry("clean/b.parquet", 1),
            entry("mixed/c.parquet", 1),
            entry("mixed/readme.txt", 1),
            entry("nested/deep/d.parquet", 1),
        ];
        let dirs = homogeneous_parquet_dirs(&entries);
        assert_eq!(dirs, vec!["clean", "nested", "nested/deep"]);
    }

    #[test]
    fn human_bytes_matches_readme_style() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(3_200_000), "3.2 MB");
        assert_eq!(human_bytes(41_700_000_000), "41.7 GB");
    }
}
