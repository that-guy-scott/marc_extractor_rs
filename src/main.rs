use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::postgres::PgPoolOptions;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

mod db;
mod writer;

use db::{DatabaseConfig, MarcRecord};
use writer::XmlWriter;

/// High-performance MARC record extractor for Evergreen ILS
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// PostgreSQL database URL
    /// Example: postgresql://evergreen:password@localhost/evergreen
    #[arg(long, env = "DATABASE_URL")]
    db_url: String,

    /// Output file path (defaults to stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of concurrent workers/connections
    #[arg(short, long, default_value = "10")]
    workers: u32,

    /// Number of records to fetch per chunk
    #[arg(short, long, default_value = "1000")]
    chunk_size: i64,

    /// Include deleted records
    #[arg(short = 'd', long)]
    include_deleted: bool,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Maximum number of records to process (for testing)
    #[arg(long)]
    limit: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.verbose { "info" } else { "warn" };
    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .init();

    info!("Starting MARC extraction");
    info!("Database: {}", mask_password(&args.db_url));
    info!("Workers: {}", args.workers);
    info!("Chunk size: {}", args.chunk_size);
    info!("Include deleted: {}", args.include_deleted);

    // Create database connection pool
    info!("Creating database connection pool...");
    let pool = PgPoolOptions::new()
        .max_connections(args.workers)
        .connect(&args.db_url)
        .await
        .context("Failed to connect to database")?;

    info!("Database connection established");

    // Get total record count
    let total_count = db::get_record_count(&pool, args.include_deleted)
        .await
        .context("Failed to get record count")?;

    info!("Total records to process: {}", total_count);

    if total_count == 0 {
        warn!("No records found to extract");
        return Ok(());
    }

    // Apply limit if specified
    let records_to_process = args.limit.map(|l| l.min(total_count)).unwrap_or(total_count);

    // Create progress bar
    let pb = ProgressBar::new(records_to_process as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}) {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Atomic counter for processed records
    let processed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    // Channel for passing records from fetchers to writer
    let (tx, mut rx) = mpsc::channel::<MarcRecord>(1000);

    // Spawn XML writer task
    let writer_handle = {
        let output = args.output.clone();
        let pb = pb.clone();
        let processed = Arc::clone(&processed);

        tokio::spawn(async move {
            let mut writer = XmlWriter::new(output).await?;

            while let Some(record) = rx.recv().await {
                match writer.write_record(&record).await {
                    Ok(_) => {
                        let count = processed.fetch_add(1, Ordering::Relaxed) + 1;
                        pb.set_position(count);
                    }
                    Err(e) => {
                        error!("Failed to write record ID {}: {}", record.id, e);
                    }
                }
            }

            writer.finalize().await?;
            Ok::<_, anyhow::Error>(())
        })
    };

    // Calculate chunks
    let num_chunks = (records_to_process + args.chunk_size - 1) / args.chunk_size;

    info!("Processing {} records in {} chunks", records_to_process, num_chunks);

    // Create database config
    let db_config = DatabaseConfig {
        include_deleted: args.include_deleted,
        chunk_size: args.chunk_size,
    };

    // Spawn worker tasks
    let mut handles = vec![];

    for chunk_id in 0..num_chunks {
        let pool = pool.clone();
        let tx = tx.clone();
        let db_config = db_config.clone();
        let errors = Arc::clone(&errors);
        let limit = args.limit;

        let handle = tokio::spawn(async move {
            let offset = chunk_id * db_config.chunk_size;

            // Check if we've hit the limit
            if let Some(max) = limit {
                if offset >= max {
                    return Ok::<_, anyhow::Error>(());
                }
            }

            match db::fetch_records(&pool, &db_config, offset).await {
                Ok(records) => {
                    for record in records {
                        if tx.send(record).await.is_err() {
                            error!("Channel closed, stopping chunk {}", chunk_id);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to fetch chunk {}: {}", chunk_id, e);
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }

            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    // Drop our sender so the writer knows when we're done
    drop(tx);

    // Wait for all workers to complete
    for handle in handles {
        if let Err(e) = handle.await {
            error!("Worker task failed: {}", e);
        }
    }

    // Wait for writer to finish
    writer_handle
        .await
        .context("Writer task panicked")??;

    pb.finish_with_message("Complete!");

    let final_processed = processed.load(Ordering::Relaxed);
    let final_errors = errors.load(Ordering::Relaxed);

    info!("\nExtraction completed:");
    info!("  Records processed: {}", final_processed);
    if final_errors > 0 {
        warn!("  Errors encountered: {}", final_errors);
    }

    if let Some(output) = &args.output {
        info!("  Output written to: {}", output.display());
    } else {
        info!("  Output written to STDOUT");
    }

    if final_errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Mask password in database URL for logging
fn mask_password(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            let mut masked = url.to_string();
            masked.replace_range(colon_pos + 1..at_pos, "****");
            return masked;
        }
    }
    url.to_string()
}
