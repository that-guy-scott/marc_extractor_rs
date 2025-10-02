use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use tracing::debug;

/// MARC record from database
#[derive(Debug, Clone)]
pub struct MarcRecord {
    pub id: i64,
    pub marc: String,
}

/// Database configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub include_deleted: bool,
    pub chunk_size: i64,
}

/// Get total count of records to process
pub async fn get_record_count(pool: &PgPool, include_deleted: bool) -> Result<i64> {
    let query = if include_deleted {
        "SELECT COUNT(*) FROM biblio.record_entry"
    } else {
        "SELECT COUNT(*) FROM biblio.record_entry WHERE deleted = false"
    };

    let count: i64 = sqlx::query_scalar(query)
        .fetch_one(pool)
        .await
        .context("Failed to count records")?;

    Ok(count)
}

/// Fetch a chunk of MARC records
pub async fn fetch_records(
    pool: &PgPool,
    config: &DatabaseConfig,
    offset: i64,
) -> Result<Vec<MarcRecord>> {
    debug!("Fetching chunk at offset {}", offset);

    let query = if config.include_deleted {
        r#"
        SELECT id, marc
        FROM biblio.record_entry
        ORDER BY id
        LIMIT $1 OFFSET $2
        "#
    } else {
        r#"
        SELECT id, marc
        FROM biblio.record_entry
        WHERE deleted = false
        ORDER BY id
        LIMIT $1 OFFSET $2
        "#
    };

    let rows = sqlx::query(query)
        .bind(config.chunk_size)
        .bind(offset)
        .fetch_all(pool)
        .await
        .context(format!("Failed to fetch records at offset {}", offset))?;

    let mut records = Vec::with_capacity(rows.len());

    for row in rows {
        let id: i64 = row.try_get("id")?;
        let marc: Option<String> = row.try_get("marc")?;

        if let Some(marc_data) = marc {
            // Only include records that have MARC data
            if !marc_data.trim().is_empty() {
                records.push(MarcRecord {
                    id,
                    marc: marc_data,
                });
            } else {
                debug!("Skipping record {} - empty MARC data", id);
            }
        } else {
            debug!("Skipping record {} - NULL MARC data", id);
        }
    }

    debug!("Fetched {} records at offset {}", records.len(), offset);

    Ok(records)
}

/// Optimized version using cursor for streaming (alternative approach)
/// This can be used for even better memory efficiency
#[allow(dead_code)]
pub async fn stream_records(
    pool: &PgPool,
    config: &DatabaseConfig,
) -> Result<impl futures::Stream<Item = Result<MarcRecord>>> {
    use futures::stream::TryStreamExt;

    let query = if config.include_deleted {
        "SELECT id, marc FROM biblio.record_entry ORDER BY id"
    } else {
        "SELECT id, marc FROM biblio.record_entry WHERE deleted = false ORDER BY id"
    };

    let stream = sqlx::query(query)
        .fetch(pool)
        .map_err(anyhow::Error::from)
        .and_then(|row| async move {
            let id: i64 = row.try_get("id")?;
            let marc: Option<String> = row.try_get("marc")?;

            if let Some(marc_data) = marc {
                if !marc_data.trim().is_empty() {
                    return Ok(Some(MarcRecord { id, marc: marc_data }));
                }
            }

            Ok(None)
        })
        .try_filter_map(|opt| async move { Ok(opt) });

    Ok(stream)
}
