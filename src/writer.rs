use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tracing::debug;

use crate::db::MarcRecord;

/// XML Writer for MARC records
pub struct XmlWriter {
    writer: Box<dyn AsyncWrite + Unpin + Send>,
}

impl XmlWriter {
    /// Create a new XML writer
    pub async fn new(output: Option<PathBuf>) -> Result<Self> {
        let writer: Box<dyn AsyncWrite + Unpin + Send> = if let Some(path) = output {
            let file = File::create(&path)
                .await
                .context(format!("Failed to create output file: {}", path.display()))?;
            Box::new(BufWriter::new(file))
        } else {
            Box::new(tokio::io::stdout())
        };

        let mut xml_writer = Self { writer };

        // Write XML header and collection opening tag
        xml_writer
            .writer
            .write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            .await?;
        xml_writer
            .writer
            .write_all(b"<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n")
            .await?;

        Ok(xml_writer)
    }

    /// Write a single MARC record
    pub async fn write_record(&mut self, record: &MarcRecord) -> Result<()> {
        debug!("Writing record ID {}", record.id);

        // Clean the MARC XML to remove any wrapper elements or declarations
        let cleaned_marc = self.clean_marc_xml(&record.marc);

        // Write the record
        self.writer.write_all(cleaned_marc.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;

        Ok(())
    }

    /// Finalize and close the XML document
    pub async fn finalize(mut self) -> Result<()> {
        // Write closing collection tag
        self.writer.write_all(b"</collection>\n").await?;

        // Flush any remaining buffered data
        self.writer.flush().await?;

        Ok(())
    }

    /// Clean MARC XML to remove wrapper elements and declarations
    fn clean_marc_xml(&self, marc: &str) -> String {
        let mut cleaned = marc.trim().to_string();

        // Remove XML declaration if present
        if let Some(pos) = cleaned.find("<?xml") {
            if let Some(end_pos) = cleaned[pos..].find("?>") {
                cleaned.replace_range(pos..pos + end_pos + 2, "");
            }
        }

        // Remove collection wrapper if present
        cleaned = cleaned.replace("<collection xmlns=\"http://www.loc.gov/MARC21/slim\">", "");
        cleaned = cleaned.replace("</collection>", "");

        // Remove any other common collection variations
        if let Some(pos) = cleaned.find("<collection") {
            if let Some(end_pos) = cleaned[pos..].find('>') {
                cleaned.replace_range(pos..pos + end_pos + 1, "");
            }
        }

        // Trim any extra whitespace
        cleaned.trim().to_string()
    }
}
