use std::io::{BufRead, BufReader, Read};

use nemo_physical::{error::ReadingError, resource::Resource};

use crate::io::compression_format::CompressionFormat;

use super::{is_iri, ResourceProvider};

/// Resolves resources using HTTP or HTTPS.
///
/// Handles `http:` and `https:` IRIs.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpResourceProvider {}

/// The result of fetching a resource via HTTP.
#[derive(Debug, Clone)]
pub struct HttpResource {
    /// IRI that this resource was fetched from
    url: Resource,
    /// Content of the resource
    content: Vec<u8>,
}

impl HttpResource {
    /// Return the IRI this resource was fetched from.
    pub fn url(&self) -> &Resource {
        &self.url
    }

    /// Return the content of this resource.
    pub fn content(&self) -> String {
        String::from_utf8(self.content.clone()).expect("is valid UTF-8")
    }
}

impl Read for HttpResource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let amount = self.content.as_slice().read(buf)?;
        self.content.drain(0..amount);

        Ok(amount)
    }
}

impl HttpResourceProvider {
    async fn get(url: &Resource) -> Result<HttpResource, ReadingError> {
        let response = reqwest::get(url).await?;
        let content = response.text().await?;

        Ok(HttpResource {
            url: url.to_string(),
            content: content.into(),
        })
    }
}

impl ResourceProvider for HttpResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
        compression: CompressionFormat,
    ) -> Result<Option<Box<dyn BufRead>>, ReadingError> {
        if !is_iri(resource) {
            return Ok(None);
        }

        if !(resource.starts_with("http:") || resource.starts_with("https:")) {
            // Non-http IRI; resource provider is not responsible
            return Ok(None);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ReadingError::IoReading {
                error: e,
                filename: resource.clone(),
            })?;
        let response = rt.block_on(Self::get(resource))?;
        if let Some(reader) = compression.try_decompression(BufReader::new(response)) {
            Ok(Some(reader))
        } else {
            Err(ReadingError::Decompression {
                resource: resource.to_owned(),
                decompression_format: compression.to_string(),
            })
        }
    }
}
