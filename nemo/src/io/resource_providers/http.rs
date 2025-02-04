use std::io::{BufRead, BufReader, Read};

use nemo_physical::{error::ReadingError, resource::Resource};
use reqwest::header::InvalidHeaderValue;

use crate::rule_model::components::import_export::compression::CompressionFormat;
use urlencoding::encode;
use super::ResourceProvider;

/// Resolves resources using HTTP or HTTPS.
///
/// Handles `http:` and `https:` IRIs.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpResourceProvider {}

/// The result of fetching a resource via HTTP.
#[derive(Debug, Clone)]
pub struct HttpResource {
    /// IRI that this resource was fetched from
    resource: Resource,
    /// Content of the resource
    content: Vec<u8>,
}

impl HttpResource {
    /// Return the IRI this resource was fetched from.
    pub fn url(&self) -> &Resource {
        // TODO: should this return the Resource, or just the IRI?
        &self.resource
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
    async fn get(resource: &Resource, url: String, parameters: &Vec<(String, String)>, media_type: &str) -> Result<HttpResource, ReadingError> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            media_type
                .parse()
                .map_err(|err: InvalidHeaderValue| ReadingError::ExternalError(err.into()))?,
        );
        headers.insert(
            reqwest::header::USER_AGENT,
            format!(
                "{}/{} ({})",
                option_env!("CARGO_PKG_NAME").unwrap_or("Nemo"),
                option_env!("CARGO_PKG_VERSION").unwrap_or("unknown-version"),
                option_env!("CARGO_PKG_HOMEPAGE")
                    .unwrap_or("https://iccl.inf.tu-dresden.de/web/Nemo/en")
            )
            .parse()
            .map_err(|err: InvalidHeaderValue| ReadingError::ExternalError(err.into()))?,
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        // Find the tuple where the first element is "query"
        // Only for test cases, we can also have http request without queries!
        let query = if let Some((_, query_value)) = parameters.iter().find(|(key, _)| key == "query") {
            query_value.as_str() // Extracts the value from the tuple
        } else {
            ""
        };
        

        // Unpack parameters 
        let encoded_query = encode(query);
        
        // Concatenate url and encoded query
        let full_url = format!("{}?query={}", url, encoded_query);
        println!("full query: {full_url}");

        let response = client.get(full_url).send().await?;
        // we're expecting potentially compressed data, don't try to
        // do any character set guessing, as `response.text()` would do.
        let content = response.bytes().await?;

        Ok(HttpResource {
            resource: resource.clone(),
            content: content.into(),
        })
    }
}

impl ResourceProvider for HttpResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
        compression: CompressionFormat,
        media_type: &str,
    ) -> Result<Option<Box<dyn BufRead>>, ReadingError> {
        // Add error message
        let Resource::Iri { iri, parameters } = resource else {unreachable!("The type of the resource is known already")};

        let url = iri.to_string();
        if !(url.starts_with("http:") || url.starts_with("https:")) {
            // Non-http IRI; resource provider is not responsible
            return Ok(None);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ReadingError::IoReading {
                error: e,
                filename: url.clone(),
            })?;
        let response = rt.block_on(Self::get(resource, url, parameters, media_type))?;
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
