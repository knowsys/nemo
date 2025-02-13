use std::io::Read;

use super::ResourceProvider;
use nemo_physical::{
    error::{ReadingError, ReadingErrorKind},
    resource::Resource,
};
use reqwest::header::InvalidHeaderValue;
use urlencoding::encode;

/// A char limit to decide if a request should be send via GET or POST
const HTTP_GET_CHAR_LIMIT: usize = 2000;

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
    async fn get(resource: &Resource, media_type: &str) -> Result<HttpResource, ReadingError> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            media_type.parse().map_err(|err: InvalidHeaderValue| {
                ReadingError::new(ReadingErrorKind::ExternalError(err.into()))
                    .with_resource(resource.clone())
            })?,
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
            .map_err(|err: InvalidHeaderValue| {
                ReadingError::new(ReadingErrorKind::ExternalError(err.into()))
                    .with_resource(resource.clone())
            })?,
        );

        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );

        let err_mapping =
            |err: reqwest::Error| ReadingError::new(err.into()).with_resource(resource.clone());

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(err_mapping)?;

        let Resource::Iri { iri, parameters } = resource else {
            unreachable!("The type of the resource is known already")
        };
        let mut full_url: String = iri.to_string();

        // Dynamically decide to use GET or POST request, based on the number of characters
        // Unpack parameters
        let response = if let Some(query) = parameters.query.as_ref() {
            let enc_query = encode(query);

            if full_url.len() + query.len() > HTTP_GET_CHAR_LIMIT {
                log::debug!("Make POST-request to endpoint {full_url}");

                // Make POST request, as some browsers have limitations for GET-requests
                let body = format!("query={}", enc_query);
                client
                    .post(full_url)
                    .body(body)
                    .send()
                    .await
                    .map_err(err_mapping)?
            } else {
                log::debug!("Make GET-request to endpoint {full_url}");
                // Append the encoded query to URL
                full_url.push_str("?query=");
                full_url.push_str(&enc_query);
                client.get(full_url).send().await.map_err(err_mapping)?
            }
        } else {
            // Dont append any parameters to URL
            log::debug!("Make GET-request to endpoint {full_url}");
            client.get(full_url).send().await.map_err(err_mapping)?
        };

        // Validate status code for timeouts and other errors
        if let Err(err) = response.error_for_status_ref() {
            return Err(err_mapping(err));
        }

        // we're expecting potentially compressed data, don't try to
        // do any character set guessing, as `response.text()` would do.
        let content = response.bytes().await.map_err(err_mapping)?;

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
        media_type: &str,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        // Add error message
        let Resource::Iri { iri, .. } = resource else {
            unreachable!("The type of the resource is known already")
        };

        let url = iri.to_string();
        // Not needed this is checked already in
        if !(url.starts_with("http:") || url.starts_with("https:")) {
            // Non-http IRI; resource provider is not responsible
            return Ok(None);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ReadingError::from(e).with_resource(resource.clone()))?;

        let response = rt.block_on(Self::get(resource, media_type))?;
        Ok(Some(Box::new(response)))
    }
}
