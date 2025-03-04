use super::ResourceProvider;
use nemo_physical::{
    error::{ReadingError, ReadingErrorKind},
    resource::Resource,
};
use reqwest::header::{HeaderName, HeaderValue, InvalidHeaderName, InvalidHeaderValue};
use std::io::Read;

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
    pub fn url(&self) -> String {
        self.resource.as_iri_encoded()
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
    async fn fetch(resource: &Resource, media_type: &str) -> Result<HttpResource, ReadingError> {
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

        // Unpack custom headers from resource
        for (key, value) in resource.header_parameters() {
            headers.insert(
                key.parse::<HeaderName>()
                    .map_err(|err: InvalidHeaderName| {
                        ReadingError::new(ReadingErrorKind::ExternalError(err.into()))
                            .with_resource(resource.clone())
                    })?,
                value
                    .parse::<HeaderValue>()
                    .map_err(|err: InvalidHeaderValue| {
                        ReadingError::new(ReadingErrorKind::ExternalError(err.into()))
                            .with_resource(resource.clone())
                    })?,
            );
        }

        let err_mapping =
            |err: reqwest::Error| ReadingError::new(err.into()).with_resource(resource.clone());

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(err_mapping)?;

        let full_url = resource.as_iri_encoded();

        let response = {
            // Collect Body-Parameters into one string
            if resource.has_post_parameters() {
                // Make POST-Request
                client
                    .post(full_url)
                    .form(
                        &resource
                            .post_parameters()
                            .collect::<Vec<&(String, String)>>(),
                    )
                    .send()
                    .await
                    .map_err(err_mapping)?
            } else {
                // Make GET-Request
                client.get(full_url).send().await.map_err(err_mapping)?
            }
        };

        // Validate status code for timeouts and other errors
        response.error_for_status_ref()?;

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
        // Early return if Resource does not contain an Iri
        if !resource.is_iri() {
            return Ok(None);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ReadingError::from(e).with_resource(resource.clone()))?;

        // Verify if resource has a non-empty body
        let response = rt.block_on(Self::fetch(resource, media_type))?;

        Ok(Some(Box::new(response)))
    }
}
