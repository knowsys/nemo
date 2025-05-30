use super::ResourceProvider;
use log::{debug, warn};
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
        self.resource.to_string()
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

        let new_headers = resource
            .headers()
            .map(|(name, value)| {
                Ok::<(HeaderName, HeaderValue), ReadingError>((
                    name.parse::<HeaderName>()
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
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        headers.extend(new_headers.into_iter());

        let err_mapping =
            |err: reqwest::Error| ReadingError::new(err.into()).with_resource(resource.clone());

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(err_mapping)?;

        let full_url = resource.to_string();
        debug!("Make HTTP request: {full_url:?}");

        let post_parameters = resource
            .post_parameters()
            .collect::<Vec<&(String, String)>>();
        let request_builder = if post_parameters.is_empty() {
            client.get(full_url.clone())
        } else {
            client.post(full_url.clone()).form(&post_parameters)
        };
        let response = request_builder
            .send()
            .await
            .map_err(|err| ReadingError::new(err.into()).with_resource(resource.clone()))?;

        response.error_for_status_ref()?;
        let response_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map(|value| value.to_str().expect("Content valid"))
            .unwrap_or_default();
        // Use 'contains()', because response_type contains content type and encoding information
        if !response_type.contains(media_type) {
            warn!("HTTP response to the request: \n {full_url} \n is of content type {response_type:?}, expected {media_type:?}. Make sure you specified the correct IRI and parameters.");
        }

        // we're expecting potentially compressed data, don't try to
        // do any character set guessing, as `response.text()` would do.
        let content = response.bytes().await.map_err(err_mapping)?;

        if content.is_empty() {
            warn!("HTTP response to the request: \n {full_url} \n contains no content. Make sure you specified the correct IRI and parameters.");
        }

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
        if !resource.is_http() {
            // We cannot handle this resource
            return Ok(None);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ReadingError::from(e).with_resource(resource.clone()))?;

        let response = rt.block_on(Self::fetch(resource, media_type))?;

        Ok(Some(Box::new(response)))
    }
}
