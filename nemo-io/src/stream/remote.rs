//! Remote resources: requests as plain data, their failures,
//! and a blocking client that sends them.
//!
//! Readers do not fetch.
//! A remote resource is described by a [request](Request),
//! and its response is either the body's bytes or a [Failure].
//! The blocking client behind the `http` feature is one way to send requests,
//! retrying on the [Retry] schedule.
//! A browser sends them through `fetch`, and an async runtime through a client of its own.
//! Recognizing a remote resource and building its request need no feature,
//! so every build can say what it would fetch.

#[cfg(feature = "http")]
mod blocking;
mod retry;

use nemo_physical::diagnostics::Diagnostic;

#[cfg(feature = "http")]
pub use self::blocking::BlockingClient;
pub use self::retry::Retry;
use crate::{AboutResource, error::IoError};

/// One HTTP request for a resource's bytes.
///
/// A request is plain data, so it builds, and validates, without the `http` feature that sends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// The URL fetched.
    pub url: String,
    /// Additional request headers, applied in order.
    pub headers: Vec<(String, String)>,
    /// The `Accept` value, if a format has a preference.
    pub accept: Option<&'static str>,
    /// Extra query parameters, appended URL-encoded.
    pub parameters: Vec<(String, String)>,
    /// Form fields, whose presence makes the request a POST.
    pub form: Option<Vec<(String, String)>>,
}

impl Request {
    /// Returns whether a resource name addresses a remote resource, an `http` or `https` URL,
    /// whose bytes a request fetches.
    pub fn is_remote(resource: &str) -> bool {
        resource.starts_with("http://") || resource.starts_with("https://")
    }

    /// Creates a plain GET for a URL.
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
            accept: None,
            parameters: Vec::new(),
            form: None,
        }
    }

    /// Returns the HTTP method the request travels as:
    /// a POST if it carries a form, and a GET otherwise.
    pub fn method(&self) -> &'static str {
        if self.form.is_some() { "POST" } else { "GET" }
    }
}

/// Why a request got no body:
/// the response's status if a response came, and what the transport or the response said.
///
/// A failure is final, so a request is retried before its failure is reported.
/// Only a request too large for the server is sent again, in smaller pieces,
/// if it can be split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The HTTP status, if a response came.
    pub status: Option<u16>,
    /// The transport's reason, or an excerpt of the response body.
    pub message: String,
}

impl Failure {
    /// Creates the failure of a response outside the success range,
    /// with an excerpt of what it said.
    pub fn response(status: u16, message: impl Into<String>) -> Self {
        Self {
            status: Some(status),
            message: message.into(),
        }
    }

    /// Creates the failure of no response: no connection, a timeout, a broken transfer.
    pub fn transport(message: impl Into<String>) -> Self {
        Self {
            status: None,
            message: message.into(),
        }
    }

    /// Returns whether the server found the request too large (413) or its URL too long (414),
    /// in which case a smaller request may succeed.
    pub fn too_large(&self) -> bool {
        matches!(self.status, Some(413 | 414))
    }

    /// Returns the diagnostic of this failure, for the URL fetched.
    pub fn diagnostic(&self, url: &str) -> Diagnostic {
        IoError::Unfetchable {
            url: url.to_owned(),
            reason: self.to_string(),
        }
        .diagnostic()
        .with_resource(url.to_owned())
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.status, self.message.is_empty()) {
            (Some(status), true) => write!(f, "the server responded with status {status}"),
            (Some(status), false) => write!(
                f,
                "the server responded with status {status}: {}",
                self.message
            ),
            (None, _) => f.write_str(&self.message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_show_the_status_and_message_of_the_response() {
        assert_eq!(
            Failure::response(503, "busy").to_string(),
            "the server responded with status 503: busy"
        );
        assert_eq!(
            Failure::response(404, "").to_string(),
            "the server responded with status 404"
        );
        assert_eq!(Failure::transport("timed out").to_string(), "timed out");
        assert!(Failure::response(413, "").too_large());
        assert!(Failure::response(414, "").too_large());
        assert!(!Failure::response(500, "").too_large());
        assert!(!Failure::transport("reset").too_large());
    }
}
