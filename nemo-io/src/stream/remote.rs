//! Remote resources: requests as plain data, their failures,
//! and a blocking client that sends them.
//!
//! Readers do not fetch.
//! A remote resource is described by a [request](Request),
//! and its [response](Response) is either the body's bytes or a [Failure].
//! A [Client] sends requests: the blocking client behind the `http` feature is one,
//! retrying on the [Retry] schedule.
//! A browser sends them through `fetch`, and an async runtime through a client of its own.
//! Recognizing a remote resource and building its request need no feature,
//! so every build can say what it would fetch.

#[cfg(feature = "http")]
mod blocking;
mod retry;

use std::sync::Arc;

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
/// Only a request too large for the server, or one it answered only in part,
/// is sent again, in smaller pieces, if it can be split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The HTTP status, if a response came.
    pub status: Option<u16>,
    /// The transport's reason, or an excerpt of the response body.
    pub message: String,
    /// Whether a response came but held only part of the result, as its status or headers
    /// said ([partial_response]): its body is not taken, so no partial result imports.
    pub partial: bool,
}

impl Failure {
    /// Creates the failure of a response outside the success range,
    /// with an excerpt of what it said.
    pub fn response(status: u16, message: impl Into<String>) -> Self {
        Self {
            status: Some(status),
            message: message.into(),
            partial: false,
        }
    }

    /// Creates the failure of no response: no connection, a timeout, a broken transfer.
    pub fn transport(message: impl Into<String>) -> Self {
        Self {
            status: None,
            message: message.into(),
            partial: false,
        }
    }

    /// Creates the failure of a successful response that held only part of the result,
    /// for the reason [partial_response] gives.
    pub fn partial(status: u16, reason: impl Into<String>) -> Self {
        Self {
            status: Some(status),
            message: reason.into(),
            partial: true,
        }
    }

    /// Returns whether the server found the request too large (413) or its URL too long (414),
    /// in which case a smaller request may succeed.
    pub fn too_large(&self) -> bool {
        matches!(self.status, Some(413 | 414))
    }

    /// Returns whether a smaller request may succeed:
    /// the server found the request [too large](Self::too_large),
    /// or answered it only in part, stopped by its time or row limit.
    pub fn smaller_may_succeed(&self) -> bool {
        self.too_large() || self.partial
    }

    /// Returns the diagnostic of this failure, for the URL fetched.
    pub fn diagnostic(&self, url: &str) -> Diagnostic {
        let url = url.to_owned();
        let error = if self.partial {
            IoError::PartialResult {
                url: url.clone(),
                reason: self.message.clone(),
            }
        } else {
            IoError::Unfetchable {
                url: url.clone(),
                reason: self.to_string(),
            }
        };
        error.diagnostic().with_resource(url)
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.partial {
            return write!(
                f,
                "the endpoint returned only part of the result: {}",
                self.message
            );
        }
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

/// The response to a request: the body's bytes, or the [Failure] that prevented one.
pub type Response = Result<Arc<[u8]>, Failure>;

/// The response headers [partial_response] reads, for a client that collects them first.
pub const PARTIAL_RESPONSE_HEADERS: [&str; 3] =
    ["X-SQL-State", "X-SQL-Message", "X-SPARQL-MaxRows"];

/// Returns why a successful response holds only part of its result, if its status or
/// headers say so, or `None` for a whole one.
///
/// SPARQL defines no way to say so, and a store that stops at its time or row limit
/// answers with what it has. Virtuoso, behind DBpedia and many other endpoints, says it:
/// - status 206 for an "anytime" result cut at its time limit,
///   or `X-SQL-State` (with `X-SQL-Message`) where it answers 200;
/// - `X-SPARQL-MaxRows` for a result that reached its row limit (`ResultSetMaxRows`,
///   10 000 rows at DBpedia), which it cuts without an error.
///
/// `header` returns a response header's value by its name, regardless of case.
/// An HTTP client calls this for every successful response and fails the request with a
/// [partial](Failure::partial) failure, so that no partial result is read as a whole one.
pub fn partial_response<'a>(
    status: u16,
    header: impl Fn(&str) -> Option<&'a str>,
) -> Option<String> {
    if let Some(state) = header("X-SQL-State") {
        let message = header("X-SQL-Message").unwrap_or_default();
        return Some(format!("it stopped early ({state}: {message})"));
    }
    if let Some(rows) = header("X-SPARQL-MaxRows") {
        return Some(format!("it returns at most {rows} rows"));
    }
    (status == 206).then(|| String::from("it marked its response partial (status 206)"))
}

/// What sends the requests of a method that needs data from the network.
///
/// The `BlockingClient` behind the `http` feature sends them over HTTP.
/// Another client can answer them from a cache or from fixtures,
/// or send them through a proxy or with credentials.
pub trait Client {
    /// Sends several requests, and returns one response per request, in order.
    ///
    /// Returns `None` if `cancelled` turned true before every response came.
    fn fetch_all(
        &self,
        requests: &[Request],
        cancelled: &dyn Fn() -> bool,
    ) -> Option<Vec<Response>>;
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

        let partial = Failure::partial(200, "it returns at most 10000 rows");
        assert!(partial.smaller_may_succeed() && !partial.too_large());
        assert_eq!(
            partial.to_string(),
            "the endpoint returned only part of the result: it returns at most 10000 rows"
        );
        let problem = partial.diagnostic("https://dbpedia.org/sparql");
        assert_eq!(problem.code(), crate::codes::PARTIAL_RESULT, "{problem:?}");
        assert!(Failure::response(413, "").smaller_may_succeed());
        assert!(!Failure::response(500, "").smaller_may_succeed());
    }

    #[test]
    fn partial_responses_are_told_by_status_and_headers() {
        let none = |_: &str| None;
        assert_eq!(partial_response(200, none), None);
        assert!(partial_response(206, none).is_some());
        let capped = |name: &str| (name == "X-SPARQL-MaxRows").then_some("10000");
        assert!(
            partial_response(200, capped).is_some_and(|reason| reason.contains("10000")),
            "{:?}",
            partial_response(200, capped)
        );
        let timed_out = |name: &str| match name {
            "X-SQL-State" => Some("S1TAT"),
            "X-SQL-Message" => Some("RC...: Returning incomplete results"),
            _ => None,
        };
        assert!(partial_response(200, timed_out).is_some_and(|reason| reason.contains("S1TAT")));
    }
}
