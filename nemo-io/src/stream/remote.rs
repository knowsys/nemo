//! Remote resources: the requests for them, their responses, and the clients that send the
//! requests.
//!
//! Readers do not fetch.
//! A remote resource is described by a [Request], which is plain data,
//! and its [Response] is either the body's bytes or a [Failure].
//! A [Client] sends requests.
//! The crate has a blocking HTTP client behind the `http` feature,
//! and other clients can send requests by other means, such as a browser's `fetch`.
//! Building a request needs no feature, so every build can say what it would fetch.

#[cfg(feature = "http")]
mod blocking;
mod retry;

use std::{fmt, sync::Arc};

use nemo_physical::diagnostics::Diagnostic;

#[cfg(feature = "http")]
pub use self::blocking::BlockingClient;
pub use self::retry::Retry;
use crate::{AboutResource, error::IoError};

/// One HTTP request for a resource's bytes.
///
/// A request is plain data, so it can be built and inspected without the `http` feature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// The URL to fetch.
    pub url: String,
    /// Additional request headers, applied in order.
    pub headers: Vec<(String, String)>,
    /// The value of the `Accept` header, or `None` for no preference.
    pub accept: Option<&'static str>,
    /// Extra query parameters, appended to the URL in URL-encoded form.
    pub parameters: Vec<(String, String)>,
    /// The fields of a form, or `None` for no form.
    ///
    /// A request with a form is a POST.
    pub form: Option<Vec<(String, String)>>,
}

impl Request {
    /// Returns whether a resource name is an `http` or `https` URL, which names a remote
    /// resource.
    pub fn is_remote(resource: &str) -> bool {
        resource.starts_with("http://") || resource.starts_with("https://")
    }

    /// Creates a GET request for a URL, without headers, parameters or form.
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
            accept: None,
            parameters: Vec::new(),
            form: None,
        }
    }

    /// Returns the HTTP method of the request: POST if it has a form, and GET otherwise.
    pub fn method(&self) -> &'static str {
        if self.form.is_some() { "POST" } else { "GET" }
    }
}

/// The reason a request got no body.
///
/// A failure has the response's status, if a response came,
/// and what the transport or the response said.
/// A failure is final, since a [Client] retries a request before it responds with one.
/// [smaller_may_succeed](Self::smaller_may_succeed) says whether a smaller request may succeed
/// instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The HTTP status, if a response came.
    pub status: Option<u16>,
    /// The transport's reason, or an excerpt of the response body.
    pub message: String,
    /// Whether a response came, but its status or headers said that it contains only part of
    /// the result ([of_partial_response](Self::of_partial_response)).
    ///
    /// Such a failure has no body, so no partial result is read.
    pub partial: bool,
}

impl Failure {
    /// Creates the failure of a response with a status outside the success range.
    ///
    /// `message` is an excerpt of the response body.
    pub fn response(status: u16, message: impl Into<String>) -> Self {
        Self {
            status: Some(status),
            message: message.into(),
            partial: false,
        }
    }

    /// Creates the failure of a request that got no response,
    /// for example because there was no connection, a timeout, or a broken transfer.
    pub fn transport(message: impl Into<String>) -> Self {
        Self {
            status: None,
            message: message.into(),
            partial: false,
        }
    }

    /// Creates the failure of a successful response that held only part of the result,
    /// for the given reason.
    pub fn partial(status: u16, reason: impl Into<String>) -> Self {
        Self {
            status: Some(status),
            message: reason.into(),
            partial: true,
        }
    }

    /// The names of the response headers that [of_partial_response](Self::of_partial_response)
    /// reads.
    pub const PARTIAL_RESPONSE_HEADERS: [&'static str; 3] =
        ["X-SQL-State", "X-SQL-Message", "X-SPARQL-MaxRows"];

    /// Returns the failure of a successful response that contains only part of its result,
    /// or `None` if the response is whole.
    ///
    /// `status` is the response's status,
    /// and `header` returns the value of a response header by its name, regardless of case.
    ///
    /// SPARQL defines no way to mark a partial result,
    /// and a store that stops at its time or row limit responds with what it has.
    /// So this reads the signals of one store, Virtuoso, which serves DBpedia:
    /// - status 206, for an "anytime" result cut off at its time limit,
    /// - `X-SQL-State`, with `X-SQL-Message`, for such a result sent with status 200,
    /// - `X-SPARQL-MaxRows`, for a result cut off without an error at its row limit
    ///   (`ResultSetMaxRows`).
    pub fn of_partial_response<'a>(
        status: u16,
        header: impl Fn(&str) -> Option<&'a str>,
    ) -> Option<Self> {
        let reason = if let Some(state) = header("X-SQL-State") {
            let message = header("X-SQL-Message").unwrap_or_default();
            format!("it stopped early ({state}: {message})")
        } else if let Some(rows) = header("X-SPARQL-MaxRows") {
            format!("it returns at most {rows} rows")
        } else if status == 206 {
            String::from("it marked its response partial (status 206)")
        } else {
            return None;
        };

        Some(Self::partial(status, reason))
    }

    /// Returns whether the server found the request too large (413) or its URL too long (414),
    /// in which case a smaller request may succeed.
    pub fn too_large(&self) -> bool {
        matches!(self.status, Some(413 | 414))
    }

    /// Returns whether a smaller request may succeed.
    ///
    /// That is the case if the server found the request [too large](Self::too_large),
    /// or if it responded only in part, stopped by its time or row limit.
    pub fn smaller_may_succeed(&self) -> bool {
        self.too_large() || self.partial
    }

    /// Returns the diagnostic of this failure to fetch `url`.
    ///
    /// It is a [PARTIAL_RESULT](crate::codes::PARTIAL_RESULT) for a partial response,
    /// and an [UNFETCHABLE](crate::codes::UNFETCHABLE) otherwise.
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

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// The response to a request: either the body's bytes, or the [Failure] that prevented them.
pub type Response = Result<Arc<[u8]>, Failure>;

/// A sender of requests, which collects their responses.
///
/// The `BlockingClient` behind the `http` feature sends requests over HTTP.
/// Another client can respond from a cache or from fixtures,
/// or send requests through a proxy or with credentials.
///
/// # Example
///
/// A client that responds from fixtures:
///
/// ```
/// use std::{fmt, sync::Arc};
///
/// use nemo_io::remote::{Client, Failure, Request, Response};
///
/// struct Fixtures;
///
/// impl Client for Fixtures {
///     fn fetch_all(&self, requests: &[Request], _: &dyn Fn() -> bool) -> Option<Vec<Response>> {
///         let respond = |request: &Request| match request.url.as_str() {
///             "https://example.org/p.csv" => Ok(Arc::from(&b"1\n"[..])),
///             _ => Err(Failure::response(404, "no fixture")),
///         };
///         Some(requests.iter().map(respond).collect())
///     }
/// }
///
/// let requests = [
///     Request::get("https://example.org/p.csv"),
///     Request::get("https://example.org/q.csv"),
/// ];
/// let responses = Fixtures.fetch_all(&requests, &|| false).expect("nothing cancels");
/// assert_eq!(responses[0].as_deref().ok(), Some(&b"1\n"[..]));
/// assert!(responses[1].is_err());
/// ```
pub trait Client {
    /// Sends several requests, and returns one response per request, in order.
    ///
    /// Returns `None` if `cancelled` returns true before all responses have arrived.
    ///
    /// A client retries a request before it responds with a [Failure].
    /// If [Failure::of_partial_response] finds a successful response partial,
    /// the client responds with that failure instead, so that no partial result is read as a
    /// whole one.
    fn fetch_all(
        &self,
        requests: &[Request],
        cancelled: &dyn Fn() -> bool,
    ) -> Option<Vec<Response>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A failure shows the response's status and message,
    /// and says whether a smaller request may succeed.
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

    /// A partial response is recognized by its status 206 or by its headers,
    /// and a response with neither is whole.
    #[test]
    fn partial_responses_are_recognized_by_status_and_headers() {
        let none = |_: &str| None;
        assert_eq!(Failure::of_partial_response(200, none), None);
        assert!(Failure::of_partial_response(206, none).is_some_and(|failure| failure.partial));
        let capped = |name: &str| (name == "X-SPARQL-MaxRows").then_some("10000");
        assert!(
            Failure::of_partial_response(200, capped)
                .is_some_and(|failure| failure.message.contains("10000")),
            "{:?}",
            Failure::of_partial_response(200, capped)
        );
        let timed_out = |name: &str| match name {
            "X-SQL-State" => Some("S1TAT"),
            "X-SQL-Message" => Some("RC...: Returning incomplete results"),
            _ => None,
        };
        assert!(
            Failure::of_partial_response(200, timed_out)
                .is_some_and(|failure| failure.message.contains("S1TAT"))
        );
    }
}
