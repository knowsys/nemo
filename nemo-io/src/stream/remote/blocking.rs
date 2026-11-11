//! The blocking HTTP client, built on `reqwest`.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use crate::remote::{Client, Failure, Request, Response, Retry};

/// A [Client] that sends requests over HTTP with `reqwest`'s blocking client.
///
/// `reqwest` runs an async runtime internally, so this crate needs none of its own.
/// The requests of one call run in parallel on a bounded pool of threads,
/// which share one `reqwest` client and its pooled connections.
/// A failed request is retried on the client's [Retry] schedule.
///
/// # Example
///
/// ```no_run
/// use nemo_io::remote::{BlockingClient, Client, Request, Retry};
///
/// let client = BlockingClient::new(Retry::default());
/// let responses = client
///     .fetch_all(&[Request::get("https://example.org/data.csv")], &|| false)
///     .expect("nothing cancels");
/// if let Ok(body) = &responses[0] {
///     println!("{} bytes", body.len());
/// }
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct BlockingClient {
    /// The schedule on which failed requests are sent again.
    retry: Retry,
}

/// The largest number of requests in flight at once.
///
/// It is enough to overlap their latencies, and no more than a public endpoint allows one
/// client.
/// For example, Wikidata's query service runs five queries in parallel per IP address.
const MAX_IN_FLIGHT: usize = 5;

/// How often a waiting call checks whether it is cancelled.
const POLL: Duration = Duration::from_millis(50);

/// The time one request may take, from connection to last byte.
///
/// A SPARQL endpoint may take minutes to respond to a heavy query,
/// and usually enforces a time limit of its own.
/// `reqwest`'s default of thirty seconds would cut such responses off in the middle of the body.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

/// The largest number of characters of a failed response's body that its failure quotes.
const EXCERPT_CHARS: usize = 300;

/// The state that the threads of one call share.
struct RequestBatch {
    /// The requests, in order.
    requests: Vec<Request>,
    /// The index of the next request a thread takes.
    next: AtomicUsize,
    /// Whether the call was cancelled.
    ///
    /// Once it is, the threads take no further request and stop waiting to retry.
    abandoned: AtomicBool,
}

impl BlockingClient {
    /// Creates a client that retries on the given schedule.
    pub fn new(retry: Retry) -> Self {
        Self { retry }
    }
}

impl Client for BlockingClient {
    /// Sends the requests on a bounded number of threads.
    ///
    /// If `cancelled` returns true, the requests in flight finish on their own threads,
    /// and their responses are dropped.
    fn fetch_all(
        &self,
        requests: &[Request],
        cancelled: &dyn Fn() -> bool,
    ) -> Option<Vec<Response>> {
        if cancelled() {
            return None;
        }
        let client = match client() {
            Ok(client) => client,
            Err(failure) => return Some(requests.iter().map(|_| Err(failure.clone())).collect()),
        };
        let batch = Arc::new(RequestBatch {
            requests: requests.to_vec(),
            next: AtomicUsize::new(0),
            abandoned: AtomicBool::new(false),
        });
        let (sender, receiver) = mpsc::channel();
        for _ in 0..MAX_IN_FLIGHT.min(requests.len()) {
            let batch = Arc::clone(&batch);
            let sender = sender.clone();
            let client = client.clone();
            let retry = self.retry;
            std::thread::spawn(move || {
                while !batch.abandoned.load(Ordering::Relaxed) {
                    let index = batch.next.fetch_add(1, Ordering::Relaxed);
                    let Some(request) = batch.requests.get(index) else {
                        break;
                    };
                    let response = fetch_retrying(&client, request, retry, &batch.abandoned);
                    if sender.send((index, response)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(sender);

        let mut responses: Vec<Option<Response>> = vec![None; requests.len()];
        let mut received = 0;
        while received < requests.len() {
            match receiver.recv_timeout(POLL) {
                Ok((index, response)) => {
                    responses[index] = Some(response);
                    received += 1;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if cancelled() {
                        batch.abandoned.store(true, Ordering::Relaxed);
                        return None;
                    }
                }
                // Every thread has ended, and a thread that panicked left its slot empty.
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        Some(
            responses
                .into_iter()
                .map(|response| {
                    response.unwrap_or_else(|| Err(Failure::transport("the fetch thread panicked")))
                })
                .collect(),
        )
    }
}

/// Returns the `reqwest` client of one call, which pools connections across the call's
/// requests.
fn client() -> Result<reqwest::blocking::Client, Failure> {
    reqwest::blocking::Client::builder()
        .user_agent(concat!("nemo/", env!("CARGO_PKG_VERSION")))
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|error| Failure::transport(format!("the HTTP client could not be built: {error}")))
}

/// Sends one request, and sends it again for as long as the schedule says so.
///
/// It stops retrying once the call is abandoned.
fn fetch_retrying(
    client: &reqwest::blocking::Client,
    request: &Request,
    retry: Retry,
    abandoned: &AtomicBool,
) -> Response {
    let mut attempt = 0;
    loop {
        let (failure, retry_after) = match send(client, request) {
            Ok(bytes) => return Ok(bytes),
            Err(failed) => failed,
        };
        let delay = failure
            .status
            .and_then(|status| retry.delay(attempt, status, retry_after.as_deref()));
        let Some(delay) = delay else {
            return Err(failure);
        };
        // This is a warning, since a wait that the server asks for can take minutes.
        tracing::warn!(
            url = request.url.as_str(),
            status = failure.status,
            attempt,
            wait_seconds = delay.as_secs_f64(),
            "waiting before retrying a remote request"
        );
        if !wait(delay, abandoned) {
            return Err(failure);
        }
        attempt += 1;
    }
}

/// Sleeps for a duration in short slices, and returns whether it slept the whole duration.
///
/// It stops early if the call is abandoned.
fn wait(duration: Duration, abandoned: &AtomicBool) -> bool {
    let end = Instant::now() + duration;
    loop {
        if abandoned.load(Ordering::Relaxed) {
            return false;
        }
        let now = Instant::now();
        if now >= end {
            return true;
        }
        std::thread::sleep(POLL.min(end - now));
    }
}

/// Sends one request once, and returns its body,
/// or its failure together with the response's `Retry-After` value.
fn send(
    client: &reqwest::blocking::Client,
    request: &Request,
) -> Result<Arc<[u8]>, (Failure, Option<String>)> {
    let mut builder = match &request.form {
        Some(form) => client.post(&request.url).form(form),
        None => client.get(&request.url),
    };
    if !request.parameters.is_empty() {
        builder = builder.query(&request.parameters);
    }
    if let Some(accept) = request.accept {
        builder = builder.header(reqwest::header::ACCEPT, accept);
    }
    for (name, value) in &request.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let response = builder
        .send()
        .map_err(|error| (Failure::transport(transport_reason(error)), None))?;
    let status = response.status();
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response.text().unwrap_or_default();
        let excerpt = excerpt(&body);
        let message = if excerpt.is_empty() {
            status.canonical_reason().unwrap_or_default().to_owned()
        } else {
            excerpt
        };
        return Err((Failure::response(status.as_u16(), message), retry_after));
    }
    let partial = Failure::of_partial_response(status.as_u16(), |name| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
    });
    if let Some(failure) = partial {
        return Err((failure, None));
    }
    let bytes = response
        .bytes()
        .map_err(|error| (Failure::transport(transport_reason(error)), None))?;
    tracing::debug!(
        url = request.url.as_str(),
        status = status.as_u16(),
        bytes = bytes.len(),
        "fetched a remote resource"
    );
    Ok(Arc::from(bytes.as_ref()))
}

/// Returns why a transfer failed, including the causes in the error, such as a refused
/// connection.
///
/// The URL is left out, since the failure's [diagnostic](Failure::diagnostic) names it already.
fn transport_reason(error: reqwest::Error) -> String {
    let error = error.without_url();
    let mut reason = error.to_string();
    let mut cause = std::error::Error::source(&error);
    while let Some(inner) = cause {
        let text = inner.to_string();
        // Some layers repeat the cause they wrap in their own text.
        if !reason.ends_with(&text) {
            reason.push_str(": ");
            reason.push_str(&text);
        }
        cause = std::error::Error::source(inner);
    }
    reason
}

/// Returns the start of a response body on one line, with its whitespace collapsed.
///
/// A longer text is cut after [EXCERPT_CHARS] characters and ends in an ellipsis.
fn excerpt(body: &str) -> String {
    let mut excerpt = String::new();
    for (index, word) in body.split_whitespace().enumerate() {
        if index > 0 {
            excerpt.push(' ');
        }
        excerpt.push_str(word);
        if excerpt.chars().count() > EXCERPT_CHARS {
            let cut: String = excerpt.chars().take(EXCERPT_CHARS).collect();
            return format!("{cut}…");
        }
    }
    excerpt
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    /// An excerpt keeps the start of a body on one line, and cuts a long one.
    #[test]
    fn excerpts_keep_the_start_of_a_body_on_one_line() {
        assert_eq!(
            excerpt("  Service\n  Unavailable \n"),
            "Service Unavailable"
        );
        let long = "word ".repeat(200);
        let cut = excerpt(&long);
        assert!(cut.ends_with('…'));
        assert_eq!(cut.chars().count(), EXCERPT_CHARS + 1);
    }

    /// A cancelled call returns without waiting for a server that never responds.
    #[test]
    fn a_cancelled_call_stops_waiting_for_its_requests() {
        // A server that accepts the connection and never responds.
        let listener = TcpListener::bind("127.0.0.1:0").expect("a local port");
        let url = format!("http://{}/slow", listener.local_addr().expect("an address"));
        let started = Instant::now();
        let client = BlockingClient::default();
        let responses = client.fetch_all(&[Request::get(url)], &|| {
            started.elapsed() > Duration::from_millis(200)
        });
        assert!(responses.is_none());
        assert!(started.elapsed() < Duration::from_secs(5));
        drop(listener);
    }
}
