//! The blocking client over `reqwest`.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use crate::stream::remote::{Failure, Request, Retry};

/// The client of builds with the `http` feature: `reqwest`'s blocking client.
///
/// It runs an async runtime internally, so this crate needs none of its own.
/// The requests of one call overlap on a bounded pool of threads over one pooled connection
/// client, and each failed request is retried on the [Retry] schedule before its failure is
/// returned.
#[derive(Debug, Default, Clone, Copy)]
pub struct BlockingClient {
    /// When failed requests are sent again.
    retry: Retry,
}

/// The number of requests in flight at once.
///
/// It is enough to overlap their latencies,
/// and as many as a public endpoint allows one client:
/// Wikidata's query service runs five queries in parallel per IP address.
const MAX_IN_FLIGHT: usize = 5;

/// How often a waiting call looks at its cancellation flag.
const POLL: Duration = Duration::from_millis(50);

/// The time one request may take, from connection to last byte.
///
/// A SPARQL endpoint may take minutes to respond to a heavy query, and enforces a limit of its
/// own.
/// The client's default of thirty seconds would cut such responses off mid-body.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

/// How much of a failed response's body its failure quotes.
const EXCERPT_CHARS: usize = 300;

/// What the threads of one call share: the requests, the next one to
/// take, and whether the call was cancelled.
struct RequestBatch {
    /// The requests, in order.
    requests: Vec<Request>,
    /// The index of the next request a thread takes.
    next: AtomicUsize,
    /// Set when the call is cancelled: threads take no further
    /// request and stop backing off.
    abandoned: AtomicBool,
}

impl BlockingClient {
    /// Creates a client retrying on the given schedule.
    pub fn new(retry: Retry) -> Self {
        Self { retry }
    }

    /// Sends several requests, and returns one result per request in order:
    /// the response body or a [Failure].
    ///
    /// Returns `None` if `cancelled` turned true before every response came.
    /// The requests in flight then finish on their own threads, and their responses are dropped.
    pub fn fetch_all(
        &self,
        requests: &[Request],
        cancelled: &dyn Fn() -> bool,
    ) -> Option<Vec<Result<Arc<[u8]>, Failure>>> {
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

        let mut responses: Vec<Option<Result<Arc<[u8]>, Failure>>> = vec![None; requests.len()];
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
                // Every thread ended; a panicked one left its slot empty.
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

/// Returns the client of one call, whose connections pool across the call's requests.
fn client() -> Result<reqwest::blocking::Client, Failure> {
    reqwest::blocking::Client::builder()
        .user_agent(concat!("nemo/", env!("CARGO_PKG_VERSION")))
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|error| Failure::transport(format!("the HTTP client could not be built: {error}")))
}

/// Performs one request, sending it again while the schedule says so.
///
/// It gives up on the retries once the call is abandoned.
fn fetch_retrying(
    client: &reqwest::blocking::Client,
    request: &Request,
    retry: Retry,
    abandoned: &AtomicBool,
) -> Result<Arc<[u8]>, Failure> {
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
        tracing::debug!(
            url = request.url.as_str(),
            status = failure.status,
            attempt,
            wait_seconds = delay.as_secs_f64(),
            "retrying a remote request"
        );
        if !wait(delay, abandoned) {
            return Err(failure);
        }
        attempt += 1;
    }
}

/// Sleeps for a duration, in slices, unless the call is abandoned first,
/// and returns whether the whole wait passed.
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

/// Sends one request: its body, or the failure with the response's
/// `Retry-After` value.
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

/// Returns why a transfer failed, with the causes the error carries,
/// such as a refused connection,
/// and without the URL, which the diagnostic names already.
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

/// Returns the start of a response body on one line, whitespace collapsed.
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

    #[test]
    fn a_cancelled_call_stops_waiting_for_its_requests() {
        // A server that accepts and never responds.
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
