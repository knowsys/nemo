//! The live dashboard: a small HTTP server that shows a run's telemetry.
//!
//! It listens on the loopback interface, with one thread that accepts connections
//! and one thread per connection.
//! It serves the [report page](nemo::telemetry::ReportDocument::page) in live mode at `/`,
//! and at `/events` a stream of server-sent events that sends the current report document
//! whenever the [Recorder] changes.
//! The engine's thread only writes to the recorder,
//! and the serving threads build and send the documents.

use std::{
    io::{BufRead, BufReader, ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex, PoisonError},
    thread,
    time::Duration,
};

use nemo::telemetry::{Recorder, ReportDocument, RunFacts};

/// How often the event stream checks for changes, which limits how often it sends a document.
const POLL: Duration = Duration::from_millis(250);

/// The number of checks without a change after which the event stream sends a keep-alive
/// comment.
const KEEPALIVE_ROUNDS: u32 = 60;

/// The read and write timeout of a connection.
///
/// A client that sends no request, or accepts no write, for this long is treated as gone,
/// so that a silent socket does not hold its thread for as long as it stays open.
const PATIENCE: Duration = Duration::from_secs(10);

/// A live dashboard, served by background threads.
#[derive(Debug)]
pub(crate) struct Dashboard {
    /// The bound port, which differs from the requested one if that was 0.
    port: u16,
    /// The state shared with the serving threads.
    shared: Arc<Shared>,
}

/// The state that the serving threads share.
#[derive(Debug)]
struct Shared {
    /// The title of the report document, which lists the run's program paths.
    title: String,
    /// The recorder of the run's telemetry.
    recorder: Arc<Recorder>,
    /// The run facts, present once the run has finished.
    run: Mutex<Option<RunFacts>>,
}

impl Dashboard {
    /// Binds `port` on the loopback interface, or a free port for 0,
    /// and starts serving the telemetry of `recorder`.
    ///
    /// `title` is the title of the report document.
    /// Fails if the port cannot be bound.
    pub(crate) fn start(
        port: u16,
        title: String,
        recorder: Arc<Recorder>,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        let port = listener.local_addr()?.port();
        let shared = Arc::new(Shared {
            title,
            recorder,
            run: Mutex::new(None),
        });
        let accepting = Arc::clone(&shared);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let shared = Arc::clone(&accepting);
                thread::spawn(move || serve(stream, &shared));
            }
        });
        Ok(Self { port, shared })
    }

    /// Returns the address the dashboard is served at.
    pub(crate) fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Stores the facts of the finished run, which the documents sent from then on include.
    pub(crate) fn finish(&self, run: RunFacts) {
        *lock(&self.shared.run) = Some(run);
    }
}

/// Locks the run facts.
///
/// A panic in another thread that held the lock does not stop the dashboard.
fn lock(run: &Mutex<Option<RunFacts>>) -> std::sync::MutexGuard<'_, Option<RunFacts>> {
    run.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Builds the current report document from the shared state, and returns what `render`
/// makes of it.
fn current<T>(shared: &Shared, render: impl FnOnce(&ReportDocument<'_>) -> T) -> T {
    let run = lock(&shared.run);
    render(&ReportDocument {
        title: &shared.title,
        run: run.as_ref(),
        telemetry: Some(&shared.recorder),
    })
}

/// Serves one connection.
///
/// It responds by the request's path: the page at `/`, the event stream at `/events`,
/// and `404 Not Found` elsewhere.
/// An I/O error means that the client has gone, so it ends the connection without a report.
fn serve(stream: TcpStream, shared: &Shared) {
    if stream.set_read_timeout(Some(PATIENCE)).is_err()
        || stream.set_write_timeout(Some(PATIENCE)).is_err()
    {
        return;
    }
    let Ok(peer) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(peer);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    // No route uses the request headers, so they are read and dropped.
    loop {
        let mut header = String::new();
        match reader.read_line(&mut header) {
            Ok(_) if header.trim().is_empty() => break,
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }

    match path.as_str() {
        "/" => {
            let page = current(shared, |document| document.page(true));
            let _ = respond(stream, "text/html; charset=utf-8", &page);
        }
        "/events" => stream_events(stream, reader.into_inner(), shared),
        _ => {
            let _ = respond_status(stream, "404 Not Found", "text/plain", "not found");
        }
    }
}

/// Writes a complete `200 OK` response with `body`, and closes the connection.
fn respond(stream: TcpStream, content_type: &str, body: &str) -> std::io::Result<()> {
    respond_status(stream, "200 OK", content_type, body)
}

/// Writes a complete response with `status` and `body`, and closes the connection.
fn respond_status(
    mut stream: TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()
}

/// Sends report documents as server-sent events until the client leaves.
///
/// The first document is sent at once, and then a new one whenever the telemetry or the run
/// facts have changed, which is checked every [POLL].
/// `peer` is a second handle of the connection, which is read between the checks,
/// so that a client that hangs up is noticed within one check rather than at the next write.
fn stream_events(mut stream: TcpStream, mut peer: TcpStream, shared: &Shared) {
    if write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\
         Connection: keep-alive\r\n\r\n"
    )
    .is_err()
        || peer.set_read_timeout(Some(POLL)).is_err()
    {
        return;
    }

    let mut sent = None;
    let mut quiet_rounds = 0;
    loop {
        // The version pairs the recorder's counter with whether the run facts are stored,
        // so that the document with the run facts is always sent.
        let version = (shared.recorder.version(), lock(&shared.run).is_some());
        if sent != Some(version) {
            sent = Some(version);
            quiet_rounds = 0;
            let document = current(shared, |document| document.json());
            if write!(stream, "data: {document}\n\n")
                .and_then(|()| stream.flush())
                .is_err()
            {
                return;
            }
        } else {
            quiet_rounds += 1;
            if quiet_rounds >= KEEPALIVE_ROUNDS {
                quiet_rounds = 0;
                if write!(stream, ": keep-alive\n\n")
                    .and_then(|()| stream.flush())
                    .is_err()
                {
                    return;
                }
            }
        }
        // The wait between checks is a read. The client of an event stream sends nothing
        // more, so the read times out after `POLL` while the client is connected,
        // and returns at once when it has left.
        match peer.read(&mut [0u8; 64]) {
            Ok(0) => return,
            Ok(_) => {}
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Read;

    use nemo::telemetry::{EngineEvent, EventSink};

    /// Sends a `GET` request for `path` to the dashboard on `url_port`, and returns what it
    /// reads until that contains `until`, the connection ends, or the read times out.
    fn fetch(url_port: u16, path: &str, until: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", url_port)).expect("the server accepts");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("timeouts are settable");
        write!(stream, "GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .expect("the request writes");
        let mut collected = String::new();
        let mut buffer = [0u8; 4096];
        while !collected.contains(until) {
            let Ok(read) = stream.read(&mut buffer) else {
                break;
            };
            if read == 0 {
                break;
            }
            collected.push_str(&String::from_utf8_lossy(&buffer[..read]));
        }
        collected
    }

    /// The dashboard serves the live page at `/`, documents with the run's facts at
    /// `/events`, and `404 Not Found` elsewhere.
    #[test]
    fn the_dashboard_serves_the_page_and_the_stream() {
        let recorder = Arc::new(Recorder::new());
        let dashboard = Dashboard::start(0, "test run".to_string(), Arc::clone(&recorder))
            .expect("a free port binds");
        let port: u16 = dashboard
            .url()
            .rsplit(':')
            .next()
            .expect("the url carries a port")
            .parse()
            .expect("as a number");

        let page = fetch(port, "/", "</html>");
        assert!(page.contains("200 OK"), "{page}");
        assert!(page.contains("const LIVE = true;"), "the page is live");
        assert!(page.contains("test run"), "the title is embedded");

        recorder.record(&EngineEvent::Finished {
            fixpoint: true,
            derived: 42,
            memory: 0,
        });
        dashboard.finish(RunFacts::default());
        let events = fetch(port, "/events", "\"derived\":42");
        assert!(events.contains("text/event-stream"), "{events}");
        assert!(events.contains("data: {\"title\":\"test run\""), "{events}");

        let missing = fetch(port, "/nowhere", "not found");
        assert!(missing.contains("404"), "{missing}");
    }
}
