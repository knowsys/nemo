//! The live dashboard: a small HTTP/SSE server over a run's telemetry.
//!
//! One thread accepts connections on the loopback interface,
//! and each client gets a thread of its own.
//! `GET /` serves the [report page](nemo::telemetry::ReportDocument::page) in live mode,
//! seeded with the current document.
//! `GET /events` is a server-sent-event stream that pushes a fresh document whenever the
//! [Recorder] has changed, throttled to a few pushes per second.
//! The engine's thread only ever touches the recorder,
//! and serialization and writing stay here.

use std::{
    io::{BufRead, BufReader, ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex, PoisonError},
    thread,
    time::Duration,
};

use nemo::telemetry::{Recorder, ReportDocument, RunFacts};

/// How often the event stream checks for changes.
const POLL: Duration = Duration::from_millis(250);

/// Poll rounds between keep-alive comments on a quiet stream.
const KEEPALIVE_ROUNDS: u32 = 60;

/// How long a connection may take to send its request, and a write may
/// take to be accepted, before the client counts as gone: a silent
/// socket frees its thread rather than holding it for as long as the
/// socket stays open.
const PATIENCE: Duration = Duration::from_secs(10);

/// The serving end of a live dashboard.
#[derive(Debug)]
pub(crate) struct Dashboard {
    /// The port actually bound.
    port: u16,
    /// The state shared with the serving threads.
    shared: Arc<Shared>,
}

/// What the serving threads read.
#[derive(Debug)]
struct Shared {
    /// The document title, the run's program names.
    title: String,
    /// The telemetry the engine writes into.
    recorder: Arc<Recorder>,
    /// The run facts, present once the run has finished.
    run: Mutex<Option<RunFacts>>,
}

impl Dashboard {
    /// Binds the loopback port (0 picks a free one) and starts serving
    /// the given recorder's telemetry.
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

    /// Hands over the finished run's facts, which the documents pushed from here on carry.
    pub(crate) fn finish(&self, run: RunFacts) {
        *lock(&self.shared.run) = Some(run);
    }
}

/// Returns the run facts under their lock.
///
/// A poisoning panic elsewhere does not take the dashboard down.
fn lock(run: &Mutex<Option<RunFacts>>) -> std::sync::MutexGuard<'_, Option<RunFacts>> {
    run.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Renders the current document over the shared state.
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
/// Errors mean the client left, which is theirs to do.
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
    // The request headers matter to no route; drain them.
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

/// Writes one complete `200 OK` response and closes.
fn respond(stream: TcpStream, content_type: &str, body: &str) -> std::io::Result<()> {
    respond_status(stream, "200 OK", content_type, body)
}

/// Writes one complete response and closes.
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

/// Streams documents as server-sent events until the client leaves:
/// one immediately, then one whenever the telemetry has changed. The
/// client's side of the connection (`peer`) is watched between pushes,
/// so a client that hangs up is noticed within a poll round rather
/// than at the next write.
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
        // The version pairs the recorder's counter with whether the run
        // facts have landed, so the closing push is never missed.
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
        // The poll interval is spent listening: an event stream's
        // client sends nothing more, so the read ends with the round's
        // timeout while the client is there, and at once when it left.
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

    /// One request against a served path, read to the first body bytes.
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
