//! Running engine methods that need data from the network, over HTTP.
//!
//! A method that needs data from the network suspends (see [pending](Engine::pending)).
//! The [HttpDriver] fetches the pending requests with the blocking HTTP client,
//! passes each response to [respond](Engine::respond),
//! and calls [resume](Engine::resume), until the method completes.

use nemo_io::remote::{BlockingClient, Request};
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::EngineError,
    engine::{Engine, remote::Retry},
};

/// Runs engine methods to completion, fetching the data they need over HTTP.
///
/// If a method suspends for data from the network (see [pending](Engine::pending)),
/// the driver sends the pending requests together over one pooled client.
/// It retries failed requests on its [Retry] schedule,
/// passes the responses to [respond](Engine::respond),
/// and [resumes](Engine::resume) the method, until it completes or fails.
///
/// ```no_run
/// use nemo::{driver::HttpDriver, engine::Engine};
/// # fn run(program: nemo::model::Program) -> Result<(), nemo::diagnostics::Diagnostic> {
/// let mut engine = Engine::new();
/// let driver = HttpDriver::new();
/// driver.drive(&mut engine, |engine| engine.load(program))?;
/// driver.drive(&mut engine, Engine::reason)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct HttpDriver {
    /// The client the requests go through.
    client: BlockingClient,
}

impl HttpDriver {
    /// Creates a driver retrying on the default schedule.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a driver retrying on the given schedule.
    pub fn with_retry(retry: Retry) -> Self {
        Self {
            client: BlockingClient::new(retry),
        }
    }

    /// Calls a method on the engine and runs it to completion.
    ///
    /// While the method is suspended, the driver fetches the pending requests,
    /// passes the responses to [respond](Engine::respond), and calls [resume](Engine::resume).
    ///
    /// The engine's [cancel token](Engine::cancel_token) also cancels the wait for responses.
    /// `drive` then returns N0492 and the requests stay pending,
    /// so a later `drive` sends them again.
    /// As for every engine call, a cancellation requested before `drive` starts is cleared when
    /// it starts.
    ///
    /// # Errors
    ///
    /// Returns the error the method ends with:
    /// its own failure (a cancelled reasoning run is N0207),
    /// a request that still failed after its retries,
    /// or N0492 if the wait for responses was cancelled.
    pub fn drive(
        &self,
        engine: &mut Engine,
        call: impl FnOnce(&mut Engine) -> Result<(), Diagnostic>,
    ) -> Result<(), Diagnostic> {
        let token = engine.cancel_token();
        token.clear();
        let mut outcome = call(engine);
        loop {
            // A method that completed or failed leaves nothing pending.
            let pending = engine.pending();
            if pending.is_empty() {
                return outcome;
            }
            let requests: Vec<Request> = pending
                .iter()
                .map(|pending| pending.request.clone())
                .collect();
            tracing::debug!(requests = requests.len(), "sending remote requests");
            let Some(responses) = self.client.fetch_all(&requests, &|| token.is_cancelled()) else {
                return Err(EngineError::WaitCancelled.diagnostic());
            };
            for (pending, response) in pending.into_iter().zip(responses) {
                engine.respond(pending.id, response);
            }
            outcome = engine.resume();
        }
    }
}
