//! Remote data: the requests of engine methods that need data from the network,
//! the client that sends them, and the methods suspended until their responses come.
//!
//! A method needs data from the network at one point:
//! - a loading method (`load`, `extend`, `reload`) for its remote imports,
//!   before it registers or stores anything,
//! - [reason](Engine::reason) for the fetches of a bound import, an import with a `where`
//!   clause, at the rule application that fetches.
//!
//! [load](Engine::load) and the other plain methods send the requests through the engine's
//! [Client] (see [set_client](Engine::set_client)), wait for the responses, and complete.
//! The `start_*` methods, such as [start_load](Engine::start_load), send nothing:
//! they return [Progress::Suspended] with the requests, each under a [RequestId],
//! and the method is suspended until [resume](Engine::resume) has the responses under these
//! ids and continues it.
//! The engine holds at most one suspended method.
//! While it is suspended, the methods that would change the program or the stored data are
//! refused (N0493) until it is completed or [discarded](Engine::discard_pending).
//!
//! The types of a request and its outcome are re-exported here:
//! the [Request] itself, its [Response], the [Failure] that prevents one,
//! the [Client] trait, and the [Retry] schedule a failed request is sent again on.

mod pending_request;

pub use self::pending_request::{PendingRequest, Progress, RequestId};
#[cfg(feature = "http")]
pub use nemo_io::remote::BlockingClient;
pub use nemo_io::remote::{
    Client, Failure, PARTIAL_RESPONSE_HEADERS, Request, Response, Retry, partial_response,
};

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    codes,
    diagnostics::EngineError,
    engine::{
        Engine, compile::translation::TranslationState, evaluate::reason::SuspendedReasoning,
        staging::Staged,
    },
    model::Statement,
};

/// The response to a request: its body, or the failure that prevented one.
pub(super) type RemoteResponse = Result<Arc<[u8]>, Failure>;

/// A remote resource a loading method needs: the import that reads it, its name,
/// and the request that fetches it.
///
/// The import is the position of its directive among the statements of the staged program,
/// under which the response is passed to the translation.
/// The name is the one [add_resource](Engine::add_resource) registers the resource's bytes
/// under, and the one diagnostics give.
#[derive(Debug, Clone)]
pub(super) struct ResourceRequest {
    /// The position of the import's directive among the statements translated.
    pub(super) statement: usize,
    /// The resource name.
    pub(super) resource: String,
    /// The request fetching the resource.
    pub(super) request: Request,
}

/// A remote resource a loading method fetched: the request, and the response to it.
#[derive(Debug, Clone)]
pub(super) struct FetchedResource {
    /// The request.
    pub(super) request: ResourceRequest,
    /// The response.
    pub(super) response: RemoteResponse,
}

/// One request of a suspended call: its id, the key the call knows it by, and its
/// response once given.
#[derive(Debug, Clone)]
pub(super) struct RequestSlot<K> {
    /// The id the response is passed under.
    pub(super) id: u64,
    /// The request.
    pub(super) request: Request,
    /// The key the suspended call knows the request by.
    pub(super) key: K,
    /// The response, once given.
    pub(super) response: Option<RemoteResponse>,
}

/// The requests of a suspended call.
#[derive(Debug, Clone)]
pub(super) struct RequestSlots<K>(pub(super) Vec<RequestSlot<K>>);

impl<K> Default for RequestSlots<K> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<K> RequestSlots<K> {
    /// Adds a request under a fresh id.
    pub(super) fn push(&mut self, request: Request, key: K, ids: &mut u64) {
        *ids += 1;
        self.0.push(RequestSlot {
            id: *ids,
            request,
            key,
            response: None,
        });
    }

    /// Returns whether some request has no response yet.
    pub(super) fn has_pending(&self) -> bool {
        self.0.iter().any(|slot| slot.response.is_none())
    }

    /// Returns the ids of the requests without a response yet.
    pub(super) fn pending_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.0
            .iter()
            .filter(|slot| slot.response.is_none())
            .map(|slot| slot.id)
    }

    /// Returns the requests without a response yet.
    pub(super) fn pending(&self) -> impl Iterator<Item = PendingRequest> + '_ {
        self.0
            .iter()
            .filter(|slot| slot.response.is_none())
            .map(|slot| PendingRequest {
                id: RequestId(slot.id),
                request: slot.request.clone(),
            })
    }

    /// Stores the response to a pending request,
    /// and returns `false` if no pending request has the id.
    pub(super) fn respond(&mut self, id: u64, response: RemoteResponse) -> bool {
        match self
            .0
            .iter_mut()
            .find(|slot| slot.id == id && slot.response.is_none())
        {
            Some(slot) => {
                slot.response = Some(response);
                true
            }
            None => false,
        }
    }
}

/// The loading methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LoadKind {
    /// [load](Engine::load).
    Load,
    /// [extend](Engine::extend).
    Extend,
    /// [reload](Engine::reload).
    Reload,
}

/// The state of a loading method after the pipeline stages ran:
/// its prepared translation, and the remote resources it fetched and still needs.
///
/// A loading method prepares its translation, which registers and stores nothing,
/// and is suspended as this until the remote resources the preparation listed come.
/// Once every response is in, it commits from the prepared translation state,
/// passing each response to the import that asked for it.
#[derive(Debug)]
pub(super) struct SuspendedLoad {
    /// The loading method.
    pub(super) kind: LoadKind,
    /// The program, with the revisions the pipeline made of it.
    pub(super) staged: Staged,
    /// The number of rows added through [import_rows](Engine::import_rows) before a first load,
    /// which the load took into its program.
    ///
    /// They are the last statements of its written revision,
    /// and go back to the engine if the suspended call is discarded.
    pub(super) imported_rows: usize,
    /// The translation state the preparation left, once the load was suspended for data,
    /// from which it commits without preparing again.
    pub(super) prepared: Option<TranslationState>,
    /// For a reload, the predicates it rebuilds because a stored import no longer covers what
    /// the new rules read.
    pub(super) unstable: HashSet<String>,
    /// The remote resources fetched so far, by the position of the import that reads each.
    pub(super) fetched: HashMap<usize, FetchedResource>,
    /// The requests sent, with their responses once given.
    pub(super) slots: RequestSlots<ResourceRequest>,
    /// The imports asking for the same request as one in a slot,
    /// which take its response once it comes.
    pub(super) sharing: Vec<ResourceRequest>,
    /// Whether the loading call chose the instant the next run evaluates at,
    /// a choice undone if the call fails or is discarded.
    pub(super) chose_instant: bool,
    /// The index of the first report the loading call made,
    /// from which its reports are withdrawn if it is discarded.
    pub(super) reports_from: usize,
}

impl SuspendedLoad {
    /// Creates the state of a loading method, before any request,
    /// whose reports begin at index `reports_from`.
    pub(super) fn new(kind: LoadKind, staged: Staged, reports_from: usize) -> Self {
        Self {
            kind,
            staged,
            reports_from,
            imported_rows: 0,
            prepared: None,
            unstable: HashSet::new(),
            fetched: HashMap::new(),
            slots: RequestSlots::default(),
            sharing: Vec::new(),
            chose_instant: false,
        }
    }

    /// Returns which of the requests a preparation listed have not been fetched.
    pub(super) fn missing(&self, requests: Vec<ResourceRequest>) -> Vec<ResourceRequest> {
        requests
            .into_iter()
            .filter(|request| !self.fetched.contains_key(&request.statement))
            .collect()
    }

    /// Adds the given requests under fresh ids.
    ///
    /// A request is sent once: an import asking for the same request (URL, form and headers)
    /// as one fetched or pending takes that one's response,
    /// so that imports of one document read the same bytes.
    pub(super) fn add_requests(&mut self, requests: Vec<ResourceRequest>, ids: &mut u64) {
        for request in requests {
            if let Some(response) = self.response_to(&request) {
                self.fetched
                    .insert(request.statement, FetchedResource { request, response });
            } else if self
                .slots
                .0
                .iter()
                .any(|slot| slot.key.request == request.request)
            {
                self.sharing.push(request);
            } else {
                self.slots.push(request.request.clone(), request, ids);
            }
        }
    }

    /// Returns the response fetched for the same request as the given one, if there is one.
    fn response_to(&self, request: &ResourceRequest) -> Option<RemoteResponse> {
        self.fetched
            .values()
            .find(|fetched| fetched.request.request == request.request)
            .map(|fetched| fetched.response.clone())
    }

    /// Moves the responses given so far to the fetched resources,
    /// keeping the requests still without a response pending.
    pub(super) fn take_responses(&mut self) {
        let slots = std::mem::take(&mut self.slots.0);
        for slot in slots {
            match slot.response {
                Some(response) => {
                    self.fetched.insert(
                        slot.key.statement,
                        FetchedResource {
                            request: slot.key,
                            response,
                        },
                    );
                }
                None => self.slots.0.push(slot),
            }
        }
        for request in std::mem::take(&mut self.sharing) {
            match self.response_to(&request) {
                Some(response) => {
                    self.fetched
                        .insert(request.statement, FetchedResource { request, response });
                }
                None => self.sharing.push(request),
            }
        }
    }
}

/// A method suspended for data from the network.
// An engine holds at most one, so the size difference costs nothing worth a box.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(in crate::engine) enum Suspension {
    /// A loading method, its translation prepared, suspended with its staged program.
    Load(SuspendedLoad),
    /// A reasoning run, suspended in the application of a fetch rule.
    Reason(SuspendedReasoning),
}

impl Suspension {
    /// Returns the requests without a response yet.
    pub(in crate::engine) fn pending(&self) -> Vec<PendingRequest> {
        match self {
            Suspension::Load(load) => load.slots.pending().collect(),
            Suspension::Reason(reasoning) => reasoning.pending(),
        }
    }

    /// Returns the ids of the requests without a response yet.
    pub(in crate::engine) fn pending_ids(&self) -> HashSet<u64> {
        match self {
            Suspension::Load(load) => load.slots.pending_ids().collect(),
            Suspension::Reason(reasoning) => reasoning.pending_ids(),
        }
    }

    /// Returns the name of the suspended method.
    fn method(&self) -> &'static str {
        match self {
            Suspension::Load(load) => match load.kind {
                LoadKind::Load => "load",
                LoadKind::Extend => "extend",
                LoadKind::Reload => "reload",
            },
            Suspension::Reason(_) => "reason",
        }
    }

    /// Stores the response to a pending request,
    /// and returns `false` if no pending request has the id.
    pub(in crate::engine) fn respond(&mut self, id: u64, response: RemoteResponse) -> bool {
        match self {
            Suspension::Load(load) => load.slots.respond(id, response),
            Suspension::Reason(reasoning) => reasoning.respond(id, response),
        }
    }

    /// Returns whether some request has no response yet.
    pub(in crate::engine) fn has_pending(&self) -> bool {
        match self {
            Suspension::Load(load) => load.slots.has_pending(),
            Suspension::Reason(reasoning) => reasoning.has_pending(),
        }
    }
}

/// The client an engine sends the requests of its methods through, if it has one.
pub(super) struct ClientSlot(pub(super) Option<Box<dyn Client + Send>>);

impl Default for ClientSlot {
    /// Returns the HTTP client in a build with the `http` feature, and no client otherwise.
    fn default() -> Self {
        #[cfg(feature = "http")]
        return Self(Some(Box::new(BlockingClient::default())));
        #[cfg(not(feature = "http"))]
        Self(None)
    }
}

impl std::fmt::Debug for ClientSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() {
            "ClientSlot(Some(..))"
        } else {
            "ClientSlot(None)"
        })
    }
}

impl Engine {
    /// Refuses the given method, which would change the program or the stored data,
    /// while another method is suspended for data from the network (N0493).
    pub(super) fn refuse_while_suspended(&self, refused: &str) -> Result<(), Diagnostic> {
        let Some(suspension) = &self.suspension else {
            return Ok(());
        };
        Err(EngineError::MethodSuspended {
            refused: refused.to_owned(),
            suspended: suspension.method().to_owned(),
            count: suspension.pending_ids().len().into(),
        }
        .diagnostic())
    }

    /// Returns how a step of a method ended:
    /// done, suspended with the requests of the method it suspended, or failed.
    pub(super) fn progress(&self, outcome: Result<(), Diagnostic>) -> Result<Progress, Diagnostic> {
        match outcome {
            Ok(()) => Ok(Progress::Done),
            Err(problem)
                if problem.code() == codes::AWAITING_REMOTE && self.suspension.is_some() =>
            {
                Ok(Progress::Suspended(self.pending()))
            }
            Err(problem) => Err(problem),
        }
    }

    /// Passes the responses to the suspended method and continues it,
    /// once every request it sent has its response.
    ///
    /// # Panics
    ///
    /// Panics if a response is given under an id that [pending](Engine::pending) does not
    /// list, or two responses under one id.
    pub(super) fn pass_responses(
        &mut self,
        responses: impl IntoIterator<Item = (RequestId, Response)>,
    ) -> Result<Progress, Diagnostic> {
        let responses: Vec<(RequestId, Response)> = responses.into_iter().collect();
        let pending = self
            .suspension
            .as_ref()
            .map_or_else(HashSet::new, Suspension::pending_ids);
        let mut given = HashSet::new();
        for (id, _) in &responses {
            assert!(
                given.insert(id.0),
                "`resume` was given two responses under the id {id}"
            );
            assert!(
                pending.contains(&id.0),
                "`resume` was given a response under the id {id}, which `pending()` does not list"
            );
        }
        let Some(suspension) = self.suspension.as_mut() else {
            return Ok(Progress::Done);
        };
        for (id, response) in responses {
            suspension.respond(id.0, response);
        }
        if suspension.has_pending() {
            return Ok(Progress::Suspended(self.pending()));
        }
        let outcome = self.resume_suspended();
        self.progress(outcome)
    }

    /// Completes a method by sending the requests it is suspended for through the engine's
    /// client, until it is done.
    ///
    /// Without a client, the suspended method is discarded, and this fails with N0491.
    /// If the engine's cancel token stops the wait for the client, this fails with N0492:
    /// a suspended loading method is discarded, having changed nothing,
    /// and a suspended reasoning run stays suspended, for the next `reason()` to continue.
    pub(super) fn complete(
        &mut self,
        progress: Result<Progress, Diagnostic>,
    ) -> Result<(), Diagnostic> {
        let mut progress = progress?;
        loop {
            let Progress::Suspended(pending) = progress else {
                return Ok(());
            };
            let (ids, requests): (Vec<RequestId>, Vec<Request>) = pending
                .into_iter()
                .map(|pending| (pending.id, pending.request))
                .unzip();
            let token = self.cancel.clone();
            let responses = match &self.client.0 {
                Some(client) => client.fetch_all(&requests, &|| token.is_cancelled()),
                None => {
                    let problem = EngineError::AwaitingRemote {
                        count: requests.len().into(),
                        first: requests
                            .first()
                            .map(|request| request.url.clone())
                            .unwrap_or_default(),
                    }
                    .diagnostic();
                    self.abandon();
                    return Err(problem);
                }
            };
            let Some(responses) = responses else {
                if matches!(self.suspension, Some(Suspension::Load(_))) {
                    self.abandon();
                }
                return Err(EngineError::WaitCancelled.diagnostic());
            };
            // A client giving fewer responses than requests fails the rest,
            // rather than leaving them to be sent again forever.
            let mut responses = responses.into_iter();
            let responses: Vec<(RequestId, Response)> = ids
                .into_iter()
                .map(|id| {
                    let response = responses.next().unwrap_or_else(|| {
                        Err(Failure::transport(
                            "the client gave no response to the request",
                        ))
                    });
                    (id, response)
                })
                .collect();
            progress = self.pass_responses(responses)?;
        }
    }

    /// Continues the suspended method with the responses given.
    pub(in crate::engine) fn resume_suspended(&mut self) -> Result<(), Diagnostic> {
        match self.suspension.take() {
            None => Ok(()),
            Some(Suspension::Load(load)) => {
                let chose = load.chose_instant;
                let already = self.pipeline.reports().len();
                let outcome = match load.kind {
                    LoadKind::Load => self.load_staged(load, already),
                    LoadKind::Extend => self.extend_staged(load, already),
                    LoadKind::Reload => self.reload_staged(load, already),
                };
                self.settle_instant(chose, &outcome);
                outcome
            }
            Some(suspension @ Suspension::Reason(_)) => {
                self.suspension = Some(suspension);
                self.reason_step()
            }
        }
    }

    /// Discards the suspended method, if any, dropping its requests and their responses.
    ///
    /// A suspended first load gives the rows added through `import_rows` before it back to
    /// the engine, and a suspended loading call withdraws its reports and undoes its choice of
    /// the instant.
    /// A suspended run ends as a cancelled one does, reporting what its fetches skipped.
    pub(super) fn abandon(&mut self) {
        let load = match self.suspension.take() {
            Some(Suspension::Load(load)) => load,
            Some(Suspension::Reason(_)) => {
                self.end_discarded_run();
                return;
            }
            None => return,
        };
        if load.chose_instant {
            self.instant = None;
        }
        self.pipeline.withdraw_reports(load.reports_from);
        if load.imported_rows > 0 {
            let statements: Vec<Statement> = self
                .pipeline
                .statements(load.staged.written)
                .map(|(_, statement)| statement.clone())
                .collect();
            let first = statements.len() - load.imported_rows;
            self.unloaded_rows
                .extend(statements.into_iter().skip(first));
        }
    }

    /// Records a suspended method, returning the diagnostic (N0491) its step ends with,
    /// which [progress](Self::progress) turns into [Progress::Suspended].
    pub(super) fn suspend(&mut self, suspension: Suspension) -> Diagnostic {
        let pending = suspension.pending();
        self.suspension = Some(suspension);
        let first = pending
            .first()
            .map(|request| request.request.url.clone())
            .unwrap_or_default();
        EngineError::AwaitingRemote {
            count: pending.len().into(),
            first,
        }
        .diagnostic()
    }
}
