//! Remote data: the requests of the engine methods that need data from the network,
//! the client that sends them, and the methods suspended until their responses arrive.
//!
//! A method needs data from the network at one point:
//! - A loading method ([load](Engine::load), [extend](Engine::extend),
//!   [reload](Engine::reload)) needs it for its remote imports,
//!   before it registers or stores anything.
//! - [reason](Engine::reason) needs it for the fetches of a bound import, which is an import
//!   with a `where` clause, at the rule application that fetches.
//!
//! The *plain* methods, [load](Engine::load), [extend](Engine::extend),
//! [reload](Engine::reload) and [reason](Engine::reason), send the requests through the
//! engine's [Client] (see [set_client](Engine::set_client)), wait for the responses, and
//! complete.
//! The `start_*` methods, such as [start_load](Engine::start_load), send nothing.
//! They return [Progress::Suspended] with the requests, each under a [RequestId],
//! and the method is then *suspended*.
//! [resume](Engine::resume) accepts the responses under these ids and continues the method.
//! The engine has at most one suspended method.
//! Until it is completed or [discarded](Engine::discard_pending),
//! the methods that would change the program or the stored data are refused.
//!
//! The types of a request and its outcome are re-exported here:
//! the [Request] itself, its [Response], the [Failure] that prevents one,
//! the [Client] trait, and the [Retry] schedule on which a failed request is sent again.

mod pending_request;
mod progress;

pub use self::{
    pending_request::{PendingRequest, RequestId},
    progress::Progress,
};
#[cfg(feature = "http")]
pub use nemo_io::remote::BlockingClient;
pub use nemo_io::remote::{Client, Failure, Request, Response, Retry};

use std::collections::{HashMap, HashSet};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::EngineError,
    engine::{
        Engine, compile::translation::TranslationState, evaluate::reason::SuspendedReasoning,
        staging::Staged,
    },
};

/// A remote resource that a loading method needs: the import that reads it, its name,
/// and the request that fetches it.
#[derive(Debug, Clone)]
pub(super) struct ResourceRequest {
    /// The position of the import's directive among the statements translated.
    ///
    /// The response is passed to the translation under this position.
    pub(super) statement: usize,
    /// The resource name, which diagnostics give.
    ///
    /// It is the name under which [add_resource](Engine::add_resource) would register the
    /// resource's bytes, which are then read without a request.
    pub(super) resource: String,
    /// The request that fetches the resource.
    pub(super) request: Request,
}

/// A remote resource that a loading method fetched: the request, and the response to it.
#[derive(Debug, Clone)]
pub(super) struct FetchedResource {
    /// The request.
    pub(super) request: ResourceRequest,
    /// The response.
    pub(super) response: Response,
}

/// One request of a suspended method: its id, the key by which the method knows it,
/// and its response once given.
#[derive(Debug, Clone)]
pub(super) struct RequestSlot<K> {
    /// The id under which the response is passed.
    pub(super) id: u64,
    /// The request.
    pub(super) request: Request,
    /// The key by which the suspended method knows the request.
    pub(super) key: K,
    /// The response, once given.
    pub(super) response: Option<Response>,
}

/// The requests of a suspended method.
#[derive(Debug, Clone)]
pub(super) struct RequestSlots<K>(pub(super) Vec<RequestSlot<K>>);

impl<K> Default for RequestSlots<K> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<K> RequestSlots<K> {
    /// Adds a request under a fresh id.
    ///
    /// `ids` is the last id given, which this advances.
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

    /// Stores the response to the pending request with the given id, if there is one.
    pub(super) fn respond(&mut self, id: u64, response: Response) {
        if let Some(slot) = self
            .0
            .iter_mut()
            .find(|slot| slot.id == id && slot.response.is_none())
        {
            slot.response = Some(response);
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
/// A loading method first prepares its translation, which registers and stores nothing,
/// and lists the remote resources it needs.
/// While the method is suspended for them, it is kept as this state.
/// Once every response is in, it commits from the prepared translation state,
/// and passes each response to the import that asked for it.
#[derive(Debug)]
pub(super) struct SuspendedLoad {
    /// The loading method.
    pub(super) kind: LoadKind,
    /// The program, with the revisions the pipeline made of it.
    pub(super) staged: Staged,
    /// The translation state that the preparation left, once the method was suspended,
    /// from which it commits without preparing again.
    pub(super) prepared: Option<TranslationState>,
    /// For a reload, the predicates it rebuilds because a stored import no longer covers what
    /// the new rules read.
    pub(super) unstable: HashSet<String>,
    /// The remote resources fetched so far, by the position of the import that reads each.
    pub(super) fetched: HashMap<usize, FetchedResource>,
    /// The requests sent, with their responses once given.
    pub(super) slots: RequestSlots<ResourceRequest>,
    /// The imports that ask for the same request as one in a slot,
    /// and receive its response once it arrives.
    pub(super) sharing: Vec<ResourceRequest>,
    /// Whether the method chose the evaluation context of the next run.
    ///
    /// The choice is undone if the method fails or is discarded.
    pub(super) chose_context: bool,
    /// The index of the method's first report.
    ///
    /// If the method is discarded, its reports from this index on are withdrawn.
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
            prepared: None,
            unstable: HashSet::new(),
            fetched: HashMap::new(),
            slots: RequestSlots::default(),
            sharing: Vec::new(),
            chose_context: false,
        }
    }

    /// Returns the requests, among those a preparation listed, whose resources have not been
    /// fetched.
    pub(super) fn missing(&self, requests: Vec<ResourceRequest>) -> Vec<ResourceRequest> {
        requests
            .into_iter()
            .filter(|request| !self.fetched.contains_key(&request.statement))
            .collect()
    }

    /// Adds the given requests under fresh ids.
    ///
    /// Each distinct request is sent once.
    /// An import that asks for the same request (URL, form and headers) as one that was
    /// fetched or is pending receives that request's response,
    /// so that imports of one document read the same bytes.
    /// `ids` is the last id given, which this advances.
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
    fn response_to(&self, request: &ResourceRequest) -> Option<Response> {
        self.fetched
            .values()
            .find(|fetched| fetched.request.request == request.request)
            .map(|fetched| fetched.response.clone())
    }

    /// Moves the responses given so far to the fetched resources,
    /// and keeps the requests without a response pending.
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
// An engine has at most one, so the size difference costs nothing worth a box.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(in crate::engine) enum Suspension {
    /// A suspended loading method, with its staged program and prepared translation.
    Load(SuspendedLoad),
    /// A run suspended in the rule application of a bound import.
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

    /// Stores the response to the pending request with the given id, if there is one.
    pub(in crate::engine) fn respond(&mut self, id: u64, response: Response) {
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

/// The client through which an engine sends the requests of its methods, if it has one.
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
    /// Fails if a method is suspended for data from the network.
    ///
    /// `refused` names the method that calls this, which would change the program or the
    /// stored data.
    pub(super) fn refuse_while_suspended(&self, refused: &str) -> Result<(), Diagnostic> {
        let Some(suspension) = &self.suspension else {
            return Ok(());
        };
        Err(EngineError::MethodSuspended {
            refused: refused.to_owned(),
            suspended: suspension.method().to_owned(),
            count: suspension.pending().len().into(),
        }
        .diagnostic())
    }

    /// Passes the responses to the suspended method,
    /// and continues the method once every request it sent has a response.
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
        let pending: HashSet<RequestId> = self
            .suspension
            .as_ref()
            .map_or_else(Vec::new, Suspension::pending)
            .into_iter()
            .map(|pending| pending.id)
            .collect();
        let mut given = HashSet::new();
        for (id, _) in &responses {
            assert!(
                given.insert(*id),
                "`resume` was given two responses under the id {id}"
            );
            assert!(
                pending.contains(id),
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
        self.resume_suspended()
    }

    /// Completes a method by sending the requests it is suspended for through the engine's
    /// client, until it is done.
    ///
    /// `progress` is what the `start_*` form of the method returned.
    /// Without a client, the suspended method is discarded, and this fails.
    /// If the engine's cancel token stops the wait for the client, this fails as cancelled.
    /// A suspended loading method is then discarded, having changed nothing,
    /// and a suspended run stays suspended, for the next [reason](Engine::reason) to continue.
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
                    let problem = EngineError::NoClient {
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
            // If the client gives fewer responses than requests, the rest fail,
            // rather than be sent again forever.
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

    /// Continues the suspended method with the responses given, if a method is suspended.
    pub(in crate::engine) fn resume_suspended(&mut self) -> Result<Progress, Diagnostic> {
        match self.suspension.take() {
            None => Ok(Progress::Done),
            Some(Suspension::Load(load)) => {
                let chose = load.chose_context;
                let already = self.pipeline.reports().len();
                let outcome = match load.kind {
                    LoadKind::Load => self.load_staged(load, already),
                    LoadKind::Extend => self.extend_staged(load, already),
                    LoadKind::Reload => self.reload_staged(load, already),
                };
                self.settle_context(chose, &outcome);
                outcome
            }
            Some(suspension @ Suspension::Reason(_)) => {
                self.suspension = Some(suspension);
                self.reason_until_suspended()
            }
        }
    }

    /// Discards the suspended method, if any, with its requests and their responses.
    ///
    /// A suspended loading method withdraws its reports and undoes its choice of the
    /// evaluation context.
    /// A suspended run ends as a cancelled one does, and reports what its fetches skipped.
    pub(super) fn abandon(&mut self) {
        let load = match self.suspension.take() {
            Some(Suspension::Load(load)) => load,
            Some(Suspension::Reason(_)) => {
                self.end_discarded_run();
                return;
            }
            None => return,
        };
        if load.chose_context {
            self.next_context = None;
        }
        self.pipeline.withdraw_reports(load.reports_from);
    }

    /// Records a suspended method, and returns [Progress::Suspended] with the requests it is
    /// suspended for.
    pub(super) fn suspend(&mut self, suspension: Suspension) -> Progress {
        self.suspension = Some(suspension);
        Progress::Suspended(self.pending())
    }
}
