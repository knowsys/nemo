//! Suspended calls: engine methods waiting for data from the network.
//!
//! The engine does no network IO itself.
//! A method that needs data from the network suspends at the point it needs it:
//! - a loading method (`load`, `extend`, `reload`) for its remote imports,
//! - [reason](Engine::reason) for the fetches of a bound import, an import with a `where`
//!   clause.
//!
//! The method returns N0491 and keeps its state.
//! [pending](Engine::pending) lists its requests as [PendingRequest]s,
//! [respond](Engine::respond) passes in each response,
//! and [resume](Engine::resume) continues the method.
//! The engine holds at most one suspended call,
//! and a method that changes the stored data or the program abandons it.
//!
//! The types of a request and its outcome are re-exported here:
//! the [Request] itself, the [Failure] that prevents a response,
//! and the [Retry] schedule a failed request is sent again on.

mod pending_request;

pub use self::pending_request::PendingRequest;
pub use nemo_io::remote::{Failure, Request, Retry};

use std::{collections::HashMap, sync::Arc};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::EngineError,
    engine::{Engine, evaluate::reason::SuspendedReasoning, staging::Staged},
    model::Statement,
};

/// The response to a request: its body, or the failure that prevented one.
pub(super) type RemoteResponse = Result<Arc<[u8]>, Failure>;

/// A remote resource a loading method needs: its name and the request that fetches it.
///
/// The name is the one [add_resource](Engine::add_resource) registers the resource's bytes
/// under.
#[derive(Debug, Clone)]
pub(super) struct ResourceRequest {
    /// The resource name.
    pub(super) resource: String,
    /// The request fetching the resource.
    pub(super) request: Request,
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

    /// Returns whether some request waits for its response.
    #[cfg_attr(not(feature = "sparql"), expect(dead_code))]
    pub(super) fn has_pending(&self) -> bool {
        self.0.iter().any(|slot| slot.response.is_none())
    }

    /// Returns the requests waiting for their responses.
    pub(super) fn pending(&self) -> impl Iterator<Item = PendingRequest> + '_ {
        self.0
            .iter()
            .filter(|slot| slot.response.is_none())
            .map(|slot| PendingRequest {
                id: slot.id,
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
/// what its translation needs, and the remote resources it requested so far.
///
/// A loading method that suspends undoes its translation and is kept as this,
/// to translate again once the responses are passed in.
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
    /// and go back to the engine if the suspended call is abandoned.
    pub(super) imported_rows: usize,
    /// Per remote resource, its request and response.
    pub(super) slots: RequestSlots<String>,
}

impl SuspendedLoad {
    /// Creates the state of a loading method, before any request.
    pub(super) fn new(kind: LoadKind, staged: Staged) -> Self {
        Self {
            kind,
            staged,
            imported_rows: 0,
            slots: RequestSlots::default(),
        }
    }

    /// Returns the responses given so far, by resource name.
    pub(super) fn responses(&self) -> HashMap<String, RemoteResponse> {
        self.slots
            .0
            .iter()
            .filter_map(|slot| Some((slot.key.clone(), slot.response.clone()?)))
            .collect()
    }

    /// Adds the requests for the resources a translation found missing,
    /// keeping the ids of those requested before.
    pub(super) fn ask(&mut self, requests: Vec<ResourceRequest>, ids: &mut u64) {
        for ResourceRequest { resource, request } in requests {
            if !self.slots.0.iter().any(|slot| slot.key == resource) {
                self.slots.push(request, resource, ids);
            }
        }
    }
}

/// A method suspended for data from the network.
#[derive(Debug)]
pub(in crate::engine) enum Suspension {
    /// A loading method, its translation undone, waiting with its staged program.
    Load(SuspendedLoad),
    /// A reasoning run, waiting in the application of a fetch rule.
    Reason(SuspendedReasoning),
}

impl Suspension {
    /// Returns the requests waiting for their responses.
    pub(in crate::engine) fn pending(&self) -> Vec<PendingRequest> {
        match self {
            Suspension::Load(load) => load.slots.pending().collect(),
            Suspension::Reason(reasoning) => reasoning.pending(),
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
}

impl Engine {
    /// Continues the suspended method with the responses given.
    pub(in crate::engine) fn resume_suspended(&mut self) -> Result<(), Diagnostic> {
        match self.suspension.take() {
            None => Ok(()),
            Some(Suspension::Load(load)) => {
                let already = self.pipeline.reports().len();
                match load.kind {
                    LoadKind::Load => self.load_staged(load, already),
                    LoadKind::Extend => self.extend_staged(load, already),
                    LoadKind::Reload => self.reload_staged(load, already),
                }
            }
            Some(suspension @ Suspension::Reason(_)) => {
                self.suspension = Some(suspension);
                self.reason()
            }
        }
    }

    /// Abandons the suspended call, if any, dropping its requests and their responses.
    ///
    /// A suspended first load gives the rows added through `import_rows` before it back to
    /// the engine.
    pub(super) fn abandon(&mut self) {
        let Some(Suspension::Load(load)) = self.suspension.take() else {
            return;
        };
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

    /// Records a suspended call, returning the diagnostic (N0491) its method ends with.
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
