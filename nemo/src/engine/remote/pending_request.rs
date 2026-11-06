//! The requests a waiting method needs answered, as the engine lists them,
//! and how a step of such a method ends.

use nemo_io::remote::Request;

/// The id of a request a waiting method needs answered,
/// under which [resume](crate::engine::Engine::resume) takes its response.
///
/// Ids are unique within an engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub(crate) u64);

impl RequestId {
    /// Returns the id as a number, as hosts that pass it on to other languages need it.
    pub fn value(self) -> u64 {
        self.0
    }
}

impl From<u64> for RequestId {
    /// Takes back an id that [value](RequestId::value) gave out.
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A request a waiting method needs answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRequest {
    /// The id to pass the response to [resume](crate::engine::Engine::resume) under.
    pub id: RequestId,
    /// The request.
    pub request: Request,
}

/// How a step of a method that may need data from the network ended:
/// the method is done, or it waits for the responses to these requests.
///
/// The `start_*` methods of the [Engine](crate::engine::Engine) return it,
/// and so does [resume](crate::engine::Engine::resume).
#[must_use = "a waiting method continues only once its requests are answered with `resume`"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// The method completed.
    Done,
    /// The method waits for the responses to these requests.
    Waiting(Vec<PendingRequest>),
}
