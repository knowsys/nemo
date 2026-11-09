//! The requests that a suspended method needs responses to, as the engine lists them.

use nemo_io::remote::Request;

/// The id of a request that a suspended method needs a response to.
///
/// [resume](crate::engine::Engine::resume) accepts the response under this id.
/// Ids are unique within an engine.
///
/// # Example
///
/// ```
/// use nemo::engine::remote::RequestId;
///
/// let id = RequestId::from(7);
/// assert_eq!(id.value(), 7);
/// assert_eq!(RequestId::from(id.value()), id);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub(crate) u64);

impl RequestId {
    /// Returns the id as a number, which can be passed where the type cannot,
    /// and turned back into the id through [From].
    pub fn value(self) -> u64 {
        self.0
    }
}

impl From<u64> for RequestId {
    /// Returns the id that [value](RequestId::value) turned into this number.
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A request that a suspended method needs a response to.
///
/// The example of [Progress](super::Progress) shows how a suspended method lists its requests
/// and accepts their responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRequest {
    /// The id under which to pass the response to [resume](crate::engine::Engine::resume).
    pub id: RequestId,
    /// The request.
    pub request: Request,
}
