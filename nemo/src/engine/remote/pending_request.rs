//! The requests a suspended method needs responses to, as the engine lists them.

use nemo_io::remote::Request;

/// The id of a request a suspended method needs a response to,
/// under which [resume](crate::engine::Engine::resume) takes its response.
///
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
    /// Returns the id as a number, for bindings that pass it on to other languages.
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

/// A request a suspended method needs a response to.
///
/// [Progress](super::Progress) shows how a suspended method lists its requests
/// and takes their responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRequest {
    /// The id to pass the response to [resume](crate::engine::Engine::resume) under.
    pub id: RequestId,
    /// The request.
    pub request: Request,
}
