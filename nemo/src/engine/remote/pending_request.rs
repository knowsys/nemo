//! The requests a suspended method needs responses to, as the engine lists them,
//! and how a step of such a method ends.

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
/// [Progress] shows how a suspended method lists its requests and takes their responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRequest {
    /// The id to pass the response to [resume](crate::engine::Engine::resume) under.
    pub id: RequestId,
    /// The request.
    pub request: Request,
}

/// How a step of a method that may need data from the network ended:
/// the method is done, or it is suspended until these requests have responses.
///
/// The `start_*` methods of the [Engine](crate::engine::Engine) return it,
/// and so does [resume](crate::engine::Engine::resume).
///
/// # Example
///
/// ```
/// use std::sync::Arc;
///
/// use nemo::{
///     engine::{Engine, remote::Progress},
///     model::{FileId, Program},
/// };
///
/// let (program, _) = Program::parse(
///     r#"@import p from csv { resource = "https://example.org/p.csv" } ."#,
///     FileId::new(0),
/// );
/// let mut engine = Engine::new();
/// let Progress::Suspended(pending) = engine.start_load(program)? else {
///     panic!("the import needs its resource");
/// };
/// assert_eq!(pending[0].request.url, "https://example.org/p.csv");
///
/// // Fetched in some other way, the bodies go back under the ids of their requests.
/// let responses = pending
///     .iter()
///     .map(|pending| (pending.id, Ok(Arc::from(&b"1\n"[..]))));
/// assert_eq!(engine.resume(responses)?, Progress::Done);
/// assert_eq!(engine.count("p")?, 1);
/// # Ok::<(), nemo::diagnostics::Diagnostic>(())
/// ```
#[must_use = "a suspended method continues only once `resume` has the responses to its requests"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// The method completed.
    Done,
    /// The method is suspended until these requests have responses.
    Suspended(Vec<PendingRequest>),
}
