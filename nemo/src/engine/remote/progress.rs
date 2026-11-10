//! How a call of a method that may need data from the network ends.

use crate::engine::remote::PendingRequest;

/// How a call of a method that may need data from the network ended.
///
/// Either the method is done, or it is suspended until the listed requests have responses.
/// The `start_*` methods of the [Engine](crate::engine::Engine) return it,
/// and so does [resume](crate::engine::Engine::resume).
///
/// # Example
///
/// ```
/// use std::sync::Arc;
///
/// use nemo::{engine::{Engine, remote::Progress}, model::Program};
///
/// let (program, _) = Program::parse(
///     r#"@import p from csv { resource = "https://example.org/p.csv" } ."#,
/// );
/// let mut engine = Engine::new();
/// let Progress::Suspended(pending) = engine.start_load(program)? else {
///     panic!("the import needs its resource");
/// };
/// assert_eq!(pending[0].request.url, "https://example.org/p.csv");
///
/// // The bodies, fetched in some other way, are passed back under the ids of their requests.
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
