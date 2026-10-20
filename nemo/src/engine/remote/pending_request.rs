//! The requests a suspended method waits on, as the engine lists them.

use nemo_io::remote::Request;

/// A request a suspended method waits on.
///
/// Its response is passed to [respond](crate::engine::Engine::respond) under its id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRequest {
    /// The id to pass to [respond](crate::engine::Engine::respond) with the response.
    pub id: u64,
    /// The request.
    pub request: Request,
}
