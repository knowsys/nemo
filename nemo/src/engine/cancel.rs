//! Cancellation of a running engine method.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// A handle that cancels the method an engine is running.
///
/// It can be cloned and used from any thread.
/// [cancel_token](crate::engine::Engine::cancel_token) returns one.
///
/// [reason](crate::engine::Engine::reason) checks it between rule applications.
/// A rule application under way runs to its end first.
/// A cancelled run then stops and returns a cancellation error.
/// The tables stay consistent and every rule's progress is recorded,
/// so calling [reason](crate::engine::Engine::reason) again continues toward the fixpoint.
/// Each method that checks the token clears it when it starts,
/// so a token cancels the method that is running, not later ones.
///
/// The methods that send requests through the engine's [client](crate::engine::remote::Client)
/// pass it the token, and stop waiting for responses when it is cancelled.
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    /// Requests cancellation.
    ///
    /// It can be called from any thread.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// Clears the request.
    ///
    /// Every method that checks the token calls this when it starts.
    pub(crate) fn clear(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}
