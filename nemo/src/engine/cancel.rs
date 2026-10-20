//! Cancellation of running engine calls.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// A cloneable, thread-safe handle that cancels the engine's running call,
/// obtained from [cancel_token](crate::engine::Engine::cancel_token).
///
/// [reason](crate::engine::Engine::reason) checks it between rule applications.
/// A cancelled run stops there with code N0207.
/// The tables stay consistent and every rule's progress is recorded,
/// so calling `reason()` again continues toward the fixpoint.
/// An application under way runs to its end first.
/// Each call that checks the token clears it as it starts,
/// so a token cancels the call that is running, not future ones.
///
/// With the `http` feature, `nemo::driver::HttpDriver` also stops waiting for HTTP responses
/// when the token is cancelled.
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    /// Requests cancellation, which is safe from any thread.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// Clears the request, as every call that checks the token does when it starts.
    pub(crate) fn clear(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}
