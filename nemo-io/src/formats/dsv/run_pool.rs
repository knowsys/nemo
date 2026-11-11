//! A pool of runs kept for reuse.

use std::sync::{Mutex, PoisonError};

use crate::dsv::Parsed;

/// A pool of runs that were interned and wait to be filled again.
///
/// Runs are filled on one thread and interned on another.
/// Handing each interned run back to the pool keeps its buffers,
/// so after the first few runs the parse allocates nothing per run.
/// This also keeps the heap from fragmenting
/// into thousands of buffers allocated on one thread and freed on another.
#[derive(Debug, Default)]
pub(crate) struct RunPool {
    /// The runs waiting to be reused.
    spare: Mutex<Vec<Parsed>>,
}

impl RunPool {
    /// Returns an empty run of the given arity: a spare one if there is one, and a new one
    /// otherwise.
    pub(crate) fn take(&self, arity: usize) -> Parsed {
        let spare = self
            .spare
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        match spare {
            Some(mut run) => {
                run.reset(arity);
                run
            }
            None => Parsed::new(arity),
        }
    }

    /// Keeps an interned run for reuse.
    pub(crate) fn give(&self, run: Parsed) {
        self.spare
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(run);
    }
}
