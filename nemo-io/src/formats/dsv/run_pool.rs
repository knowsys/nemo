//! A pool of runs kept for reuse.

use std::sync::{Mutex, PoisonError};

use nemo_physical::function::EvaluationContext;

use crate::formats::dsv::Parsed;

/// A pool of runs that were interned and wait to be filled again.
///
/// A batch fills runs on its worker threads and interns them on the calling thread.
/// Handing each interned run back to the pool keeps its buffers,
/// so after the first few runs the parse allocates nothing per run.
/// This also keeps the heap from fragmenting
/// into thousands of buffers allocated on one thread and freed on another.
#[derive(Debug)]
pub(crate) struct RunPool {
    /// The runs waiting to be reused.
    spare: Mutex<Vec<Parsed>>,
    /// The evaluation context of the selection's condition, for every run.
    context: EvaluationContext,
}

impl RunPool {
    /// Creates an empty pool.
    ///
    /// `context` is the evaluation context of the selection's condition,
    /// as for [Parsed::new], and every run of the pool uses it.
    pub(crate) fn new(context: EvaluationContext) -> Self {
        Self {
            spare: Mutex::new(Vec::new()),
            context,
        }
    }

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
                run.reset(arity, self.context);
                run
            }
            None => Parsed::new(arity, self.context),
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
