//! Runs kept for reuse across a batch.

use std::sync::{Mutex, PoisonError};

use nemo_physical::function::EvaluationContext;

use crate::formats::dsv::Parsed;

/// The runs of a batch that were interned and wait to be filled again.
///
/// A batch makes runs on its worker threads and interns them on the calling thread.
/// Handing each interned run back keeps its buffers,
/// so after the first few runs the parse allocates nothing per run.
/// The heap later work allocates in is then not left fragmented
/// by thousands of buffers made on one thread and freed on another.
#[derive(Debug)]
pub(crate) struct RunPool {
    /// The runs waiting to be reused.
    spare: Mutex<Vec<Parsed>>,
    /// The context every run's conditions evaluate in.
    context: EvaluationContext,
}

impl RunPool {
    /// Creates an empty pool whose runs evaluate their conditions in `context`.
    pub(crate) fn new(context: EvaluationContext) -> Self {
        Self {
            spare: Mutex::new(Vec::new()),
            context,
        }
    }

    /// Returns an empty run of the given arity: a spare one if there is one, a new one otherwise.
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
