//! How a phase is cut into tasks for the worker pool.

use crate::{parallel::Workers, tuning::Tuning};

/// How a phase is cut for the worker pool:
/// into how many tasks, and whether they go to the pool at all.
///
/// Every phase obtains one before it cuts its input,
/// so no task count reaches the pool that its policy did not allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tasks {
    /// The number of tasks the phase is cut into.
    pub(super) count: usize,
    /// Whether the tasks may run on the pool
    /// rather than one after another on the calling thread.
    pub(super) parallel: bool,
}

impl Tasks {
    /// Cuts a phase of the given work into tasks.
    ///
    /// The phase is one task if the work is below the tuning's `parallel_work`,
    /// or if there is a single thread.
    /// Otherwise it is as many tasks of at least the tuning's `work_per_task`
    /// as the work holds, but at most `limit`
    /// and at most the maximum the thread budget allows.
    pub(crate) fn cut(work: usize, limit: usize) -> Self {
        let threads = Workers::get().threads();
        let tuning = Tuning::get();

        let count = if threads <= 1 || work < tuning.parallel_work {
            1
        } else {
            (work / tuning.work_per_task)
                .min(threads * tuning.tasks_per_thread)
                .min(limit)
                .max(1)
        };

        Self {
            count,
            parallel: count > 1,
        }
    }

    /// Cuts a phase of the given work into the given number of tasks,
    /// fixed by its shape, such as one task per column:
    /// on the pool when the work is worth cutting into at least two tasks.
    pub(crate) fn fixed(work: usize, count: usize) -> Self {
        Self {
            count,
            parallel: Self::cut(work, count).count > 1,
        }
    }

    /// Creates exactly the given number of tasks, on the pool if there is one,
    /// for a phase that cut its work by its own measure,
    /// such as the parsing of an input in windows of bytes.
    pub fn of(count: usize) -> Self {
        Self {
            count,
            parallel: true,
        }
    }

    /// Creates exactly the given number of tasks, run one after another on the calling thread,
    /// for a phase that judged its work too small for the pool.
    pub fn sequential(count: usize) -> Self {
        Self {
            count,
            parallel: false,
        }
    }

    /// Returns the same decision over a different number of tasks,
    /// for a phase whose data yields fewer or more cut points
    /// than it aimed at.
    pub(crate) fn over(self, count: usize) -> Self {
        Self { count, ..self }
    }

    /// Returns the number of tasks the phase is cut into.
    pub fn count(self) -> usize {
        self.count
    }

    /// Returns the pool the tasks run on,
    /// or `None` if they run on the caller.
    pub(super) fn pool(self) -> Option<&'static rayon_core::ThreadPool> {
        Workers::get()
            .pool()
            .filter(|_| self.parallel && self.count > 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_counts_follow_the_work() {
        let tuning = Tuning::get();
        assert_eq!(Tasks::cut(0, 100).count(), 1);
        assert_eq!(Tasks::cut(tuning.parallel_work - 1, 100).count(), 1);
        assert!(!Tasks::fixed(tuning.parallel_work - 1, 100).parallel);
        assert_eq!(Tasks::fixed(tuning.parallel_work - 1, 100).count(), 100);
        let threads = Workers::get().threads();
        if threads > 1 {
            assert_eq!(
                Tasks::cut(tuning.parallel_work, 100).count(),
                100.min(tuning.parallel_work / tuning.work_per_task)
            );
            assert_eq!(Tasks::cut(tuning.parallel_work, 2).count(), 2);
            assert_eq!(
                Tasks::cut(usize::MAX, usize::MAX).count(),
                threads * tuning.tasks_per_thread
            );
            assert!(Tasks::fixed(usize::MAX, 3).parallel);
            assert!(!Tasks::fixed(usize::MAX, 1).parallel);
        } else {
            assert_eq!(Tasks::cut(usize::MAX, usize::MAX).count(), 1);
            assert!(!Tasks::fixed(usize::MAX, 3).parallel);
        }
    }
}
