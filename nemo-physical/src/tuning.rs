//! Thresholds that decide how this crate runs its operations in parallel.
//!
//! Executing an operation plan proceeds in [phases](crate::parallel),
//! steps whose work the worker threads can share.
//! Each phase estimates its work as the number of values that it will visit or write.
//! From that estimate, [parallel_work](Tuning::parallel_work),
//! [work_per_task](Tuning::work_per_task) and [tasks_per_thread](Tuning::tasks_per_thread)
//! decide whether the phase runs on the calling thread
//! or is cut into tasks for the worker pool, and into how many.
//!
//! The thresholds have defaults, and [set](Tuning::set) replaces them once per process.

use std::sync::OnceLock;

/// The thresholds that this crate runs with.
///
/// # Example
///
/// ```
/// use nemo_physical::tuning::Tuning;
///
/// // For fewer, larger tasks, set the tuning once, at startup.
/// let tuning = Tuning {
///     work_per_task: 1 << 14,
///     ..Tuning::default()
/// };
/// if tuning.set() {
///     assert_eq!(Tuning::get(), &tuning);
/// }
///
/// // The first tuning stays for the whole process.
/// assert!(!Tuning::default().set());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tuning {
    /// The work below which a phase runs whole on the calling thread.
    ///
    /// Handing a phase to the worker pool has a cost that pays off
    /// only when the phase takes several times as long.
    pub parallel_work: usize,
    /// The least work of a task when a phase is cut into tasks for the worker pool.
    ///
    /// A phase with at least `parallel_work` work is cut into its work divided by this number
    /// of tasks, up to the limit of [tasks_per_thread](Self::tasks_per_thread).
    pub work_per_task: usize,
    /// The largest number of tasks per worker thread that a phase is cut into.
    ///
    /// With one task per thread, a phase would last as long as its heaviest task,
    /// and the tasks are uneven.
    /// With several tasks per thread, a worker that finishes early
    /// can take up the tasks that no worker has started yet.
    pub tasks_per_thread: usize,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            parallel_work: 1 << 17,
            work_per_task: 1 << 12,
            tasks_per_thread: 8,
        }
    }
}

/// The tuning that is used for the whole process.
static TUNING: OnceLock<Tuning> = OnceLock::new();

impl Tuning {
    /// Sets this tuning for the whole process, and returns whether it took effect.
    ///
    /// It takes effect only if no tuning was set before and no phase has read the tuning yet.
    /// Otherwise, the tuning that is in effect stays.
    pub fn set(self) -> bool {
        TUNING.set(self).is_ok()
    }

    /// Returns the tuning that is set for the whole process,
    /// or the [default](Self::default) if none was set.
    pub fn get() -> &'static Self {
        TUNING.get_or_init(Self::default)
    }
}
