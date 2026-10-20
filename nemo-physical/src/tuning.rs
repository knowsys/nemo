//! Thresholds that decide how the physical layer runs its operations.
//!
//! Executing an operation plan proceeds in phases,
//! steps whose work the worker threads can share.
//! Each phase estimates its work as the number of values it will visit or write.
//! From that estimate, [parallel_work](Tuning::parallel_work),
//! [work_per_task](Tuning::work_per_task) and [tasks_per_thread](Tuning::tasks_per_thread)
//! decide whether the phase runs on the calling thread
//! or is cut into tasks for the worker pool, and into how many.
//!
//! The thresholds are gathered here with their defaults,
//! and a host may [set](Tuning::set) its own once per process.

use std::sync::OnceLock;

/// The thresholds the physical layer runs with.
///
/// # Example
///
/// ```
/// use nemo_physical::tuning::Tuning;
///
/// // A host that wants fewer, larger tasks sets its tuning once, at startup.
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
    /// Handing a phase to the worker pool has a cost that is amortized
    /// only when the phase takes several times as long.
    pub parallel_work: usize,
    /// The least work a task holds
    /// when a phase is cut up for the worker pool.
    ///
    /// A phase past `parallel_work` is cut into its work divided by this
    /// many tasks.
    pub work_per_task: usize,
    /// The most tasks per worker thread a phase is cut into.
    ///
    /// Cut into one task per thread, a phase would last as long as its
    /// heaviest task, but the tasks are uneven.
    /// Several tasks per thread let a worker that finishes early
    /// take up the tasks nobody has started yet.
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
    /// Sets this tuning for the whole process.
    ///
    /// Takes effect only before the first phase reads the tuning
    /// and returns whether it did.
    /// The [default](Self::default) applies otherwise.
    pub fn set(self) -> bool {
        TUNING.set(self).is_ok()
    }

    /// Returns the tuning that is set for the whole process,
    /// or the [default](Self::default) if none was set.
    pub fn get() -> &'static Self {
        TUNING.get_or_init(Self::default)
    }
}
