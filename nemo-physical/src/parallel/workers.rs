//! The worker threads of the process and their pool.

use std::{sync::OnceLock, thread};

/// The worker threads of the process:
/// their number, and the thread pool they run on when there is more than one.
///
/// # Example
///
/// ```
/// use nemo_physical::parallel::Workers;
///
/// // A host that shares the machine caps the worker threads once, at startup.
/// if Workers::set(2) {
///     assert_eq!(Workers::get().threads(), 2);
/// }
///
/// // The first number stays for the whole process, as does the default once read.
/// assert!(!Workers::set(4));
/// ```
#[derive(Debug)]
pub struct Workers {
    /// The number of worker threads parallel phases may use.
    threads: usize,
    /// The pool, spawned by the first phase that runs on it.
    ///
    /// `None` on a single thread, or if the pool could not be created,
    /// which leaves the parallel phases sequential rather than failing them.
    pool: OnceLock<Option<rayon_core::ThreadPool>>,
}

/// The workers that are used for the whole process.
static WORKERS: OnceLock<Workers> = OnceLock::new();

impl Workers {
    /// Sets the number of worker threads for the whole process.
    ///
    /// Takes effect only before the first parallel phase reads the number
    /// and returns whether it did.
    /// The machine's available parallelism applies otherwise.
    ///
    /// Counts are clamped to at least one.
    pub fn set(threads: usize) -> bool {
        WORKERS.set(Self::new(threads.max(1))).is_ok()
    }

    /// Returns the workers of the process,
    /// with the machine's available parallelism if no number was [set](Self::set).
    pub fn get() -> &'static Self {
        WORKERS.get_or_init(|| {
            Self::new(thread::available_parallelism().map_or(1, |count| count.get()))
        })
    }

    /// Returns the number of worker threads parallel phases may use.
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Initializes the workers with the given number of threads.
    fn new(threads: usize) -> Self {
        Self {
            threads,
            pool: OnceLock::new(),
        }
    }

    /// Returns the pool,
    /// spawning it on the first call if there is more than one thread.
    pub(super) fn pool(&self) -> Option<&rayon_core::ThreadPool> {
        self.pool
            .get_or_init(|| {
                if self.threads <= 1 {
                    return None;
                }
                match rayon_core::ThreadPoolBuilder::new()
                    .num_threads(self.threads)
                    .thread_name(|index| format!("nemo-worker-{index}"))
                    .build()
                {
                    Ok(pool) => Some(pool),
                    Err(error) => {
                        tracing::warn!(%error, "no worker pool could be created; running sequentially");
                        None
                    }
                }
            })
            .as_ref()
    }
}
