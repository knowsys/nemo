//! The worker threads of the process and their pool.

use std::{sync::OnceLock, thread};

/// The number of worker threads of the process, and their thread pool if there is more than one.
///
/// # Example
///
/// ```
/// use nemo_physical::parallel::Workers;
///
/// // The number of worker threads can be capped once, at startup.
/// if Workers::set(2) {
///     assert_eq!(Workers::get().threads(), 2);
/// }
///
/// // The first number stays for the whole process, and so does the default once it was read.
/// assert!(!Workers::set(4));
/// ```
#[derive(Debug)]
pub struct Workers {
    /// The number of worker threads that parallel phases may use.
    threads: usize,
    /// The pool, created by the first phase that runs on it.
    ///
    /// It is `None` for a single thread, or if the pool could not be created.
    /// The parallel phases then run sequentially rather than fail.
    pool: OnceLock<Option<rayon_core::ThreadPool>>,
}

/// The workers that are used for the whole process.
static WORKERS: OnceLock<Workers> = OnceLock::new();

impl Workers {
    /// Sets the number of worker threads for the whole process,
    /// and returns whether it took effect.
    ///
    /// It takes effect only if no number was set or [read](Self::get) before.
    /// A count of zero is raised to one.
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

    /// Returns the number of worker threads that parallel phases may use.
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Creates workers with the given number of threads, without a pool yet.
    fn new(threads: usize) -> Self {
        Self {
            threads,
            pool: OnceLock::new(),
        }
    }

    /// Returns the pool, which the first call creates,
    /// or `None` if there is a single thread or the pool could not be created.
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
