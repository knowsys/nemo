//! The clock execution timings are measured with.

/// A wall-clock time measurement.
///
/// On targets without a monotonic clock (wasm),
/// every measurement returns zero.
#[derive(Debug, Clone, Copy)]
pub struct Stopwatch {
    /// The starting instant.
    #[cfg(not(target_arch = "wasm32"))]
    started: std::time::Instant,
}

impl Stopwatch {
    /// Starts a measurement.
    pub fn start() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            started: std::time::Instant::now(),
        }
    }

    /// Returns the number of nanoseconds since the start.
    pub fn nanos(self) -> u64 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
        }
        #[cfg(target_arch = "wasm32")]
        {
            0
        }
    }
}
