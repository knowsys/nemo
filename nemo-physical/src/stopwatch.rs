//! The clock that measures execution times.

/// A measurement of elapsed wall-clock time.
///
/// On the `wasm32` target, which has no monotonic clock,
/// every measurement is zero.
#[derive(Debug, Clone, Copy)]
pub struct Stopwatch {
    /// The instant at which the measurement started.
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

    /// Returns the number of nanoseconds since the start,
    /// or `u64::MAX` if they do not fit in 64 bits.
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
