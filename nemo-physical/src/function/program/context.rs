//! The context programs evaluate in.

use web_time::{SystemTime, UNIX_EPOCH};

use crate::value::{DateTime, TimezoneOffset};

/// What a program reads beyond its row and its constants: the state of the run
/// it evaluates in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationContext {
    /// The run's instant, in UTC.
    now: DateTime,
}

impl EvaluationContext {
    /// Creates a context whose instant is the current one.
    pub fn now() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
            .unwrap_or(0);

        Self::at(DateTime::from_unix_millis(
            millis,
            Some(TimezoneOffset::UTC),
        ))
    }

    /// Creates a context at the given instant.
    pub fn at(now: DateTime) -> Self {
        Self { now }
    }

    /// Returns the run's instant.
    pub fn instant(self) -> DateTime {
        self.now
    }
}
