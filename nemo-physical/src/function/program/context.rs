//! The context that a program is evaluated in.

use web_time::{SystemTime, UNIX_EPOCH};

use crate::value::{DateTime, TimezoneOffset};

/// What a program reads beyond its inputs and its constants.
///
/// A context fixes the instant that [Now](crate::function::OpCode::Now) returns,
/// so that every evaluation in one context sees the same instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationContext {
    /// The instant that [Now](crate::function::OpCode::Now) returns.
    now: DateTime,
}

impl EvaluationContext {
    /// Creates a context whose instant is the current time, in UTC.
    ///
    /// If the system clock is set before 1970, the instant is the start of 1970.
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

    /// Creates a context with the given instant.
    pub fn at(now: DateTime) -> Self {
        Self { now }
    }

    /// Returns the instant of this context.
    pub fn instant(self) -> DateTime {
        self.now
    }
}
