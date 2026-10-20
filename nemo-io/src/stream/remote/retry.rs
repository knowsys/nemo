//! The retry schedule of failed requests.

use std::time::Duration;

/// When a failed request is sent again.
///
/// A 429 or 503 is retried after the server's `Retry-After`, if it gives one in seconds.
/// A 429, 502, 503 or 504 is retried otherwise after a doubling backoff.
/// Any other failure is final at once.
///
/// # Example
///
/// ```
/// use std::time::Duration;
///
/// use nemo_io::remote::Retry;
///
/// let retry = Retry::default();
/// // A rate limit with a `Retry-After` of three seconds waits that long.
/// assert_eq!(retry.delay(0, 429, Some("3")), Some(Duration::from_secs(3)));
/// // An overloaded gateway backs off, doubling.
/// assert_eq!(retry.delay(1, 502, None), Some(Duration::from_secs(2)));
/// // A missing resource is final at once.
/// assert_eq!(retry.delay(0, 404, None), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retry {
    /// Retries per request at most.
    pub retries: u32,
    /// The first backoff, which each further one doubles.
    pub backoff: Duration,
    /// The longest `Retry-After` honored.
    ///
    /// If a server asks for more, the request is not retried and its failure is final.
    pub longest_wait: Duration,
}

impl Default for Retry {
    /// Returns the schedule of five retries, backing off for 1, 2, 4, 8, and 16 seconds,
    /// that honors a server's `Retry-After` up to two minutes.
    fn default() -> Self {
        Self {
            retries: 5,
            backoff: Duration::from_secs(1),
            longest_wait: Duration::from_secs(120),
        }
    }
}

impl Retry {
    /// Returns the wait before a request is sent again,
    /// after its failure number `attempt` (counted from zero) responded with `status`,
    /// given the response's `Retry-After` value.
    ///
    /// Returns `None` if the failure is final.
    pub fn delay(&self, attempt: u32, status: u16, retry_after: Option<&str>) -> Option<Duration> {
        if attempt >= self.retries || !matches!(status, 429 | 502 | 503 | 504) {
            return None;
        }
        let asked = matches!(status, 429 | 503)
            .then(|| retry_after?.trim().parse::<u64>().ok())
            .flatten()
            .map(Duration::from_secs);
        match asked {
            Some(wait) if wait <= self.longest_wait => Some(wait),
            Some(_) => None,
            None => Some(self.backoff.saturating_mul(1 << attempt.min(16))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overload_backs_off_doubling_until_the_retries_are_spent() {
        let retry = Retry::default();
        let waits: Vec<_> = (0..6)
            .map(|attempt| retry.delay(attempt, 503, None))
            .collect();
        assert_eq!(
            waits,
            [1, 2, 4, 8, 16]
                .map(|seconds| Some(Duration::from_secs(seconds)))
                .into_iter()
                .chain([None])
                .collect::<Vec<_>>()
        );
        assert_eq!(retry.delay(0, 502, None), Some(Duration::from_secs(1)));
        assert_eq!(retry.delay(1, 504, None), Some(Duration::from_secs(2)));
    }

    #[test]
    fn rate_limits_wait_as_long_as_the_server_asks() {
        let retry = Retry::default();
        assert_eq!(retry.delay(0, 429, Some("7")), Some(Duration::from_secs(7)));
        assert_eq!(retry.delay(3, 429, Some(" 0 ")), Some(Duration::ZERO));
        // A date, or nothing, backs off instead.
        assert_eq!(
            retry.delay(1, 429, Some("Wed, 21 Oct 2026 07:28:00 GMT")),
            Some(Duration::from_secs(2))
        );
        assert_eq!(retry.delay(2, 429, None), Some(Duration::from_secs(4)));
        // Asking for more than the longest wait is final.
        assert_eq!(retry.delay(0, 429, Some("600")), None);
        // Only 429 and 503 carry a meaningful `Retry-After`.
        assert_eq!(retry.delay(0, 502, Some("7")), Some(Duration::from_secs(1)));
    }

    #[test]
    fn other_failures_are_final() {
        let retry = Retry::default();
        for status in [400, 404, 413, 500] {
            assert_eq!(retry.delay(0, status, None), None, "{status}");
        }
    }
}
