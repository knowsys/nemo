//! The curves the recorder keeps over a run, rule applications and iterations against time,
//! and how they stay bounded.

/// An entry of a curve: stamped, and mergeable with a later entry into
/// one that covers both.
pub(super) trait CurveEntry: Copy {
    /// Returns the stamp, in nanoseconds since the recorder started.
    fn at(&self) -> u64;

    /// Returns this entry and a later one as one.
    fn merged(self, later: Self) -> Self;
}

/// One timeline entry: one rule application, or several merged adjacent ones.
#[derive(Debug, Clone, Copy)]
pub(super) struct TimelinePoint {
    /// Stamp of the (last merged) application's end.
    pub(super) at: u64,
    /// New rows across the merged applications.
    pub(super) derived: u64,
    /// Wall time across the merged applications.
    pub(super) duration: u64,
}

impl CurveEntry for TimelinePoint {
    fn at(&self) -> u64 {
        self.at
    }

    fn merged(self, later: Self) -> Self {
        Self {
            at: later.at,
            derived: self.derived + later.derived,
            duration: self.duration + later.duration,
        }
    }
}

/// One finished iteration, or several merged adjacent ones.
#[derive(Debug, Clone, Copy)]
pub(super) struct IterationPoint {
    /// Stamp of the (last merged) iteration's end.
    pub(super) at: u64,
    /// The stratum of the last merged iteration.
    pub(super) stratum: u64,
    /// New rows across the merged iterations.
    pub(super) derived: u64,
    /// Stored rows after the last merged iteration.
    pub(super) total: u64,
    /// Bytes after the last merged iteration.
    pub(super) memory: u64,
}

impl CurveEntry for IterationPoint {
    fn at(&self) -> u64 {
        self.at
    }

    fn merged(self, later: Self) -> Self {
        Self {
            at: later.at,
            stratum: later.stratum,
            derived: self.derived + later.derived,
            total: later.total,
            memory: later.memory,
        }
    }
}

/// A curve over the run whose size stays bounded.
///
/// Once it holds `CAP` entries, consecutive entries within one time window merge,
/// so its size stays bounded however long the run is,
/// while it still covers the whole run.
#[derive(Debug)]
pub(super) struct BoundedCurve<T, const CAP: usize> {
    /// The entries, in stamp order.
    entries: Vec<T>,
    /// The width of the merge windows, in nanoseconds.
    window: u64,
}

impl<T, const CAP: usize> Default for BoundedCurve<T, CAP> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            window: 0,
        }
    }
}

impl<T: CurveEntry, const CAP: usize> BoundedCurve<T, CAP> {
    /// Appends an entry, coarsening the curve if it reaches its cap.
    pub(super) fn push(&mut self, entry: T) {
        self.entries.push(entry);
        if self.entries.len() >= CAP {
            // The window is the span of stamps so far divided by `CAP / 2`,
            // and never shrinks. Derived from the span alone, it grows only
            // as fast as the run does.
            self.window = self.window.max(entry.at() / (CAP as u64 / 2)).max(1);
            self.coarsen();
        }
    }

    /// Returns the entries, in order.
    pub(super) fn entries(&self) -> &[T] {
        &self.entries
    }

    /// Coarsens the curve in place:
    /// consecutive entries whose stamps share a time window merge into one.
    ///
    /// Early entries only ever merge with their own neighborhood,
    /// so the curve keeps covering the whole run.
    ///
    /// With the window at `span / (cap / 2)`,
    /// at most `cap / 2 + 1` windows exist over the stamps seen so far,
    /// so the curve at its cap at least halves.
    fn coarsen(&mut self) {
        let entries = &mut self.entries;
        let window = self.window;
        let mut kept = 0;
        for index in 0..entries.len() {
            if kept > 0 && entries[kept - 1].at() / window == entries[index].at() / window {
                entries[kept - 1] = entries[kept - 1].merged(entries[index]);
            } else {
                entries[kept] = entries[index];
                kept += 1;
            }
        }
        entries.truncate(kept);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bare stamp as an entry: merging keeps the later one.
    impl CurveEntry for u64 {
        fn at(&self) -> u64 {
            *self
        }

        fn merged(self, later: Self) -> Self {
            later
        }
    }

    #[test]
    fn coarsening_merges_within_windows_only() {
        // 1, 2, 3 share window 0, 11 and 12 share window 1, and 25
        // stands alone. Each window keeps its latest entry.
        let mut curve = BoundedCurve::<u64, 1024> {
            entries: vec![1, 2, 3, 11, 12, 25],
            window: 10,
        };
        curve.coarsen();
        assert_eq!(curve.entries(), [3, 12, 25]);
    }
}
