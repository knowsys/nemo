//! The curves that the recorder keeps over a run, of rule applications and iterations against
//! time, and how they stay bounded.

/// An entry of a curve, which has a stamp and can be merged with a later entry
/// into one that covers both.
pub(super) trait CurveEntry: Copy {
    /// Returns the stamp, in nanoseconds since the recorder started.
    fn at(&self) -> u64;

    /// Returns this entry and a later one, merged into one.
    fn merged(self, later: Self) -> Self;
}

/// One entry of the timeline, for one rule application or for several adjacent ones merged.
#[derive(Debug, Clone, Copy)]
pub(super) struct TimelinePoint {
    /// The stamp of the end of the last application.
    pub(super) at: u64,
    /// The number of new rows that the applications derived.
    pub(super) derived: u64,
    /// The wall time of the applications, summed.
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

/// One entry of the iteration curve, for one finished iteration or for several adjacent ones
/// merged.
#[derive(Debug, Clone, Copy)]
pub(super) struct IterationPoint {
    /// The stamp of the end of the last iteration.
    pub(super) at: u64,
    /// The stratum of the last iteration.
    pub(super) stratum: u64,
    /// The number of new rows that the iterations derived.
    pub(super) derived: u64,
    /// The number of stored rows after the last iteration.
    pub(super) total: u64,
    /// The bytes in use after the last iteration.
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
/// Once it has `CAP` entries, consecutive entries within one time window are merged.
/// So its size stays bounded however long the run is,
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
            // The window is the span of the stamps so far divided by `CAP / 2`,
            // and it never shrinks. Since it depends only on the span,
            // it grows only as fast as the run does.
            self.window = self.window.max(entry.at() / (CAP as u64 / 2)).max(1);
            self.coarsen();
        }
    }

    /// Returns the entries, in order.
    pub(super) fn entries(&self) -> &[T] {
        &self.entries
    }

    /// Coarsens the curve in place, by merging consecutive entries whose stamps share a time
    /// window.
    ///
    /// An early entry only ever merges with its neighbors,
    /// so the curve keeps covering the whole run.
    /// With the window at `span / (cap / 2)`,
    /// the stamps seen so far fall into at most `cap / 2 + 1` windows,
    /// so a curve at its cap shrinks to at most `cap / 2 + 1` entries.
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

    /// A bare stamp as an entry, which keeps the later stamp when merged.
    impl CurveEntry for u64 {
        fn at(&self) -> u64 {
            *self
        }

        fn merged(self, later: Self) -> Self {
            later
        }
    }

    /// Coarsening merges consecutive entries in the same window, and only those.
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
