use std::fmt::Debug;
use std::ops::Range;

use crate::management::ByteSized;

pub trait IntervalLookup: Clone + Debug + ByteSized {
    /// Return the interval bounds of the successor nodes
    /// for the input node at the given index.
    ///
    /// Returns `None` node has no successor in this column.
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>>;
}
