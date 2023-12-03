use std::{fmt::Debug, ops::Range};

use crate::management::ByteSized;

pub mod lookup_bitvector;
pub mod lookup_column_dual;
pub mod lookup_column_single;

pub trait IntervalLookup: Debug + Clone + ByteSized {
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>>;
}
