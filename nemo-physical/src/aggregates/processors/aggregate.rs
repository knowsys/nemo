//! Aggregate value type

use std::fmt::Debug;

use num::CheckedAdd;

use crate::datatypes::StorageValueT;

/// A value that can be aggregated using any of the supported aggregates.
///
/// [`CheckedAdd`] is required for sum aggregates.
/// [`Clone`] and [`Into<StorageValueT>`] is required to return a storage value in `finish` function in min/max/sum aggregates.
/// [`Debug`] for debugging
/// [`Default`] is required to initialize the aggregator in sum aggregates.
/// [`PartialOrd`] is required for min/max aggregates.
/// `'static` is required to store the value e.g. in min/max aggregates.
pub trait Aggregate:
    CheckedAdd + Clone + Debug + Default + Into<StorageValueT> + PartialEq + PartialOrd + 'static
{
}

impl<T> Aggregate for T where
    T: CheckedAdd
        + Clone
        + Debug
        + Default
        + Into<StorageValueT>
        + PartialEq
        + PartialOrd
        + 'static
{
}
