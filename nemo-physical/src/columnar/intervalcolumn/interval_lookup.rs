//! This module defines the trait [IntervalLookup] and [IntervalLookupBuilder],
//! and provides several implementations of them.

pub(crate) mod lookup_bitvector;
pub(crate) mod lookup_column_dual;
pub(crate) mod lookup_column_single;

use std::{fmt::Debug, ops::Range};

use crate::{datatypes::StorageTypeName, management::bytesized::ByteSized};
/// Trait for looking up interval bounds in [IntervalColumn][super::super::intervalcolumn::IntervalColumn]
pub(crate) trait IntervalLookup: Debug + Clone + ByteSized {
    /// [IntervalLookupBuilder] type for building objects that implement this trait
    type Builder: IntervalLookupBuilder<Lookup = Self>;

    /// Given an index of a value from the previous layer,
    /// return a range of indices representing the slice in the data column
    /// that is associated with the successors of that value.
    ///
    /// Returns `None` if the value has no successor in this interval column.
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>>;
}

#[derive(Debug, Clone)]
pub(crate) struct IntervalLookupT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    lookup_id32: LookupMethod,
    lookup_id64: LookupMethod,
    lookup_int64: LookupMethod,
    lookup_float: LookupMethod,
    lookup_double: LookupMethod,
}

impl<LookupMethod> IntervalLookupT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Given a storage type and an index of a value from the previous layer,
    /// return a range of indices representing the slice in the data column
    /// that is associated with the successors of that value.
    ///
    /// The end of the interval will be [usize::MAX] and will therefore have
    /// to be clipped to the length of the data column.
    ///
    /// Returns `None` if the value has no successor in this interval column.
    pub fn interval_bounds(
        &self,
        storage_type: StorageTypeName,
        index: usize,
    ) -> Option<Range<usize>> {
        match storage_type {
            StorageTypeName::Id32 => self.lookup_id32.interval_bounds(index),
            StorageTypeName::Id64 => self.lookup_id64.interval_bounds(index),
            StorageTypeName::Int64 => self.lookup_int64.interval_bounds(index),
            StorageTypeName::Float => self.lookup_float.interval_bounds(index),
            StorageTypeName::Double => self.lookup_double.interval_bounds(index),
        }
    }
}

impl<LookupMethod> ByteSized for IntervalLookupT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    fn size_bytes(&self) -> bytesize::ByteSize {
        self.lookup_id32.size_bytes()
            + self.lookup_id32.size_bytes()
            + self.lookup_id32.size_bytes()
            + self.lookup_id32.size_bytes()
            + self.lookup_id32.size_bytes()
    }
}

/// Trait for constructing [IntervalLookup] objects
pub(crate) trait IntervalLookupBuilder: Debug + Default {
    /// [IntervalLookup] that will be built
    type Lookup: IntervalLookup<Builder = Self>;

    /// Add the given interval.
    fn add(&mut self, interval: Range<usize>);

    /// Return the constructed [IntervalLookup].
    fn finalize(self) -> Self::Lookup;
}

#[derive(Debug)]
pub(crate) struct IntervalLookupBuilderT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    builder_id32: LookupMethod::Builder,
    builder_id64: LookupMethod::Builder,
    builder_int64: LookupMethod::Builder,
    builder_float: LookupMethod::Builder,
    builder_double: LookupMethod::Builder,

    current_interval: Range<usize>,
}

impl<LookupMethod> Default for IntervalLookupBuilderT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    fn default() -> Self {
        Self {
            builder_id32: Default::default(),
            builder_id64: Default::default(),
            builder_int64: Default::default(),
            builder_float: Default::default(),
            builder_double: Default::default(),
            current_interval: 0..0,
        }
    }
}

impl<LookupMethod> IntervalLookupBuilderT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    pub fn add(&mut self, storage_type: StorageTypeName, data_count: usize) {
        self.current_interval.start = self.current_interval.end;
        self.current_interval.end = data_count;

        let current_interval = self.current_interval.clone();

        match storage_type {
            StorageTypeName::Id32 => self.builder_id32.add(current_interval),
            StorageTypeName::Id64 => self.builder_id64.add(current_interval),
            StorageTypeName::Int64 => self.builder_int64.add(current_interval),
            StorageTypeName::Float => self.builder_float.add(current_interval),
            StorageTypeName::Double => self.builder_double.add(current_interval),
        }
    }

    pub fn finalize(self) -> IntervalLookupT<LookupMethod> {
        IntervalLookupT {
            lookup_id32: self.builder_id32.finalize(),
            lookup_id64: self.builder_id64.finalize(),
            lookup_int64: self.builder_int64.finalize(),
            lookup_float: self.builder_float.finalize(),
            lookup_double: self.builder_double.finalize(),
        }
    }
}
