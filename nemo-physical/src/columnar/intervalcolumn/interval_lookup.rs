//! This module defines the trait [IntervalLookup] and [IntervalLookupBuilder],
//! and provides several implementations of them.
//!
pub(crate) mod lookup_column;

use std::fmt::Debug;

use crate::{
    datatypes::{storage_type_name::STORAFE_TYPES, StorageTypeName},
    management::bytesized::ByteSized,
};
/// Trait for looking up interval bounds in [IntervalColumn][super::super::intervalcolumn::IntervalColumn]
pub(crate) trait IntervalLookup: Debug + Clone + ByteSized {
    /// [IntervalLookupBuilder] type for building objects that implement this trait
    type Builder: IntervalLookupBuilder<Lookup = Self>;

    /// Given an index of a value from the previous layer,
    /// return the index of the interval that representing the slice in the data column
    /// associated with the successors of that value.
    ///
    /// Returns `None` if the value has no successor in this interval column.
    fn interval_index(&self, index: usize) -> Option<usize>;
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

    ///
    skip: Option<StorageTypeName>,
}

impl<LookupMethod> IntervalLookupT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Given a storage type and an index of a value from the previous layer,
    /// return the index of the interval that representing the slice in the data column
    /// associated with the successors of that value.
    ///
    /// Returns `None` if the value has no successor in this interval column.
    pub fn interval_index(&self, storage_type: StorageTypeName, index: usize) -> Option<usize> {
        if self.skip == Some(storage_type) {
            return Some(index);
        }

        match storage_type {
            StorageTypeName::Id32 => self.lookup_id32.interval_index(index),
            StorageTypeName::Id64 => self.lookup_id64.interval_index(index),
            StorageTypeName::Int64 => self.lookup_int64.interval_index(index),
            StorageTypeName::Float => self.lookup_float.interval_index(index),
            StorageTypeName::Double => self.lookup_double.interval_index(index),
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

    /// Add the given interval index.
    fn add_interval(&mut self, interval_index: usize);

    /// Add an empty entry, signaling that this node has no successor.
    fn add_empty(&mut self);

    /// Returns true if this builder does not contain any gaps.
    fn is_exclusive(&self) -> bool;

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
}

impl<LookupMethod> IntervalLookupBuilderT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Return an iterator over all builders and their associated [StorageTypeName]
    fn builders(&self) -> impl Iterator<Item = (StorageTypeName, &LookupMethod::Builder)> {
        let builders = vec![
            &self.builder_id32,
            &self.builder_id64,
            &self.builder_int64,
            &self.builder_float,
            &self.builder_double,
        ];

        debug_assert!(builders.len() == STORAFE_TYPES.len());

        STORAFE_TYPES.iter().cloned().zip(builders.into_iter())
    }
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
        }
    }
}

impl<LookupMethod> IntervalLookupBuilderT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    pub fn add_interval(&mut self, storage_type: StorageTypeName, interval_index: usize) {
        match storage_type {
            StorageTypeName::Id32 => self.builder_id32.add_interval(interval_index),
            StorageTypeName::Id64 => self.builder_id64.add_interval(interval_index),
            StorageTypeName::Int64 => self.builder_int64.add_interval(interval_index),
            StorageTypeName::Float => self.builder_float.add_interval(interval_index),
            StorageTypeName::Double => self.builder_double.add_interval(interval_index),
        }
    }

    pub fn add_empty(&mut self, storage_type: StorageTypeName) {
        match storage_type {
            StorageTypeName::Id32 => self.builder_id32.add_empty(),
            StorageTypeName::Id64 => self.builder_id64.add_empty(),
            StorageTypeName::Int64 => self.builder_int64.add_empty(),
            StorageTypeName::Float => self.builder_float.add_empty(),
            StorageTypeName::Double => self.builder_double.add_empty(),
        }
    }

    pub fn finalize(self) -> IntervalLookupT<LookupMethod> {
        let mut skip: Option<StorageTypeName> = None;

        // If only one builder is non-empty,
        // then we can skip the lookup
        for (storage_type, builder) in self.builders() {
            if builder.is_exclusive() {
                if skip.is_none() {
                    skip = Some(storage_type);
                } else {
                    skip = None;
                    break;
                }
            }
        }

        IntervalLookupT {
            lookup_id32: self.builder_id32.finalize(),
            lookup_id64: self.builder_id64.finalize(),
            lookup_int64: self.builder_int64.finalize(),
            lookup_float: self.builder_float.finalize(),
            lookup_double: self.builder_double.finalize(),

            skip,
        }
    }
}
