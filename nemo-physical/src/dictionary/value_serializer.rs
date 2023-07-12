use std::{fmt::Display, ops::Deref};

use crate::{
    datatypes::{data_value::PhysicalString, DataTypeName, DataValueT, StorageValueT},
    management::database::Dict,
    tabular::traits::table_schema::TableSchema,
};
use std::convert::TryFrom;

use super::Dictionary;

/// Prefix for physical null representation
pub const NULL_PREFIX: &str = "NULL:";

/// Load constant from dictionary with fallback if it is not found
pub fn serialize_constant_with_dict<C, D>(constant: C, dict: D) -> PhysicalString
where
    usize: TryFrom<C>,
    C: Copy + Display,
    D: Deref<Target = Dict>,
{
    usize::try_from(constant)
        .ok()
        .and_then(|constant| dict.entry(constant))
        .unwrap_or_else(|| format!("{NULL_PREFIX}{constant}"))
        .into()
}

/// Helper trait for mapping [`StorageValueT`] back into some (higher level) value space
/// by virtue of the schema index that a value appears at (inside a table).
pub trait StorageValueMapping<Output> {
    /// Map the [`StorageValueT`], which appeared at index `layer` to the Output type.
    fn map(&self, value: StorageValueT, layer: usize) -> Output;
}

/// Reads [TableSchema] and [Dict] to convert physical data types into strings.
#[derive(Debug)]
pub struct ValueSerializer<D, S> {
    /// The dict defining the mapping between constants and strings.
    pub dict: D,
    /// The table schema defining the mapping between physical and logical types.
    pub schema: S,
}

impl<D, S> StorageValueMapping<DataValueT> for ValueSerializer<D, S>
where
    D: Deref<Target = Dict>,
    S: Deref<Target = TableSchema>,
{
    fn map(&self, value: StorageValueT, layer: usize) -> DataValueT {
        match self.schema[layer] {
            DataTypeName::String => {
                let StorageValueT::U64(constant) = value else {
                    unreachable!("strings are always encoded as U64 constants")
                };
                DataValueT::String(serialize_constant_with_dict(constant, self.dict.deref()))
            }
            DataTypeName::I64 => match value {
                StorageValueT::I64(val) => DataValueT::I64(val), // TODO: do we allow nulls here? if yes, how do we distinguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
            DataTypeName::U64 => match value {
                StorageValueT::U64(val) => DataValueT::U64(val), // TODO: do we allow nulls here? if yes, how do we distinguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
            DataTypeName::U32 => match value {
                StorageValueT::U32(val) => DataValueT::U32(val), // TODO: do we allow nulls here? if yes, how do we distinguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
            DataTypeName::Float => match value {
                StorageValueT::Float(val) => DataValueT::Float(val), // TODO: do we allow nulls here? if yes, how do we distinguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
            DataTypeName::Double => match value {
                StorageValueT::Double(val) => DataValueT::Double(val), // TODO: do we allow nulls here? if yes, how do we distinguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
        }
    }
}

impl<D, S> StorageValueMapping<String> for ValueSerializer<D, S>
where
    D: Deref<Target = Dict>,
    S: Deref<Target = TableSchema>,
{
    fn map(&self, value: StorageValueT, layer: usize) -> String {
        let data_value: DataValueT = self.map(value, layer);
        data_value.to_string()
    }
}

/// An iterator walking over a trie, while serializing every value.
pub trait TrieSerializer {
    /// The type each field will be serialized to.
    ///
    /// The type must trivially be convertible to a byte array, examples
    /// for this are [`String`], [`str`] and [`[u8]`][byteslice].
    ///
    /// [byteslice]: slice
    type SerializedValue: AsRef<[u8]>;
    /// The type representing an entire row of serialized values.
    type SerializedRecord<'a>: IntoIterator<Item = &'a Self::SerializedValue>
    where
        Self: 'a;

    /// Serializes the next row in the trie and moves the iterator one step.
    fn next_serialized(&mut self) -> Option<Self::SerializedRecord<'_>>;
}

impl<T: TrieSerializer> TrieSerializer for Option<T> {
    type SerializedValue = T::SerializedValue;
    type SerializedRecord<'a> = T::SerializedRecord<'a> where T: 'a;

    fn next_serialized(&mut self) -> Option<Self::SerializedRecord<'_>> {
        match self {
            Some(inner) => inner.next_serialized(),
            None => None,
        }
    }
}
