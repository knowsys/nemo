use crate::physical::{
    datatypes::{DataTypeName, StorageValueT},
    management::database::Dict,
    tabular::traits::table_schema::TableSchema,
};

use super::Dictionary;

/// Reads [TableSchema] and [Dict] to convert physical data types into strings
#[derive(Debug)]
pub struct ValueSerializer<'a> {
    /// The dict defining the mapping between constants and strings
    pub dict: &'a Dict,
    /// The table schema defining the mapping between physical and logical types
    pub schema: &'a TableSchema,
}

impl ValueSerializer<'_> {
    /// Convert a physical [StorageValueT] into a String
    pub fn value_to_string(&self, schema_index: usize, value: StorageValueT) -> String {
        match self.schema[schema_index] {
            DataTypeName::String => {
                let StorageValueT::U64(constant) = value else {
                    unreachable!("strings are always encoded as U64 constants") 
                };
                self.dict
                    .entry(constant as usize)
                    .unwrap_or_else(|| format!("<__Null#{constant}>"))
            }
            DataTypeName::U64 => match value {
                StorageValueT::U64(val) => val.to_string(), // TODO: do we allow nulls here? if yes, how do we distiguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
            DataTypeName::U32 => match value {
                StorageValueT::U32(val) => val.to_string(), // TODO: do we allow nulls here? if yes, how do we distiguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
            DataTypeName::Float => match value {
                StorageValueT::Float(val) => val.to_string(), // TODO: do we allow nulls here? if yes, how do we distiguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
            DataTypeName::Double => match value {
                StorageValueT::Double(val) => val.to_string(), // TODO: do we allow nulls here? if yes, how do we distiguish them?
                _ => unreachable!(
                    "DataType and Storage Type are incompatible. This should never happen!"
                ),
            },
        }
    }
}
