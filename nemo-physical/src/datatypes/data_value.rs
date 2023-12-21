use crate::datavalues::AnyDataValue;
use crate::dictionary::DvDict;
use crate::management::database::Dict;

use super::double::Double;
use super::float::Float;
use super::{DataTypeName, StorageValueT};

/// An Api wrapper fot the physical string type
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalString(String);

impl From<String> for PhysicalString {
    fn from(value: String) -> Self {
        PhysicalString(value)
    }
}

impl From<PhysicalString> for String {
    fn from(value: PhysicalString) -> Self {
        value.0
    }
}

impl<'a> From<&'a PhysicalString> for &'a str {
    fn from(value: &'a PhysicalString) -> Self {
        &value.0
    }
}

impl std::fmt::Display for PhysicalString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Enum for values that pass the barrier of the physical layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataValueT {
    /// Case String
    String(PhysicalString),

    // StorageValueT from here on
    //
    /// Case u32
    U32(u32),
    /// Case u64
    U64(u64),
    /// Case i64
    I64(i64),
    /// Case Float
    Float(Float),
    /// Case Double
    Double(Double),
}

impl DataValueT {
    /// Returns the type of the DataValueT as DataTypeName
    pub fn get_type(&self) -> DataTypeName {
        match self {
            Self::String(_) => DataTypeName::String,
            Self::U32(_) => DataTypeName::U32,
            Self::U64(_) => DataTypeName::U64,
            Self::I64(_) => DataTypeName::I64,
            Self::Float(_) => DataTypeName::Float,
            Self::Double(_) => DataTypeName::Double,
        }
    }

    /// Get the appropriate [`StorageValueT`]` for the given [`DataValueT`].
    /// May change the given dictionary.
    pub(crate) fn to_storage_value_mut(&self, dict: &mut Dict) -> StorageValueT {
        match self {
            Self::String(val) => {
                // dictionary indices
                let _id = dict
                    .add_datavalue(AnyDataValue::new_string(val.clone().into()))
                    .value();
                // TupleWriter::storage_value_for_usize(id)
                todo!("DataValueT will go")
            }
            Self::U32(val) => StorageValueT::Id32(*val),
            Self::U64(val) => StorageValueT::Id64(*val),
            Self::I64(val) => StorageValueT::Int64(*val),
            Self::Float(val) => StorageValueT::Float(*val),
            Self::Double(val) => StorageValueT::Double(*val),
        }
    }

    /// Get the appropriate [`StorageValueT`]` for the given [`DataValueT`]
    pub fn to_storage_value(&self, dict: &Dict) -> Option<StorageValueT> {
        match self {
            Self::String(val) => {
                if let Some(_uid) =
                    dict.datavalue_to_id(&AnyDataValue::new_string((*val).clone().into()))
                {
                    todo!("DataValueT will go")
                } else {
                    None
                }
            }
            Self::U32(val) => Some(StorageValueT::Id32(*val)),
            Self::U64(val) => Some(StorageValueT::Id64(*val)),
            Self::I64(val) => Some(StorageValueT::Int64(*val)),
            Self::Float(val) => Some(StorageValueT::Float(*val)),
            Self::Double(val) => Some(StorageValueT::Double(*val)),
        }
    }
}

impl std::fmt::Display for DataValueT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(val) => write!(f, "{val}"),
            Self::U32(val) => write!(f, "{val}"),
            Self::U64(val) => write!(f, "{val}"),
            Self::I64(val) => write!(f, "{val}"),
            Self::Float(val) => write!(f, "{val}"),
            Self::Double(val) => write!(f, "{val}"),
        }
    }
}

/// Iterator over one kind of possible data values
#[allow(missing_debug_implementations)]
pub enum DataValueIteratorT<'a> {
    /// String Variant
    String(Box<dyn Iterator<Item = PhysicalString> + 'a>),
    /// U32 Variant
    U32(Box<dyn Iterator<Item = u32> + 'a>),
    /// U64 Variant
    U64(Box<dyn Iterator<Item = u64> + 'a>),
    /// I64 Variant
    I64(Box<dyn Iterator<Item = i64> + 'a>),
    /// Float Variant
    Float(Box<dyn Iterator<Item = Float> + 'a>),
    /// Double Variant
    Double(Box<dyn Iterator<Item = Double> + 'a>),
}
