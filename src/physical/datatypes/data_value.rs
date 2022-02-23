use super::double::Double;
use super::float::Float;

/// Enum for values of all supported basic types.
/// This should not be used to represent large numbers of values,
/// due to the overhead for each value, but it can be a convenient
/// option to interface with unknown values.
#[derive(Clone, Copy, Debug)]
pub enum DataValueT {
    /// Case u64
    DataValueU64(u64),
    /// Case Float
    DataValueFloat(Float),
    /// Case Double
    DataValuDouble(Double),
}
