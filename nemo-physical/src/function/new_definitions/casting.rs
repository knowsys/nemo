//! This module defines all supported casting functions.

pub(crate) mod any;
pub(crate) mod storage_t;

/// Trait for types on which boolean operations are defined
pub(crate) trait OperableCasting {
    /// Casting of values into 64-bit integers
    ///
    /// Returns an integer number representing its input
    /// as close as possible.
    ///
    /// This operation is defined for
    ///   * signed 64-bit integers
    ///   * unsigned integers that can be represented in 63 bits
    ///   * floating point numbers that don't contain a fractional part
    ///   * booleans
    ///   * strings
    ///   * other
    ///
    /// Returns `None` when called on values outside the range described above
    /// or if the value cannot be converted to an integer.
    #[allow(unused)]
    fn casting_into_integer(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Casting of a value into a 32bit floating point number
    ///
    /// Returns a 32-bit floating point number representing its input
    /// as close as possible.
    ///
    /// This operation is defined for:
    ///   * 32bit floating point numbers
    ///   * all integers
    ///   * 64bit floating point numbers
    ///   * strings
    ///   * other
    ///
    /// Returns `None` when called on values outside the range described above
    /// or if value cannot be converted into a 32-bit float.
    #[allow(unused)]
    fn casting_into_float(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Casting of a value into a 64-bit floating point number
    ///
    /// Returns a 64-bit floating point number representing its input
    /// as close as possible.
    ///
    /// This operation is defined for:
    ///   * 64bit floating point numbers
    ///   * all integers
    ///   * 32-bit floating point numbers
    ///   * strings
    ///   * other
    ///
    /// Returns `None` when called on values outside the range described above
    /// or if value cannot be converted into a 64-bit float.
    #[allow(unused)]
    fn casting_into_double(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Casting of a string value into an IRI
    ///
    /// Returns an IRI with the same content as the given string.
    ///
    /// Returns `None` when called on values other than plain strings.
    #[allow(unused)]
    fn casting_into_iri(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}
