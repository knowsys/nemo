//! This module defines all supported numeric functions.

/// Trait for types on which numeric operations are defines
pub(crate) trait OperableNumeric {
    /// Addition of two numbers
    ///
    /// Returns the sum of `first` and `second`
    /// or `None` if sum overflows or inputs are non-numeric.
    #[allow(unused)]
    fn numeric_addition(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Subtraction of two numbers
    ///
    /// Returns the difference of `first` and `second`
    /// or `None` if inputs are non-numeric
    /// or the result cannot be represented within the range of the result's value type.
    #[allow(unused)]
    fn numeric_subtraction(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Multiplication of two numbers
    ///
    /// Returns the product of `first` and `second`
    /// or `None` if inputs are non-numeric
    /// or the result cannot be represented within the range of the result's value type.
    #[allow(unused)]
    fn numeric_multiplication(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Division of two numbers
    ///
    /// Returns the ratio of `first` and `second`
    /// or `None` if inputs are non-numeric
    /// or the result cannot be represented within the range of the result's value type.
    #[allow(unused)]
    fn numeric_division(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Logarithm of two numbers w.r.t. an arbitrary numeric base
    ///
    /// Returns the logarithm of the first parameter,
    /// where the base is given by the second parameter.    
    ///
    /// Returns `None` if the input parameters are not numeric
    /// or if the result cannot be represented within the range of the result's value type.
    #[allow(unused)]
    fn numeric_logarithm(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Raising a numeric value to some power
    ///
    /// Returns the first parameter raised to the power of the second parameter.
    ///
    /// Returns `None` if the input parameters are not numeric
    /// or if the result cannot be represented within the range of the result's value type.
    #[allow(unused)]
    fn numeric_power(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Remainder operation
    ///
    /// Returns the remainder of the (truncated) division `first / second`.
    /// The value of the result has always the same sign as `second`.
    ///
    /// Returns `None` if `second` is zero.
    ///
    /// Returns `None` if the input parameters are not numeric
    /// or if the result cannot be represented within the range of the result's value type.
    #[allow(unused)]
    fn numeric_remainder(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Absolute value of numeric values
    ///
    /// Returns the absolute value of the given parameter.
    ///
    /// Returns `None` if the input parameter is not a numeric value space.
    #[allow(unused)]
    fn numeric_absolute(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Negation of a numeric value
    ///
    /// Returns the multiplicative inverse of the input parameter.
    ///
    /// Returns `None` if the input parameter is not a numeric value space.
    #[allow(unused)]
    fn numeric_negation(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Square root of a numeric value
    ///
    /// Returns the square root of the given input paramter.
    ///
    /// Returns `None` if the input parameter is not a numeric value space
    /// or is negative.
    #[allow(unused)]
    fn numeric_squareroot(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Sine of a numeric value
    ///
    /// Returns the sine of the input parameter.
    ///
    /// Returns `None` if the input paramter is not in a floating point value space.
    #[allow(unused)]
    fn numeric_sine(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Cosine of a numeric value
    ///
    /// Returns the cosine of the input parameter.
    ///
    /// Returns `None` if the input paramter is not in a floating point value space.
    #[allow(unused)]
    fn numeric_cosine(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Tangent of a numeric value
    ///
    /// Returns the tangent of the input parameter.
    ///
    /// Returns `None` if the input paramter is not in a floating point value space.
    #[allow(unused)]
    fn numeric_tangent(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Rounding of a numeric value
    ///
    /// Returns the nearest integer of the input parameter.
    /// If the result is half-way between two integers, round away from 0.0.
    ///
    /// Returns `None` if the input parameter is not from a numeric value space.
    #[allow(unused)]
    fn numeric_round(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Rounding up to the smallest integer less than or equal than input parameter
    ///
    /// Returns the smallest integer less than or equal than input parameter
    ///
    /// Returns `None` if the input parameter is not from a numeric value space.
    #[allow(unused)]
    fn numeric_ceil(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Rounding down to the largest integer greater than or equal than input parameter
    ///
    /// Returns the largest integer greater than or equal than input parameter
    ///
    /// Returns `None` if the input parameter is not from a numeric value space.
    #[allow(unused)]
    fn numeric_floor(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Less than comparison of two numbers
    ///
    /// Returns `true` from the boolean value space
    /// if the first argument is smaller than the second argument,
    /// and `false` otherwise.
    ///
    /// Returns `None` if the arguments are not from the numeric value space.
    #[allow(unused)]
    fn numeric_less_than(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Less than or equals comparison of two numbers
    ///
    /// Returns `true` from the boolean value space
    /// if the first argument is smaller than or equal to the second argument,
    /// and `false` otherwise.
    ///
    /// Returns `None` if the arguments are not from the numeric value space.
    #[allow(unused)]
    fn numeric_less_than_eq(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Greater than comparison of two numbers
    ///
    /// Returns `true` from the boolean value space
    /// if the first argument is greater than the second argument,
    /// and `false` otherwise.
    ///
    /// Returns `None` if the arguments are not from the numeric value space.
    #[allow(unused)]
    fn numeric_greater_than(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Greater than or equals comparison of two numbers
    ///
    /// Returns `true` from the boolean value space
    /// if the first argument is greater than or equal to the second argument,
    /// and `false` otherwise.
    ///
    /// Returns `None` if the arguments are not from the numeric value space.
    #[allow(unused)]
    fn numeric_greater_than_eq(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Numeric summation
    ///
    /// Returns the sum of the given parameters.
    ///
    /// Returns `None` if the input parameters are not numeric
    /// or no input parameters are given.
    /// Returns `None` if the result (or an intermediate result) cannot be represented
    /// within the range of the numeric value type.
    #[allow(unused)]
    fn numeric_sum(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Numeric product
    ///
    /// Returns the product of the given parameters.
    ///
    /// Returns `None` if the input parameters are not numeric
    /// or no input parameters are given.
    /// Returns `None` if the result (or an intermediate result) cannot be represented
    /// within the range of the numeric value type.
    #[allow(unused)]
    fn numeric_product(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Numeric minimum
    ///
    /// Returns the minimum of the given parameters.
    ///
    /// Returns `None` if the input parameters are not numeric
    /// or no input parameters are given.
    #[allow(unused)]
    fn numeric_minimum(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Numeric maximum
    ///
    /// Returns the maximum of the given parameters.
    ///
    /// Returns `None` if the input parameters are not numeric
    /// or no input parameters are given.
    #[allow(unused)]
    fn numeric_maximum(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Lukasiewicz t-norm
    ///
    /// Returns the Lukasiewicz t-norm of the given parameters.
    ///
    /// Returns `None` if the input parameters are not numeric
    /// or no input parameters are given.
    /// Returns `None` if the input parameters are not of a floating point type.
    #[allow(unused)]
    fn numeric_lukasiewicz(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Bitwise and
    ///
    /// For a list of integers,
    /// returns the integer resulting from perfoming an "and" on their bit representation.
    ///
    /// Returns `None` if the input parameters are not integers or no input parameters are given.
    #[allow(unused)]
    fn numeric_bit_add(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Bitwise or
    ///
    /// For a list of integers,
    /// returns the integer resulting from performing an "or" on their bit representation.
    ///
    /// Returns the zero from the integer value space if no parameters are given.
    /// Returns `None` if the input parameters are not integers or no input parameters are given.
    #[allow(unused)]
    fn numeric_bit_or(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Bitwise xor
    ///
    /// For a list of integers,
    /// returns the integer resulting from performing an "xor" on their bit representation.
    ///
    /// Returns zero from the integer value space if no parameters are given.
    /// Returns `None` if the input parameters are not integers or no input parameters are given.
    #[allow(unused)]
    fn numeric_bit_xor(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Bitwise Left shift
    ///
    /// Returns `None` if the input parameter pair are not integers.
    #[allow(unused)]
    fn numeric_bit_shift_left(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Bitwise arithmetic right shift
    ///
    /// Returns `None` if the input parameter pair are not integers.
    #[allow(unused)]
    fn numeric_bit_shift_right(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Bitwise logical (unsigned) right shift
    ///
    /// Returns `None` if the input parameter pair are not integers or no input parameters are given.
    #[allow(unused)]
    fn numeric_bit_shift_right_unsigned(parameter: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}
