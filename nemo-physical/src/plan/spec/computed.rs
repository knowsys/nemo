//! Specification of the computed column operation.

use crate::{
    function::StackProgram,
    plan::{Marker, PlanError, spec::Expression},
};

/// A computed column: a computed value appended to every row of the input.
#[derive(Debug, Clone)]
pub struct ComputedColumnSpec {
    /// The markers feeding the program, in program input order.
    ///
    /// Without inputs, the column computes a constant value.
    pub(crate) inputs: Vec<Marker>,
    /// The program computing the value.
    pub(crate) program: StackProgram,
    /// The fresh marker naming the result column.
    pub(crate) result: Marker,
}

impl ComputedColumnSpec {
    /// Creates the column holding an expression's value, named by a fresh marker.
    ///
    /// # Errors
    ///
    /// Returns [ExpressionTooLarge](PlanError::ExpressionTooLarge)
    /// if the expression reads more columns or constants than an instruction addresses.
    ///
    /// # Panics
    ///
    /// Panics if the expression is malformed. See [Expression].
    pub fn new(value: &Expression, result: Marker) -> Result<Self, PlanError> {
        let (inputs, program) = value.compile()?;

        Ok(Self::from_program(inputs, program, result))
    }

    /// Creates the column holding a compiled program's value
    /// over the given markers, in program input order, named by a fresh marker.
    ///
    /// # Panics
    ///
    /// Panics if the program reads a different number of inputs than markers are given.
    pub fn from_program(inputs: Vec<Marker>, program: StackProgram, result: Marker) -> Self {
        assert_eq!(
            program.inputs(),
            inputs.len(),
            "a computed column's program arity must match its markers"
        );

        Self {
            inputs,
            program,
            result,
        }
    }
}
