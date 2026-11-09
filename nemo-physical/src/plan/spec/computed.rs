//! The specification of a computed column.

use crate::{
    function::StackProgram,
    plan::{Marker, PlanError, spec::Expression},
};

/// A computed column: a computed value appended to every row of the input.
#[derive(Debug, Clone)]
pub struct ComputedColumnSpec {
    /// The markers whose values are the program's inputs, in order.
    ///
    /// A column without inputs has the same value on every row,
    /// unless its program is nondeterministic.
    pub(crate) inputs: Vec<Marker>,
    /// The program computing the value.
    pub(crate) program: StackProgram,
    /// The fresh marker naming the result column.
    pub(crate) result: Marker,
}

impl ComputedColumnSpec {
    /// Creates the column of an expression's value.
    ///
    /// `result` is the fresh marker naming the column.
    ///
    /// # Errors
    ///
    /// Returns [ExpressionTooLarge](PlanError::ExpressionTooLarge)
    /// if the expression reads more columns or constants than an instruction can address.
    ///
    /// # Panics
    ///
    /// Panics if the expression is malformed. See [Expression].
    pub fn new(value: &Expression, result: Marker) -> Result<Self, PlanError> {
        let (inputs, program) = value.compile()?;

        Ok(Self::from_program(inputs, program, result))
    }

    /// Creates the column of a compiled program's value.
    ///
    /// `inputs` are the markers whose values are the program's inputs, in order,
    /// and `result` is the fresh marker naming the column.
    ///
    /// # Panics
    ///
    /// Panics if the program has a different number of inputs than `inputs` has markers.
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
