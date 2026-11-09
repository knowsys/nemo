//! The specification of a filter.

use crate::{
    function::StackProgram,
    plan::{Marker, PlanError, spec::Expression},
};

/// The condition of a [filter](crate::plan::OperationPlan::filter):
/// a program over markers of the filtered relation.
///
/// A row for which the condition does not hold is dropped.
#[derive(Debug, Clone)]
pub struct FilterSpec {
    /// The markers whose values are the program's inputs, in order.
    pub(crate) inputs: Vec<Marker>,
    /// The condition.
    pub(crate) program: StackProgram,
}

impl FilterSpec {
    /// Creates the condition an expression states.
    ///
    /// # Errors
    ///
    /// Returns [ExpressionTooLarge](PlanError::ExpressionTooLarge)
    /// if the expression reads more columns or constants than an instruction can address.
    ///
    /// # Panics
    ///
    /// Panics if the expression is malformed. See [Expression].
    pub fn new(condition: &Expression) -> Result<Self, PlanError> {
        let (inputs, program) = condition.compile()?;

        Ok(Self::from_program(inputs, program))
    }

    /// Creates the condition a compiled program states.
    ///
    /// `inputs` are the markers whose values are the program's inputs, in order.
    ///
    /// # Panics
    ///
    /// Panics if the program has a different number of inputs than `inputs` has markers.
    pub fn from_program(inputs: Vec<Marker>, program: StackProgram) -> Self {
        assert_eq!(
            program.inputs(),
            inputs.len(),
            "a filter's program arity must match its markers"
        );

        Self { inputs, program }
    }
}
