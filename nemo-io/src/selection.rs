//! Selections: which columns of a resource a reader delivers, and which rows it admits.
//!
//! The *delivered* columns are the columns of the resource that the reader keeps.
//! The *width* of a resource is the number of its columns,
//! and the *arity* of a read is the number of delivered columns.

use nemo_physical::{
    function::{EvaluationContext, StackProgram},
    value::{DataValue, EvaluationError},
};

/// The projection and restriction that a reader applies to its resource.
///
/// A selection says which columns of the resource are delivered,
/// and gives a [condition](RowCondition) over the delivered columns that a row must pass.
/// A column that is not delivered still decides whether a row is well-formed.
/// A row that the condition excludes is never interned.
///
/// The default selection delivers every column and admits every row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowSelection {
    /// For each column of the resource, whether it is delivered, or `None` to deliver all.
    ///
    /// A column past the end of the list is delivered.
    pub columns: Option<Vec<bool>>,
    /// The condition that delivered rows must pass, or `None` to admit all rows.
    pub condition: Option<RowCondition>,
}

impl RowSelection {
    /// Returns whether the resource's column at `position` is delivered.
    pub fn keeps(&self, position: usize) -> bool {
        self.columns
            .as_ref()
            .is_none_or(|columns| columns.get(position).copied().unwrap_or(true))
    }

    /// Returns the number of delivered columns of a resource of the given width.
    pub fn arity(&self, width: usize) -> usize {
        (0..width).filter(|&position| self.keeps(position)).count()
    }

    /// Returns whether a delivered row passes the condition, which every row passes if there is
    /// none.
    ///
    /// `context` is the evaluation context of the condition, such as the instant its `NOW()`
    /// reads.
    ///
    /// # Errors
    ///
    /// Returns the evaluation error that left the row undecided,
    /// as [RowCondition::evaluate] does.
    ///
    /// # Panics
    ///
    /// Panics if the row has no value at a column that the condition reads.
    pub fn evaluate(
        &self,
        context: EvaluationContext,
        row: &[DataValue],
    ) -> Result<bool, EvaluationError> {
        self.condition
            .as_ref()
            .map_or(Ok(true), |condition| condition.evaluate(context, row))
    }

    /// Returns whether a delivered row passes the condition,
    /// counting an evaluation error as a failure.
    ///
    /// # Panics
    ///
    /// Panics if the row has no value at a column that the condition reads.
    pub fn admits(&self, context: EvaluationContext, row: &[DataValue]) -> bool {
        self.evaluate(context, row).unwrap_or(false)
    }
}

/// A condition over the columns that a reader delivers.
///
/// The condition is a stack program whose inputs are some of the delivered columns,
/// in the order of [inputs](Self::inputs).
/// A row for which the condition is false is *excluded*,
/// and a row on which its evaluation fails is dropped.
/// Neither row is interned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowCondition {
    /// The delivered columns that are the program's inputs, in input order.
    inputs: Vec<usize>,
    /// The program that computes the condition.
    program: StackProgram,
}

impl RowCondition {
    /// Creates a condition that passes the delivered columns `inputs`, in this order,
    /// to `program`.
    ///
    /// # Panics
    ///
    /// Panics if the program does not take exactly that many inputs.
    pub fn new(inputs: Vec<usize>, program: StackProgram) -> Self {
        assert_eq!(
            program.inputs(),
            inputs.len(),
            "a row condition's program must read exactly its input columns"
        );

        Self { inputs, program }
    }

    /// Returns the delivered columns the condition reads.
    pub fn inputs(&self) -> &[usize] {
        &self.inputs
    }

    /// Returns the condition's program.
    pub fn program(&self) -> &StackProgram {
        &self.program
    }

    /// Returns the condition's effective boolean value for a delivered row.
    ///
    /// `context` is the evaluation context, such as the instant that `NOW()` reads.
    ///
    /// # Errors
    ///
    /// Returns the evaluation error that left the row undecided.
    ///
    /// # Panics
    ///
    /// Panics if the row has no value at one of the condition's [inputs](Self::inputs).
    pub fn evaluate(
        &self,
        context: EvaluationContext,
        row: &[DataValue],
    ) -> Result<bool, EvaluationError> {
        let inputs: Vec<_> = self
            .inputs
            .iter()
            .map(|&position| row[position].as_ref())
            .collect();
        let mut stack = Vec::with_capacity(self.program.stack_bound());

        self.program.filter(context, &mut stack, &inputs)
    }
}

#[cfg(test)]
mod tests {
    use nemo_physical::function::OpCode;

    use super::*;

    use crate::testing::greater_than;

    /// A row passes if the condition holds for it, and fails otherwise.
    #[test]
    fn rows_pass_or_fail_the_condition() {
        let condition = greater_than(1, 5);
        assert_eq!(
            condition.evaluate(
                EvaluationContext::now(),
                &[DataValue::string("x"), DataValue::integer(7)]
            ),
            Ok(true)
        );
        assert_eq!(
            condition.evaluate(
                EvaluationContext::now(),
                &[DataValue::string("x"), DataValue::integer(3)]
            ),
            Ok(false)
        );
    }

    /// A row on which the condition fails yields the error, and the next row is evaluated afresh.
    #[test]
    fn an_erroring_row_is_rejected_and_names_its_error() {
        let condition = greater_than(1, 5);
        let row = [DataValue::string("x"), DataValue::string("seven")];
        assert_eq!(
            condition.evaluate(EvaluationContext::now(), &row),
            Err(EvaluationError::Incomparable)
        );
        assert_eq!(
            condition.evaluate(
                EvaluationContext::now(),
                &[DataValue::string("x"), DataValue::integer(7)]
            ),
            Ok(true)
        );
    }

    /// Creating a condition whose program takes a different number of inputs than it has columns
    /// panics.
    #[test]
    #[should_panic(expected = "exactly its input columns")]
    fn the_input_count_is_checked() {
        let program = StackProgram::new(
            vec![OpCode::Input(0), OpCode::Input(1), OpCode::Equal],
            Vec::new(),
            2,
        );
        RowCondition::new(vec![0], program);
    }
}
