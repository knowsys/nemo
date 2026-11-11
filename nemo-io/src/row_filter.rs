//! Row conditions, which a reader applies to its rows before interning them.

use nemo_physical::{
    function::{EvaluationContext, StackProgram},
    value::{DataValue, EvaluationError},
};

/// A condition over the columns that a reader delivers.
///
/// The condition is a stack program whose inputs are some of the delivered columns,
/// in the order of [inputs](Self::inputs).
/// A row for which the condition is false is *excluded*,
/// and a row on which its evaluation fails is dropped.
/// Neither row is interned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowFilter {
    /// The delivered columns that are the program's inputs, in input order.
    inputs: Vec<usize>,
    /// The program that computes the condition.
    program: StackProgram,
}

impl RowFilter {
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
            "a row filter's program must read exactly its input columns"
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
        let filter = greater_than(1, 5);
        assert_eq!(
            filter.evaluate(
                EvaluationContext::now(),
                &[DataValue::string("x"), DataValue::integer(7)]
            ),
            Ok(true)
        );
        assert_eq!(
            filter.evaluate(
                EvaluationContext::now(),
                &[DataValue::string("x"), DataValue::integer(3)]
            ),
            Ok(false)
        );
    }

    /// A row on which the condition fails yields the error, and the next row is evaluated afresh.
    #[test]
    fn an_erroring_row_is_rejected_and_names_its_error() {
        let filter = greater_than(1, 5);
        let row = [DataValue::string("x"), DataValue::string("seven")];
        assert_eq!(
            filter.evaluate(EvaluationContext::now(), &row),
            Err(EvaluationError::Incomparable)
        );
        assert_eq!(
            filter.evaluate(
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
        RowFilter::new(vec![0], program);
    }
}
