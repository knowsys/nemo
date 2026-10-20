//! The row condition a reader applies before interning.

use nemo_physical::{
    function::{EvaluationContext, StackProgram},
    value::{DataValue, EvaluationError},
};

/// A condition over the columns a reader delivers:
/// a stack program over some of the row's values, read in the listed order.
///
/// A row the program rejects is excluded,
/// and a row it errors on is dropped like a row any condition errors on.
/// Either way the row is counted and never interned, an error by its kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowFilter {
    /// The delivered columns feeding the program, in input order.
    inputs: Vec<usize>,
    /// The condition.
    program: StackProgram,
}

impl RowFilter {
    /// Creates a filter reading the given columns into a program.
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

    /// Returns the condition's verdict on a delivered row, evaluated in the context:
    /// its effective boolean value.
    ///
    /// # Errors
    ///
    /// Returns the evaluation error that left the row undecided.
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

    use crate::testing::greater_than_five;

    #[test]
    fn rows_pass_or_fail_the_condition() {
        let filter = greater_than_five();
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

    #[test]
    fn an_erroring_row_is_rejected_and_names_its_error() {
        let filter = greater_than_five();
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
