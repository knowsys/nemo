//! Selections: which columns of a resource a reader delivers, and which rows it admits.
//!
//! The *delivered* columns are the columns of the resource that the reader keeps.
//! The *width* of a resource is the number of its columns,
//! and the *arity* of a read is the number of delivered columns.

use nemo_physical::{
    function::{EvaluationContext, StackProgram, StackValue},
    value::{DataValue, EvaluationError, ValueRef},
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
        let mut buffers = ConditionBuffers::default();

        self.evaluate_with(context, &mut buffers, |position| row[position].as_ref())
    }

    /// Returns the condition's effective boolean value for a delivered row,
    /// as [evaluate](Self::evaluate) does, without allocating for the row.
    ///
    /// `value` returns the row's value at a delivered column,
    /// and is called for the condition's [inputs](Self::inputs) only.
    /// `buffers` are kept between the rows of a read.
    pub(crate) fn evaluate_with<'a>(
        &'a self,
        context: EvaluationContext,
        buffers: &mut ConditionBuffers,
        value: impl Fn(usize) -> ValueRef<'a>,
    ) -> Result<bool, EvaluationError> {
        let mut inputs: Vec<ValueRef<'a>> = recycled(std::mem::take(&mut buffers.inputs));
        let mut stack: Vec<StackValue<'a>> = recycled(std::mem::take(&mut buffers.stack));

        inputs.extend(self.inputs.iter().map(|&position| value(position)));
        let result = self.program.filter(context, &mut stack, &inputs);

        buffers.inputs = recycled(inputs);
        buffers.stack = recycled(stack);

        result
    }
}

/// The buffers of a condition's evaluations, which are kept between the rows of a read.
#[derive(Debug, Default)]
pub(crate) struct ConditionBuffers {
    /// The buffer of the condition's inputs for one row.
    inputs: Vec<ValueRef<'static>>,
    /// The buffer of the evaluation stack.
    stack: Vec<StackValue<'static>>,
}

/// Returns an emptied vector as a vector of another type of the same layout,
/// such as the same type with another lifetime.
///
/// The vector keeps its allocation, since collecting in place reuses it.
fn recycled<T, U>(mut values: Vec<T>) -> Vec<U> {
    values.clear();

    values
        .into_iter()
        .map(|_| unreachable!("the vector is empty"))
        .collect()
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

    /// The buffers of an evaluation keep their allocations for the next row.
    #[test]
    fn evaluations_reuse_their_buffers() {
        let condition = greater_than(1, 5);
        let mut buffers = ConditionBuffers::default();
        let mut evaluate = |text: &str, number: i64| {
            // The row's values live only for this evaluation.
            let row = [DataValue::string(text), DataValue::integer(number)];
            let result =
                condition.evaluate_with(EvaluationContext::now(), &mut buffers, |position| {
                    row[position].as_ref()
                });

            (result, buffers.inputs.as_ptr(), buffers.stack.as_ptr())
        };

        let (first, inputs, stack) = evaluate("x", 7);
        assert_eq!(first, Ok(true));
        for number in 0..100 {
            assert_eq!(evaluate("y", number), (Ok(number > 5), inputs, stack));
        }
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
