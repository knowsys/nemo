//! Expressions: computations and conditions over the columns of a relation,
//! compiled into [programs](StackProgram).

use crate::{
    function::{OpCode, StackProgram},
    plan::{Marker, PlanError},
    value::DataValue,
};

/// An expression over the columns of a relation.
///
/// It names columns by marker and holds its constants in place.
/// An application is well-formed if it takes one operand per value its instruction pops,
/// and if its instruction is neither [Input](OpCode::Input) nor [Constant](OpCode::Constant):
/// columns and constants take their place.
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     dictionary::Dictionary,
///     function::{EvaluationContext, OpCode},
///     management::{ColumnOrder, Revision, TableManager},
///     plan::{ComputedColumnSpec, Expression, FilterSpec, OperationPlan},
///     storage::StorageValue,
///     trie::RowBuffer,
/// };
///
/// let int = StorageValue::integer;
/// let mut manager = TableManager::new();
/// let pairs = manager.register("pair", 2);
/// let mut rows = RowBuffer::new(2);
/// for (x, y) in [(1, 2), (3, 2), (2, 5)] {
///     rows.push_row(&[int(x), int(y)]);
/// }
/// manager.add_rows(pairs, Revision(1), ColumnOrder::identity(2), rows);
///
/// // The pairs with x < y, extended by x + y.
/// let mut plan = OperationPlan::new();
/// let [x, y, sum] = plan.markers();
/// let rows = plan.load(pairs, Revision(0)..Revision(2), vec![x, y]);
/// let less = Expression::apply(OpCode::Less, [Expression::column(x), Expression::column(y)]);
/// let ordered = plan.filter(rows, FilterSpec::new(&less).expect("two columns"));
/// let total = Expression::apply(OpCode::Add, [Expression::column(x), Expression::column(y)]);
/// let column = ComputedColumnSpec::new(&total, sum).expect("two columns");
/// let extended = plan.compute(ordered, column);
/// plan.output(extended);
///
/// let mut dictionary = Dictionary::new();
/// let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
/// let mut found = Vec::new();
/// results.outputs[0].for_each_row(|row| found.push(row.to_vec()));
/// assert_eq!(found, [[int(1), int(2), int(3)], [int(2), int(5), int(7)]]);
/// ```
#[derive(Debug, Clone)]
pub enum Expression {
    /// The value of a column.
    Column(Marker),
    /// A constant.
    Constant(DataValue),
    /// An instruction applied to the values of its operands.
    Apply(OpCode, Vec<Expression>),
}

impl Expression {
    /// Returns the value of a column.
    pub fn column(marker: Marker) -> Self {
        Self::Column(marker)
    }

    /// Returns a constant.
    pub fn constant(value: DataValue) -> Self {
        Self::Constant(value)
    }

    /// Returns an instruction applied to the values of its operands.
    pub fn apply(operation: OpCode, operands: impl IntoIterator<Item = Expression>) -> Self {
        Self::Apply(operation, operands.into_iter().collect())
    }

    /// Compiles the expression into a program,
    /// and returns the markers feeding it, in program input order, with the program.
    ///
    /// Returns [ExpressionTooLarge](PlanError::ExpressionTooLarge)
    /// if the expression reads more columns or constants than an instruction addresses.
    ///
    /// # Panics
    ///
    /// Panics if an application's operands do not match what its instruction pops,
    /// or if an applied instruction reads an input or a constant itself.
    pub(super) fn compile(&self) -> Result<(Vec<Marker>, StackProgram), PlanError> {
        let mut operations = Vec::new();
        let mut constants = Vec::new();
        let mut inputs = Vec::new();
        self.emit(&mut operations, &mut constants, &mut inputs)?;

        // Emission checked every operand count and index, so the program is valid.
        let program = StackProgram::new(operations, constants, inputs.len());

        Ok((inputs, program))
    }

    /// Emits the instructions of the expression, postorder,
    /// registering each column on first sight and each constant as it comes.
    fn emit(
        &self,
        operations: &mut Vec<OpCode>,
        constants: &mut Vec<DataValue>,
        inputs: &mut Vec<Marker>,
    ) -> Result<(), PlanError> {
        match self {
            Expression::Column(marker) => {
                let index = match inputs.iter().position(|input| input == marker) {
                    Some(index) => index,
                    None => {
                        inputs.push(*marker);
                        inputs.len() - 1
                    }
                };

                operations.push(OpCode::Input(operand(index)?));
            }
            Expression::Constant(value) => {
                constants.push(value.clone());
                operations.push(OpCode::Constant(operand(constants.len() - 1)?));
            }
            Expression::Apply(operation, operands) => {
                assert!(
                    !matches!(operation, OpCode::Input(_) | OpCode::Constant(_)),
                    "an expression reads columns and constants as operands, not as instructions"
                );
                assert_eq!(
                    operands.len(),
                    operation.pops(),
                    "an instruction needs one operand per value it pops"
                );

                for operand in operands {
                    operand.emit(operations, constants, inputs)?;
                }
                operations.push(*operation);
            }
        }

        Ok(())
    }
}

/// Returns an index as an instruction addresses it,
/// or [ExpressionTooLarge](PlanError::ExpressionTooLarge) beyond that.
fn operand(index: usize) -> Result<u16, PlanError> {
    u16::try_from(index).map_err(|_| PlanError::ExpressionTooLarge {
        limit: StackProgram::OPERAND_LIMIT,
    })
}
