//! Bindings, the conditions that one input is the same term as a constant
//! or as an expression over the other inputs.
//!
//! A lookup of the constant or of the expression's value can decide such a condition.

use crate::{
    function::{IdentityOperand, OpCode, StackProgram},
    value::DataValue,
};

impl StackProgram {
    /// Returns the condition that input 0 is the same term as this program's result.
    ///
    /// The inputs of this program move up by one position, to make room for input 0.
    /// The comparison is term identity, so that the condition accepts exactly
    /// the rows that a lookup of the result would find.
    pub fn shifted_equality(&self) -> Self {
        let mut operations = Vec::with_capacity(self.operations.len() + 2);
        operations.push(OpCode::Input(0));
        operations.extend(self.operations.iter().map(|&operation| match operation {
            OpCode::Input(index) => OpCode::Input(index + 1),
            other => other,
        }));
        operations.push(OpCode::Same);

        // Shifting a valid program keeps it valid.
        Self::new(operations, self.constants.clone(), self.inputs + 1)
    }

    /// Recognizes a binding, the condition that one input is the same term
    /// as an expression over the other inputs.
    ///
    /// Returns the compared input and the expression as a program,
    /// whose inputs are the other inputs of this program, in their order.
    /// The compared input may stand on either side of the comparison.
    /// This inverts [shifted_equality](Self::shifted_equality).
    ///
    /// Returns `None` if the other side of the comparison
    /// - is a bare input, since the [identity form](crate::function::IdentityCondition) decides
    ///   such a condition on storage values;
    /// - is a constant, which makes a [constant binding](Self::constant_binding);
    /// - reads no input or reads the compared input.
    ///
    /// Returns `None` for a [nondeterministic](Self::is_nondeterministic) program too,
    /// since its expression may yield a different value for every row.
    pub(crate) fn binding(&self) -> Option<(usize, Self)> {
        if self.is_nondeterministic() {
            return None;
        }
        let [comparison @ .., OpCode::Same] = self.operations.as_slice() else {
            return None;
        };

        let (target, expression) = match comparison {
            [OpCode::Input(target), expression @ ..] if is_expression(expression) => {
                (usize::from(*target), expression)
            }
            [expression @ .., OpCode::Input(target)] if is_expression(expression) => {
                (usize::from(*target), expression)
            }
            _ => return None,
        };

        let mut reads_input = false;
        for &operation in expression {
            if let OpCode::Input(index) = operation {
                if usize::from(index) == target {
                    return None;
                }
                reads_input = true;
            }
        }
        if !reads_input || expression.len() < 2 {
            return None;
        }

        // The inputs after the compared one move down by one position.
        let renumbered = expression
            .iter()
            .map(|&operation| match operation {
                OpCode::Input(index) if usize::from(index) > target => OpCode::Input(index - 1),
                other => other,
            })
            .collect();
        // A complete subexpression of a valid program is valid.
        let program = Self::new(renumbered, self.constants.clone(), self.inputs - 1);

        Some((target, program))
    }

    /// Recognizes a constant binding, the condition that an input is the same term as a constant.
    ///
    /// Returns the compared input and the constant,
    /// or `None` if the program is not such a condition.
    /// The constant may stand on either side of the comparison.
    pub(crate) fn constant_binding(&self) -> Option<(usize, DataValue)> {
        let condition = self.identity_specialization()?;
        match condition.operand {
            IdentityOperand::Constant(value) if !condition.negated => {
                Some((condition.input, value))
            }
            _ => None,
        }
    }
}

/// Returns whether the instructions form one complete expression.
///
/// That is, they never pop a value that they did not push, and they leave exactly one value.
fn is_expression(operations: &[OpCode]) -> bool {
    let mut depth = 0usize;
    for operation in operations {
        let Some(below) = depth.checked_sub(operation.pops()) else {
            return false;
        };
        depth = below + 1;
    }

    depth == 1
}
