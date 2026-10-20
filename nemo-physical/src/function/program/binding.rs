//! The bindings: conditions that one input is the same term as a constant
//! or as an expression over the others, which a lookup of that value decides.

use crate::{
    function::{IdentityOperand, OpCode, StackProgram},
    value::DataValue,
};

impl StackProgram {
    /// Returns the condition that input 0 is the same term as this program's result
    /// over the inputs after it:
    /// this program's inputs shift up one position to make room for the compared value.
    ///
    /// The comparison is term identity, so that the condition accepts exactly
    /// the rows a lookup of the result would find.
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

    /// Recognizes a binding: the condition that one input is the same term
    /// as an expression over the other inputs.
    ///
    /// Returns the compared input and the expression,
    /// whose inputs are the program's other inputs, in order.
    /// The compared input may stand on either side of the comparison.
    /// The inverse of [shifted_equality](Self::shifted_equality).
    ///
    /// A bare input on the other side is no binding:
    /// the identity form decides such a condition on storage values.
    /// A constant there is a [constant binding](Self::constant_binding).
    /// Neither is an expression without inputs, nor a nondeterministic one,
    /// which draws once per row.
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

        // The expression reads the other inputs, renumbered past the compared one.
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

    /// Recognizes a constant binding: the condition that an input is the same term as a constant.
    ///
    /// Returns the compared input and the constant.
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

/// Returns whether instructions form one complete expression:
/// they never pop below their start, and leave exactly one value.
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
