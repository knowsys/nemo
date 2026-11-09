//! The identity form of a program.
//!
//! A program that only asks whether two terms are identical, or whether they differ,
//! can be decided on their storage values,
//! since interning is canonical and gives identical terms identical storage values.
//! This needs neither decoding the inputs nor running the interpreter,
//! whatever the tags of the columns that the inputs read.
//! The identity test never fails, so the form and the program always agree.

use crate::{function::opcode::OpCode, value::DataValue};

/// What an [IdentityCondition] compares its input with.
#[derive(Debug, Clone)]
pub(crate) enum IdentityOperand {
    /// Another input, by its index.
    Input(usize),
    /// A constant.
    Constant(DataValue),
}

/// The identity form of a stack program,
/// the condition that an input is, or is not, the same term as another input or a constant.
#[derive(Debug, Clone)]
pub(crate) struct IdentityCondition {
    /// The index of the compared input.
    pub(crate) input: usize,
    /// What the input is compared with.
    pub(crate) operand: IdentityOperand,
    /// Whether the condition holds if the terms differ, rather than if they are identical.
    pub(crate) negated: bool,
}

impl IdentityCondition {
    /// Recognizes the identity form of the program with the given instructions and constants.
    ///
    /// Returns `None` if the program does more than compare two terms for identity.
    pub(crate) fn recognize(operations: &[OpCode], constants: &[DataValue]) -> Option<Self> {
        let (comparison, negated) = match operations {
            [comparison @ .., OpCode::Same, OpCode::Not] => (comparison, true),
            [comparison @ .., OpCode::Same] => (comparison, false),
            _ => return None,
        };
        let (input, operand) = match *comparison {
            [OpCode::Input(left), OpCode::Input(right)] => {
                (left, IdentityOperand::Input(usize::from(right)))
            }
            [OpCode::Input(input), OpCode::Constant(index)]
            | [OpCode::Constant(index), OpCode::Input(input)] => (
                input,
                IdentityOperand::Constant(constants[usize::from(index)].clone()),
            ),
            _ => return None,
        };

        Some(Self {
            input: usize::from(input),
            operand,
            negated,
        })
    }
}
