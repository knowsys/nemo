//! The identity form of a program.
//!
//! A program that only asks whether two terms are identical, or that they are not,
//! is decided on their storage values:
//! interning is canonical, so identical terms have identical storage values.
//! The executor then neither decodes the inputs nor runs the interpreter,
//! whatever the tags of the columns they read.
//! Identity never errors, so the form and the program cannot disagree.

use crate::{function::opcode::OpCode, value::DataValue};

/// What an [IdentityCondition] compares its input with.
#[derive(Debug, Clone)]
pub(crate) enum IdentityOperand {
    /// Another row input.
    Input(usize),
    /// A constant.
    Constant(DataValue),
}

/// The identity form of a stack program; see the module documentation.
#[derive(Debug, Clone)]
pub(crate) struct IdentityCondition {
    /// The row input compared.
    pub(crate) input: usize,
    /// What the input is compared with.
    pub(crate) operand: IdentityOperand,
    /// Whether the condition holds if the terms differ, rather than if they are identical.
    pub(crate) negated: bool,
}

impl IdentityCondition {
    /// Recognizes the identity form of a program,
    /// or `None` if the program does more than compare two terms for identity.
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
