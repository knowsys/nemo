//! The builtin functions.
//!
//! An expression compiles into a [StackProgram],
//! a flat sequence of [instructions](OpCode) evaluated over a stack of [StackValue]s,
//! which reads a row of borrowed [ValueRef](crate::value::ValueRef)s
//! and leaves one result.
//! The stack belongs to the caller and is reused across rows.
//! An evaluation error is a value on the stack:
//! it propagates through the operations as in SPARQL,
//! where the logical connectives are three-valued,
//! and the caller counts what reaches the result rather than dropping it silently.

mod infer;
mod narrow;
mod opcode;
pub(crate) mod operation;
mod program;
mod specialized;

pub use infer::Inferred;
pub use narrow::Outcome;
pub use opcode::OpCode;
pub use program::{EvaluationContext, StackProgram, StackValue};
pub(crate) use specialized::{
    IdentityCondition, IdentityOperand, IntegerProgram, IntegerValue, SpecializedProgram,
};

#[cfg(test)]
mod tests;
