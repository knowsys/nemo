//! The builtin functions, evaluated by stack programs.
//!
//! An expression over builtin functions is compiled into a [StackProgram],
//! a flat sequence of [instructions](OpCode) that work on a stack of [StackValue]s.
//! A program reads one row of [ValueRef](crate::value::ValueRef)s and leaves one result.
//! The stack is passed to each evaluation, so that one stack can serve many rows.
//!
//! An evaluation error is a value on the stack, and it propagates through the operations
//! as in SPARQL.
//! The logical connectives are three-valued, so a conjunction with a false operand is false
//! even if the other operand is an error.
//! An error that reaches the result is returned like any other result.
//!
//! A program can also be evaluated on types.
//! [Inference](StackProgram::infer) bounds its result,
//! and [narrowing](StackProgram::narrow) bounds its inputs for a required [Outcome].

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
