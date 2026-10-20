//! The operation families: what the instructions of a stack program compute.
//!
//! Each family is a set of plain functions over borrowed [ValueRef](crate::value::ValueRef)s
//! returning a [StackValue](super::StackValue):
//! the result, or the [evaluation error](crate::value::EvaluationError) that propagates.
//! The families know nothing of the stack or the instruction set;
//! the interpreter in [program](super::StackProgram) dispatches to them,
//! and the type analysis in `infer` mirrors them.

pub(super) mod boolean;
pub(super) mod compare;
pub(super) mod generic;
pub(super) mod hashing;
pub(crate) mod numeric;
pub(super) mod pattern;
pub(super) mod random;
pub(super) mod string;
pub(super) mod structure;
pub(super) mod temporal;
