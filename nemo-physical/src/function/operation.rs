//! The operations that the instructions of a stack program compute, grouped in families.
//!
//! An operation is a plain function on borrowed values that returns a
//! [StackValue](super::StackValue), the result or an
//! [evaluation error](crate::value::EvaluationError).
//! Most operations accept [ValueRef](crate::value::ValueRef)s,
//! and those that must see the errors among their operands accept stack values.
//! The operations never pop or push values themselves.

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
