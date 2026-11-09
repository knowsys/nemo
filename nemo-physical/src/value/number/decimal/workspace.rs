//! The workspaces of exact arithmetic.
//!
//! A workspace value exists only while an operation computes, and is never stored as a value.
//! Arithmetic starts in the [`i128` workspace](Fixed),
//! which can represent every operand within the compact ranges and most results.
//! It continues in the [big workspace](Big) if a result does not fit or an operand is wide.
//! Every result becomes a value again through [Decimal::new](super::Decimal::new)
//! or `From<Big>`, which choose its representation.

pub(crate) mod big;
pub(crate) mod fixed;

pub(crate) use big::Big;
pub(crate) use fixed::Fixed;
