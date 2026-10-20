//! The workspaces of exact arithmetic.
//!
//! A workspace value is transient: it exists while an operation computes
//! and never becomes a term.
//! The arithmetic starts in the [`i128` workspace](Fixed),
//! which holds every compact operand and most results,
//! and escalates to the [big workspace](Big)
//! when a result outgrows it or an operand is wide.
//! Every result returns to a value through [Decimal::new](super::Decimal::new)
//! or `From<Big>`, which decide the case that holds it.

pub(crate) mod big;
pub(crate) mod fixed;

pub(crate) use big::Big;
pub(crate) use fixed::Fixed;
