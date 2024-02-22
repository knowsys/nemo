//! This module contains the built-in aggregate operators, which determine how to aggregate multiple values in a group into a single output value.

pub(crate) mod aggregate;
pub(crate) mod count_aggregate;
pub(crate) mod max_aggregate;
pub(crate) mod min_aggregate;
pub(crate) mod processor;
pub(crate) mod sum_aggregate;
