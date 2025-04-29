//! This module defines [FilterHook].

use std::sync::Arc;

use crate::datavalues::AnyDataValue;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// Result of calling a filter function
pub enum FilterResult {
    /// Accept the current tuple
    Accept,
    /// Reject the current tuple and look for the next one
    Reject,
    /// Reject the current tuple and stop looking for new ones
    Abort,
}

/// Callable function used to filter table entries
#[derive(Clone)]
pub struct FilterHook(Arc<dyn Fn(&str, &[AnyDataValue]) -> FilterResult>);

impl FilterHook {
    /// Call the inner function.
    pub fn call(&self, string: &str, values: &[AnyDataValue]) -> FilterResult {
        (self.0)(string, values)
    }
}

impl<Function> From<Function> for FilterHook
where
    Function: Fn(&str, &[AnyDataValue]) -> FilterResult + 'static,
{
    fn from(value: Function) -> Self {
        FilterHook(Arc::new(value))
    }
}

impl std::fmt::Debug for FilterHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "Fn(&str, &[AnyDataValue]) -> bool".fmt(f)
    }
}
