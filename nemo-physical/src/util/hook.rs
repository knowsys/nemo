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

/// A callback function that implements filtering of tuples
pub trait FilterFunction: 'static + Send + Sync {
    /// Call to the callback function on a derived tuple
    fn call(&self, string: &str, values: &[AnyDataValue]) -> FilterResult;
}

/// Callable function used to filter table entries
#[derive(Clone)]
pub struct FilterHook(Arc<dyn FilterFunction>);

impl FilterHook {
    /// Create a new FilterHook from a [FilterFunction] implementation
    pub fn new(inner: impl FilterFunction) -> Self {
        Self(Arc::new(inner))
    }
}

impl FilterFunction for FilterHook {
    /// Call the inner function.
    fn call(&self, string: &str, values: &[AnyDataValue]) -> FilterResult {
        self.0.call(string, values)
    }
}

struct FnWrapper<F>(F);

impl<F> FilterFunction for FnWrapper<F>
where
    F: Fn(&str, &[AnyDataValue]) -> FilterResult + 'static + Send + Sync,
{
    fn call(&self, string: &str, values: &[AnyDataValue]) -> FilterResult {
        (self.0)(string, values)
    }
}

impl<Function> From<Function> for FilterHook
where
    Function: Fn(&str, &[AnyDataValue]) -> FilterResult + 'static + Send + Sync,
{
    fn from(value: Function) -> Self {
        FilterHook(Arc::new(FnWrapper(value)))
    }
}

impl std::fmt::Debug for FilterHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "Fn(&str, &[AnyDataValue]) -> bool".fmt(f)
    }
}
