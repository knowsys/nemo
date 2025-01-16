//! This module defines [FilterHook].

use std::sync::Arc;

use crate::datavalues::AnyDataValue;

/// Callable function used to filter table entries
#[derive(Clone)]
pub struct FilterHook(Arc<dyn Fn(&str, &[AnyDataValue]) -> bool>);

impl FilterHook {
    /// Call the inner function.
    pub fn call(&self, string: &str, values: &[AnyDataValue]) -> bool {
        (self.0)(string, values)
    }
}

impl<Function> From<Function> for FilterHook
where
    Function: Fn(&str, &[AnyDataValue]) -> bool + 'static,
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
