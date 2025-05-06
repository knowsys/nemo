//! This module defines a wrapper for FilterHook that allows using the rule name

use std::sync::Arc;

use nemo_physical::{
    datavalues::AnyDataValue,
    util::hook::{FilterFunction, FilterResult},
};

#[derive(Clone)]
/// Defines a filter hook that can be applied globally to a program
/// which is able to access the rule name on each call.
pub struct GlobalRuleFilterHook {
    inner: Arc<dyn Fn(Option<&str>, &str, &[AnyDataValue]) -> FilterResult + 'static + Send + Sync>,
}

impl std::fmt::Debug for GlobalRuleFilterHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterHook").finish()
    }
}

impl GlobalRuleFilterHook {
    /// Instantiate a global filter hook for a single rule yielding a physical filter hook
    pub fn instantiate(&self, name: Option<Arc<str>>) -> nemo_physical::util::hook::FilterHook {
        nemo_physical::util::hook::FilterHook::new(InstantiatedFilterHook {
            name,
            hook: self.clone(),
        })
    }
}

#[derive(Debug, Clone)]
struct InstantiatedFilterHook {
    name: Option<Arc<str>>,
    hook: GlobalRuleFilterHook,
}

impl FilterFunction for InstantiatedFilterHook {
    fn call(&self, string: &str, values: &[AnyDataValue]) -> FilterResult {
        (self.hook.inner)(self.name.as_deref(), string, values)
    }
}

impl<F> From<F> for GlobalRuleFilterHook
where
    F: Fn(Option<&str>, &str, &[AnyDataValue]) -> FilterResult + 'static + Send + Sync,
{
    fn from(value: F) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}
