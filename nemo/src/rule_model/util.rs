//! This module collects miscellaneous functionality for the rule model.

/// Macro that parses individual [Term][super::components::term::Term]s
#[macro_export]
macro_rules! term_list {
    // Base case
    () => {};
    // Match a single constant
    ($terms:ident; < $constant:ident >) => {
        $terms.push($crate::rule_model::components::term::Term::constant(stringify!($constant)));
    };
    // Match constants
    ($terms:ident; < $constant:ident >, $($others:tt)* ) => {
        $terms.push($crate::rule_model::components::term::Term::constant(stringify!($constant))); term_list!($terms; $($others)*)
    };
    // Match a single universally quantified variable
    ($terms:ident; ? $var:ident) => {
        $terms.push($crate::rule_model::components::term::Term::universal_variable(stringify!($var)));
    };
    // Match universally quantified variables
    ($terms:ident; ? $var:ident, $($others:tt)* ) => {
        $terms.push($crate::rule_model::components::term::Term::universal_variable(stringify!($var))); term_list!($terms; $($others)*)
    };
    // Match a single existentially quantified variable
    ($terms:ident; ! $var:ident) => {
        $terms.push($crate::rule_model::components::term::Term::existential_variable(stringify!($var)));
    };
    // Match existentially quantified variables
    ($terms:ident; ! $var:ident, $($others:tt)* ) => {
        $terms.push($crate::rule_model::components::term::Term::existential_variable(stringify!($var))); term_list!($terms; $($others)*)
    };
    // Match a single occurrence of anything
    ($terms:ident; $e:tt) => {
        $terms.push($crate::rule_model::components::term::Term::from($e));
    };
    // Match a list of anything
    ($terms:ident; $e:tt, $($others:tt)* ) => {
        $terms.push($crate::rule_model::components::term::Term::from($e)); term_list!($terms; $($others)*)
    };
}

/// Fallible version of [std::convert::AsRef]
pub trait TryAsRef<T> {
    /// Try to convert `&self` into `&T`.
    ///
    /// Returns `None` if unsuccessful.
    fn try_as_ref(&self) -> Option<&T>;
}
