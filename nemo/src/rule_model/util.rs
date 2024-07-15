//! This module collects miscellaneous functionality for the rule model.

/// Macro that parses individual [super::component::term::Term]s
#[macro_export]
macro_rules! term_list {
    // Base case
    () => {};
    // Match a single universally quantified variable
    ($terms:ident; ? $var:ident) => {
        $terms.push(crate::rule_model::components::term::Term::universal_variable(stringify!($var)));
    };
    // Match universally quantified variables
    ($terms:ident; ? $var:ident, $($others:tt)* ) => {
        $terms.push(crate::rule_model::components::term::Term::universal_variable(stringify!($var))); term_list!($terms; $($others)*)
    };
    // Match a single existentially quantified variable
    ($terms:ident; ! $var:ident) => {
        $terms.push(crate::rule_model::components::term::Term::existential_variable(stringify!($var)));
    };
    // Match existentially quantified variables
    ($terms:ident; ! $var:ident, $($others:tt)* ) => {
        $terms.push(crate::rule_model::components::term::Term::existential_variable(stringify!($var))); term_list!($terms; $($others)*)
    };
    // Match a single occurence of anything
    ($terms:ident; $e:tt) => {
        $terms.push(crate::rule_model::components::term::Term::from($e));
    };
    // Match a list of anything
    ($terms:ident; $e:tt, $($others:tt)* ) => {
        $terms.push(crate::rule_model::components::term::Term::from($e)); term_list!($terms; $($others)*)
    };
}
