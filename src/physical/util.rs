//! This module collects miscellaneous functionality.

/// Module for utility functions used in tests
#[cfg(test)]
pub mod test_util;
#[cfg(test)]
pub use test_util::make_gic;
#[cfg(test)]
pub use test_util::make_gict;

/// A macro that generates forwarding macros to dispatch along
/// datatype-tagged enums.
///
/// `$name` is the name of the generated forwarder, and `$pattern`
/// takes the enum variants to dispatch to.
///
/// # Example
/// ```ignore
/// generate_forwarder!(forward_to_scan; IntervalTrieScan, TrieScanJoin);
/// ```
/// will generate a forwarder called `forward_to_scan` that dispatches to variants
/// `Self::IntervalTrieScan` and `Self::TrieScanJoin`, which can be used as follows:
/// ```ignore
/// fn up(&mut self) {
///     forward_to_scan!(self, up)
/// }
/// ```
#[macro_export]
macro_rules! generate_forwarder {
    ($name:ident; $( $pattern:ident ),*) => {
        macro_rules! $name {
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?) => {
                match $$self {
                    $( Self::$pattern(value) => value.$$func($$($$($$arg),*)?) ),*
                }
            }
        }
    }
}
