//! This module collects miscellaneous functionality.

pub mod mapping;
pub mod tagged_tree;
pub use tagged_tree::TaggedTree;

/// A macro that generates forwarding macros to dispatch along
/// datatype-tagged enums.
///
/// `$name` is the name of the generated forwarder, and `$variant`
/// takes the enum variants to dispatch to.
///
/// # Example
///
/// ```ignore
/// generate_forwarder!(forward_to_scan; TrieScanGeneric, TrieScanJoin);
/// ```
///
/// will generate a forwarder called `forward_to_scan` that dispatches
/// to variants `Self::TrieScanGeneric` and `Self::TrieScanJoin`,
/// which can be used as follows:
///
/// ```ignore
/// fn up(&mut self) {
///     forward_to_scan!(self, up)
/// }
///
/// /// The following will map the return value of the forwarded
/// /// `returns()` call to the appropriate variant of [`StorageValueT`]:
/// fn returns(&mut self) -> StorageValueT {
///     forward_to_scan!(self, returns.map_to(StorageValueT))
/// }
///
/// /// The return value can be suppressed by adding a semicolon, this
/// /// is useful if the return type would differ between match arms:
/// fn doesnt_return(&mut self) {
///     forward_to_scan!(self, returns;)
/// }
/// ```
#[macro_export]
macro_rules! generate_forwarder {
    ($name:ident; $( $variant:ident ),*) => {
        macro_rules! $name {
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?) => {
                match $$self {
                    $( Self::$variant(value) => value.$$func($$($$($$arg),*)?) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?;) => {
                match $$self {
                    $( Self::$variant(value) => { value.$$func($$($$($$arg),*)?); } ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.map_to($$enum:ident) ) => {
                match $$self {
                    $( Self::$variant(value) => value.$$func($$($$($$arg),*)?).map($$enum::$variant) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.wrap_with($$wrap:path) ) => {
                match $$self {
                    $( Self::$variant(value) => $$wrap(value.$$func($$($$($$arg),*)?)) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.as_variant_of($$enum:ident) ) => {
                match $$self {
                    $( Self::$variant(value) => $$enum::$variant(value.$$func($$($$($$arg),*)?)) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.wrap_with($$wrap:path).as_variant_of($$enum:ident) ) => {
                match $$self {
                    $( Self::$variant(value) => $$enum::$variant($$wrap(value.$$func($$($$($$arg),*)?))) ),*
                }
            };
            ($$self:ident, $$func:ident$$( ($$( $$arg:tt ),*) )?.as_variant_of($$enum:ident).wrap_with($$wrap:path) ) => {
                match $$self {
                    $( Self::$variant(value) => $$wrap($$enum::$variant(value.$$func($$($$($$arg),*)?))) ),*
                }
            };
        }
    }
}

/// A specialised version of [`generate_forwarder`] for the possible
/// variants of [`crate::datatypes::storage_value::StorageValueT`].
#[macro_export]
macro_rules! generate_datatype_forwarder {
    ($name:ident) => {
        $crate::generate_forwarder!($name;
                                    U32,
                                    U64,
                                    I64,
                                    Float,
                                    Double);
    }
}

// clippy complains if these occur after mod test_util...
#[cfg(test)]
pub use test_util::make_column_with_intervals;
#[cfg(test)]
pub use test_util::make_column_with_intervals_t;

/// Module for utility functions used in tests
#[cfg(test)]
pub mod test_util;
