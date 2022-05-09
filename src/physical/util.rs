//! This module collects miscellaneous functionality.

/// Module for utility functions used in tests
#[cfg(test)]
pub mod test_util;
#[cfg(test)]
pub use test_util::make_gic;
#[cfg(test)]
pub use test_util::make_gict;
