//! Counts in diagnostic messages.

use std::fmt;

/// A count in a diagnostic message.
///
/// It displays as its number, so a field holding it stays a number.
/// With the alternate flag, it displays as the plural suffix
/// of the noun it counts instead:
/// in a message template, `{columns} column{columns:#}`
/// reads `1 column` or `3 columns`.
///
/// # Example
///
/// ```
/// use nemo_physical::diagnostics::Count;
///
/// let one = Count::from(1usize);
/// let three = Count::from(3usize);
/// assert_eq!(format!("{one} column{one:#}"), "1 column");
/// assert_eq!(format!("{three} column{three:#}"), "3 columns");
/// assert_eq!(three.to_string(), "3");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Count(pub u64);

impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.write_str(if self.0 == 1 { "" } else { "s" })
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl From<u64> for Count {
    fn from(count: u64) -> Self {
        Self(count)
    }
}

impl From<usize> for Count {
    fn from(count: usize) -> Self {
        Self(count as u64)
    }
}
