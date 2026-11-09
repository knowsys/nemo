//! The errors that building a plan can report.

use crate::diagnostic_kinds;

diagnostic_kinds! {
    /// An error from a limit of the plan's representation, which large input can reach.
    pub enum PlanError {
        ExpressionTooLarge {
            /// The largest number of distinct columns, and of constants, an expression can read.
            limit: usize,
        } => EXPRESSION_TOO_LARGE = 2, error,
               "expression reads more than {limit} distinct columns or constants",
               explain "An expression reads more distinct columns, or more constants, \
                        than a compiled expression can address. \
                        Its instructions refer to columns and constants by 16-bit operands.";
    }
}
