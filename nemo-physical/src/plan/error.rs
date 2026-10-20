//! The kinds of problems a plan reports about what it is given.

use crate::diagnostic_kinds;

diagnostic_kinds! {
    /// A limit of the plan's representation that large input can reach.
    pub enum PlanError {
        ExpressionTooLarge {
            /// The most columns, and the most constants, an expression reads.
            limit: usize,
        } => EXPRESSION_TOO_LARGE = 2, error,
               "expression reads more than {limit} distinct columns or constants",
               explain "An expression reads more distinct columns, or more constants, \
                        than a compiled expression can address. \
                        Its instructions refer to columns and constants by 16-bit operands.";
    }
}
