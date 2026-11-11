//! Helpers shared by the unit tests.

mod temp_directory;

pub(crate) use temp_directory::TempDirectory;

use nemo_physical::{
    function::{OpCode, StackProgram},
    value::DataValue,
};

use crate::row_filter::RowFilter;

/// Returns the condition that the delivered column at `column` is greater than the integer
/// `bound`.
pub(crate) fn greater_than(column: usize, bound: i64) -> RowFilter {
    RowFilter::new(
        vec![column],
        StackProgram::new(
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
            vec![DataValue::integer(bound)],
            1,
        ),
    )
}
