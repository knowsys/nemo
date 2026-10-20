//! Helpers shared by the unit tests.

mod temp_directory;

pub(crate) use temp_directory::TempDirectory;

use nemo_physical::{
    function::{OpCode, StackProgram},
    value::DataValue,
};

use crate::row_filter::RowFilter;

/// Returns the condition that the delivered column 1 is greater than 5.
pub(crate) fn greater_than_five() -> RowFilter {
    RowFilter::new(
        vec![1],
        StackProgram::new(
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
            vec![DataValue::integer(5)],
            1,
        ),
    )
}
