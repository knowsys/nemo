//! Types related to input and output formats.

use nemo_physical::datavalues::AnyDataValue;

use crate::error::Error;

/// Direction of import/export activities.
/// We often share code for the two directions, and a direction
/// is then used to enable smaller distinctions where needed.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum Direction {
    /// Processing input.
    Import,
    /// Processing output.
    Export,
}

/// A trait for exporting table data, e.g., to some file.
// TODO Maybe this should be directly in io, since it is the interface to the OutputManager?
pub trait TableWriter {
    /// Export a table.
    fn export_table_data<'a>(
        &mut self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error>;
}
