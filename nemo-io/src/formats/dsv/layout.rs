//! Where the columns that are read are found in the records of a resource.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    error::IoError,
    value_format::{ColumnFormat, ValueFormat},
};

/// The positions in a record of the cells that are read.
///
/// Without a header, the cells are read by position, and a record has one cell per format.
/// With a header, each named column is found by its name,
/// and the cells of the columns that are not named are not read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnLayout {
    /// The number of cells of a record.
    width: usize,
    /// For each format of the options, the position of its cell in a record.
    positions: Vec<usize>,
}

impl ColumnLayout {
    /// Returns the layout of a resource without a header,
    /// whose records have one cell per format, in the order of the formats.
    pub(crate) fn positional(formats: usize) -> Self {
        Self {
            width: formats,
            positions: (0..formats).collect(),
        }
    }

    /// Returns the layout that a header gives to the columns with the given names,
    /// which are in the order of the formats.
    ///
    /// The name of a [Skip](ValueFormat::Skip) column is not looked up,
    /// since the column is not read.
    ///
    /// # Errors
    ///
    /// Returns an error if the header lacks one of the other names, or has it more than once.
    pub(crate) fn of_header(
        header: &csv::StringRecord,
        names: &[String],
        formats: &[ColumnFormat],
    ) -> Result<Self, Diagnostic> {
        let positions = names
            .iter()
            .zip(formats)
            .map(|(name, format)| {
                if format.value == ValueFormat::Skip {
                    return Ok(0);
                }

                let mut found = header
                    .iter()
                    .enumerate()
                    .filter(|(_, cell)| cell == name)
                    .map(|(position, _)| position);
                let name = name.clone();

                match (found.next(), found.next()) {
                    (Some(position), None) => Ok(position),
                    (None, _) => Err(IoError::HeaderLacksColumn { name }.diagnostic()),
                    (Some(_), Some(_)) => Err(IoError::HeaderRepeatsColumn { name }.diagnostic()),
                }
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            width: header.len(),
            positions,
        })
    }

    /// Returns the number of cells of a record.
    pub(crate) fn width(&self) -> usize {
        self.width
    }

    /// Returns the position in a record of the cell that the format at `column` reads.
    pub(crate) fn position(&self, column: usize) -> usize {
        self.positions[column]
    }
}
