//! Loading: reading resources into row buffers.
//!
//! The rows read from one resource form a *load*, a [Loaded].
//! A row of a resource is *well-formed* if the reader can parse every cell of the row,
//! and *malformed* otherwise.
//! A row with a *missing value*, such as an empty cell, in a column that does not admit one is
//! neither: it is dropped and counted apart, since the fix is another format for the column.
//! A well-formed row is *admitted* if the condition of the
//! [selection](crate::selection::RowSelection) holds for it, and only admitted rows are stored.
//! An admitted row with a value that the dictionary cannot hold is counted as malformed after
//! all.
//! The [counts](RowCounts) of a load say how many rows were not stored, by cause.

pub(crate) mod batch;
mod blank_nodes;
mod counts;
mod intake;
mod reader;
mod resource;
pub(crate) mod run;

pub(crate) use blank_nodes::BlankNodes;
pub use counts::RowCounts;
pub(crate) use intake::RowIntake;
pub use reader::Reader;
pub use resource::Resource;

use nemo_physical::trie::RowBuffer;
#[cfg(test)]
use nemo_physical::{dictionary::Dictionary, value::DataValue};

/// The rows loaded from one resource, and the counts of the rows that were not stored.
///
/// The rows are in the resource's order, unsorted.
#[derive(Debug)]
pub struct Loaded {
    /// The loaded rows, encoded by the dictionary.
    pub rows: RowBuffer,
    /// The counts of the rows that were not stored, by cause.
    pub counts: RowCounts,
}

impl Loaded {
    /// Decodes every loaded row back into owned values.
    #[cfg(test)]
    pub(crate) fn decoded(&self, dictionary: &Dictionary) -> Vec<Vec<DataValue>> {
        (0..self.rows.len())
            .map(|index| {
                self.rows
                    .row(index)
                    .iter()
                    .map(|&value| {
                        dictionary
                            .decode(value)
                            .expect("loaded values decode")
                            .to_owned()
                    })
                    .collect()
            })
            .collect()
    }
}
