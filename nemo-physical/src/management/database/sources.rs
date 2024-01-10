//! This module defines possible sources of tables to be stored as [Trie][crate::tabular::trie::Trie]s.

use std::{fmt::Display, mem::size_of};

use bytesize::ByteSize;

use crate::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::AnyDataValue,
    management::ByteSized,
};

/// Simple row-based table containing [AnyDataValue]s
#[derive(Debug)]
pub struct SimpleTable {
    column_number: usize,
    data: Vec<AnyDataValue>,
}

impl SimpleTable {
    /// Create a new [SimpleTable].
    pub fn new(column_number: usize) -> Self {
        Self {
            column_number,
            data: Vec::new(),
        }
    }

    /// Return the number of columns of this table.
    pub fn column_number(&self) -> usize {
        self.column_number
    }

    /// Add a new row to table
    pub fn add_row(&mut self, row: Vec<AnyDataValue>) {
        debug_assert!(row.len() == self.column_number);

        self.data.extend(row);
    }

    /// Add content of this table to a [TupleWriter]
    pub(crate) fn write_tuples(self, writer: &mut TupleWriter) {
        for value in self.data {
            writer.add_tuple_value(value)
        }
    }
}

impl ByteSized for SimpleTable {
    fn size_bytes(&self) -> ByteSize {
        // We cast everything to u64 separately to avoid overflows
        ByteSize::b(
            size_of::<Self>() as u64
                + self.data.capacity() as u64 * size_of::<AnyDataValue>() as u64,
        )
    }
}

/// Source of a table
#[derive(Debug)]
pub enum TableSource {
    /// Table is stored externally and can be obtained
    /// by any implementation of [TableProvider]
    ///
    /// Requires the number of columns to be known
    External(Box<dyn TableProvider>, usize),
    /// Table is stored in memory as a [SimpleTable]
    SimpleTable(SimpleTable),
}

impl TableSource {
    /// Return the number of columns of the table represented by this source.
    pub fn column_number(&self) -> usize {
        match self {
            TableSource::External(_, column_number) => *column_number,
            TableSource::SimpleTable(table) => table.column_number,
        }
    }
}

impl Display for TableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // TODO: maybe do not use the debug impl of the reader but I'm not sure if it should enforce display...; maybe have a method on it returning the file name or so?
            TableSource::External(reader, _) => write!(f, "TableReader implementation: {reader:?}"),
            TableSource::SimpleTable(_) => write!(f, "Simple Table"),
        }
    }
}

impl ByteSized for TableSource {
    fn size_bytes(&self) -> ByteSize {
        match self {
            TableSource::External(_, _) => ByteSize(0),
            TableSource::SimpleTable(table) => table.size_bytes(),
        }
    }
}
