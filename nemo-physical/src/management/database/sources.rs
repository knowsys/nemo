//! This module defines possible sources of tables to be stored as [Trie][crate::tabular::trie::Trie]s.

use std::{error::Error, fmt::Display, mem::size_of};

use crate::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::AnyDataValue,
    management::bytesized::ByteSized,
};

/// Simple row-based table containing [AnyDataValue]s
#[derive(Debug, Clone)]
pub struct SimpleTable {
    arity: usize,
    data: Vec<AnyDataValue>,
}

impl SimpleTable {
    /// Create a new [SimpleTable].
    pub fn new(arity: usize) -> Self {
        Self {
            arity,
            data: Vec::new(),
        }
    }

    /// Return the number of columns of this table.
    pub(crate) fn arity(&self) -> usize {
        self.arity
    }

    /// Add a new row to table
    pub fn add_row(&mut self, row: Vec<AnyDataValue>) {
        debug_assert!(row.len() == self.arity);

        self.data.extend(row);
    }

    /// Add content of this table to a [TupleWriter]
    pub(crate) fn write_tuples(self, writer: &mut TupleWriter) {
        for value in self.data {
            writer.add_tuple_value(value)
        }
    }
}

impl TableProvider for SimpleTable {
    fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), Box<dyn Error>> {
        self.write_tuples(tuple_writer);
        Ok(())
    }

    fn arity(&self) -> usize {
        self.arity
    }
}

impl ByteSized for SimpleTable {
    fn size_bytes(&self) -> u64 {
        // cast everything to u64 separately to avoid overflows
        size_of::<Self>() as u64 + self.data.capacity() as u64 * size_of::<AnyDataValue>() as u64
    }
}

/// Source of a table
pub type TableSource = Box<dyn TableProvider>;
