//! This module defines possible sources of tables to be stored as [Trie][crate::tabular::trie::Trie]s.

use std::{error::Error, fmt::Display, mem::size_of};

use bytesize::ByteSize;

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
    pub fn arity(&self) -> usize {
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
pub struct TableSource {
    /// [TableProvider] form which the table can be read into a [TupleWriter]
    provider: Box<dyn TableProvider>,
    /// Number of columns in the table
    arity: usize,
}

impl TableSource {
    /// Construct a new [TableSource].
    pub fn new(provider: Box<dyn TableProvider>, arity: usize) -> Self {
        Self { provider, arity }
    }

    /// Construct a new [TableSource] from a [SimpleTable].
    pub fn from_simple_table(table: SimpleTable) -> Self {
        let arity = table.arity();

        Self::new(Box::new(table), arity)
    }

    /// Return the number of columns of the table represented by this source.
    pub fn arity(&self) -> usize {
        self.arity
    }

    /// Load the data represented by this [TableSource]
    /// and write it into the given [TupleWriter]
    pub fn provide_table_data(self, tuple_writer: &mut TupleWriter) -> Result<(), Box<dyn Error>> {
        self.provider.provide_table_data(tuple_writer)
    }
}

impl Display for TableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableReader implementation: {:?}", self.provider)
    }
}

impl ByteSized for TableSource {
    fn size_bytes(&self) -> ByteSize {
        ByteSize::b(size_of::<Self>() as u64) + self.provider.size_bytes()
    }
}
