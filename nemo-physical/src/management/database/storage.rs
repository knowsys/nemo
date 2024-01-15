//! This module defin

use std::{cell::RefCell, error::Error};

use bytesize::ByteSize;

use crate::{
    datasources::tuple_writer::TupleWriter,
    management::{bytesized::sum_bytes, bytesized::ByteSized},
    tabular::trie::Trie,
};

use super::{sources::TableSource, Dict};

/// Represents the stored table
#[derive(Debug)]
pub(super) enum TableStorage {
    /// Represents an empty entry
    ///
    /// This can be used as a default state or after deleting a table
    Empty,
    /// Table is stored as a [Trie] in memory
    InMemory(Trie),
    /// Table is not present as [Trie]
    /// and needs to be loaded from [TableSource]s
    FromSources(Vec<TableSource>),
}

impl TableStorage {
    /// Load the table from a list of [TableSource]s and convert it into a [Trie].
    ///
    /// This function assumes that at least one source is provided.
    fn load_sources(
        sources: Vec<TableSource>,
        dictionary: &RefCell<Dict>,
    ) -> Result<Trie, Box<dyn Error>> {
        debug_assert!(!sources.is_empty());

        let column_number = sources
            .first()
            .expect("Function assumes that at least one source is provided")
            .column_number();
        let mut tuple_writer = TupleWriter::new(dictionary, column_number);

        for source in sources {
            log::info!("Loading source {source}");
            debug_assert!(source.column_number() == column_number);

            match source {
                TableSource::External(provider, _) => {
                    provider.provide_table_data(&mut tuple_writer)?;
                }
                TableSource::SimpleTable(table) => {
                    table.write_tuples(&mut tuple_writer);
                }
            }
        }

        Ok(Trie::from_tuple_writer(tuple_writer))
    }

    /// Return the [Trie] stored by this object.
    ///
    /// If the table is not already loaded into memory as a [Trie],
    /// this function will load the [TableSource] and transform it into a [Trie].
    pub fn trie<'a>(&'a mut self, dictionary: &RefCell<Dict>) -> Result<&'a Trie, Box<dyn Error>> {
        // Load trie if not already in memory
        match self {
            TableStorage::InMemory(_) => {}
            TableStorage::FromSources(sources) => {
                let sources = std::mem::take(sources);
                let trie = Self::load_sources(sources, dictionary)?;

                *self = TableStorage::InMemory(trie);
            }
            TableStorage::Empty => unreachable!("This trie has been deleted"),
        }

        // Return in-memory trie
        if let TableStorage::InMemory(trie) = self {
            Ok(trie)
        } else {
            unreachable!("Trie should have been loaded into memory by this point")
        }
    }

    /// Return the [Trie] stored by this object if it is already loaded in memory.
    ///
    /// Returns `None` otherwise.
    pub fn trie_in_memory(&self) -> Option<&Trie> {
        match self {
            TableStorage::InMemory(trie) => Some(trie),
            TableStorage::FromSources(_) => None,
            TableStorage::Empty => None,
        }
    }

    /// Returns the number of columns for the table associated with this storage object.
    pub fn arity(&self) -> usize {
        match self {
            TableStorage::InMemory(trie) => trie.arity(),
            TableStorage::FromSources(sources) => {
                match sources
                    .first()
                    .expect("At least one source must be present")
                {
                    TableSource::External(_, arity) => *arity,
                    TableSource::SimpleTable(table) => table.column_number(),
                }
            }
            TableStorage::Empty => 0,
        }
    }

    /// Return the number of rows that are stored in this table.
    pub fn count_rows(&self) -> usize {
        match self {
            TableStorage::InMemory(trie) => trie.num_rows(),
            // TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
            TableStorage::FromSources(_) => 0,
            TableStorage::Empty => 0,
        }
    }
}

impl ByteSized for TableStorage {
    fn size_bytes(&self) -> ByteSize {
        match self {
            TableStorage::InMemory(trie) => trie.size_bytes(),
            TableStorage::FromSources(sources) => {
                sum_bytes(sources.iter().map(|source| source.size_bytes()))
            }
            TableStorage::Empty => ByteSize::b(0),
        }
    }
}
