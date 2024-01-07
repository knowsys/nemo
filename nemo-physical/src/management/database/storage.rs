//! This module defin

use std::cell::RefCell;

use bytesize::ByteSize;

use crate::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    error::ReadingError,
    management::ByteSized,
    tabular::trie::Trie,
};

use super::{sources::TableSource, Dict};

/// Represents the stored table
#[derive(Debug)]
pub(super) enum TableStorage {
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
        sources: &[TableSource],
        dictionary: &RefCell<Dict>,
    ) -> Result<Trie, ReadingError> {
        debug_assert!(!sources.is_empty());

        let column_number = sources
            .first()
            .expect("Function assumes that at least one source is provided");
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
    pub fn trie<'a>(&'a mut self, dictionary: &RefCell<Dict>) -> Result<&'a Trie, ReadingError> {
        match self {
            TableStorage::InMemory(trie) => Ok(trie),
            TableStorage::FromSources(sources) => Self::load_sources(sources, dictionary),
        }
    }

    /// Return the [Trie] stored by this object if it is already loaded in memory.
    ///
    /// Returns `None` otherwise.
    pub fn trie_in_memory(&self) -> Option<&Trie> {
        match self {
            TableStorage::InMemory(trie) => Some(trie),
            TableStorage::FromSources(_) => None,
        }
    }
}

impl ByteSized for TableStorage {
    fn size_bytes(&self) -> ByteSize {
        match self {
            TableStorage::InMemory(trie) => trie.size_bytes(),
            TableStorage::FromSources(sources) => {
                sources.iter().map(|source| source.size_bytes()).sum()
            }
        }
    }
}
