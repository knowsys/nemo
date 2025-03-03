//! This module defines [TableStorage],
//! which manages tables that can either be present in memory
//! or alternatively loaded into via an external source.

use std::cell::RefCell;

use crate::{
    datasources::tuple_writer::TupleWriter, error::Error, management::bytesized::ByteSized,
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
    fn load_sources(sources: Vec<TableSource>, dictionary: &RefCell<Dict>) -> Result<Trie, Error> {
        debug_assert!(!sources.is_empty());

        let arity = sources
            .first()
            .expect("Function assumes that at least one source is provided")
            .arity();
        let mut tuple_writer = TupleWriter::new(dictionary, arity);

        for source in sources {
            log::info!("Loading source {source:?}");
            debug_assert!(source.arity() == arity);

            source.provide_table_data(&mut tuple_writer)?;
        }

        Ok(Trie::from_tuple_writer(tuple_writer))
    }

    /// Return the [Trie] stored by this object.
    ///
    /// If the table is not already loaded into memory as a [Trie],
    /// this function will load the [TableSource] and transform it into a [Trie].
    pub(crate) fn trie<'a>(&'a mut self, dictionary: &RefCell<Dict>) -> Result<&'a Trie, Error> {
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
    pub(crate) fn trie_in_memory(&self) -> Option<&Trie> {
        match self {
            TableStorage::InMemory(trie) => Some(trie),
            TableStorage::FromSources(_) => None,
            TableStorage::Empty => None,
        }
    }

    /// Returns the number of columns for the table associated with this storage object.
    pub(crate) fn arity(&self) -> usize {
        match self {
            TableStorage::InMemory(trie) => trie.arity(),
            TableStorage::FromSources(sources) => sources
                .first()
                .expect("At least one source must be present")
                .arity(),
            TableStorage::Empty => 0,
        }
    }

    /// Return the number of rows that are stored in this table.
    pub(crate) fn count_rows(&self) -> usize {
        match self {
            TableStorage::InMemory(trie) => trie.num_rows(),
            // TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
            TableStorage::FromSources(_) => 0,
            TableStorage::Empty => 0,
        }
    }
}

impl ByteSized for TableStorage {
    fn size_bytes(&self) -> u64 {
        match self {
            TableStorage::InMemory(trie) => trie.size_bytes(),
            TableStorage::FromSources(sources) => {
                sources.iter().map(|source| source.size_bytes()).sum()
            }
            TableStorage::Empty => 0,
        }
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datavalues::AnyDataValue,
        management::database::{sources::SimpleTable, Dict},
        storagevalues::{float::Float, storagevalue::StorageValueT},
    };

    use super::TableStorage;

    #[test]
    fn load_sources() {
        let arity: usize = 2;

        let mut table_a = SimpleTable::new(arity);
        table_a.add_row(vec![
            AnyDataValue::new_integer_from_i64(-5),
            AnyDataValue::new_plain_string(String::from("Test")),
        ]);
        table_a.add_row(vec![
            AnyDataValue::new_float_from_f32(-2.0).unwrap(),
            AnyDataValue::new_float_from_f32(12.0).unwrap(),
        ]);

        let mut table_b = SimpleTable::new(arity);
        table_b.add_row(vec![
            AnyDataValue::new_integer_from_i64(-10),
            AnyDataValue::new_float_from_f32(12.0).unwrap(),
        ]);
        table_b.add_row(vec![
            AnyDataValue::new_integer_from_i64(-5),
            AnyDataValue::new_plain_string(String::from("Test")),
        ]);

        let dictionary = RefCell::new(Dict::default());

        let mut storage = TableStorage::FromSources(vec![Box::new(table_a), Box::new(table_b)]);
        let trie = storage.trie(&dictionary).unwrap();

        let expected_rows = vec![
            vec![
                StorageValueT::Int64(-10),
                StorageValueT::Float(Float::new(12.0).unwrap()),
            ],
            vec![StorageValueT::Int64(-5), StorageValueT::Id32(0)],
            vec![
                StorageValueT::Float(Float::new(-2.0).unwrap()),
                StorageValueT::Float(Float::new(12.0).unwrap()),
            ],
        ];

        let trie_rows = trie.row_iterator().collect::<Vec<_>>();

        assert_eq!(trie_rows, expected_rows);
        assert!(storage.trie_in_memory().is_some());
    }
}
