//! This module defines a type of column,
//! where data entries are divided into several intervals
//! or blocks of sorted values.
//!
//! Such columns represent a layer in a [Trie][crate::tabular::trie_storage::trie::Trie].

pub(crate) mod column_intervals;
pub(crate) mod interval_lookup;
