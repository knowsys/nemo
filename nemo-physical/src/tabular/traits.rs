//! This module defines basic table related traits

/// Module for defining [`Table`][table::Table]
pub mod table;

/// Module for defining [`TableSchema`][table_schema::TableSchema]
pub mod table_schema;

/// Module defining the [`PartialTrieScan`][partial_trie_scan::PartialTrieScan] interface
pub mod partial_trie_scan;

/// Module defining the [`TrieScan`][trie_scan::TrieScan] interface
pub mod trie_scan;
