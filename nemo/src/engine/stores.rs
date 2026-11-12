//! The stores of an engine: the data that it holds for the predicates.

use std::collections::HashMap;

use nemo_physical::{
    dictionary::Dictionary,
    management::{TableId, TableManager},
};

use crate::engine::evaluate::extensional::ExtensionalBlock;

/// A registered predicate: the table that stores it, and the table's number of columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Registered {
    /// The table that stores the predicate's rows.
    pub(crate) table: TableId,
    /// The table's number of columns,
    /// which are the columns that the predicate's loader delivers.
    pub(crate) arity: usize,
}

/// The stores of an engine.
#[derive(Debug, Default)]
pub(in crate::engine) struct Stores {
    /// The table manager, which stores the tables of the predicates.
    pub(in crate::engine) manager: TableManager,
    /// The dictionary, which encodes the stored values.
    pub(in crate::engine) dictionary: Dictionary,
    /// The registered predicates by resolved name, each with its table
    /// and its number of columns.
    pub(in crate::engine) predicates: HashMap<String, Registered>,
    /// Per table, its extensional blocks in ascending order.
    pub(in crate::engine) extensional: HashMap<TableId, Vec<ExtensionalBlock>>,
}
