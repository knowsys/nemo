//! This module defines [PermanentTableId] and [ExecutionId],
//! which are used to identify tables in [DatabaseInstance][super::DatabaseInstance]
//! and in an [ExecutionPlan][super::ExecutionPlan].

use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

/// Trait for types that represent ids of tables
pub trait TableId: Debug + Default + Display + Copy + Clone + Eq + PartialEq + Hash {
    /// Increment the id by one.
    /// Return the old (non-incremented) id.
    fn increment(&mut self) -> Self;

    /// Return the integer value that represents the id.
    fn get(&self) -> usize;
}

/// Id of a permanent table in the [DatabaseInstance][super::DatabaseInstance]
///
/// Note that an one [PermanentTableId] may represent tables,
/// which contain the same content but in different orders.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct PermanentTableId(usize);

impl Display for PermanentTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl TableId for PermanentTableId {
    fn increment(&mut self) -> Self {
        let old = *self;
        self.0 += 1;
        old
    }

    fn get(&self) -> usize {
        self.0
    }
}

/// Id of a table computed during the execution of an [ExecutionPlan][super::super::ExecutionPlan]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ExecutionId(usize);

impl Display for ExecutionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl TableId for ExecutionId {
    fn increment(&mut self) -> Self {
        let old = *self;
        self.0 += 1;
        old
    }

    fn get(&self) -> usize {
        self.0
    }
}
