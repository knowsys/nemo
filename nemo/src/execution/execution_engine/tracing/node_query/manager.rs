//! This module implements [TraceNodeManager].

use std::collections::HashMap;

use nemo_physical::management::database::id::PermanentTableId;

use crate::execution::tracing::node_query::TreeAddress;

/// Represents a mapping from step to a [PermanentTableId].
#[derive(Debug, Default)]
struct StepToId {
    /// Steps
    steps: Vec<usize>,
    /// Ids
    ///
    /// Each entry in `steps` has a corresponsing entry in `ids`
    ids: Vec<PermanentTableId>,
}

impl StepToId {
    /// Add a new table assigned to a given step
    ///
    /// # Panics
    /// Assumes the step is higher than the steps of all previously added tables
    pub fn add_table(&mut self, step: usize, id: PermanentTableId) {
        assert!(
            self.steps
                .last()
                .map(|&last_step| last_step < step)
                .unwrap_or(true)
        );

        self.steps.push(step);
        self.ids.push(id);
    }

    /// Return an iterator over all tables before a given step.
    pub fn tables_before(&self, step: usize) -> impl Iterator<Item = PermanentTableId> + '_ {
        self.steps
            .iter()
            .zip(self.ids.iter())
            .take_while(move |(current_step, _)| **current_step < step)
            .map(|(_, id)| *id)
    }

    /// Return an iterator over all stored tables.
    pub fn all_tables(&self) -> impl Iterator<Item = PermanentTableId> + '_ {
        self.ids.iter().cloned()
    }
}

/// Stores the ids of all tables computed
/// during answering a node query
#[derive(Debug, Default)]
pub(super) struct TraceNodeManager {
    /// Holds the id of the table
    /// representing the facts contained in a node
    query: HashMap<TreeAddress, PermanentTableId>,

    /// "Valid" tables are tables that hold facts
    /// satisfying the constraints of the subtree
    /// of the corresponsing node
    valid: HashMap<TreeAddress, StepToId>,
    /// Consolidated version of `valid`
    valid_final: HashMap<TreeAddress, PermanentTableId>,

    /// For each "valid" table, the variable assignment
    /// used to compute it
    assignment: HashMap<TreeAddress, StepToId>,
    /// Consolidated version of `assignemnt`
    assignment_final: HashMap<TreeAddress, PermanentTableId>,

    /// For each node in the query,
    /// holds the final table containing the response facts
    result: HashMap<TreeAddress, PermanentTableId>,

    /// Stores of each node, which columns of its table can be ignored
    discard: HashMap<TreeAddress, Vec<usize>>,
}

impl TraceNodeManager {
    /// For a node, add which of its columns can be ignored.
    pub fn add_discard(&mut self, address: &TreeAddress, discard: &[usize]) {
        self.discard
            .insert(address.clone(), discard.to_vec());
    }

    /// Return the discarded table for the given node.
    pub fn discard(&self, address: &TreeAddress) -> &[usize] {
        self.discard.get(address).expect("missing discard")
    }

    /// Add a valid table for a given step.
    pub fn add_valid_table(&mut self, address: &TreeAddress, step: usize, id: PermanentTableId) {
        self.valid
            .entry(address.clone())
            .or_default()
            .add_table(step, id);
    }

    /// Add a assignment table for a given step.
    pub fn add_assignment_table(
        &mut self,
        address: &TreeAddress,
        step: usize,
        id: PermanentTableId,
    ) {
        self.assignment
            .entry(address.clone())
            .or_default()
            .add_table(step, id);
    }

    /// Add a result table.
    pub fn add_result_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.result.insert(address.clone(), id);
    }

    /// Add a consolidated valid table.
    pub fn add_final_valid_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.valid_final.insert(address.clone(), id);
    }

    /// Add a consolidated assignment table.
    pub fn add_final_assignment_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.assignment_final.insert(address.clone(), id);
    }

    /// Add a query table.
    pub fn add_query_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.query.insert(address.clone(), id);
    }

    /// Return an iterator over all "valid" tables before a given step.
    pub fn valid_tables_before(
        &self,
        address: &TreeAddress,
        step: usize,
    ) -> Box<dyn Iterator<Item = PermanentTableId> + '_> {
        match self.valid.get(address) {
            Some(tables) => Box::new(tables.tables_before(step)),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Return an iterator over all valid tables.
    pub fn valid_tables(
        &self,
        address: &TreeAddress,
    ) -> Box<dyn Iterator<Item = PermanentTableId> + '_> {
        match self.valid.get(address) {
            Some(tables) => Box::new(tables.all_tables()),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Return an iterator over all assignment tables.
    pub fn assignment_tables(
        &self,
        address: &TreeAddress,
    ) -> Box<dyn Iterator<Item = PermanentTableId> + '_> {
        match self.assignment.get(address) {
            Some(tables) => Box::new(tables.all_tables()),
            None => Box::new(std::iter::empty()),
        }
    }

    /// For a given address, return the id of the associated result table,
    /// if it exists.
    pub fn result_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.result.get(address).cloned()
    }

    /// For a given address, return the id of the associated consolidated valid table,
    /// if it exists.
    pub fn final_valid_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.valid_final.get(address).cloned()
    }

    /// For a given address, return the id of the associated consolidated assignment table,
    /// if it exists.
    pub fn final_assignment_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.assignment_final.get(address).cloned()
    }

    /// For a given address, return the id of the associated query table,
    /// if it exists.
    pub fn query_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.query.get(address).cloned()
    }

    /// Return an iterator over all result tables.
    pub fn _results(&self) -> impl Iterator<Item = PermanentTableId> {
        self.result.values().copied()
    }
}
