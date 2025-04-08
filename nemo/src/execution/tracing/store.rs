//! This module defines [].

use std::collections::HashMap;

use crate::chase_model::GroundAtom;

#[derive(Debug, Default)]
pub(crate) struct FactStore {
    /// All the facts considered during tracing
    facts: Vec<GroundAtom>,
}

impl FactStore {
    pub(crate) fn add_fact(&mut self, fact: GroundAtom) -> usize {
        if let Some(id) = self
            .facts
            .iter()
            .position(|stored_fact| *stored_fact == fact)
        {
            id
        } else {
            let id = self.facts.len();
            self.facts.push(fact);

            id
        }
    }
}
