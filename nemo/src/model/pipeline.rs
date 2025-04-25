//! This module defines [ProgramPipeline].

use std::collections::HashMap;

use address::{AddressSegment, Addressable, ProgramComponentAddress};
use id::ProgramComponentId;

use super::components::{atom::Atom, program::Program, rule::Rule, ProgramComponent};

pub mod address;
pub mod id;

#[derive(Debug, Clone, Copy)]
struct StatementValidity {
    pub step: usize,
    pub count: usize,
}

#[derive(Debug)]
struct Statement<Component> {
    pub list: Vec<Component>,
    pub valid: StatementValidity,
}

/// Big manager object
#[derive(Debug)]
pub struct ProgramPipeline {
    id_to_address: HashMap<ProgramComponentId, ProgramComponentAddress>,

    program: Program,
}

// Address resolution
impl ProgramPipeline {
    fn last_addressable<'a, 'b>(
        &'a self,
        address: &'b ProgramComponentAddress,
    ) -> Option<(&'a dyn Addressable, &'b AddressSegment)> {
        let mut current_component: &dyn Addressable = &self.program;
        let mut segment_iter = address.iter().peekable();

        while let Some(segment) = segment_iter.next() {
            if segment_iter.peek().is_some() {
                current_component = *current_component.next_component(segment)?;
            } else {
                return Some((current_component, segment));
            }
        }

        None
    }

    /// Return the [Rule] associated with the given [ProgramComponentId],
    /// if it exists.
    pub fn rule_by_id(&self, id: ProgramComponentId) -> Option<&Rule> {
        let address = self.id_to_address.get(&id)?;
        let (component, segment) = self.last_addressable(address)?;
        component.address_rule(segment)
    }

    /// Return the [Atom] associated with the given [ProgramComponentId],
    /// if it exists.
    pub fn atom_by_id(&self, id: ProgramComponentId) -> Option<&Atom> {
        let address = self.id_to_address.get(&id)?;
        let (component, segment) = self.last_addressable(address)?;
        component.address_atom(segment)
    }
}
