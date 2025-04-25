//! This module defines [Program].

use std::ops::Add;

use crate::model::pipeline::address::{AddressSegment, Addressable};

use super::{atom::Atom, rule::Rule, IterableProgramComponent, ProgramComponent};

#[derive(Debug)]
pub struct Program {
    /// Rules
    rules: Vec<Rule>,
    /// Facts
    facts: Vec<Atom>,
}

impl Program {
    pub fn empty() -> Self {
        Self {
            rules: Vec::default(),
            facts: Vec::default(),
        }
    }

    /// Return the [Rule] at the given position.
    pub fn rule(&self, index: usize) -> &Rule {
        &self.rules[index]
    }

    /// Return the [Atom] at the given position
    pub fn fact(&self, index: usize) -> &Atom {
        &self.facts[index]
    }
}

impl IterableProgramComponent for Program {
    fn components<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        Box::new(self.rules.iter().map(|rule| {
            let rule: &dyn ProgramComponent = rule;
            rule
        }))
    }

    fn components_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        todo!()
    }

    fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter()
    }
}

impl Addressable for Program {
    fn next_component(&self, segment: &AddressSegment) -> Option<Box<&dyn Addressable>> {
        Some(Box::new(match segment {
            AddressSegment::Rule(index) => self.rule(*index),
            AddressSegment::Fact(index) => self.fact(*index),
            _ => return None,
        }))
    }

    fn address_rule(&self, segment: &AddressSegment) -> Option<&Rule> {
        if let &AddressSegment::Rule(index) = segment {
            Some(self.rule(index))
        } else {
            None
        }
    }

    fn address_fact(&self, segment: &AddressSegment) -> Option<&Atom> {
        if let &AddressSegment::Fact(index) = segment {
            Some(self.fact(index))
        } else {
            None
        }
    }
}
