//! This module defines [ProgramComponentAddress]

use std::ops::Deref;

use crate::model::components::{atom::Atom, rule::Rule, ProgramComponent};

/// Used to locate a [super::super::components::ProgramComponent] within
/// a [super::ProgramPipeline]
#[derive(Debug, Clone, Default)]
pub struct ProgramComponentAddress(Vec<AddressSegment>);

impl ProgramComponentAddress {
    /// Push a [ProgramComponentAddress] onto the address.
    pub(crate) fn push(&mut self, segment: AddressSegment) {
        self.0.push(segment);
    }
}

impl Deref for ProgramComponentAddress {
    type Target = [AddressSegment];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Part of [ProgramComponentAddress],
/// which can be used to navigate through one layer
/// in the tree structure of [super::super::components::ProgramComponent]s
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AddressSegment {
    /// Top-level statement: Rule
    Rule(usize),
    /// Top-level statement: Fact
    Fact(usize),
    /// Top-level statement: Import statement
    Import(usize),
    /// Top-level statement: Export statement
    Export(usize),
    /// Rule: Head atom
    Head(usize),
    /// Rule: Body literal
    Body(usize),
    // TODO: ...
}

pub(crate) trait Addressable {
    fn next_component(&self, segment: &AddressSegment) -> Option<Box<&dyn Addressable>>;

    fn address_rule(&self, _segment: &AddressSegment) -> Option<&Rule> {
        None
    }

    fn address_atom(&self, _segment: &AddressSegment) -> Option<&Atom> {
        None
    }

    fn address_fact(&self, _segment: &AddressSegment) -> Option<&Atom> {
        None
    }
}
