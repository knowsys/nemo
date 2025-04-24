//! This module defines [ProgramComponentAddress]

/// Used to locate a [super::super::components::ProgramComponent] within
/// a [super::ProgramPipeline]
#[derive(Debug, Clone, Default)]
pub struct ProgramComponentAddress(Vec<AddressSegment>);

impl ProgramComponentAddress {
    /// Push a [ProgramComponentAddress] onto the address.
    pub fn push(&mut self, segment: AddressSegment) {
        self.0.push(segment);
    }

    /// Get an iterator over all [AddressSegment]s.
    pub fn iter(&self) -> impl Iterator<Item = AddressSegment> + '_ {
        self.0.iter().cloned()
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
