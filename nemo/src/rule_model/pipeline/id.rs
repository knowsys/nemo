//! This module defines [ProgramComponentId].

use std::{cmp::Ordering, hash::Hash};

use crate::rule_model::components::statement::Statement;

/// Identifies a [super::super::components::ProgramComponent] within a [super::ProgramPipeline]
///
/// The id `Self::UNASSIGNED` represents the [super::super::components::ProgramComponent]
/// not being associated with any [super::ProgramPipeline].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ProgramComponentId {
    /// Identifier of the statement within a [super::ProgramPipeline]
    statement: Option<id_arena::Id<Statement>>,
    /// Identifier of a component within a statement
    ///
    /// A local id of zero refers to the statement itself.
    component: u32,
}

impl ProgramComponentId {
    /// Indicates that the [super::super::components::ProgramComponent]
    /// does not belong to any [super::ProgramPipeline]
    const UNASSIGNED: Self = Self {
        statement: None,
        component: u32::MAX,
    };

    /// Create a new [ProgramComponentId].
    pub fn new_component(statement: id_arena::Id<Statement>, component: usize) -> Self {
        Self {
            statement: Some(statement),
            component: component
                .try_into()
                .expect("maximum number of statements reached"),
        }
    }

    /// Create a new [ProgramComponentId]
    /// referring to a statement
    pub fn new_statement(id: id_arena::Id<Statement>) -> Self {
        Self {
            statement: Some(id),
            component: 0,
        }
    }

    /// Create a new [ProgramComponentId]
    /// that indicates that the [super::super::components::ProgramComponent]
    /// does not belong to any [super::ProgramPipeline]
    pub fn new_unassigned() -> Self {
        Self::UNASSIGNED
    }

    /// Return the statement id.
    pub fn statement(&self) -> Option<id_arena::Id<Statement>> {
        self.statement
    }

    /// Return the local component id.
    pub fn component(&self) -> usize {
        self.component as usize
    }

    /// Increment the component part of the id and return the new id.
    pub fn increment_component(&self) -> Self {
        let mut result = *self;

        if *self != Self::UNASSIGNED {
            result.component += 1;
        }

        result
    }

    /// Returns whether a id has been assigned yet.
    pub fn is_assigned(&self) -> bool {
        *self != Self::UNASSIGNED
    }

    /// Return whether this id is assigned to a statement or a child of a statement
    pub fn is_statement(&self) -> bool {
        self.component == 0
    }
}

impl Default for ProgramComponentId {
    fn default() -> Self {
        Self::new_unassigned()
    }
}

impl PartialOrd for ProgramComponentId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.statement.partial_cmp(&other.statement) {
            Some(Ordering::Equal) => self.component.partial_cmp(&other.component),
            ordering => ordering,
        }
    }
}
