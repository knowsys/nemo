//! This module defines [Origin].

use crate::parser::ast::ProgramAST;

use super::{
    components::{rule::Rule, ComponentDuplicate, ComponentSource, ProgramComponent},
    pipeline::id::ProgramComponentId,
};

/// Origin of a [super::components::ProgramComponent]
#[derive(Debug, Clone)]
pub enum Origin {
    /// Component has no special origin
    Created,
    /// Component was created by parsing a file
    File {
        /// Start position of the character
        start: usize,
        /// End position of the character
        end: usize,
    },

    /// Reference to another object by id
    Reference(ProgramComponentId),
    /// Component was created from another component
    Component(Box<dyn ProgramComponent>),

    /// Combination of two rules
    RuleCombination(Box<Origin>, Box<Origin>),
}

impl Default for Origin {
    fn default() -> Self {
        Self::Created
    }
}

impl Origin {
    /// Create an Oriign pointing to character range represented
    /// by the given ast node.
    pub fn ast<'a, Component: ComponentSource<Source = Self>, Node: ProgramAST<'a>>(
        mut component: Component,
        node: &Node,
    ) -> Component {
        let start = node.span().range().range().start;
        let end = node.span().range().range().start;

        let origin = Self::File { start, end };
        component.set_origin(origin);

        component
    }

    /// Create an [Origin] that ... combination
    pub fn rule_combination(first: &Rule, second: &Rule) -> Self {
        let first_origin = first.duplicated_origin();
        let second_origin = second.duplicated_origin();

        Self::RuleCombination(Box::new(first_origin), Box::new(second_origin))
    }
}
