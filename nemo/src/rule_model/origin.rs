//! This module defines [Origin].

use std::ops::Range;

use crate::parser::ast::ProgramAST;

use super::{
    components::{ComponentSource, ProgramComponent},
    pipeline::{id::ProgramComponentId, ProgramPipeline},
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
        let end = node.span().range().range().end;

        let origin = Self::File { start, end };
        component.set_origin(origin);

        component
    }

    /// Translate [Origin] into a range of characters,
    /// if it the component originated from parsing a file.
    pub fn to_range(&self) -> Option<Range<usize>> {
        if let Self::File { start, end } = self {
            Some(*start..*end)
        } else {
            None
        }
    }

    /// Translate [Origin] into a range of characters,
    /// if it the component originated from parsing a file.
    pub fn to_range_pipeline(&self, pipeline: &ProgramPipeline) -> Option<Range<usize>> {
        match self {
            Origin::Created => None,
            Origin::File { start, end } => Some(*start..*end),
            Origin::Reference(id) => pipeline
                .find_component(*id)?
                .origin()
                .to_range_pipeline(pipeline),
            Origin::Component(component) => component.origin().to_range_pipeline(pipeline),
            Origin::RuleCombination(first, _second) => first.to_range_pipeline(pipeline),
        }
    }
}
