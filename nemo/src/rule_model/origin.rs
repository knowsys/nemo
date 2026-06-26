//! This module defines [Origin].

use crate::parser::ast::ProgramAST;

use super::{
    components::{ComponentSource, ProgramComponent},
    pipeline::id::ProgramComponentId,
};

/// Origin of a [super::components::ProgramComponent]
#[derive(Debug, Clone, Default)]
pub enum Origin {
    /// Component has no special origin
    #[default]
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

    /// Value was supplied externally, e.g. via command line parameters
    Extern,

    /// Substitution
    Substitution {
        /// Old component that was replaced
        replaced: ProgramComponentId,
        /// New component that is replacing it
        replacing: ProgramComponentId,
    },

    /// Rule that was created by normalizing a rule
    /// (see [crate::rule_model::pipeline::transformations::normalize::TransformationNormalize]).
    Normalization(ProgramComponentId),

    /// Rule that was created by substituting global variables in a rule
    /// (see [crate::rule_model::pipeline::transformations::global::TransformationGlobal]).
    Global(ProgramComponentId),

    /// Rule that was created by inlining incremental imports into a rule
    /// (see [crate::rule_model::pipeline::transformations::incremental::TransformationIncremental]).
    Incremental(ProgramComponentId),

    /// Rule that was created by merging SPARQL imports in a rule
    /// (see [crate::rule_model::pipeline::transformations::merge_sparql::TransformationMergeSparql]).
    MergeSparql(ProgramComponentId),
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

    /// Substition
    pub fn substitution<ComponentReplaced, ComponentReplacing>(
        replaced: &ComponentReplaced,
        replacing: &ComponentReplacing,
    ) -> ComponentReplacing
    where
        ComponentReplaced: ProgramComponent,
        ComponentReplacing: ProgramComponent + Clone,
    {
        let replaced_id = replaced.id();
        let replacing_id = replacing.id();

        let mut replacing = replacing.clone();
        replacing.set_origin(Origin::Substitution {
            replaced: replaced_id,
            replacing: replacing_id,
        });

        replacing
    }
}
