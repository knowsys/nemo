//! This module defines [VariableAtom].

use std::fmt::Display;

use crate::{
    chase_model::components::ChaseComponent,
    rule_model::{
        components::{
            atom::Atom,
            tag::Tag,
            term::{
                primitive::{variable::Variable, Primitive},
                Term,
            },
            IterableVariables, ProgramComponent,
        },
        origin::Origin,
    },
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

use super::ChaseAtom;

/// An atom which may only use [Variable]s.
#[derive(Debug, Clone)]
pub(crate) struct VariableAtom {
    /// Origin of this component
    origin: Origin,

    /// Predicate name of this atom
    predicate: Tag,
    /// Variables contained in this atom
    variables: Vec<Variable>,
}

impl VariableAtom {
    /// Construct a new [VariableAtom].
    pub(crate) fn new(predicate: Tag, variables: Vec<Variable>) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
            variables,
        }
    }
}

impl Display for VariableAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let terms = DisplaySeperatedList::display(
            self.terms(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );
        let predicate = self.predicate();

        f.write_str(&format!(
            "{predicate}{}{terms}{}",
            syntax::expression::atom::OPEN,
            syntax::expression::atom::CLOSE
        ))
    }
}

impl ChaseAtom for VariableAtom {
    type TypeTerm = Variable;

    fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    fn terms(&self) -> impl Iterator<Item = &Self::TypeTerm> {
        self.variables.iter()
    }

    fn terms_mut(&mut self) -> impl Iterator<Item = &mut Self::TypeTerm> {
        self.variables.iter_mut()
    }
}

impl IterableVariables for VariableAtom {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms())
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms_mut())
    }
}

impl ChaseComponent for VariableAtom {
    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }
}

/// Error struct for converting logical atoms to [VariableAtom]s
#[derive(Debug)]
pub(crate) struct VariableAtomConversionError;

impl Display for VariableAtomConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("atom contains non-variable terms")
    }
}

impl TryFrom<Atom> for VariableAtom {
    type Error = VariableAtomConversionError;

    fn try_from(value: Atom) -> Result<Self, Self::Error> {
        let origin = *value.origin();
        let predicate = value.predicate();
        let mut terms = Vec::new();

        for term in value.arguments().cloned() {
            if let Term::Primitive(Primitive::Variable(variable)) = term {
                terms.push(variable)
            } else {
                return Err(VariableAtomConversionError);
            }
        }

        Ok(Self::new(predicate, terms).set_origin(origin))
    }
}
