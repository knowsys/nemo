//! This module defines an [Atom].

use std::{
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut, Index, IndexMut},
};

use crate::{
    parser::ParserErrorReport,
    rule_model::{
        error::{validation_error::ValidationError, ValidationReport},
        origin::Origin,
        pipeline::id::ProgramComponentId,
        translation::{ProgramParseReport, TranslationComponent},
    },
};

use super::{
    component_iterator, component_iterator_mut,
    literal::Literal,
    tag::Tag,
    term::{
        primitive::{variable::Variable, Primitive},
        Term,
    },
    ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent, IterablePrimitives,
    IterableVariables, ProgramComponent, ProgramComponentKind,
};

/// Atom
///
/// Tagged list of [Term]s.
/// It forms the core component of rules,
/// representing a logical proposition that can be true or false.
#[derive(Debug, Clone)]
pub struct Atom {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Predicate name associated with this atom
    predicate: Tag,
    /// Subterms of the function
    terms: Vec<Term>,
}

/// Construct an [Atom].
#[macro_export]
macro_rules! atom {
    // Base case: no elements
    ($name:ident) => {
        $crate::rule_model::components::atom::Atom::new(
            $crate::rule_model::components::tag::Tag::from(stringify!($name)),
            Vec::new()
        )
    };
    // Recursive case: handle each term, separated by commas
    ($name:ident($($tt:tt)*)) => {{
        #[allow(clippy::vec_init_then_push)] {
            let mut terms = Vec::new();
            term_list!(terms; $($tt)*);
            $crate::rule_model::components::atom::Atom::new(
                $crate::rule_model::components::tag::Tag::from(stringify!($name)),
                terms
            )
        }
    }};
}

impl Atom {
    /// Create a new [Atom].
    pub fn new<Terms: IntoIterator<Item = Term>>(predicate: Tag, subterms: Terms) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            predicate,
            terms: subterms.into_iter().collect(),
        }
    }

    /// Construct this object from a string.
    pub fn parse(input: &str) -> Result<Self, ProgramParseReport> {
        if let Literal::Positive(atom) = Literal::parse(input)? {
            Ok(atom)
        } else {
            Err(ProgramParseReport::Parsing(ParserErrorReport::empty()))
        }
    }

    /// Return the predicate of this atom.
    pub fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    /// Return an iterator over the terms of this atom.
    pub fn terms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an iterator over the terms of this atom.
    pub fn terms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
    }

    /// Push a [Term] to the end of this atom.
    pub fn push(&mut self, term: Term) {
        self.terms.push(term);
    }

    /// Remove the [Term] at the given index and return it.
    ///
    /// # Panics
    /// Panics if the index is out of bounds.
    pub fn remove(&mut self, index: usize) -> Term {
        self.terms.remove(index)
    }
}

impl Index<usize> for Atom {
    type Output = Term;

    fn index(&self, index: usize) -> &Self::Output {
        &self.terms[index]
    }
}

impl IndexMut<usize> for Atom {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.terms[index]
    }
}

impl Deref for Atom {
    type Target = [Term];

    fn deref(&self) -> &Self::Target {
        &self.terms
    }
}

impl DerefMut for Atom {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terms
    }
}

impl Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}(", self.predicate))?;

        for (term_index, term) in self.terms.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl PartialEq for Atom {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate && self.terms == other.terms
    }
}

impl Eq for Atom {}

impl Hash for Atom {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.terms.hash(state);
    }
}

impl ComponentBehavior for Atom {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Atom
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        if !self.predicate.is_valid() {
            report.add(
                &self.predicate,
                ValidationError::InvalidPredicateName {
                    predicate_name: self.predicate.to_string(),
                },
            );
        }

        if self.is_empty() {
            report.add(self, ValidationError::UnsupportedAtomEmpty);
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for Atom {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for Atom {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for Atom {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let subterm_iterator = component_iterator(self.terms.iter());
        Box::new(subterm_iterator)
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let subterm_iterator = component_iterator_mut(self.terms.iter_mut());
        Box::new(subterm_iterator)
    }
}

impl IterableVariables for Atom {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms.iter_mut().flat_map(|term| term.variables_mut()))
    }
}

impl IterablePrimitives for Atom {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(self.terms.iter().flat_map(|term| term.primitive_terms()))
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        Box::new(
            self.terms
                .iter_mut()
                .flat_map(|term| term.primitive_terms_mut()),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::components::{term::primitive::variable::Variable, IterableVariables};

    #[test]
    fn atom_basic() {
        let variable = Variable::universal("u");
        let atom = atom!(p(12, variable, !e, "abc", ?v));

        let variables = atom.variables().cloned().collect::<Vec<_>>();
        assert_eq!(
            variables,
            vec![
                Variable::universal("u"),
                Variable::existential("e"),
                Variable::universal("v")
            ]
        );
    }
}
