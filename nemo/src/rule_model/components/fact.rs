//! This module defines [Fact].

use std::{
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut, Index, IndexMut},
};

use crate::{
    chase_model::components::atom::{ground_atom::GroundAtom, ChaseAtom},
    rule_model::{
        error::{hint::Hint, validation_error::ValidationError, ValidationReport},
        origin::Origin,
        pipeline::id::ProgramComponentId,
    },
};

use super::{
    atom::Atom,
    component_iterator, component_iterator_mut,
    tag::Tag,
    term::{primitive::Primitive, Term},
    ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent, IterablePrimitives,
    IterableVariables, ProgramComponent, ProgramComponentKind,
};

/// A (ground) fact
#[derive(Debug, Clone)]
pub struct Fact {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Predicate of the fact
    predicate: Tag,
    /// List of [Term]s
    terms: Vec<Term>,
}

impl Fact {
    /// Create a new [Fact].
    pub fn new<Terms: IntoIterator<Item = Term>>(predicate: Tag, subterms: Terms) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            predicate,
            terms: subterms.into_iter().collect(),
        }
    }

    /// TODO:
    pub fn parse(_input: &str) -> Result<Self, ()> {
        // Ok(Fact::from(HeadAtom::parse(input)?.into_inner()))
        todo!()
    }

    /// Return the predicate associated with this fact.
    pub fn predicate(&self) -> &Tag {
        &self.predicate
    }

    /// Return an iterator over the subterms of this fact.
    pub fn terms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an mutable iterator over the subterms of this fact.
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

    /// If the given [Term] is a function term,
    /// then this function returns a [Hint] returning the operation
    /// with the closest name to its tag.
    fn hint_term_operation(term: &Term) -> Option<Hint> {
        if let Term::FunctionTerm(function) = term {
            Hint::similar_operation(function.tag().to_string())
        } else {
            None
        }
    }
}

impl Index<usize> for Fact {
    type Output = Term;

    fn index(&self, index: usize) -> &Self::Output {
        &self.terms[index]
    }
}

impl IndexMut<usize> for Fact {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.terms[index]
    }
}

impl Deref for Fact {
    type Target = [Term];

    fn deref(&self) -> &Self::Target {
        &self.terms
    }
}

impl DerefMut for Fact {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terms
    }
}

impl From<Atom> for Fact {
    fn from(value: Atom) -> Self {
        Self {
            origin: value.origin(),
            id: ProgramComponentId::default(),
            predicate: value.predicate(),
            terms: value.terms().cloned().collect(),
        }
    }
}

impl Display for Fact {
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

impl PartialEq for Fact {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate && self.terms == other.terms
    }
}

impl Eq for Fact {}

impl Hash for Fact {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.terms.hash(state);
    }
}

impl ComponentBehavior for Fact {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Fact
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

        for term in self.terms() {
            if term.is_map() || term.is_tuple() || term.is_function() {
                report
                    .add(term, ValidationError::UnsupportedComplexTerm)
                    .add_hint_option(Self::hint_term_operation(term));
            }

            if term.is_aggregate() {
                report.add(term, ValidationError::FactSubtermAggregate);
            }

            if let Some(variable) = term.variables().find(|variable| !variable.is_global()) {
                report.add(variable, ValidationError::FactNonGround);
                continue;
            }
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for Fact {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for Fact {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for Fact {
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

impl IterablePrimitives for Fact {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(self.terms().flat_map(|term| term.primitive_terms()))
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        Box::new(
            self.terms
                .iter_mut()
                .flat_map(|term| term.primitive_terms_mut()),
        )
    }
}

impl From<GroundAtom> for Fact {
    fn from(value: GroundAtom) -> Self {
        let origin = Origin::Created; // TODO
        let predicate = value.predicate();
        let terms = value.terms().cloned().map(Term::from).collect();

        Self {
            origin,
            id: ProgramComponentId::default(),
            predicate,
            terms,
        }
    }
}
