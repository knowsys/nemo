//! This module defines [Fact].

use std::{fmt::Display, hash::Hash};

use crate::{
    chase_model::components::{
        atom::{ground_atom::GroundAtom, ChaseAtom},
        ChaseComponent,
    },
    rule_model::{
        error::{hint::Hint, ComponentParseError, ValidationErrorBuilder},
        origin::Origin,
        pipeline::id::ProgramComponentId,
        translation::{literal::HeadAtom, TranslationComponent},
    },
};

use super::{
    atom::Atom,
    component_iterator, component_iterator_mut,
    tag::Tag,
    term::{primitive::Primitive, Term},
    ComponentBehavior, ComponentIdentity, IterableComponent, IterablePrimitives, ProgramComponent,
    ProgramComponentKind,
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

    pub fn parse(input: &str) -> Result<Self, ComponentParseError> {
        Ok(Fact::from(HeadAtom::parse(input)?.into_inner()))
    }

    /// Return the predicate associated with this fact.
    pub fn predicate(&self) -> &Tag {
        &self.predicate
    }

    /// Return an iterator over the subterms of this fact.
    pub fn subterms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an mutable iterator over the subterms of this fact.
    pub fn subterms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
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

impl From<Atom> for Fact {
    fn from(value: Atom) -> Self {
        Self {
            origin: value.origin().clone(),
            id: ProgramComponentId::default(),
            predicate: value.predicate(),
            terms: value.arguments().cloned().collect(),
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

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()> {
        // if !self.predicate.is_valid() {
        //             builder.report_error(
        //                 *self.predicate.origin(),
        //                 ValidationErrorKind::InvalidTermTag(self.predicate.to_string()),
        //             );
        //         }

        //         for term in self.subterms() {
        //             if term.is_map() || term.is_tuple() || term.is_function() {
        //                 builder
        //                     .report_error(*term.origin(), ValidationErrorKind::UnsupportedComplexTerm)
        //                     .add_hint_option(Self::hint_term_operation(term));
        //                 return None;
        //             }

        //             if term.is_aggregate() {
        //                 builder.report_error(*term.origin(), ValidationErrorKind::FactSubtermAggregate);
        //                 return None;
        //             }

        //             if let Some(variable) = term.variables().next() {
        //                 builder.report_error(*variable.origin(), ValidationErrorKind::FactNonGround);
        //                 continue;
        //             }

        //             term.validate(builder)?;
        //         }

        Some(())
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for Fact {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
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
        Box::new(self.subterms().flat_map(|term| term.primitive_terms()))
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        Box::new(
            self.terms
                .iter_mut()
                .flat_map(|term| term.primitive_terms_mut()),
        )
    }
}

impl From<GroundAtom> for Fact {
    fn from(value: GroundAtom) -> Self {
        let origin = value.origin().clone();
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
