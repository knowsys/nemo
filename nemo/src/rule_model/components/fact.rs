//! This module defines [Fact].

use std::{fmt::Display, hash::Hash};

use crate::{
    chase_model::components::{
        atom::{ground_atom::GroundAtom, ChaseAtom},
        ChaseComponent,
    },
    rule_model::{
        error::{
            validation_error::ValidationErrorKind, ComponentParseError, ValidationErrorBuilder,
        },
        origin::Origin,
        translation::{literal::HeadAtom, TranslationComponent},
    },
};

use super::{
    atom::Atom,
    tag::Tag,
    term::{primitive::Primitive, Term},
    IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
};

/// A (ground) fact
#[derive(Debug, Clone, Eq)]
pub struct Fact {
    /// Origin of this component
    origin: Origin,

    /// Predicate of the fact
    predicate: Tag,
    /// List of [Term]s
    terms: Vec<Term>,
}

impl Fact {
    /// Create a new [Fact].
    pub fn new<Terms: IntoIterator<Item = Term>>(predicate: &str, subterms: Terms) -> Self {
        Self {
            origin: Origin::Created,
            predicate: Tag::new(predicate.to_string()),
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

    /// Returns whether the fact contains a term that is cyclic.
    pub fn is_cyclic(&self) -> bool {
        let mut reused_function_tags: Vec<&Tag> = Vec::<&Tag>::new();
        self.subterms()
            .any(|term| term.is_cyclic(&mut reused_function_tags))
    }
}

impl From<(&Tag, Vec<Term>)> for Fact {
    fn from((predicate, terms): (&Tag, Vec<Term>)) -> Self {
        Self {
            origin: Origin::Created,
            predicate: predicate.clone(),
            terms,
        }
    }
}

impl From<Atom> for Fact {
    fn from(value: Atom) -> Self {
        Self {
            origin: *value.origin(),
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

impl Hash for Fact {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.terms.hash(state);
    }
}

impl ProgramComponent for Fact {
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        if !self.predicate.is_valid() {
            builder.report_error(
                *self.predicate.origin(),
                ValidationErrorKind::InvalidTermTag(self.predicate.to_string()),
            );
        }

        for term in self.subterms() {
            if let Some(variable) = term.variables().next() {
                builder.report_error(*variable.origin(), ValidationErrorKind::FactNonGround);
                continue;
            }

            term.validate(builder)?;
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Fact
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
        let origin = *value.origin();
        let predicate = value.predicate();
        let terms = value.terms().cloned().map(Term::from).collect();

        Self {
            origin,
            predicate,
            terms,
        }
    }
}
