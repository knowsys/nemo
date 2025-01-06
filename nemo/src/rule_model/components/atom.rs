//! This module defines an [Atom].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    error::{validation_error::ValidationErrorKind, ValidationErrorBuilder},
    origin::Origin,
    translation::TranslationComponent,
};

use super::{
    literal::Literal,
    parse::ComponentParseError,
    tag::Tag,
    term::{
        primitive::{variable::Variable, Primitive},
        Term,
    },
    IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
};

/// Atom
///
/// Tagged list of [Term]s.
/// It forms the core component of rules,
/// representing a logical proposition that can be true or false.
#[derive(Debug, Clone, Eq)]
pub struct Atom {
    /// Origin of this component.
    origin: Origin,

    /// Predicate name associated with this atom
    predicate: Tag,
    /// Subterms of the function
    terms: Vec<Term>,
}

/// Construct an [Atom].
#[macro_export]
macro_rules! atom {
    // Base case: no elements
    ($name:tt) => {
        $crate::rule_model::components::atom::Atom::new(
            $crate::rule_model::components::tag::Tag::from($name),
            Vec::new()
        )
    };
    // Recursive case: handle each term, separated by commas
    ($name:tt; $($tt:tt)*) => {{
        #[allow(clippy::vec_init_then_push)] {
            let mut terms = Vec::new();
            term_list!(terms; $($tt)*);
            $crate::rule_model::components::atom::Atom::new(
                $crate::rule_model::components::tag::Tag::from($name),
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
            predicate,
            terms: subterms.into_iter().collect(),
        }
    }

    pub fn parse(input: &str) -> Result<Self, ComponentParseError> {
        let Literal::Positive(atom) = Literal::parse(input)? else {
            return Err(ComponentParseError::ParseError);
        };

        Ok(atom)
    }

    /// Return the predicate of this atom.
    pub fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    /// Return an iterator over the arguments of this atom.
    pub fn arguments(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return the number of subterms in this atom.
    pub fn len(&self) -> usize {
        self.terms.len()
    }

    /// Return whether this atom contains no subterms.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        self.origin == other.origin
            && self.predicate == other.predicate
            && self.terms == other.terms
    }
}

impl Hash for Atom {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.terms.hash(state);
    }
}

impl ProgramComponent for Atom {
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

        if self.is_empty() {
            builder.report_error(self.origin, ValidationErrorKind::UnsupportedAtomEmpty);
        }

        for term in self.arguments() {
            term.validate(builder)?;
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Atom
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

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
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
        let atom = atom!("p"; 12, variable, !e, "abc", ?v);

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
