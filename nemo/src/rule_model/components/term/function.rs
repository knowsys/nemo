//! This module defines [FunctionTerm].

use std::{fmt::Display, hash::Hash, vec};

use crate::rule_model::{
    components::{
        tag::Tag, IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
    },
    error::{validation_error::ValidationErrorKind, ValidationErrorBuilder},
    origin::Origin,
    substitution::Substitution,
};

use super::{
    primitive::{variable::Variable, Primitive},
    value_type::ValueType,
    Term,
};

/// Function term
///
/// List of [Term]s with a [Tag].
#[derive(Debug, Clone, Eq)]
pub struct FunctionTerm {
    /// Origin of this component
    origin: Origin,

    /// Name of the function
    tag: Tag,
    /// Subterms of the function
    terms: Vec<Term>,
}

/// Construct a [FunctionTerm].
#[macro_export]
macro_rules! function {
    // Base case: no elements
    ($name:tt) => {
        $crate::rule_model::components::term::function::FunctionTerm::new(
            $crate::rule_model::components::tag::Tag::from($name), Vec::new()
        )
    };
    // Recursive case: handle each term, separated by commas
    ($name:tt; $($tt:tt)*) => {{
        #[allow(clippy::vec_init_then_push)] {
            let mut terms = Vec::new();
            term_list!(terms; $($tt)*);
            $crate::rule_model::components::term::function::FunctionTerm::new(
                $name, terms
            )
        }
    }};
}

impl FunctionTerm {
    /// Create a new [FunctionTerm].
    pub fn new<Terms: IntoIterator<Item = Term>>(name: &str, subterms: Terms) -> Self {
        Self {
            origin: Origin::Created,
            tag: Tag::new(name.to_string()),
            terms: subterms.into_iter().collect(),
        }
    }

    /// Create a new [FunctionTerm] with a [Tag].
    pub(crate) fn new_tag<Terms: IntoIterator<Item = Term>>(tag: Tag, subterms: Terms) -> Self {
        Self {
            origin: Origin::Created,
            tag,
            terms: subterms.into_iter().collect(),
        }
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        ValueType::FunctionTerm
    }

    /// Return an iterator over the arguments of this function term.
    pub fn arguments(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return the number of subterms contains in this function term.
    pub fn len(&self) -> usize {
        self.terms.len()
    }

    /// Return whether this function terms contains no subterms.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the [Tag] of the function term.
    pub fn tag(&self) -> &Tag {
        &self.tag
    }

    /// Return whether this term is ground,
    /// i.e. if it does not contain any variables.
    pub fn is_ground(&self) -> bool {
        self.terms.iter().all(Term::is_ground)
    }

    /// Reduce each sub [Term] in the function returning a copy.
    pub fn reduce_with_substitution(&self, bindings: &Substitution) -> Self {
        Self {
            origin: self.origin,
            tag: self.tag.clone(),
            terms: self
                .terms
                .iter()
                .map(|term| term.reduce_with_substitution(bindings))
                .collect(),
        }
    }
}

impl Display for FunctionTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}(", self.tag))?;

        for (term_index, term) in self.terms.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl IntoIterator for FunctionTerm {
    type Item = Term;
    type IntoIter = vec::IntoIter<Term>;

    fn into_iter(self) -> Self::IntoIter {
        self.terms.into_iter()
    }
}

impl PartialEq for FunctionTerm {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag && self.terms == other.terms
    }
}

impl PartialOrd for FunctionTerm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.tag.partial_cmp(&other.tag) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.terms.partial_cmp(&other.terms)
    }
}

impl Hash for FunctionTerm {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tag.hash(state);
        self.terms.hash(state);
    }
}

impl ProgramComponent for FunctionTerm {
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
        if !self.tag.is_valid() {
            builder.report_error(
                *self.tag.origin(),
                ValidationErrorKind::InvalidTermTag(self.tag.to_string()),
            );
        }

        for term in self.arguments() {
            term.validate(builder)?
        }

        if self.is_empty() {
            builder.report_error(self.origin, ValidationErrorKind::FunctionTermEmpty);
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::FunctionTerm
    }
}

impl IterableVariables for FunctionTerm {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms.iter_mut().flat_map(|term| term.variables_mut()))
    }
}

impl IterablePrimitives for FunctionTerm {
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
    use crate::rule_model::{
        components::{
            term::{function::FunctionTerm, primitive::variable::Variable, Term},
            IterableVariables,
        },
        translation::TranslationComponent,
    };

    #[test]
    fn function_basic() {
        let variable = Variable::universal("u");
        let function = function!("f"; 12, variable, !e, "abc", ?v);

        let variables = function.variables().cloned().collect::<Vec<_>>();
        assert_eq!(
            variables,
            vec![
                Variable::universal("u"),
                Variable::existential("e"),
                Variable::universal("v")
            ]
        );
    }

    #[test]
    fn parse_function() {
        let function = FunctionTerm::parse("f(?x, 2, ?y)").unwrap();

        assert_eq!(
            FunctionTerm::new(
                "f",
                vec![
                    Term::from(Variable::universal("x")),
                    Term::from(2),
                    Term::from(Variable::universal("y"))
                ]
            ),
            function
        );
    }
}
