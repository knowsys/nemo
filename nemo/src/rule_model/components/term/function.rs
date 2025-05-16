//! This module defines [FunctionTerm].

use std::{
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut, Index, IndexMut},
    vec,
};

use crate::rule_model::{
    components::{
        component_iterator, component_iterator_mut, tag::Tag, ComponentBehavior, ComponentIdentity,
        IterableComponent, IterablePrimitives, IterableVariables, ProgramComponent,
        ProgramComponentKind,
    },
    error::{validation_error::ValidationError, ValidationReport},
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

use super::{
    primitive::{variable::Variable, Primitive},
    value_type::ValueType,
    Term,
};

/// Function term
///
/// List of [Term]s with a [Tag].
#[derive(Debug, Clone)]
pub struct FunctionTerm {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Name of the function
    tag: Tag,
    /// Subterms of the function
    terms: Vec<Term>,
}

/// Construct a [FunctionTerm].
#[macro_export]
macro_rules! function {
    // Base case: no elements
    ($name:ident()) => {
        $crate::rule_model::components::term::function::FunctionTerm::new(
            $crate::rule_model::components::tag::Tag::from(stringify!($name)), Vec::new()
        )
    };
    // Recursive case: handle each term, separated by commas
    ($name:ident($($tt:tt)*)) => {{
        #[allow(clippy::vec_init_then_push)] {
            let mut terms = Vec::new();
            term_list!(terms; $($tt)*);
            $crate::rule_model::components::term::function::FunctionTerm::new(
                stringify!($name), terms
            )
        }
    }};
}

impl FunctionTerm {
    /// Create a new [FunctionTerm].
    pub fn new<Terms: IntoIterator<Item = Term>>(name: &str, subterms: Terms) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            tag: Tag::new(name.to_string()),
            terms: subterms.into_iter().collect(),
        }
    }

    /// Create a new [FunctionTerm] with a [Tag].
    pub(crate) fn new_tagged<Terms: IntoIterator<Item = Term>>(tag: Tag, subterms: Terms) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            tag,
            terms: subterms.into_iter().collect(),
        }
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        ValueType::FunctionTerm
    }

    /// Return an iterator over the arguments of this function term.
    pub fn terms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an iterator over the arguments of this function term.
    pub fn terms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
    }

    /// Push a [Term] to the end of this function term.
    pub fn push(&mut self, term: Term) {
        self.terms.push(term);
    }

    /// Remove the [Term] at the given index from this function term
    /// and return it.
    ///
    /// # Panics
    /// Panics if index out of bounds.
    pub fn remove(&mut self, index: usize) -> Term {
        self.terms.remove(index)
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

    /// Reduce this term by evaluating all contained expressions,
    /// and return a new [FunctionTerm] with the same [Origin] as `self`.
    ///
    /// This function does nothing if `self` is not ground.
    ///
    /// Returns `None` if any intermediate result is undefined.
    pub fn reduce(&self) -> Option<Self> {
        Some(Self {
            origin: self.origin.clone(),
            id: ProgramComponentId::default(),
            tag: self.tag.clone(),
            terms: self
                .terms
                .iter()
                .map(|term| term.reduce())
                .collect::<Option<Vec<_>>>()?,
        })
    }
}

impl Index<usize> for FunctionTerm {
    type Output = Term;

    fn index(&self, index: usize) -> &Self::Output {
        &self.terms[index]
    }
}

impl IndexMut<usize> for FunctionTerm {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.terms[index]
    }
}

impl Deref for FunctionTerm {
    type Target = [Term];

    fn deref(&self) -> &Self::Target {
        &self.terms
    }
}

impl DerefMut for FunctionTerm {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terms
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

impl Eq for FunctionTerm {}

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

impl ComponentBehavior for FunctionTerm {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::FunctionTerm
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        if !self.tag.is_valid() {
            report.add(
                &self.tag,
                ValidationError::InvalidTermTag {
                    function_name: self.tag.to_string(),
                },
            );
        }

        if self.is_empty() {
            report.add(self, ValidationError::FunctionTermEmpty);
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for FunctionTerm {
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
        self.origin = origin
    }
}

impl IterableComponent for FunctionTerm {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let term_iterator = component_iterator(self.terms.iter());
        Box::new(term_iterator)
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let term_iterator = component_iterator_mut(self.terms.iter_mut());
        Box::new(term_iterator)
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
        let function = function!(f(12, variable, !e, "abc", ?v));

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
