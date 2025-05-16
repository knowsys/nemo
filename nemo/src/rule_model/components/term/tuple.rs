//! This module defines [Tuple].

use std::{
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut, Index, IndexMut},
};

use crate::rule_model::{
    components::{
        component_iterator, component_iterator_mut, ComponentBehavior, ComponentIdentity,
        IterableComponent, IterablePrimitives, IterableVariables, ProgramComponent,
        ProgramComponentKind,
    },
    error::ValidationReport,
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

use super::{
    primitive::{variable::Variable, Primitive},
    value_type::ValueType,
    Term,
};

/// Tuple
///
/// An ordered list of [Term]s.
#[derive(Debug, Clone)]
pub struct Tuple {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Ordered list of terms contained in this tuple
    terms: Vec<Term>,
}

/// Construct a [Tuple].
#[macro_export]
macro_rules! tuple {
    // Base case: no elements
    () => {
        $crate::rule_model::components::term::tuple::Tuple::new(Vec::new())
    };
    // Recursive case: handle each term, separated by commas
    ($($tt:tt)*) => {{
        #[allow(clippy::vec_init_then_push)] {
            let mut terms = Vec::new();
            term_list!(terms; $($tt)*);
            $crate::rule_model::components::term::tuple::Tuple::new(terms)
        }
    }};
}

impl Tuple {
    /// Create a new [Tuple].
    pub fn new<Terms: IntoIterator<Item = Term>>(terms: Terms) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            terms: terms.into_iter().collect(),
        }
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        ValueType::Tuple
    }

    /// Return an iterator over the terms of this tuple.
    pub fn terms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return a mutable iterator over the terms of this tuple.
    pub fn terms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
    }

    /// Push a [Term] to the end of this tuple.
    pub fn push(&mut self, term: Term) {
        self.terms.push(term);
    }

    /// Remove the [Term] at the given index from this tuple
    /// and return it.
    ///
    /// # Panics
    /// Panics if index out of bounds.
    pub fn remove(&mut self, index: usize) -> Term {
        self.terms.remove(index)
    }

    /// Return whether this term is ground,
    /// i.e. if it does not contain any variables.
    pub fn is_ground(&self) -> bool {
        self.terms.iter().all(Term::is_ground)
    }

    /// Reduce this term by evaluating all contained expressions,
    /// and return a new [Tuple] with the same [Origin] as `self`.
    ///
    /// This function does nothing if `self` is not ground.
    ///
    /// Returns `None` if
    pub fn reduce(&self) -> Option<Self> {
        Some(Self {
            origin: self.origin.clone(),
            id: ProgramComponentId::default(),
            terms: self
                .terms
                .iter()
                .map(|term| term.reduce())
                .collect::<Option<Vec<_>>>()?,
        })
    }
}

impl Index<usize> for Tuple {
    type Output = Term;

    fn index(&self, index: usize) -> &Self::Output {
        &self.terms[index]
    }
}

impl IndexMut<usize> for Tuple {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.terms[index]
    }
}

impl Deref for Tuple {
    type Target = [Term];

    fn deref(&self) -> &Self::Target {
        &self.terms
    }
}

impl DerefMut for Tuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terms
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("(")?;

        for (term_index, term) in self.terms.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        self.terms == other.terms
    }
}

impl Eq for Tuple {}

impl Hash for Tuple {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.terms.hash(state);
    }
}

impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.terms.partial_cmp(&other.terms)
    }
}

impl ComponentBehavior for Tuple {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Tuple
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for Tuple {
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

impl IterableComponent for Tuple {
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

impl IterableVariables for Tuple {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms.iter_mut().flat_map(|term| term.variables_mut()))
    }
}

impl IterablePrimitives for Tuple {
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

impl IntoIterator for Tuple {
    type Item = Term;
    type IntoIter = std::vec::IntoIter<Term>;

    fn into_iter(self) -> Self::IntoIter {
        self.terms.into_iter()
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::{
        components::{
            term::{primitive::variable::Variable, tuple::Tuple, Term},
            IterableVariables,
        },
        translation::TranslationComponent,
    };

    #[test]
    fn tuple_basic() {
        let variable = Variable::universal("u");
        let tuple = tuple!(12, variable, !e, "abc", ?v);

        let variables = tuple.variables().cloned().collect::<Vec<_>>();
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
    fn parse_tuple() {
        let tuple = Tuple::parse("(?x, 2)").unwrap();

        assert_eq!(
            Tuple::new(vec![Term::from(Variable::universal("x")), Term::from(2)]),
            tuple
        );
    }
}
