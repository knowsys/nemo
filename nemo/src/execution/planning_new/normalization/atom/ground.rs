//! This module defines [GroundAtom].

use std::fmt::Display;

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    rule_model::components::{tag::Tag, term::primitive::ground::GroundTerm},
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

/// An atom that only uses [GroundTerm]s,
/// which can be converted to [AnyDataValue]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroundAtom {
    /// Predicate name of this atom
    predicate: Tag,
    /// Terms contained in this atom
    terms: Vec<GroundTerm>,
}

impl Display for GroundAtom {
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

impl GroundAtom {
    /// Construct a new [GroundAtom].
    pub fn new<TermIter>(predicate: Tag, terms: TermIter) -> Self
    where
        TermIter: IntoIterator<Item = GroundTerm>,
    {
        Self {
            predicate,
            terms: terms.into_iter().collect::<Vec<_>>(),
        }
    }

    /// Return an iterator over all terms contained in this atom.
    pub fn terms(&self) -> impl Iterator<Item = &GroundTerm> {
        self.terms.iter()
    }

    /// Return the arity of this atom.
    pub fn arity(&self) -> usize {
        self.terms.len()
    }

    /// Return the predicate of this atom.
    pub fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    /// Return an iterator over the terms of this atom
    /// converted to [AnyDataValue].
    pub fn datavalues(&self) -> impl Iterator<Item = AnyDataValue> {
        self.terms().map(|term| term.value())
    }
}

impl GroundAtom {
    /// Converts a [crate::rule_model::components::fact::Fact] into a [GroundAtom].
    ///
    /// Return `None` if any expression within the fact is undefined.
    ///
    /// # Panics
    /// Panics if the program is ill-formed and hence the fact contains non-ground terms.
    pub fn normalize_fact(fact: &crate::rule_model::components::fact::Fact) -> Option<Self> {
        let predicate = fact.predicate().clone();
        let mut terms = Vec::new();

        for term in fact.terms() {
            let reduced = term.reduce()?;

            if let crate::rule_model::components::term::Term::Primitive(
                crate::rule_model::components::term::primitive::Primitive::Ground(value),
            ) = reduced
            {
                terms.push(value.clone());
            } else {
                panic!("invalid program: fact contains non-primitive value {reduced}")
            }
        }

        Some(Self { predicate, terms })
    }
}
