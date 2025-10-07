//! This module defines [HeadAtom].

use std::{collections::HashSet, fmt::Display};

use crate::{
    execution::planning_new::normalization::{
        aggregate::Aggregation, generator::VariableGenerator, operation::Operation,
    },
    rule_model::components::{
        tag::Tag,
        term::primitive::{Primitive, variable::Variable},
    },
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

/// An atom that only uses [Primitive]s.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HeadAtom {
    /// Predicate name of this atom
    predicate: Tag,
    /// Terms contained in this atom
    terms: Vec<Primitive>,
}

impl Display for HeadAtom {
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

impl HeadAtom {
    /// Construct a new [HeadAtom].
    pub fn new<TermIter>(predicate: Tag, terms: TermIter) -> Self
    where
        TermIter: IntoIterator<Item = Primitive>,
    {
        Self {
            predicate,
            terms: terms.into_iter().collect::<Vec<_>>(),
        }
    }

    /// Return an iterator over all terms contained in this atom.
    pub fn terms(&self) -> impl Iterator<Item = &Primitive> {
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

    /// Return an iterator over all variables contained in this atom.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.terms().filter_map(|term| match term {
            Primitive::Variable(variable) => Some(variable),
            Primitive::Ground(_) => None,
        })
    }
}

impl HeadAtom {
    /// Receives an [crate::rule_model::components::atom::Atom]
    /// and normalizes it into a [HeadAtom].
    ///
    /// Also returns
    ///
    /// # Panics
    /// Panics if the program is ill-formed and hence
    /// the atom contains structured terms, a recursive aggregate or multiple aggregates.
    pub fn normalize_atom(
        generator: &mut VariableGenerator,
        atom: &crate::rule_model::components::atom::Atom,
    ) -> (Self, Vec<Operation>, Option<Aggregation>) {
        let mut operations = Vec::<Operation>::default();
        let mut aggregate: Option<(
            &crate::rule_model::components::term::aggregate::Aggregate,
            HashSet<Variable>, // Variables contained in the expression outside of the aggregate
        )> = None;
        let mut head_terms = Vec::<Primitive>::default();

        for term in atom.terms() {
            match term {
                crate::rule_model::components::term::Term::Primitive(primitive) => {
                    head_terms.push(primitive.clone());
                }
                crate::rule_model::components::term::Term::Aggregate(term_aggregate) => {
                    aggregate = Some((term_aggregate, HashSet::default()));
                }
                crate::rule_model::components::term::Term::Operation(operation) => {
                    let (new_operation, term_aggregate) =
                        Operation::normalize_head_operation(operation);
                    let new_variable = Primitive::Variable(generator.universal("HEAD"));

                    if let Some(term_aggregate) = term_aggregate {
                        let mut variables =
                            new_operation.variables().cloned().collect::<HashSet<_>>();
                        variables.remove(&Variable::universal("_AGGREGATION_OUT"));

                        aggregate = Some((term_aggregate, variables));
                    }

                    head_terms.push(new_variable);
                    operations.push(new_operation);
                }
                crate::rule_model::components::term::Term::Map(_)
                | crate::rule_model::components::term::Term::FunctionTerm(_)
                | crate::rule_model::components::term::Term::Tuple(_) => {
                    panic!("invalid program: head atom contains structured terms")
                }
            }
        }

        let mut aggregation: Option<Aggregation> = None;
        if let Some((aggregate, mut group_by)) = aggregate {
            for term in &head_terms {
                if let Primitive::Variable(variable) = term {
                    if variable.name() != Some("_AGGREGATION_OUT") {
                        group_by.insert(variable.clone());
                    }
                }
            }

            let (new_aggregation, new_operation) =
                Aggregation::normalize_aggregation(group_by, aggregate);
            if let Some(operation) = new_operation {
                operations.push(operation);
            }

            aggregation = Some(new_aggregation);
        }

        let head_atom = Self::new(atom.predicate(), head_terms);
        (head_atom, operations, aggregation)
    }
}
