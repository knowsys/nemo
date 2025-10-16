//! This module defines [BodyAtom].

use std::{collections::HashSet, fmt::Display};

use crate::{
    execution::planning::normalization::{generator::VariableGenerator, operation::Operation},
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

/// An atom that only uses [Variable]s.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BodyAtom {
    /// Predicate name of this atom
    predicate: Tag,
    /// Terms contained in this atom
    terms: Vec<Variable>,
}

impl Display for BodyAtom {
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

impl BodyAtom {
    /// Construct a new [BodyAtom].
    pub fn new<TermIter>(predicate: Tag, terms: TermIter) -> Self
    where
        TermIter: IntoIterator<Item = Variable>,
    {
        Self {
            predicate,
            terms: terms.into_iter().collect::<Vec<_>>(),
        }
    }

    /// Return an iterator over all terms contained in this atom.
    pub fn terms(&self) -> impl Iterator<Item = &Variable> {
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
}

impl BodyAtom {
    /// Receives an [crate::rule_model::components::atom::Atom] that appears in the body
    /// and normalizes it into a [BodyAtom].
    ///
    /// Also returns a list of [Operation]s that should be added to the rule.
    ///
    /// # Panics
    /// Panics if the program is ill-formed and hence one of the following conditions is met:
    ///     * Atom contains a structured term (like tuples or maps)
    ///     * Atom contains existential variables
    pub fn normalize_atom(
        generator: &mut VariableGenerator,
        atom: &crate::rule_model::components::atom::Atom,
    ) -> (Self, Vec<Operation>) {
        let predicate = atom.predicate();
        let mut terms = Vec::<Variable>::default();
        let mut additional_operations = Vec::<Operation>::default();

        let mut used_variables = HashSet::<Variable>::default();

        for term in atom.terms() {
            match term {
                crate::rule_model::components::term::Term::Primitive(
                    crate::rule_model::components::term::primitive::Primitive::Variable(variable),
                ) => {
                    if variable.is_anonymous() {
                        let new_variable = generator.universal("BODY");
                        terms.push(new_variable)
                    } else if !used_variables.insert(variable.clone()) {
                        let new_variable = generator.universal("BODY");
                        let new_operation = Operation::new_assignment(
                            new_variable.clone(),
                            Operation::new_variable(variable.clone()),
                        );

                        terms.push(new_variable);
                        additional_operations.push(new_operation);
                    } else {
                        terms.push(variable.clone());
                    }
                }
                crate::rule_model::components::term::Term::Primitive(
                    crate::rule_model::components::term::primitive::Primitive::Ground(ground_term),
                ) => {
                    let new_variable = generator.universal("BODY");
                    let new_operation = Operation::new_assignment(
                        new_variable.clone(),
                        Operation::new_ground(ground_term.clone()),
                    );

                    terms.push(new_variable);
                    additional_operations.push(new_operation);
                }
                crate::rule_model::components::term::Term::Operation(operation) => {
                    let new_variable = generator.universal("BODY");
                    let new_operation = Operation::normalize_body_operation(operation);

                    terms.push(new_variable);
                    additional_operations.push(new_operation);
                }
                _ => panic!("invalid program: body atom contains structured terms or aggregates"),
            }
        }

        let normalized_atom = Self { predicate, terms };

        (normalized_atom, additional_operations)
    }
}
