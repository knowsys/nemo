//! Defines a variant of [`crate::model::Rule`], suitable for computing the chase.

use std::collections::HashMap;

use crate::{
    error::Error,
    model::{
        Condition, FilterOperation, Identifier, Literal, Rule, Term, TermOperation, TermTree,
        Variable,
    },
};

use super::{ChaseAggregate, ChaseAtom};

/// Prefix used for placeholder aggregate variables in a [`ChaseRule`]
pub const AGGREGATE_VARIABLE_PREFIX: &str = "_AGGREGATE_";

/// Representation of a rule in a [`super::ChaseProgram`].
///
/// Chase rules may include placeholder variables, which start with `_`
/// * Additional filters: `_CONSTANT_AND_DUPLICATE_VARIABLE_FILTER_{term_counter}`
/// * Head operations: `_HEAD_OPERATION_{term_counter}`:
/// * Aggregates: `_AGGREGATE_{term_counter}`
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct ChaseRule {
    /// Head atoms of the rule
    head: Vec<ChaseAtom>,
    /// Head constructions
    constructors: HashMap<Variable, TermTree>,
    /// Positive Body literals of the rule
    positive_body: Vec<ChaseAtom>,
    /// Filters applied to the body
    positive_filters: Vec<Condition>,
    /// Negative Body literals of the rule
    negative_body: Vec<ChaseAtom>,
    /// Filters applied to the body
    negative_filters: Vec<Condition>,
    /// In the transformation from a [`Rule`], every aggregate term gets it's own placeholder variable.
    /// The [`ChaseAggregate`] then specifies how the values for this variable should be computed (i.e. which aggregate operation to use, on which input variables, ...).
    aggregates: Vec<ChaseAggregate>,
}

#[allow(dead_code)]
impl ChaseRule {
    /// Construct a new rule.
    pub fn new(
        mut head: Vec<ChaseAtom>,
        mut constructors: HashMap<Variable, TermTree>,
        mut positive_body: Vec<ChaseAtom>,
        mut positive_filters: Vec<Condition>,
        mut negative_body: Vec<ChaseAtom>,
        aggregates: Vec<ChaseAggregate>,
    ) -> Self {
        // apply equality constraints
        positive_filters.retain(|filter| {
            if filter.operation == FilterOperation::Equals {
                if let Term::Variable(variable) = &filter.rhs {
                    positive_body
                        .iter_mut()
                        .for_each(|atom| atom.substitute_variable(&filter.lhs, variable));

                    negative_body
                        .iter_mut()
                        .for_each(|atom| atom.substitute_variable(&filter.lhs, variable));

                    head.iter_mut()
                        .for_each(|atom| atom.substitute_variable(&filter.lhs, variable));

                    for term_tree in constructors.values_mut() {
                        term_tree.substitute_variable(&filter.lhs, variable)
                    }

                    return false;
                }
            }
            true
        });

        let mut generated_variable_index = 0;
        let mut generate_variable = move || {
            generated_variable_index += 1;
            let name = format!("__GENERATED_VARIABLE_{}", generated_variable_index);
            Variable::Universal(Identifier::from(name))
        };

        // introduce equality constraints for positive atoms
        for positive_atom in &mut positive_body {
            positive_atom.normalize(&mut generate_variable, &mut positive_filters);
        }

        // introduce equality constraints for negative atoms
        let mut negative_filters = Vec::new();
        for negative_atom in &mut negative_body {
            negative_atom.normalize(&mut generate_variable, &mut negative_filters);
        }

        Self {
            head,
            constructors,
            positive_body,
            positive_filters,
            negative_body,
            negative_filters,
            aggregates,
        }
    }
    /// Return the head atoms of the rule - immutable.
    #[must_use]
    pub fn head(&self) -> &Vec<ChaseAtom> {
        &self.head
    }

    /// Return the head atoms of the rule - mutable.
    #[must_use]
    pub fn head_mut(&mut self) -> &mut Vec<ChaseAtom> {
        &mut self.head
    }

    /// Return the constructors of the rule.
    pub fn constructors(&self) -> &HashMap<Variable, TermTree> {
        &self.constructors
    }

    /// Return the aggregates of the rule.
    pub fn aggregates(&self) -> &Vec<ChaseAggregate> {
        &self.aggregates
    }

    /// Return all the atoms occuring in this rule.
    /// This includes the postive body atoms, the negative body atoms as well as the head atoms.
    pub fn all_atoms(&self) -> impl Iterator<Item = &ChaseAtom> {
        self.all_body().chain(self.head.iter())
    }

    /// Return the all the atoms of the rules.
    /// This does not distinguish between positive and negative atoms.
    pub fn all_body(&self) -> impl Iterator<Item = &ChaseAtom> {
        self.positive_body.iter().chain(self.negative_body.iter())
    }

    /// Return the positive body atoms of the rule - immutable.
    #[must_use]
    pub fn positive_body(&self) -> &Vec<ChaseAtom> {
        &self.positive_body
    }

    /// Return the positive body atoms of the rule - mutable.
    #[must_use]
    pub fn positive_body_mut(&mut self) -> &mut Vec<ChaseAtom> {
        &mut self.positive_body
    }

    /// Return all the filters of the rule.
    pub fn all_filters(&self) -> impl Iterator<Item = &Condition> {
        self.positive_filters
            .iter()
            .chain(self.negative_filters.iter())
    }

    /// Return the positive filters of the rule - immutable.
    #[must_use]
    pub fn positive_filters(&self) -> &Vec<Condition> {
        &self.positive_filters
    }

    /// Return the positive filters of the rule - mutable.
    #[must_use]
    pub fn positive_filters_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.positive_filters
    }

    /// Return the negative body atons of the rule - immutable.
    #[must_use]
    pub fn negative_body(&self) -> &Vec<ChaseAtom> {
        &self.negative_body
    }

    /// Return the negative body atoms of the rule - mutable.
    #[must_use]
    pub fn negative_body_mut(&mut self) -> &mut Vec<ChaseAtom> {
        &mut self.negative_body
    }

    /// Return the negative filters of the rule - immutable.
    #[must_use]
    pub fn negative_filters(&self) -> &Vec<Condition> {
        &self.negative_filters
    }

    /// Return the negative filters of the rule - mutable.
    #[must_use]
    pub fn negative_filters_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.negative_filters
    }
}

impl TryFrom<Rule> for ChaseRule {
    type Error = Error;

    fn try_from(rule: Rule) -> Result<ChaseRule, Error> {
        let mut positive_body = Vec::new();
        let mut negative_body = Vec::new();
        let mut chase_aggregates = Vec::new();

        for literal in rule.body().iter().cloned() {
            match literal {
                Literal::Positive(atom) => positive_body.push(ChaseAtom::from_flat_atom(atom)?),
                Literal::Negative(atom) => negative_body.push(ChaseAtom::from_flat_atom(atom)?),
            }
        }

        let mut constructors = HashMap::<Variable, TermTree>::new();
        let mut head_atoms = Vec::<ChaseAtom>::new();
        let mut term_counter: usize = 1;
        for atom in rule.head() {
            let mut new_terms = Vec::<Term>::new();

            for term_tree in atom.term_trees() {
                if let TermOperation::Term(term) = term_tree.operation() {
                    if let Term::Aggregate(aggregate) = term {
                        // Introduce place holder variable for the aggregates
                        let new_variable_identifier =
                            Identifier(format!("{AGGREGATE_VARIABLE_PREFIX}{term_counter}"));
                        let new_variable = Variable::Universal(new_variable_identifier.clone());
                        new_terms.push(Term::Variable(new_variable));

                        // Add [`ChaseAggregate`] which ensures that the variable will get bound to the correct value
                        chase_aggregates.push(ChaseAggregate::from_aggregate(
                            aggregate.clone(),
                            new_variable_identifier,
                        ));
                    } else {
                        new_terms.push(term.clone());
                    }
                } else {
                    let new_variable =
                        Variable::Universal(Identifier(format!("_HEAD_OPERATION_{term_counter}")));
                    new_terms.push(Term::Variable(new_variable.clone()));
                    let exists = constructors.insert(new_variable, term_tree.clone());

                    assert!(exists.is_none())
                }

                term_counter += 1;
            }

            head_atoms.push(ChaseAtom::new(atom.predicate(), new_terms));
        }

        Ok(Self::new(
            head_atoms,
            constructors,
            positive_body,
            rule.filters().clone(),
            negative_body,
            chase_aggregates,
        ))
    }
}
