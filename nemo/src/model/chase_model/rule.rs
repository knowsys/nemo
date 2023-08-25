//! Defines a variant of [`crate::model::Rule`], suitable for computing the chase.

use std::collections::HashMap;

use crate::{
    error::Error,
    model::{Filter, Identifier, Literal, Rule, Term, TermOperation, TermTree, Variable},
};

use super::ChaseAtom;

/// Representation of a rule in a [`super::ChaseProgram`].
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
    positive_filters: Vec<Filter>,
    /// Negative Body literals of the rule
    negative_body: Vec<ChaseAtom>,
    /// Filters applied to the body
    negative_filters: Vec<Filter>,
}

#[allow(dead_code)]
impl ChaseRule {
    /// Construct a new rule.
    pub fn new(
        head: Vec<ChaseAtom>,
        constructors: HashMap<Variable, TermTree>,
        positive_body: Vec<ChaseAtom>,
        positive_filters: Vec<Filter>,
        negative_body: Vec<ChaseAtom>,
        negative_filters: Vec<Filter>,
    ) -> Self {
        Self {
            head,
            constructors,
            positive_body,
            positive_filters,
            negative_body,
            negative_filters,
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
    pub fn all_filters(&self) -> impl Iterator<Item = &Filter> {
        self.positive_filters
            .iter()
            .chain(self.negative_filters.iter())
    }

    /// Return the positive filters of the rule - immutable.
    #[must_use]
    pub fn positive_filters(&self) -> &Vec<Filter> {
        &self.positive_filters
    }

    /// Return the positive filters of the rule - mutable.
    #[must_use]
    pub fn positive_filters_mut(&mut self) -> &mut Vec<Filter> {
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
    pub fn negative_filters(&self) -> &Vec<Filter> {
        &self.negative_filters
    }

    /// Return the negative filters of the rule - mutable.
    #[must_use]
    pub fn negative_filters_mut(&mut self) -> &mut Vec<Filter> {
        &mut self.negative_filters
    }
}

impl TryFrom<Rule> for ChaseRule {
    type Error = Error;

    fn try_from(rule: Rule) -> Result<ChaseRule, Error> {
        let mut positive_body = Vec::new();
        let mut negative_body = Vec::new();

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
                    new_terms.push(term.clone());
                } else {
                    let new_variable =
                        Variable::Universal(Identifier(format!("HEAD_OPERATION_{term_counter}")));
                    new_terms.push(Term::Variable(new_variable.clone()));
                    let exists = constructors.insert(new_variable, term_tree.clone());

                    assert!(exists.is_none())
                }

                term_counter += 1;
            }

            head_atoms.push(ChaseAtom::new(atom.predicate(), new_terms));
        }

        Ok(Self {
            head: head_atoms,
            constructors,
            positive_body,
            negative_body,
            positive_filters: rule.filters().clone(),
            negative_filters: Vec::new(),
        })
    }
}
