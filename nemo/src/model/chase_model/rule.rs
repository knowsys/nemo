//! Defines a variant of [`crate::model::Rule`], suitable for computing the chase.

use std::collections::{HashMap, HashSet};

use crate::{
    error::Error,
    model::{Constraint, Identifier, Literal, PrimitiveTerm, Rule, Term, Variable},
};

use super::{ChaseAggregate, Constructor, PrimitiveAtom, VariableAtom};

/// Prefix used for generated aggregate variables in a [`ChaseRule`]
pub const AGGREGATE_VARIABLE_PREFIX: &str = "_AGGREGATE_";
/// Prefix used for generated variables encoding equality constraints in a [`ChaseRule`]
pub const EQUALITY_VARIABLE_PREFIX: &str = "_EQUALITY_";
/// Prefix used for generated variables for storing the value of complex terms in a [`ChaseRule`].
pub const CONSTRUCT_VARIABLE_PREFIX: &str = "_CONSTRUCT_";

/// Representation of a rule in a [`super::ChaseProgram`].
///
/// Chase rules may include placeholder variables, which start with `_`
/// * Additional constraints: `_EQUALITY_{term_counter}`
/// * Additional values: `_CONSTRUCT_{term_counter}`
/// * Aggregates: `_AGGREGATE_{term_counter}`
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct ChaseRule {
    /// Head atoms of the rule
    head: Vec<PrimitiveAtom>,
    /// Head constructions
    constructors: Vec<Constructor>,
    /// Aggregates
    aggregates: Vec<ChaseAggregate>,
    /// Positive Body literals of the rule
    positive_body: Vec<VariableAtom>,
    /// constraints applied to the body
    positive_constraints: Vec<Constraint>,
    /// Negative Body literals of the rule
    negative_body: Vec<VariableAtom>,
    /// constraints applied to the body
    negative_constraints: Vec<Constraint>,
}

#[allow(dead_code)]
impl ChaseRule {
    /// Construct a new [`ChaseRule`].
    pub fn new(
        head: Vec<PrimitiveAtom>,
        constructors: Vec<Constructor>,
        aggregates: Vec<ChaseAggregate>,
        positive_body: Vec<VariableAtom>,
        positive_constraints: Vec<Constraint>,
        negative_body: Vec<VariableAtom>,
        negative_constraints: Vec<Constraint>,
    ) -> Self {
        Self {
            head,
            constructors,
            aggregates,
            positive_body,
            positive_constraints,
            negative_body,
            negative_constraints,
        }
    }

    /// Return the head atoms of the rule - immutable.
    #[must_use]
    pub fn head(&self) -> &Vec<PrimitiveAtom> {
        &self.head
    }

    /// Return the head atoms of the rule - mutable.
    #[must_use]
    pub fn head_mut(&mut self) -> &mut Vec<PrimitiveAtom> {
        &mut self.head
    }

    /// Return the constructors of the rule.
    pub fn constructors(&self) -> &Vec<Constructor> {
        &self.constructors
    }

    /// Return the aggregates of the rule.
    pub fn aggregates(&self) -> &Vec<ChaseAggregate> {
        &self.aggregates
    }

    /// Return the all the atoms of the rules.
    /// This does not distinguish between positive and negative atoms.
    pub fn all_body(&self) -> impl Iterator<Item = &VariableAtom> {
        self.positive_body.iter().chain(self.negative_body.iter())
    }

    /// Return the positive body atoms of the rule - immutable.
    #[must_use]
    pub fn positive_body(&self) -> &Vec<VariableAtom> {
        &self.positive_body
    }

    /// Return the positive body atoms of the rule - mutable.
    #[must_use]
    pub fn positive_body_mut(&mut self) -> &mut Vec<VariableAtom> {
        &mut self.positive_body
    }

    /// Return all the constraints of the rule.
    pub fn all_constraints(&self) -> impl Iterator<Item = &Constraint> {
        self.positive_constraints
            .iter()
            .chain(self.negative_constraints.iter())
    }

    /// Return the positive constraints of the rule - immutable.
    #[must_use]
    pub fn positive_constraints(&self) -> &Vec<Constraint> {
        &self.positive_constraints
    }

    /// Return the positive constraints of the rule - mutable.
    #[must_use]
    pub fn positive_constraints_mut(&mut self) -> &mut Vec<Constraint> {
        &mut self.positive_constraints
    }

    /// Return the negative body atons of the rule - immutable.
    #[must_use]
    pub fn negative_body(&self) -> &Vec<VariableAtom> {
        &self.negative_body
    }

    /// Return the negative body atoms of the rule - mutable.
    #[must_use]
    pub fn negative_body_mut(&mut self) -> &mut Vec<VariableAtom> {
        &mut self.negative_body
    }

    /// Return the negative constraints of the rule - immutable.
    #[must_use]
    pub fn negative_constraints(&self) -> &Vec<Constraint> {
        &self.negative_constraints
    }

    /// Return the negative constraints of the rule - mutable.
    #[must_use]
    pub fn negative_constraints_mut(&mut self) -> &mut Vec<Constraint> {
        &mut self.negative_constraints
    }

    /// Return the [`Constructor`] associated with a given variable
    pub fn get_constructor(&self, variable: &Variable) -> Option<&Constructor> {
        self.constructors
            .iter()
            .find(|&constructor| constructor.variable() == variable)
    }
}

impl ChaseRule {
    fn generate_variable_name(prefix: &str, counter: usize) -> Identifier {
        Identifier(format!("{}{}", prefix, counter))
    }

    // Remove constraints of the form ?X = ?Y from the rule
    // and apply the corresponding substitution
    fn apply_equality(rule: &mut Rule) {
        let mut assignment = HashMap::<Variable, Term>::new();

        rule.constraints_mut().retain(|constraint| {
            if let Constraint::Equals(
                Term::Primitive(PrimitiveTerm::Variable(left)),
                Term::Primitive(PrimitiveTerm::Variable(right)),
            ) = constraint
            {
                if let Some(assigned) = assignment.get(left) {
                    assignment.insert(right.clone(), assigned.clone());
                } else if let Some(assigned) = assignment.get(right) {
                    assignment.insert(left.clone(), assigned.clone());
                } else {
                    assignment.insert(
                        left.clone(),
                        Term::Primitive(PrimitiveTerm::Variable(right.clone())),
                    );
                }

                return false;
            }

            true
        });

        rule.apply_assignment(&assignment);
    }

    /// Modify the rule in such a way that it only contains variables within atoms.
    /// Returns a vector of constraints that have been added for negative literals.
    fn flatten_atoms(
        rule: &mut Rule,
        constructors: &mut Vec<Constructor>,
        aggregates: &mut Vec<ChaseAggregate>,
        negative_constraints: &mut Vec<Constraint>,
    ) {
        // New constraints that will be introduced due to the flattening of the atom
        let mut positive_constraints = Vec::<Constraint>::new();

        let mut global_term_index: usize = 0;

        // Head atoms may only contain primitive terms
        for atom in rule.head_mut() {
            for term in atom.terms_mut() {
                if !term.is_primitive() {
                    let new_variable = if let Term::Aggregation(aggregate) = term {
                        let new_variable = Variable::Universal(Self::generate_variable_name(
                            AGGREGATE_VARIABLE_PREFIX,
                            global_term_index,
                        ));

                        aggregates.push(ChaseAggregate::from_aggregate(
                            aggregate.clone(),
                            new_variable.clone(),
                        ));

                        new_variable
                    } else {
                        let new_variable = Variable::Universal(Self::generate_variable_name(
                            CONSTRUCT_VARIABLE_PREFIX,
                            global_term_index,
                        ));

                        constructors.push(Constructor::new(new_variable.clone(), term.clone()));

                        new_variable
                    };

                    *term = Term::Primitive(PrimitiveTerm::Variable(new_variable));
                }

                global_term_index += 1;
            }
        }

        // Body literals must only contain variables
        // and may not repeat variables within one atom
        for literal in rule.body_mut() {
            let is_positive = literal.is_positive();
            let atom = literal.atom_mut();
            let mut current_variables = HashSet::<Variable>::new();

            for term in atom.terms_mut() {
                let new_variable = Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(
                    Self::generate_variable_name(EQUALITY_VARIABLE_PREFIX, global_term_index),
                )));

                if let Term::Primitive(PrimitiveTerm::Variable(variable)) = term.clone() {
                    if !current_variables.contains(&variable) {
                        current_variables.insert(variable);

                        continue;
                    }
                }

                let new_constraint = Constraint::Equals(term.clone(), new_variable.clone());
                if is_positive {
                    positive_constraints.push(new_constraint);
                } else {
                    negative_constraints.push(new_constraint);
                }

                *term = new_variable;

                global_term_index += 1;
            }
        }

        rule.constraints_mut().extend(positive_constraints);
    }

    /// Seperate normal equality constraints from definitions of new variables
    fn separate_equality(
        rule: &mut Rule,
        constructors: &mut Vec<Constructor>,
        aggregates: &mut Vec<ChaseAggregate>,
    ) {
        let safe_variables = rule.safe_variables();

        rule.constraints_mut().retain(|constraint| {
            if let Some((variable, term)) = constraint.has_form_assignment() {
                if safe_variables.contains(variable) {
                    return true;
                }

                if let Term::Aggregation(aggregate) = term {
                    aggregates.push(ChaseAggregate::from_aggregate(
                        aggregate.clone(),
                        variable.clone(),
                    ))
                } else {
                    constructors.push(Constructor::new(variable.clone(), term.clone()));
                }

                return false;
            }

            true
        });
    }
}

impl TryFrom<Rule> for ChaseRule {
    type Error = Error;

    fn try_from(mut rule: Rule) -> Result<ChaseRule, Error> {
        let mut constructors = Vec::<Constructor>::new();
        let mut aggregates = Vec::<ChaseAggregate>::new();
        let mut negative_constraints = Vec::<Constraint>::new();

        // Preprocess rule in order to make the translation simpler
        Self::separate_equality(&mut rule, &mut constructors, &mut aggregates);
        Self::apply_equality(&mut rule);
        Self::flatten_atoms(
            &mut rule,
            &mut constructors,
            &mut aggregates,
            &mut negative_constraints,
        );

        let positive_constraints = rule.constraints().clone();

        let head = rule
            .head()
            .iter()
            .map(PrimitiveAtom::from_flat_atom)
            .collect();

        let mut positive_body = Vec::new();
        let mut negative_body = Vec::new();
        for literal in rule.body() {
            match literal {
                Literal::Positive(atom) => positive_body.push(VariableAtom::from_flat_atom(atom)),
                Literal::Negative(atom) => negative_body.push(VariableAtom::from_flat_atom(atom)),
            }
        }

        Ok(Self {
            head,
            constructors,
            aggregates,
            positive_body,
            positive_constraints,
            negative_body,
            negative_constraints,
        })
    }
}
