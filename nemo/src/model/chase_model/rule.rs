//! Defines a variant of [crate::model::Rule], suitable for computing the chase.

use std::collections::{HashMap, HashSet};

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    error::Error,
    model::{
        chase_model::variable::{AGGREGATE_VARIABLE_PREFIX, CONSTRUCT_VARIABLE_PREFIX},
        Constraint, Literal, PrimitiveTerm, Rule, Term, Variable,
    },
};

use super::{
    variable::EQUALITY_VARIABLE_PREFIX, ChaseAggregate, ChaseAtom, Constructor, PrimitiveAtom,
    VariableAtom,
};

/// Representation of a rule in a [super::ChaseProgram].
///
/// Chase rules may include placeholder variables, which start with `_`
/// * Additional constraints: `_EQUALITY_{term_counter}`
/// * Additional values: `_CONSTRUCT_{term_counter}`
/// * Aggregates: `_AGGREGATE_{term_counter}`
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct ChaseRule {
    /// Positive part of the body
    positive_body: Vec<VariableAtom>,
    /// Derived bindings from the positive body
    /// These should appear in order,
    /// i.e. such that the computation of a value
    /// does not depend on values constructed later
    positive_constructors: Vec<Constructor>,
    /// Restriction on the positive part of the body
    positive_constraints: Vec<Constraint>,

    /// Negative part of the body
    negative_body: Vec<VariableAtom>,
    /// For each [VariableAtom] in `negative_body`,
    /// the associated filter statements
    negative_constraints: Vec<Vec<Constraint>>,

    /// Aggregate
    aggregate: Option<ChaseAggregate>,

    /// Constructors from aggregate results
    aggregate_constructors: Vec<Constructor>,
    /// Restraints on values constructed from aggregate results
    aggregate_constraints: Vec<Constraint>,

    /// Head atoms of the rule
    head: Vec<PrimitiveAtom>,
}

#[allow(dead_code)]
impl ChaseRule {
    /// Construct a new [ChaseRule].
    pub fn positive_rule(
        head: Vec<PrimitiveAtom>,
        positive_body: Vec<VariableAtom>,
        positive_constraints: Vec<Constraint>,
    ) -> Self {
        Self {
            positive_body,
            positive_constructors: vec![],
            positive_constraints,
            negative_body: vec![],
            negative_constraints: vec![],
            aggregate: None,
            aggregate_constructors: vec![],
            aggregate_constraints: vec![],
            head,
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

    /// Return the positive constructors of the rule.
    pub fn positive_constructors(&self) -> &Vec<Constructor> {
        &self.positive_constructors
    }

    /// Return the aggregate of the rule.
    pub fn aggregate(&self) -> &Option<ChaseAggregate> {
        &self.aggregate
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
            .chain(self.negative_constraints.iter().flatten())
            .chain(self.aggregate_constraints.iter())
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
    pub fn negative_constraints(&self) -> &Vec<Vec<Constraint>> {
        &self.negative_constraints
    }

    /// Return the negative constraints of the rule - mutable.
    #[must_use]
    pub fn negative_constraints_mut(&mut self) -> &mut Vec<Vec<Constraint>> {
        &mut self.negative_constraints
    }

    /// Return the aggregate constraints of the rule.
    #[must_use]
    pub fn aggregate_constraints(&self) -> &Vec<Constraint> {
        &self.aggregate_constraints
    }

    /// Return the aggregate constraints of the rule.
    #[must_use]
    pub fn aggregate_constructors(&self) -> &Vec<Constructor> {
        &self.aggregate_constructors
    }

    /// Return all [Variable]s used in this rule.
    pub fn all_variables(&self) -> Vec<Variable> {
        let variables_body = self.all_body().flat_map(|atom| atom.get_variables());
        let variables_head = self.head.iter().flat_map(|atom| atom.get_variables());
        let variables_constructors = self
            .positive_constructors
            .iter()
            .chain(self.aggregate_constructors.iter())
            .map(|constructor| constructor.variable().clone());
        let variables_aggregates = self
            .aggregate
            .iter()
            .map(|aggregate| aggregate.output_variable.clone());

        variables_body
            .chain(variables_head)
            .chain(variables_constructors)
            .chain(variables_aggregates)
            .collect()
    }

    /// Returns the [AnyDataValue]s used as constants in this rule.
    pub fn all_datavalues(&self) -> impl Iterator<Item = &AnyDataValue> {
        let datavalues_head = self.head.iter().flat_map(|atom| atom.datavalues());
        let datavalues_constructors = self
            .positive_constructors
            .iter()
            .chain(self.aggregate_constructors.iter())
            .flat_map(|constructor| constructor.datavalues());
        let datavalues_constraints = self
            .positive_constraints
            .iter()
            .chain(self.negative_constraints.iter().flatten())
            .chain(self.aggregate_constraints.iter())
            .flat_map(|constraint| constraint.datavalues());

        datavalues_head
            .chain(datavalues_constructors)
            .chain(datavalues_constraints)
    }
}

impl ChaseRule {
    /// Increments `next_variable_id`, but returns it's old value with a prefix.
    fn generate_incrementing_variable_name(prefix: &str, next_variable_id: &mut usize) -> String {
        let result = format!("{}{}", prefix, next_variable_id);
        *next_variable_id += 1;
        result
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

        let mut rule_next_variable_id: usize = 0;

        // Head atoms may only contain primitive terms
        for atom in rule.head_mut() {
            for term in atom.terms_mut() {
                // Replace aggregate terms or aggregates inside of arithmetic expressions with placeholder variables
                term.update_subterms_recursively(&mut |subterm| match subterm {
                    Term::Aggregation(aggregate) => {
                        let new_variable =
                            Variable::Universal(Self::generate_incrementing_variable_name(
                                AGGREGATE_VARIABLE_PREFIX,
                                &mut rule_next_variable_id,
                            ));

                        aggregates.push(ChaseAggregate::from_aggregate(
                            aggregate.clone(),
                            new_variable.clone(),
                        ));

                        *subterm = Term::Primitive(PrimitiveTerm::Variable(new_variable));

                        false
                    }
                    _ => true,
                });

                debug_assert!(
                    !matches!(term, Term::Aggregation(_)),
                    "Aggregate terms should have been replaced with placeholder variables"
                );

                if !term.is_primitive() {
                    let new_variable =
                        Variable::Universal(Self::generate_incrementing_variable_name(
                            CONSTRUCT_VARIABLE_PREFIX,
                            &mut rule_next_variable_id,
                        ));

                    constructors.push(Constructor::new(new_variable.clone(), term.clone()));

                    *term = Term::Primitive(PrimitiveTerm::Variable(new_variable));
                }
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
                    Self::generate_incrementing_variable_name(
                        EQUALITY_VARIABLE_PREFIX,
                        &mut rule_next_variable_id,
                    ),
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
        Self::apply_equality(&mut rule);
        Self::separate_equality(&mut rule, &mut constructors, &mut aggregates);
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
