use crate::model::{chase_model::Constraint, VariableAssignment};

use super::{PrimitiveTerm, Term, Variable};

/// Condition of the form `<term> <operation> <term>`
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Condition {
    /// Variable is assigned to the term.
    Assignment(Variable, Term),
    /// Constraint on two terms
    Constraint(Constraint),
}

impl Condition {
    /// Replaces [`Variable`]s with [`Term`]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        match self {
            Condition::Assignment(variable, term) => {
                term.apply_assignment(assignment);

                if let Some(replacing_term) = assignment.get(variable) {
                    if let Term::Primitive(PrimitiveTerm::Variable(replacing_variable)) =
                        replacing_term
                    {
                        *variable = replacing_variable.clone();
                    } else {
                        panic!("You may not replace a variable with a non-variable term within an assigment.");
                    }
                }
            }
            Condition::Constraint(constraint) => constraint.apply_assignment(assignment),
        }
    }

    /// Return all terms.
    pub fn terms(&self) -> Vec<&Term> {
        match self {
            Condition::Assignment(_variable, term) => {
                vec![term]
            }
            Condition::Constraint(constraint) => {
                let (left, right) = constraint.terms();

                vec![left, right]
            }
        }
    }

    /// Return all variables.
    pub fn variables(&self) -> Vec<&Variable> {
        match self {
            Condition::Assignment(variable, term) => {
                let mut result = term.variables().collect::<Vec<_>>();
                result.push(variable);

                result
            }
            Condition::Constraint(constraint) => {
                let (left, right) = constraint.terms();

                let mut result = left.variables().collect::<Vec<_>>();
                result.extend(right.variables());

                result
            }
        }
    }

    /// Return all universally quantified variables in the atom.
    pub fn universal_variables(&self) -> impl Iterator<Item = &Variable> {
        self.variables()
            .into_iter()
            .filter(|var| matches!(var, Variable::Universal(_)))
    }

    /// Return all existentially quantified variables in the atom.
    pub fn existential_variables(&self) -> impl Iterator<Item = &Variable> {
        self.variables()
            .into_iter()
            .filter(|var| matches!(var, Variable::Existential(_)))
    }

    /// Return whether this type of condition only works on numeric values
    pub fn is_numeric(&self) -> bool {
        match self {
            Condition::Assignment(_, _) => false,
            Condition::Constraint(constraint) => constraint.is_numeric(),
        }
    }
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Condition::Assignment(variable, term) => {
                f.write_fmt(format_args!("{} := {}", variable, term))
            }
            Condition::Constraint(constraint) => constraint.fmt(f),
        }
    }
}
