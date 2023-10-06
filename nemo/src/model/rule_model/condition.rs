use crate::model::VariableAssignment;

use super::{PrimitiveTerm, Term, Variable};

/// Condition of the form `<term> <operation> <term>`
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Condition {
    /// Variable is assigned to the term.
    Assignment(Variable, Term),
    /// Two terms are equal.
    Equals(Term, Term),
    /// Two terms are unequal.
    Unequals(Term, Term),
    /// Value of the left term is less than the value of the right term.
    LessThan(Term, Term),
    /// Value of the left term is greater than the value of the right term.
    GreaterThan(Term, Term),
    /// Value of the left term is less than or equal to the value of the right term.
    LessThanEq(Term, Term),
    /// Value of the left term is greater than or equal to the value of the right term.
    GreaterThanEq(Term, Term),
}

impl Condition {
    /// Replaces [`Variable`]s with [`Term`]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        match self {
            Condition::Assignment(variable, term) => {
                if let Some(replacing_term) = assignment.get(variable) {
                    if let Term::Primitive(PrimitiveTerm::Variable(replacing_variable)) =
                        replacing_term
                    {
                        *variable = replacing_variable.clone();
                    } else {
                        *self = Condition::Equals(replacing_term.clone(), term.clone());
                    }
                }
            }
            Condition::Equals(left, right)
            | Condition::Unequals(left, right)
            | Condition::LessThan(left, right)
            | Condition::GreaterThan(left, right)
            | Condition::LessThanEq(left, right)
            | Condition::GreaterThanEq(left, right) => {
                left.apply_assignment(assignment);
                right.apply_assignment(assignment);
            }
        }
    }

    /// Return all terms.
    pub fn terms(&self) -> Vec<&Term> {
        match self {
            Condition::Assignment(variable, term) => {
                vec![term]
            }
            Condition::Equals(left, right)
            | Condition::Unequals(left, right)
            | Condition::LessThan(left, right)
            | Condition::GreaterThan(left, right)
            | Condition::LessThanEq(left, right)
            | Condition::GreaterThanEq(left, right) => {
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
            Condition::Equals(left, right)
            | Condition::Unequals(left, right)
            | Condition::LessThan(left, right)
            | Condition::GreaterThan(left, right)
            | Condition::LessThanEq(left, right)
            | Condition::GreaterThanEq(left, right) => {
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
}

impl Condition {
    /// Returns a string representation for the operation defined by this condition.
    fn operator_string(&self) -> &'static str {
        match self {
            Condition::Assignment(_, _) => ":-",
            Condition::Equals(_, _) => "=",
            Condition::Unequals(_, _) => "!=",
            Condition::LessThan(_, _) => "<",
            Condition::GreaterThan(_, _) => ">",
            Condition::LessThanEq(_, _) => "<=",
            Condition::GreaterThanEq(_, _) => ">=",
        }
    }
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Condition::Assignment(variable, term) => f.write_fmt(format_args!(
                "{} {} {}",
                variable,
                self.operator_string(),
                term
            )),
            Condition::Equals(left, right)
            | Condition::Unequals(left, right)
            | Condition::LessThan(left, right)
            | Condition::GreaterThan(left, right)
            | Condition::LessThanEq(left, right)
            | Condition::GreaterThanEq(left, right) => f.write_fmt(format_args!(
                "{} {} {}",
                left,
                self.operator_string(),
                right
            )),
        }
    }
}
