//! Contains a representation of existential rules
//! that is more suitable for the computation of reliances.

use std::collections::HashMap;

use crate::{
    model::{self, Identifier},
    util::class_assignment::ClassValue,
};

use super::common::{Interpretation, VariableAssignment};

/// Unique identifier for a variable.
pub(super) type VariableId = isize;
/// Unique identifier for a constant
/// Since the actual value (and type of value) is not relevant for
/// the computation of reliances we just use this numeric label instead.
pub(super) type ConstantId = usize;
/// Unique identifier for a predicate symbol
pub(super) type PredicateId = Identifier;

/// Represents a (universal or existential) variable.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(super) enum Variable {
    Universal(VariableId),
    Existential(VariableId),
}

impl Variable {
    pub fn is_existential(&self) -> bool {
        matches!(self, Variable::Existential(_))
    }

    pub fn name_apart(self) -> Self {
        let mut result = self.clone();

        match &mut result {
            Variable::Universal(id) | Variable::Existential(id) => *id *= -1,
        }

        result
    }

    #[allow(dead_code)]
    pub fn is_universal(&self) -> bool {
        matches!(self, Variable::Universal(_))
    }
}

/// Representation of non-variable term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GroundTerm {
    /// "Normal" constant that may occur within a rule.
    Constant(ConstantId),
    /// Invented constant that is assigned to a variable.
    GroundedVariable(Variable),
    /// Null assigned to an existential variable as a result of restricted chase rule application.
    Null(VariableId),
}

impl GroundTerm {
    pub fn is_null(&self) -> bool {
        matches!(self, GroundTerm::Null(_))
    }
}

/// Term encoding used for computing reliances.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum Term {
    Variable(Variable),
    Ground(GroundTerm),
}

impl From<Variable> for Term {
    fn from(value: Variable) -> Self {
        Term::Variable(value)
    }
}

impl From<GroundTerm> for Term {
    fn from(value: GroundTerm) -> Self {
        Term::Ground(value)
    }
}

impl Term {
    pub fn is_universal(&self) -> bool {
        matches!(self, Term::Variable(Variable::Universal(_)))
    }

    #[allow(dead_code)]
    pub fn is_existential(&self) -> bool {
        matches!(self, Term::Variable(Variable::Existential(_)))
    }

    #[allow(dead_code)]
    pub fn is_ground(&self) -> bool {
        matches!(self, Term::Ground(_))
    }

    #[allow(dead_code)]
    pub fn is_null(&self) -> bool {
        matches!(self, Term::Ground(GroundTerm::Null(_)))
    }
}

/// Atom encoding used for computing reliances.
#[derive(Debug, Clone)]
pub(super) struct Atom {
    pub predicate: PredicateId,
    pub terms: Vec<Term>,
}

impl Atom {
    #[cfg(test)]
    pub fn new(predicate: &str, terms: Vec<Term>) -> Self {
        Self {
            predicate: Identifier(predicate.to_string()),
            terms,
        }
    }

    pub fn compatible(&self, other: &Self) -> bool {
        self.predicate == other.predicate && self.terms.len() == other.terms.len()
    }
}

/// Same as an [`Atom`] which only contains [`GroundTerm`]s.
#[derive(Debug, Clone)]
pub(super) struct Fact {
    pub predicate: PredicateId,
    pub terms: Vec<GroundTerm>,
}

impl Fact {
    #[cfg(test)]
    pub fn new(predicate: &str, terms: Vec<GroundTerm>) -> Self {
        Self {
            predicate: Identifier(predicate.to_string()),
            terms,
        }
    }

    pub fn compatible(&self, other: &Atom) -> bool {
        self.predicate == other.predicate && self.terms.len() == other.terms.len()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum AssignmentRestriction {
    Unrestricted,
    Universal,
}

#[derive(Debug, Clone)]
pub(super) struct Formula(Vec<Atom>);

impl Formula {
    pub fn new(atoms: Vec<Atom>) -> Self {
        Self(atoms)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn atoms(&self) -> &Vec<Atom> {
        &self.0
    }

    pub fn split(&self, mapping: &Vec<usize>) -> (Formula, Formula) {
        debug_assert!(mapping.is_sorted());

        let mut atoms_in = Vec::<Atom>::with_capacity(mapping.len());
        let mut atoms_out = Vec::<Atom>::with_capacity(self.0.len());

        let mut current_mapping_index = 0;
        for (atom_index, atom) in self.atoms().iter().enumerate() {
            let mapping_value = if current_mapping_index < mapping.len() {
                mapping[current_mapping_index]
            } else {
                self.atoms().len()
            };

            if atom_index == mapping_value {
                atoms_in.push(atom.clone());
                current_mapping_index += 1;
            } else {
                atoms_out.push(atom.clone());
            }
        }

        (Formula::new(atoms_in), Formula::new(atoms_out))
    }

    pub fn apply_assignment(
        &self,
        assignment: &VariableAssignment,
        restriction: AssignmentRestriction,
    ) -> Self {
        let mut new_atoms = Vec::<Atom>::with_capacity(self.len());
        for atom in self.atoms() {
            new_atoms.push(Atom {
                predicate: atom.predicate.clone(),
                terms: atom
                    .terms
                    .iter()
                    .map(|&term| match term {
                        Term::Variable(variable) => {
                            if variable.is_existential()
                                && restriction == AssignmentRestriction::Universal
                            {
                                return term;
                            }

                            match assignment.value(&variable) {
                                ClassValue::Assigned(value) => Term::Ground(*value),
                                ClassValue::Unassigned(representative) => {
                                    Term::Ground(GroundTerm::GroundedVariable(*representative))
                                }
                            }
                        }
                        Term::Ground(_) => term,
                    })
                    .collect(),
            });
        }

        Self(new_atoms)
    }

    pub fn apply_grounding(&self, assignment: &VariableAssignment) -> Interpretation {
        let mut new_atoms = Vec::<Fact>::with_capacity(self.len());
        for atom in self.atoms() {
            new_atoms.push(Fact {
                predicate: atom.predicate.clone(),
                terms: atom
                    .terms
                    .iter()
                    .map(|&term| match term {
                        Term::Variable(variable) => match assignment.value(&variable) {
                            ClassValue::Assigned(value) => *value,
                            ClassValue::Unassigned(representative) => {
                                GroundTerm::GroundedVariable(*representative)
                            }
                        },
                        Term::Ground(ground_term) => ground_term,
                    })
                    .collect(),
            });
        }

        Interpretation::new(new_atoms)
    }

    pub fn apply_restricted(&self) -> Formula {
        let mut new_atoms = Vec::<Atom>::with_capacity(self.len());
        for atom in self.atoms() {
            new_atoms.push(Atom {
                predicate: atom.predicate.clone(),
                terms: atom
                    .terms
                    .iter()
                    .map(|&term| match term {
                        Term::Variable(Variable::Existential(existential_variable)) => {
                            Term::Ground(GroundTerm::Null(existential_variable))
                        }
                        _ => term,
                    })
                    .collect(),
            });
        }

        Self(new_atoms)
    }

    pub fn name_apart_variables(mut self) -> Self {
        for atom in &mut self.0 {
            for term in &mut atom.terms {
                if let Term::Variable(variable) = term {
                    variable.name_apart();
                }
            }
        }

        self
    }
}

/// Rule encoding used for computing reliances.
#[derive(Debug, Clone)]
pub(super) struct Rule {
    body: Formula,
    head: Formula,

    is_existential: bool,
}

impl Rule {
    fn get_or_add_constant(
        constant_list: &mut Vec<model::Term>,
        constant_term: &model::Term,
    ) -> ConstantId {
        if let Some(position) = constant_list.iter().position(|c| c == constant_term) {
            return position;
        }

        let result = constant_list.len();
        constant_list.push(constant_term.clone());

        result
    }

    fn get_or_add_variable(
        variable_map: &mut HashMap<(usize, model::Variable), VariableId>,
        variable: &model::Variable,
        rule_index: usize,
    ) -> VariableId {
        let count = variable_map.len();
        *variable_map
            .entry((rule_index, variable.clone()))
            .or_insert(count as VariableId)
    }

    fn translate_term<'a>(
        constant_list: &mut Vec<model::Term>,
        variable_map: &mut HashMap<(usize, model::Variable), VariableId>,
        filter_assignments: &'a HashMap<model::Variable, model::Term>,
        mut model_term: &'a model::Term,
        rule_index: usize,
    ) -> Term {
        if let model::Term::Variable(model_variable) = model_term {
            if let Some(assigned_constant_term) = filter_assignments.get(model_variable) {
                model_term = assigned_constant_term;
            }
        }

        if let model::Term::Variable(model_variable) = model_term {
            let variable_id = Self::get_or_add_variable(variable_map, model_variable, rule_index);
            match model_variable {
                model::Variable::Universal(_) => Term::Variable(Variable::Universal(variable_id)),
                model::Variable::Existential(_) => {
                    Term::Variable(Variable::Existential(variable_id))
                }
            }
        } else {
            let constant_id = Self::get_or_add_constant(constant_list, model_term);
            Term::Ground(GroundTerm::Constant(constant_id))
        }
    }

    fn from_parsed_rule(
        constant_list: &mut Vec<model::Term>,
        variable_map: &mut HashMap<(usize, model::Variable), VariableId>,
        rule: &model::Rule,
        rule_index: usize,
    ) -> Self {
        let mut filter_assignments = HashMap::<model::Variable, model::Term>::new();

        for filter in rule.filters() {
            if filter.operation == model::FilterOperation::Equals
                && !matches!(filter.rhs, model::Term::Variable(_))
            {
                if let Some(old_constant) =
                    filter_assignments.insert(filter.lhs.clone(), filter.rhs.clone())
                {
                    // TODO:
                    // Its not quite clear to me where to handle these cases of "ill-formed" rules
                    assert_eq!(old_constant, filter.rhs);
                }
            }
        }

        let mut body = Vec::<Atom>::new();
        let mut head = Vec::<Atom>::new();

        for model_literal in rule.body() {
            if let model::Literal::Positive(model_atom) = model_literal {
                let reliance_predicate = model_atom.predicate();
                let reliance_terms = model_atom
                    .terms()
                    .iter()
                    .map(|t| {
                        Self::translate_term(
                            constant_list,
                            variable_map,
                            &filter_assignments,
                            t,
                            rule_index,
                        )
                    })
                    .collect();

                body.push(Atom {
                    terms: reliance_terms,
                    predicate: reliance_predicate,
                });
            } else {
                // TODO: We do not consider negation for now
            }
        }

        let mut is_existential = false;

        for model_atom in rule.head() {
            let reliance_predicate = model_atom.predicate();
            let reliance_terms: Vec<Term> = model_atom
                .terms()
                .iter()
                .map(|t| {
                    Self::translate_term(
                        constant_list,
                        variable_map,
                        &filter_assignments,
                        t,
                        rule_index,
                    )
                })
                .collect();

            is_existential |= reliance_terms
                .iter()
                .position(|t| matches!(t, Term::Variable(Variable::Existential(_))))
                .is_some();

            head.push(Atom {
                terms: reliance_terms,
                predicate: reliance_predicate,
            });
        }

        Self {
            body: Formula(body),
            head: Formula(head),
            is_existential,
        }
    }

    pub fn is_existential(&self) -> bool {
        self.is_existential
    }

    pub fn body(&self) -> &Formula {
        &self.body
    }

    pub fn head(&self) -> &Formula {
        &self.head
    }

    pub fn name_apart_variables(self) -> Self {
        Self {
            body: self.body.name_apart_variables(),
            head: self.head.name_apart_variables(),
            is_existential: self.is_existential,
        }
    }
}

#[derive(Debug)]
pub(super) struct Program {
    rules: Vec<Rule>,
}

impl Program {
    /// Create a [`Program`] from set list of parsed rules
    pub fn from_parsed_rules(model_rules: &[model::Rule]) -> Self {
        let mut constant_list = Vec::<model::Term>::new();
        let mut variable_map = HashMap::<(usize, model::Variable), VariableId>::new();

        let rules = model_rules
            .iter()
            .enumerate()
            .map(|(index, rule)| {
                Rule::from_parsed_rule(&mut constant_list, &mut variable_map, rule, index)
            })
            .collect();

        Self { rules }
    }

    pub fn rules(&self) -> &Vec<Rule> {
        &self.rules
    }
}
