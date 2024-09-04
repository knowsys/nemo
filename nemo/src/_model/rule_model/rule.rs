// use std::collections::{HashMap, HashSet};

// use crate::model::VariableAssignment;

// use super::{Atom, Constraint, Literal, PrimitiveTerm, Term, Variable};

// /// A rule.
// #[derive(Debug, Eq, PartialEq, Clone)]
// pub struct Rule {
//     /// Head atoms of the rule
//     head: Vec<Atom>,
//     /// Body literals of the rule
//     body: Vec<Literal>,
//     /// Constraints on the body of the rule
//     constraints: Vec<Constraint>,
// }

// impl Rule {
//     /// Construct a new rule.
//     pub fn new(head: Vec<Atom>, body: Vec<Literal>, constraints: Vec<Constraint>) -> Self {
//         Self {
//             head,
//             body,
//             constraints,
//         }
//     }

//     fn calculate_derived_variables(
//         safe_variables: &HashSet<Variable>,
//         constraints: &[Constraint],
//     ) -> HashSet<Variable> {
//         let mut derived_variables = safe_variables.clone();

//         let mut satisfied_constraints = HashSet::<usize>::new();
//         while satisfied_constraints.len() < constraints.len() {
//             let num_satisified_constraints = satisfied_constraints.len();

//             for (constraint_index, constraint) in constraints.iter().enumerate() {
//                 if satisfied_constraints.contains(&constraint_index) {
//                     continue;
//                 }

//                 if let Some((variable, term)) = constraint.has_form_assignment() {
//                     if !derived_variables.contains(variable)
//                         && term
//                             .variables()
//                             .all(|term_variable| derived_variables.contains(term_variable))
//                     {
//                         derived_variables.insert(variable.clone());
//                         satisfied_constraints.insert(constraint_index);
//                         continue;
//                     }
//                 }
//             }

//             if satisfied_constraints.len() == num_satisified_constraints {
//                 return derived_variables;
//             }
//         }

//         derived_variables
//     }

//     /// Return all variables that appear in negative literals
//     /// but cannot be derived from positive literals.
//     ///
//     /// For each variable also returns the associated index of the literal.
//     ///
//     /// Returns an error if one negative variable is associated with multiple literals.
//     fn calculate_negative_variables(
//         negative: &[Literal],
//         safe_variables: &HashSet<Variable>,
//     ) -> Result<HashMap<Variable, usize>, ParseError> {
//         let mut negative_variables = HashMap::<Variable, usize>::new();

//         for (literal_index, negative_literal) in negative.iter().enumerate() {
//             let mut current_unsafe = HashMap::<Variable, usize>::new();

//             for negative_term in negative_literal.terms() {
//                 if let Term::Primitive(PrimitiveTerm::Variable(variable)) = negative_term {
//                     if safe_variables.contains(variable) {
//                         continue;
//                     }

//                     current_unsafe.insert(variable.clone(), literal_index);

//                     if negative_variables.contains_key(variable) {
//                         return Err(ParseError::UnsafeVariableInMultipleNegativeLiterals(
//                             variable.clone(),
//                         ));
//                     }
//                 }
//             }

//             negative_variables.extend(current_unsafe)
//         }

//         Ok(negative_variables)
//     }

//     /// Construct a new rule, validating constraints on variable usage.
//     pub(crate) fn new_validated(
//         head: Vec<Atom>,
//         body: Vec<Literal>,
//         constraints: Vec<Constraint>,
//     ) -> Result<Self, ParseError> {
//         // All the existential variables used in the rule
//         let existential_variable_names = head
//             .iter()
//             .flat_map(|a| a.existential_variables().flat_map(|v| v.name()))
//             .collect::<HashSet<_>>();

//         for variable in body
//             .iter()
//             .flat_map(|l| l.variables())
//             .chain(constraints.iter().flat_map(|c| c.variables()))
//         {
//             // Existential variables may only occur in the head
//             if variable.is_existential() {
//                 return Err(ParseError::BodyExistential(variable.clone()));
//             }

//             // There may not be a universal variable whose name is the same that of an existential
//             if let Some(name) = variable.name() {
//                 if existential_variable_names.contains(&name) {
//                     return Err(ParseError::BothQuantifiers(name));
//                 }
//             }
//         }

//         // Divide the literals into a positive and a negative part
//         let (positive, negative): (Vec<_>, Vec<_>) = body
//             .iter()
//             .cloned()
//             .partition(|literal| literal.is_positive());

//         // Safe variables are considered to be
//         // all variables occuring as primitive terms in a positive body literal
//         // or every value that is equal to such a variable
//         let safe_variables = Self::safe_variables_literals(&positive);

//         // Derived variables are variables that result from functional expressions
//         // expressed as ?Variable = Term constraints,
//         // where the term only contains safe or derived variables.
//         let derived_variables = Self::calculate_derived_variables(&safe_variables, &constraints);

//         // Negative variables are variables that occur as primitive terms in negative literals
//         // bot cannot be derived
//         let negative_variables = Self::calculate_negative_variables(&negative, &derived_variables)?;

//         // Each constraint must only use derived variables
//         // or if it contains negative variables, then all variables in the constraint
//         // must be from the same atom
//         for constraint in &constraints {
//             let unknown = constraint.variables().find(|variable| {
//                 !derived_variables.contains(variable) && !negative_variables.contains_key(variable)
//             });

//             if let Some(variable) = unknown {
//                 return Err(ParseError::UnsafeComplexTerm(
//                     constraint.to_string(),
//                     variable.clone(),
//                 ));
//             }

//             if let Some(negative_variable) = constraint
//                 .variables()
//                 .find(|variable| negative_variables.contains_key(variable))
//             {
//                 let negative_literal = &negative[*negative_variables
//                     .get(negative_variable)
//                     .expect("Map must contain key")];
//                 let allowed_variables = negative_literal
//                     .variables()
//                     .cloned()
//                     .collect::<HashSet<Variable>>();

//                 if let Some(not_allowed) = constraint
//                     .variables()
//                     .find(|variable| !allowed_variables.contains(variable))
//                 {
//                     return Err(ParseError::ConstraintOutsideVariable(
//                         constraint.to_string(),
//                         negative_variable.clone(),
//                         negative_literal.to_string(),
//                         not_allowed.clone(),
//                     ));
//                 }
//             }
//         }

//         // Each complex term in the body and head must only use safe or derived variables
//         for term in body
//             .iter()
//             .flat_map(|l| l.terms())
//             .chain(head.iter().flat_map(|a| a.terms()))
//         {
//             if term.is_primitive() {
//                 continue;
//             }

//             for variable in term.variables() {
//                 if !derived_variables.contains(variable) {
//                     return Err(ParseError::UnsafeComplexTerm(
//                         term.to_string(),
//                         variable.clone(),
//                     ));
//                 }
//             }
//         }

//         let mut is_existential = false;

//         // Head atoms may only use variables that are safe or derived
//         for variable in head.iter().flat_map(|a| a.variables()) {
//             if variable.is_existential() {
//                 is_existential = true;
//             }

//             if variable.is_unnamed() {
//                 return Err(ParseError::UnnamedInHead);
//             }

//             if variable.is_universal() && !derived_variables.contains(variable) {
//                 return Err(ParseError::UnsafeHeadVariable(variable.clone()));
//             }
//         }

//         // Check for aggregates in the body of a rule
//         for literal in &body {
//             #[allow(clippy::never_loop)]
//             for aggregate in literal.aggregates() {
//                 return Err(ParseError::AggregateInBody(aggregate.clone()));
//             }
//         }
//         for constraint in &constraints {
//             #[allow(clippy::never_loop)]
//             for aggregate in constraint.aggregates() {
//                 return Err(ParseError::AggregateInBody(aggregate.clone()));
//             }
//         }

//         // We only allow one aggregate per rule,
//         // and do not allow them to appear together with existential variables
//         let mut aggregate_count = 0;
//         for head_atom in &head {
//             for term in head_atom.terms() {
//                 aggregate_count += term.aggregates().len();

//                 if aggregate_count > 1 {
//                     return Err(ParseError::MultipleAggregates);
//                 }
//             }
//         }

//         if aggregate_count > 0 && is_existential {
//             return Err(ParseError::AggregatesPlusExistentials);
//         }

//         Ok(Rule {
//             head,
//             body,
//             constraints,
//         })
//     }

//     /// Return all variables that are "safe".
//     /// A variable is safe if it occurs in a positive body literal.
//     fn safe_variables_literals(literals: &[Literal]) -> HashSet<Variable> {
//         let mut result = HashSet::new();

//         for literal in literals {
//             if let Literal::Positive(atom) = literal {
//                 for term in atom.terms() {
//                     if let Term::Primitive(PrimitiveTerm::Variable(variable)) = term {
//                         result.insert(variable.clone());
//                     }
//                 }
//             }
//         }

//         result
//     }

//     /// Return all variables that are "safe".
//     /// A variable is safe if it occurs in a positive body literal,
//     /// or is equal to such a value.
//     pub fn safe_variables(&self) -> HashSet<Variable> {
//         Self::safe_variables_literals(&self.body)
//     }

//     /// Return the head atoms of the rule - immutable.
//     #[must_use]
//     pub fn head(&self) -> &Vec<Atom> {
//         &self.head
//     }

//     /// Return the head atoms of the rule - mutable.
//     #[must_use]
//     pub fn head_mut(&mut self) -> &mut Vec<Atom> {
//         &mut self.head
//     }

//     /// Return the body literals of the rule - immutable.
//     #[must_use]
//     pub fn body(&self) -> &Vec<Literal> {
//         &self.body
//     }

//     /// Return the body literals of the rule - mutable.
//     #[must_use]
//     pub fn body_mut(&mut self) -> &mut Vec<Literal> {
//         &mut self.body
//     }

//     /// Return the constraints of the rule - immutable.
//     #[must_use]
//     pub fn constraints(&self) -> &Vec<Constraint> {
//         &self.constraints
//     }

//     /// Return the filters of the rule - mutable.
//     #[must_use]
//     pub fn constraints_mut(&mut self) -> &mut Vec<Constraint> {
//         &mut self.constraints
//     }

//     /// Replaces [Variable]s with [super::Term]s according to the provided assignment.
//     pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
//         self.body
//             .iter_mut()
//             .for_each(|l| l.apply_assignment(assignment));
//         self.head
//             .iter_mut()
//             .for_each(|a| a.apply_assignment(assignment));
//         self.constraints
//             .iter_mut()
//             .for_each(|f| f.apply_assignment(assignment));
//     }

//     /// Return the number of negative body atoms contained in the rule.
//     pub fn num_negative_body(&self) -> usize {
//         self.body
//             .iter()
//             .filter(|literal| literal.is_negative())
//             .count()
//     }
// }

// impl std::fmt::Display for Rule {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         for (index, atom) in self.head.iter().enumerate() {
//             atom.fmt(f)?;

//             if index < self.head.len() - 1 {
//                 f.write_str(", ")?;
//             }
//         }

//         f.write_str(" :- ")?;

//         for (index, literal) in self.body.iter().enumerate() {
//             literal.fmt(f)?;

//             if index < self.body.len() - 1 {
//                 f.write_str(", ")?;
//             }
//         }

//         if !self.constraints.is_empty() {
//             f.write_str(", ")?;
//         }

//         for (index, constraint) in self.constraints.iter().enumerate() {
//             constraint.fmt(f)?;

//             if index < self.constraints.len() - 1 {
//                 f.write_str(", ")?;
//             }
//         }

//         f.write_str(" .")
//     }
// }
