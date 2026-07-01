//! This module defines [TransformationSkolemize].

use std::collections::{HashMap, HashSet};

use crate::rule_model::{
    components::{
        IterableVariables,
        literal::Literal,
        statement::Statement,
        term::{
            Term,
            function::FunctionTerm,
            primitive::{
                Primitive,
                variable::{Variable, existential::ExistentialVariable},
            },
        },
    },
    error::ValidationReport,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
};

use super::ProgramTransformation;

/// Program transformation
///
/// Replaces each existential variable with skolem terms.
#[derive(Debug, Default, Copy, Clone)]
pub struct TransformationSkolemize {
    /// Skolem function counter
    skolem_count: usize,
}

impl ProgramTransformation for TransformationSkolemize {
    fn apply(mut self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        for statement in program.statements() {
            if let Statement::Rule(rule) = statement {
                let head_variables = || rule.head().iter().flat_map(|atom| atom.variables());
                if !head_variables().any(|variable| variable.is_existential()) {
                    commit.keep(statement);
                    continue;
                }

                let head_variables = head_variables().collect::<HashSet<_>>();

                let mut frontier_variables = HashSet::new();
                for literal in rule.body() {
                    let variables = match literal {
                        Literal::Positive(atom) => atom.variables(),
                        Literal::Operation(operation) => operation.variables(),
                        Literal::Negative(_) => continue,
                    };

                    for variable in variables {
                        if head_variables.contains(variable) {
                            frontier_variables.insert(variable.clone());
                        }
                    }
                }
                let frontier_variables = frontier_variables
                    .into_iter()
                    .map(Term::from)
                    .collect::<Vec<_>>();

                let mut new_rule = rule.clone();
                let mut count_by_ex_vars: HashMap<ExistentialVariable, usize> =
                    HashMap::<ExistentialVariable, usize>::new();
                for head_atom in new_rule.head_mut() {
                    for term in head_atom.terms_mut() {
                        if let Term::Primitive(Primitive::Variable(Variable::Existential(ex_var))) =
                            term
                        {
                            if let Some(sk_count) = count_by_ex_vars.get(ex_var) {
                                let name = format!("_SKOLEM_{}", sk_count);
                                *term = Term::from(FunctionTerm::new(
                                    &name,
                                    frontier_variables.clone(),
                                ));
                                continue;
                            }
                            self.skolem_count += 1;
                            count_by_ex_vars.insert(ex_var.clone(), self.skolem_count);
                            let name = format!("_SKOLEM_{}", self.skolem_count);
                            *term =
                                Term::from(FunctionTerm::new(&name, frontier_variables.clone()));
                        }
                    }
                }

                commit.add_rule(new_rule);
            } else {
                commit.keep(statement);
            }
        }

        commit.submit()
    }
}
