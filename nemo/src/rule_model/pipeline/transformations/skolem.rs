//! This module defines [TransformationSkolemize].

use std::collections::{HashMap, HashSet};

use crate::rule_model::{
    components::{
        ComponentIdentity, ComponentSource, IterableVariables,
        literal::Literal,
        statement::Statement,
        term::{
            Term,
            function::FunctionTerm,
            primitive::{Primitive, variable::Variable},
        },
    },
    error::ValidationReport,
    origin::Origin,
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
                new_rule.set_origin(Origin::Skolemization(rule.id()));
                let mut sk_terms_by_ex_var = HashMap::new();
                for head_atom in new_rule.head_mut() {
                    for term in head_atom.terms_mut() {
                        if let Term::Primitive(Primitive::Variable(Variable::Existential(
                            existential,
                        ))) = term
                        {
                            let skolem_term = sk_terms_by_ex_var
                                .entry(existential.clone())
                                .or_insert_with(|| {
                                    self.skolem_count += 1;
                                    let name = format!("_SKOLEM_{}", self.skolem_count);
                                    Term::from(FunctionTerm::new(&name, frontier_variables.clone()))
                                })
                                .clone();
                            *term = skolem_term;
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

#[cfg(test)]
mod test {
    use crate::{
        rule_file::RuleFile,
        rule_model::{
            components::term::Term,
            pipeline::transformations::skolem::TransformationSkolemize,
            programs::{ProgramRead, handle::ProgramHandle},
        },
    };

    #[test]
    fn repeated_existential_variables_use_the_same_skolem_term() {
        let program = "p(!e, !e, !f), q(!e) :- input(?x) .";
        let handle =
            ProgramHandle::from_file(&RuleFile::new(program.to_string(), String::default()))
                .expect("program parses")
                .into_object();

        let transformed = handle
            .transform(TransformationSkolemize::default())
            .expect("skolem transformation succeeds");
        let mut rules = transformed.rules();
        let rule = rules.next().expect("transformed rule is retained");
        assert!(rules.next().is_none());

        let head_terms = rule
            .head()
            .iter()
            .flat_map(|atom| atom.terms())
            .collect::<Vec<_>>();

        assert_eq!(head_terms.len(), 4);
        assert!(
            head_terms
                .iter()
                .all(|term| matches!(term, Term::FunctionTerm(_)))
        );
        assert_eq!(head_terms[0], head_terms[1]);
        assert_eq!(head_terms[0], head_terms[3]);
        assert_ne!(head_terms[0], head_terms[2]);
    }
}
