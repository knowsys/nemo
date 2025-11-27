//! This module defines [TransformationMergeSparql].

use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::rule_model::{
    components::{
        IterableVariables,
        atom::Atom,
        import_export::{ImportDirective, clause::ImportClause},
        literal::Literal,
        rule::Rule,
        statement::Statement,
        term::{
            Term,
            operation::{Operation, operation_kind::OperationKind},
            primitive::{Primitive, ground::GroundTerm, variable::Variable},
        },
    },
    error::ValidationReport,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
};

use super::ProgramTransformation;

/// A [ProgramTransformation] that merges SPARQL imports against the
/// same endpoint in the same rule.
#[derive(Debug, Default, Copy, Clone)]
pub struct TransformationMergeSparql;

impl ProgramTransformation for TransformationMergeSparql {
    #[cfg(not(feature = "import-merge"))]
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        log::debug!("not merging SPARQL queries");
        program.fork_full().submit()
    }

    #[cfg(feature = "import-merge")]
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        log::debug!("merging SPARQL queries");
        let mut commit = program.fork();

        let mut ineligible = HashSet::new();
        for fact in program.facts() {
            ineligible.insert(fact.predicate().clone());
        }

        let mut defining_rules = HashMap::new();
        for rule in program.rules() {
            for atom in rule.head() {
                defining_rules
                    .entry(atom.predicate())
                    .and_modify(|count| *count += 1)
                    .or_insert(1_usize);
            }
        }
        ineligible.extend(
            defining_rules
                .into_iter()
                .filter_map(|(predicate, count)| (count > 1).then_some(predicate)),
        );

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    let imports = rule.imports().collect::<Vec<_>>();

                    if rule
                        .head()
                        .iter()
                        .any(|atom| ineligible.contains(&atom.predicate()))
                        || imports.is_empty()
                    {
                        // nothing to merge
                        commit.keep(statement);
                        continue;
                    }

                    let head_variables = rule
                        .head()
                        .iter()
                        .flat_map(|atom| atom.variables())
                        .filter(|variable| variable.is_universal())
                        .cloned()
                        .collect::<HashSet<_>>()
                        .len();
                    let equalities = rule
                        .body_operations()
                        .filter_map(try_equality_from_operation)
                        .collect::<HashMap<_, _>>();

                    if imports.len() == 1 {
                        // nothing to merge, but push constants

                        let mut import = imports[0].clone();

                        import.push_constants(&equalities);
                        let equalities = HashSet::<_>::from_iter(equalities.into_iter());
                        let is_still_incremental = equalities.is_empty()
                            || head_variables
                                != import
                                    .import_directive()
                                    .expected_output_arity()
                                    .unwrap_or(head_variables + 1);

                        let mut rule = rule.clone();
                        rule.imports_mut().clear();

                        if is_still_incremental {
                            rule.imports_mut().push(import);
                        } else {
                            commit.add_import(unincremental_import(
                                &mut rule,
                                &import,
                                &equalities,
                            ));
                        }

                        commit.add_rule(rule);

                        continue;
                    }

                    if let Some(merged) = imports
                        .into_iter()
                        .cloned()
                        .try_reduce(|left, right| ImportClause::try_merge(left, right, &equalities))
                        .flatten()
                    {
                        let equalities = HashSet::<_>::from_iter(equalities.into_iter());
                        let is_still_incremental = equalities.is_empty()
                            || head_variables
                                != merged
                                    .import_directive()
                                    .expected_output_arity()
                                    .unwrap_or(head_variables + 1);

                        let mut rule = rule.clone();
                        rule.imports_mut().clear();

                        if is_still_incremental {
                            rule.imports_mut().push(merged);
                        } else {
                            commit.add_import(unincremental_import(
                                &mut rule,
                                &merged,
                                &equalities,
                            ));
                        }

                        commit.add_rule(rule);
                    } else {
                        commit.keep(statement);
                    }
                }
                _ => commit.keep(statement),
            }
        }

        commit.submit()
    }
}

fn try_equality_from_operation(operation: &Operation) -> Option<(Variable, GroundTerm)> {
    if operation.operation_kind() != OperationKind::Equal {
        return None;
    }

    let (left, right) = operation.terms().collect_tuple()?;

    match (left, right) {
        (Term::Primitive(left), Term::Primitive(right)) => match (left, right) {
            (Primitive::Variable(variable), Primitive::Ground(ground_term))
            | (Primitive::Ground(ground_term), Primitive::Variable(variable)) => {
                Some((variable.clone(), ground_term.clone()))
            }
            _ => None,
        },
        _ => None,
    }
}

fn unincremental_import(
    rule: &mut Rule,
    import: &ImportClause,
    equalities: &HashSet<(Variable, GroundTerm)>,
) -> ImportDirective {
    let directive = import.import_directive().clone();
    let mut body = Vec::new();

    for literal in rule.body() {
        if let Literal::Operation(operation) = literal
            && let Some(equality) = try_equality_from_operation(operation)
            && equalities.contains(&equality)
        {
            continue;
        }

        body.push(literal.clone())
    }

    body.push(Literal::Positive(Atom::new(
        directive.predicate().clone(),
        import
            .variables()
            .map(|variable| Term::Primitive(Primitive::Variable(variable.clone()))),
    )));

    *rule.body_mut() = body;

    directive
}
