//! This module defines [TransformationMergeSparql].

use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::rule_model::{
    components::{
        IterableVariables,
        atom::Atom,
        import_export::clause::{ImportClause, ImportLiteral},
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
    pipeline::commit::ProgramCommit,
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
                    let mut imports = Vec::default();

                    for import_literal in rule.imports() {
                        match import_literal {
                            ImportLiteral::Positive(clause) => {
                                imports.push(clause.clone());
                            }
                            ImportLiteral::Negative(_) => {
                                // TODO: Handle negative case
                                continue;
                            }
                        }
                    }

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

                    let equalities = rule
                        .body_operations()
                        .filter_map(try_equality_from_operation)
                        .collect::<HashMap<_, _>>();

                    if imports.len() == 1 {
                        // nothing to merge, but push constants
                        let mut import = imports[0].clone();
                        import.push_constants(&equalities);

                        update_import(&mut commit, import, rule, equalities);
                    } else if let Some(merged) = imports
                        .into_iter()
                        .try_reduce(|left, right| ImportClause::try_merge(left, right, &equalities))
                        .flatten()
                    {
                        update_import(&mut commit, merged, rule, equalities);
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

fn update_import(
    commit: &mut ProgramCommit,
    import: ImportClause,
    rule: &Rule,
    equalities: HashMap<Variable, GroundTerm>,
) {
    let binding_variables = rule
        .body_positive()
        .flat_map(|atom| atom.variables())
        .cloned()
        .collect::<HashSet<_>>();
    let equalities = HashSet::<_>::from_iter(equalities);
    let output_variables = import
        .output_variables()
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    let is_still_incremental = equalities.is_empty()
        || binding_variables
            .intersection(&output_variables)
            .next()
            .is_some();

    let mut rule = rule.clone();
    rule.imports_mut().clear();

    if is_still_incremental {
        // TODO: Handle negative case
        rule.imports_mut().push(ImportLiteral::Positive(import));
    } else {
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

        commit.add_import(directive);
    }

    commit.add_rule(rule);
}
