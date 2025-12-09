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
        log::info!("not merging SPARQL queries");
        program.fork_full().submit()
    }

    #[cfg(feature = "import-merge")]
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        log::info!("merging SPARQL queries");
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
                    let mut positive_imports = Vec::default();
                    let mut negative_imports = Vec::default();

                    for import_literal in rule.imports() {
                        match import_literal {
                            ImportLiteral::Positive(clause) => {
                                positive_imports.push(clause.clone());
                            }
                            ImportLiteral::Negative(clause) => {
                                negative_imports.push(clause.clone());
                            }
                        }
                    }

                    if rule
                        .head()
                        .iter()
                        .any(|atom| ineligible.contains(&atom.predicate()))
                        || (positive_imports.is_empty() && negative_imports.is_empty())
                    {
                        // nothing to merge
                        commit.keep(statement);
                        continue;
                    }

                    let equalities = rule
                        .body_operations()
                        .filter_map(try_equality_from_operation)
                        .collect::<HashMap<_, _>>();

                    let positive = merge_and_push_constants(positive_imports, &equalities);
                    let negative = merge_and_push_constants(negative_imports, &equalities);

                    update_import(&mut commit, positive, negative, rule, equalities);
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

fn merge_and_push_constants(
    imports: Vec<ImportClause>,
    equalities: &HashMap<Variable, GroundTerm>,
) -> Option<ImportClause> {
    if imports.len() == 1 {
        // nothing to merge, but push constants
        let mut import = imports[0].clone();
        import.push_constants(&equalities);

        Some(import)
    } else {
        imports
            .into_iter()
            .try_reduce(|left, right| ImportClause::try_merge(left, right, &equalities))
            .flatten()
    }
}

fn update_import(
    commit: &mut ProgramCommit,
    positive: Option<ImportClause>,
    negative: Option<ImportClause>,
    rule: &Rule,
    equalities: HashMap<Variable, GroundTerm>,
) {
    let binding_variables = rule
        .body_positive()
        .flat_map(|atom| atom.variables())
        .cloned()
        .collect::<HashSet<_>>();
    let equality_map = equalities.clone();
    let equalities = HashSet::<_>::from_iter(equalities);
    let positive_output_variables = positive
        .as_ref()
        .map(|import| {
            import
                .output_variables()
                .iter()
                .cloned()
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    let mut rule = rule.clone();
    rule.imports_mut().clear();

    if let Some(positive) = positive {
        let is_still_incremental = equalities.is_empty()
            || binding_variables
                .intersection(&positive_output_variables)
                .next()
                .is_some();

        if is_still_incremental {
            if let Some(negative) = negative {
                if let Some(merged) = ImportLiteral::try_merge(
                    ImportLiteral::Positive(positive.clone()),
                    ImportLiteral::Negative(negative.clone()),
                    &equality_map
                ) {
                    rule.imports_mut().push(merged);
                } else {
                    rule.imports_mut().push(ImportLiteral::Positive(positive));
                    rule.imports_mut().push(ImportLiteral::Negative(negative));
                }
            } else {
                rule.imports_mut().push(ImportLiteral::Positive(positive));
            }
        } else {
            let directive = positive.import_directive().clone();
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
                positive
                    .variables()
                    .map(|variable| Term::Primitive(Primitive::Variable(variable.clone()))),
            )));

            if let Some(negative) = negative {
                body.push(Literal::Negative(Atom::new(
                    negative.import_directive().predicate().clone(),
                    negative
                        .variables()
                        .map(|variable| Term::Primitive(Primitive::Variable(variable.clone()))),
                )));

                commit.add_import(negative.import_directive().clone());
            }

            *rule.body_mut() = body;

            commit.add_import(directive);
        }
    }

    commit.add_rule(rule);
}
