//! This module defines [TransformationIncremental].

use std::collections::{HashMap, HashSet};

use crate::{
    io::{format_builder::SupportedFormatTag, formats::sparql::SparqlTag},
    rule_model::{
        components::{
            IterableVariables,
            import_export::{ImportDirective, clause::ImportClause},
            literal::Literal,
            rule::Rule,
            statement::Statement,
            tag::Tag,
            term::{
                Term,
                operation::{Operation, operation_kind::OperationKind},
                primitive::{Primitive, variable::Variable},
            },
        },
        error::ValidationReport,
        programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
    },
};

use super::ProgramTransformation;

/// Program transformation
///
/// Inlines certain import statements into rules.
#[derive(Debug, Clone, Copy, Default)]
pub struct TransformationIncremental {}

impl TransformationIncremental {
    /// Create a new [TransformationIncremental].
    pub fn new() -> Self {
        Self {}
    }

    /// Check if there are no restrictions on an atom.
    fn has_unrestricted_atom(
        rule: &Rule,
        incremental_predicates: &HashMap<Tag, &ImportDirective>,
    ) -> bool {
        let mut available_variables = rule
            .body_positive()
            .filter(|atom| !incremental_predicates.contains_key(&atom.predicate()))
            .flat_map(|atom| atom.variables())
            .collect::<HashSet<_>>();

        let mut available_variables_len = available_variables.len();

        loop {
            for operation in rule.body_operations() {
                if let Some((variable, assignment)) = operation.variable_assignment()
                    && assignment
                        .variables()
                        .all(|variable| available_variables.contains(variable))
                {
                    available_variables.insert(variable);
                }
            }

            if available_variables_len == available_variables.len() {
                break;
            } else {
                available_variables_len = available_variables.len();
            }
        }

        for atom in rule.body_positive() {
            if !incremental_predicates.contains_key(&atom.predicate()) {
                continue;
            }

            if atom.terms().any(|term| !term.is_variable()) {
                continue;
            }

            if atom
                .variables()
                .all(|variable| !available_variables.contains(variable))
            {
                return true;
            }
        }

        false
    }

    /// Check whether a rule would allow for incremental import
    fn possible_rule(rule: &Rule, incremental_predicates: &HashMap<Tag, &ImportDirective>) -> bool {
        if rule
            .body_positive()
            .all(|atom| !incremental_predicates.contains_key(&atom.predicate()))
        {
            return false;
        }

        !Self::has_unrestricted_atom(rule, incremental_predicates)
    }

    /// Compute the predicates that will be inlined in this transformation.
    ///
    /// Returns a hash map containing those predicates together with the
    /// associated import statement from which they originated.
    fn incremental_predicates(program: &ProgramHandle) -> HashMap<Tag, &ImportDirective> {
        // All predicates that will be incrementally imported
        let mut incremental_predicates = HashMap::<Tag, &ImportDirective>::default();

        // Predicates that have to be evaluated fully
        let mut normal_predicates = HashSet::<Tag>::default();

        for import in program.imports() {
            if let Some(builder) = import.builder() {
                if let SupportedFormatTag::Sparql(SparqlTag::Sparql) = builder.format() {
                    if incremental_predicates
                        .insert(import.predicate().clone(), import)
                        .is_some()
                    {
                        normal_predicates.insert(import.predicate().clone());
                    }
                } else {
                    normal_predicates.insert(import.predicate().clone());
                }
            }
        }

        incremental_predicates.retain(|predicate, _| !normal_predicates.contains(predicate));

        for statement in program.statements() {
            match statement {
                Statement::Fact(fact) => {
                    normal_predicates.insert(fact.predicate().clone());
                }
                Statement::Rule(rule) => {
                    for head in rule.head() {
                        normal_predicates.insert(head.predicate());
                    }
                    for atom in rule.body_negative() {
                        normal_predicates.insert(atom.predicate().clone());
                    }

                    if !Self::possible_rule(rule, &incremental_predicates) {
                        for atom in rule.body_positive() {
                            normal_predicates.insert(atom.predicate());
                        }
                    }
                }
                Statement::Export(export) => {
                    normal_predicates.insert(export.predicate().clone());
                }
                Statement::Output(output) => {
                    normal_predicates.insert(output.predicate().clone());
                }
                Statement::Parameter(_) | Statement::Import(_) => {}
            }
        }

        incremental_predicates.retain(|predicate, _| !normal_predicates.contains(predicate));

        incremental_predicates
    }

    /// Create a new variable which hold the value of computed terms.
    fn new_variable(predicate: &Tag, index: usize) -> Variable {
        let name = format!("_VAR_IMPORT_{}_{}", predicate.name(), index);
        Variable::universal(&name)
    }

    /// Change the predicate of a given [ImportDirective] such that it is unique.
    fn unique_import_directive(directive: &ImportDirective, id: &mut usize) -> ImportDirective {
        let predicate = Tag::new(format!("_{}_{}", directive.predicate(), id));
        *id += 1;

        let mut result = directive.clone();
        result.set_predicate(predicate);

        result
    }

    /// Given a rule with inlined import predicates,
    /// compute a rule that includes the corresponding import statements.
    fn incremental_rule(
        rule: &Rule,
        incremental_predicates: &HashMap<Tag, &ImportDirective>,
        derived_predicates: &HashSet<Tag>,
        id: &mut usize,
    ) -> Rule {
        let mut result = rule.clone();

        let mut import_clauses = Vec::<ImportClause>::default();
        let mut computed_terms = Vec::<(Variable, Term)>::default();

        result.body_mut().retain(|literal| {
            if let Literal::Positive(atom) = literal
                && let Some(&import) = incremental_predicates.get(&atom.predicate())
            {
                let mut variables = Vec::<Variable>::new();
                for (term_index, term) in atom.terms().enumerate() {
                    if let Term::Primitive(Primitive::Variable(variable)) = term {
                        variables.push(variable.clone());
                    } else {
                        let new_variable = Self::new_variable(&atom.predicate(), term_index);

                        variables.push(new_variable.clone());
                        computed_terms.push((new_variable, term.clone()));
                    }
                }

                let import = if !derived_predicates.contains(import.predicate()) {
                    Self::unique_import_directive(import, id)
                } else {
                    import.clone()
                };

                let clause = ImportClause::new(import, variables);

                import_clauses.push(clause);
                return false;
            }

            true
        });

        for import in import_clauses {
            result.add_import(import);
        }

        for (variable, term) in computed_terms {
            let operation = Operation::new(OperationKind::Equal, vec![Term::from(variable), term]);
            result.body_mut().push(Literal::Operation(operation));
        }

        result
    }
}

impl ProgramTransformation for TransformationIncremental {
    #[cfg(not(feature = "import-incremental"))]
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        program.fork_full().submit()
    }

    #[cfg(feature = "import-incremental")]
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let incremental_predicates = Self::incremental_predicates(program);
        let derived_predicates = program.derived_predicates();

        let mut name_id: usize = 0;

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    if rule
                        .body_positive()
                        .any(|atom| incremental_predicates.contains_key(&atom.predicate()))
                    {
                        let new_rule = Self::incremental_rule(
                            rule,
                            &incremental_predicates,
                            &derived_predicates,
                            &mut name_id,
                        );

                        commit.add_rule(new_rule);
                    } else {
                        commit.keep(statement);
                    }
                }
                Statement::Import(import) => {
                    if !incremental_predicates.contains_key(import.predicate()) {
                        commit.keep(statement);
                    }
                }
                _ => {
                    commit.keep(statement);
                }
            }
        }

        commit.submit()
    }
}
