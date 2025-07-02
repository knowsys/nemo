//! This module defines types of nemo programs and common traits.

use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::rule_model::{components::statement::Statement, error::info::Info};

use super::{
    components::{
        component_iterator,
        fact::Fact,
        import_export::{ExportDirective, ImportDirective},
        literal::Literal,
        output::Output,
        parameter::ParameterDeclaration,
        rule::Rule,
        tag::Tag,
        term::primitive::variable::{global::GlobalVariable, Variable},
        IterableVariables, ProgramComponent,
    },
    error::{validation_error::ValidationError, ValidationReport},
};

pub mod handle;
pub mod program;

/// Trait implemented by program-like objects
/// that allow adding statements
pub trait ProgramWrite {
    /// Add a statement to the the program.
    fn add_statement(&mut self, statement: Statement);

    /// Add a new export statement to the program.
    fn add_export(&mut self, export: ExportDirective) {
        self.add_statement(export.into());
    }
    /// Add a new import statement to the program.
    fn add_import(&mut self, import: ImportDirective) {
        self.add_statement(import.into());
    }
    /// Add a new output statement to the program.
    fn add_output(&mut self, output: Output) {
        self.add_statement(output.into());
    }
    /// Add a new paramater declaration to the program.
    fn add_parameter_declaration(&mut self, parameter: ParameterDeclaration) {
        self.add_statement(parameter.into());
    }
    /// Add a new rule to this program.
    fn add_rule(&mut self, rule: Rule) {
        self.add_statement(rule.into());
    }
    /// Add a new fact to this program.
    fn add_fact(&mut self, fact: Fact) {
        self.add_statement(fact.into());
    }
}

/// Trait implemented by program-like objects that allows read access
/// to its statements
pub trait ProgramRead {
    /// Return an iterator over all statements.
    fn statements(&self) -> impl Iterator<Item = &Statement>;

    /// Return an iterator over all imports.
    fn imports(&self) -> impl Iterator<Item = &ImportDirective> {
        self.statements().filter_map(|statement| match statement {
            Statement::Import(import) => Some(import),
            _ => None,
        })
    }

    /// Return an iterator over all exports.
    fn exports(&self) -> impl Iterator<Item = &ExportDirective> {
        self.statements().filter_map(|statement| match statement {
            Statement::Export(export) => Some(export),
            _ => None,
        })
    }

    /// Return an iterator over all rules.
    fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.statements().filter_map(|statement| match statement {
            Statement::Rule(rule) => Some(rule),
            _ => None,
        })
    }

    /// Return an iterator over all facts.
    fn facts(&self) -> impl Iterator<Item = &Fact> {
        self.statements().filter_map(|statement| match statement {
            Statement::Fact(fact) => Some(fact),
            _ => None,
        })
    }

    /// Return an iterator over all outputs.
    fn outputs(&self) -> impl Iterator<Item = &Output> {
        self.statements().filter_map(|statement| match statement {
            Statement::Output(output) => Some(output),
            _ => None,
        })
    }

    /// Return an iterator over all parameter declarations.
    fn parameters(&self) -> impl Iterator<Item = &ParameterDeclaration> {
        self.statements().filter_map(|statement| match statement {
            Statement::Parameter(parameter) => Some(parameter),
            _ => None,
        })
    }

    /// Return the set of all predicates contained in this program.
    fn all_predicates(&self) -> HashSet<Tag> {
        let rules = self.rules().flat_map(|rule| {
            rule.head()
                .iter()
                .map(|atom| atom.predicate().clone())
                .chain(rule.body().iter().filter_map(|literal| match literal {
                    Literal::Positive(atom) | Literal::Negative(atom) => {
                        Some(atom.predicate().clone())
                    }
                    Literal::Operation(_) => None,
                }))
        });
        let facts = self.facts().map(|fact| fact.predicate().clone());

        rules.chain(facts).collect()
    }

    /// Return the set of all predicates that are defined by import statements.
    fn import_predicates(&self) -> HashSet<Tag> {
        self.imports()
            .map(|import| import.predicate().clone())
            .collect()
    }

    /// Return the set of all predicates that can be derived by applying rules.
    fn derived_predicates(&self) -> HashSet<Tag> {
        let rule_head = self
            .rules()
            .flat_map(|rule| rule.head())
            .map(|atom| atom.predicate().clone());
        let facts = self.facts().map(|fact| fact.predicate().clone());

        rule_head.chain(facts).collect()
    }

    /// Return the set of all used predicates
    fn used_predicates(&self) -> HashSet<Tag> {
        let mut used = self
            .outputs()
            .map(|output| output.predicate())
            .chain(self.exports().map(|export| export.predicate()))
            .chain(self.facts().map(|fact| fact.predicate()))
            .cloned()
            .collect::<HashSet<_>>();

        let mut used_count: usize = 0;

        loop {
            for rule in self.rules() {
                if rule
                    .head()
                    .iter()
                    .any(|atom| used.contains(&atom.predicate()))
                {
                    used.extend(rule.body_atoms().map(|atom| atom.predicate().clone()));
                }
            }

            if used.len() == used_count {
                return used;
            }

            used_count = used.len();
        }
    }

    /// Return a map associating each predicate with its arity.
    fn arities(&self) -> HashMap<Tag, usize> {
        let mut arities = HashMap::<Tag, usize>::new();

        for import in self.imports() {
            if let Some(arity) = import.expected_arity() {
                arities.insert(import.predicate().clone(), arity);
            }
        }

        for export in self.exports() {
            if let Some(arity) = export.expected_arity() {
                arities.insert(export.predicate().clone(), arity);
            }
        }

        for rule in self.rules() {
            for atom in rule.atoms() {
                arities.insert(atom.predicate().clone(), atom.len());
            }
        }

        for fact in self.facts() {
            arities.insert(fact.predicate().clone(), fact.len());
        }

        arities
    }

    /// Check whether this program contains conflicting or missing arities
    fn validate_arity(&self, report: &mut ValidationReport) {
        let mut arities = HashMap::<Tag, (usize, &dyn ProgramComponent)>::new();

        fn add<'a, Component: ProgramComponent>(
            arities: &'_ mut HashMap<Tag, (usize, &'a dyn ProgramComponent)>,
            report: &'_ mut ValidationReport,
            predicate: Tag,
            arity: usize,
            component: &'a Component,
        ) {
            let predicate_string = predicate.to_string();
            match arities.entry(predicate) {
                Entry::Occupied(entry) => {
                    let (previous_arity, previous_component) = entry.get();

                    if arity != *previous_arity {
                        report
                            .add(
                                component,
                                ValidationError::InconsistentArities {
                                    predicate: predicate_string,
                                    arity,
                                },
                            )
                            .add_context(
                                *previous_component,
                                Info::PredicateArity {
                                    arity: *previous_arity,
                                },
                            );
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((arity, component));
                }
            }
        }

        for fact in self.facts() {
            let predicate = fact.predicate().clone();
            let arity = fact.len();

            add(&mut arities, report, predicate, arity, fact);
        }

        for rule in self.rules() {
            for atom in rule.atoms() {
                let predicate = atom.predicate().clone();
                let arity = atom.len();

                add(&mut arities, report, predicate, arity, rule);
            }
        }

        let used_predicates = self.used_predicates();

        for import in self.imports() {
            let predicate = import.predicate().clone();
            if !used_predicates.contains(&predicate) {
                continue;
            }

            if let Some(arity) = import.expected_arity() {
                add(&mut arities, report, predicate, arity, import);
            } else if !arities.contains_key(&predicate) {
                report.add(
                    import,
                    ValidationError::UnknownArity {
                        predicate: predicate.to_string(),
                    },
                );
            }
        }

        for export in self.exports() {
            let predicate = export.predicate().clone();

            if let Some(arity) = export.expected_arity() {
                add(&mut arities, report, predicate, arity, export);
            } else if !arities.contains_key(&predicate) {
                report.add(
                    export,
                    ValidationError::UnknownArity {
                        predicate: predicate.to_string(),
                    },
                );
            }
        }
    }

    /// Check whether this program contains multiple
    fn validate_stdin_imports(&self, report: &mut ValidationReport) {
        let mut stdin = false;

        for import in self.imports() {
            if import.is_stdin() {
                if stdin {
                    report.add(import, ValidationError::ReachedStdinImportLimit);
                    return;
                }

                stdin = true;
            }
        }
    }

    /// Check whether each undefined paramater declaration
    /// has been defined externally and that there are
    /// no conflicting or cyclic definitions.
    fn validate_parameters(
        &self,
        report: &mut ValidationReport,
        external: HashSet<&GlobalVariable>,
    ) {
        let mut definitions = HashMap::<GlobalVariable, &ParameterDeclaration>::new();
        let mut defined = external.clone();

        for parameter in self.parameters() {
            if let Some(previous) = definitions.insert(parameter.variable().clone(), parameter) {
                report
                    .add(parameter, ValidationError::ParameterRedefinition)
                    .add_context(previous, Info::FirstDefinition);
                return;
            }

            if parameter.expression().is_none() && !external.contains(parameter.variable()) {
                report.add(parameter, ValidationError::ParameterMissingDefinition);
                defined.insert(parameter.variable());
            }
        }

        let mut defined_count: usize = defined.len();

        loop {
            for parameter in self.parameters() {
                if defined.contains(parameter.variable()) {
                    continue;
                }

                if let Some(expression) = parameter.expression() {
                    if expression.variables().all(|variable| {
                        if let Variable::Global(global) = variable {
                            defined.contains(global)
                        } else {
                            true
                        }
                    }) {
                        defined.insert(parameter.variable());
                    }
                }
            }

            if defined_count == defined.len() {
                break;
            }

            defined_count = defined.len();
        }

        for parameter in self.parameters() {
            if parameter.expression().is_some() && !defined.contains(parameter.variable()) {
                report.add(
                    parameter,
                    ValidationError::ParameterDeclarationCyclic {
                        variable: parameter.variable().clone(),
                    },
                );

                return;
            }
        }
    }

    /// Validate each component within this program.
    fn validate_components(&self, report: &mut ValidationReport) {
        let rule_iterator = component_iterator(self.rules());
        let fact_iterator = component_iterator(self.facts());
        let output_iterator = component_iterator(self.outputs());
        let import_iterator = component_iterator(self.imports());
        let export_iterator = component_iterator(self.exports());
        let parameter_iterator = component_iterator(self.parameters());

        for component in rule_iterator
            .chain(fact_iterator)
            .chain(output_iterator)
            .chain(import_iterator)
            .chain(export_iterator)
            .chain(parameter_iterator)
        {
            report.merge(component.validate());
        }
    }
}
