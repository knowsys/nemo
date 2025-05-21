//! This module defines [Program].

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Write,
};

use crate::rule_model::error::info::Info;

use super::{
    components::{
        component_iterator, component_iterator_mut,
        fact::Fact,
        import_export::{ExportDirective, ImportDirective},
        literal::Literal,
        output::Output,
        parameter::ParameterDeclaration,
        rule::Rule,
        tag::Tag,
        term::primitive::variable::{global::GlobalVariable, Variable},
        ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent,
        IterableVariables, ProgramComponent, ProgramComponentKind,
    },
    error::{validation_error::ValidationError, ValidationReport},
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

/// Representation of a nemo program
#[derive(Debug, Default, Clone)]
pub struct Program {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Imported resources
    imports: Vec<ImportDirective>,
    /// Exported resources
    exports: Vec<ExportDirective>,
    /// Rules
    rules: Vec<Rule>,
    /// Facts
    facts: Vec<Fact>,
    /// Outputs
    outputs: Vec<Output>,
    /// Parameter declarations
    parameters: Vec<ParameterDeclaration>,
}

/// Trait implemented by program-like objects
/// that allow adding statements
pub trait ProgramWrite: Default {
    /// Add a new export statement to the program.
    fn add_export(&mut self, export: ExportDirective);
    /// Add a new import statement to the program.
    fn add_import(&mut self, import: ImportDirective);
    /// Add a new output statement to the program.
    fn add_output(&mut self, output: Output);
    /// Add a new paramater declaration to the program.
    fn add_parameter_declaration(&mut self, parameter: ParameterDeclaration);
    /// Add a new rule to this program.
    fn add_rule(&mut self, rule: Rule);
    /// Add a new fact to this program.
    fn add_fact(&mut self, fact: Fact);
}

impl ProgramWrite for Program {
    fn add_export(&mut self, import: ExportDirective) {
        self.exports.push(import);
    }

    fn add_import(&mut self, export: ImportDirective) {
        self.imports.push(export);
    }

    fn add_output(&mut self, output: Output) {
        self.outputs.push(output);
    }

    fn add_parameter_declaration(&mut self, parameter: ParameterDeclaration) {
        self.parameters.push(parameter);
    }

    fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    fn add_fact(&mut self, fact: Fact) {
        self.facts.push(fact);
    }
}

/// Trait implemented by program-like objects that allows read access
/// to its statements
pub trait ProgramRead {
    /// Return an iterator over all imports.
    fn imports(&self) -> impl Iterator<Item = &ImportDirective>;

    /// Return an iterator over all exports.
    fn exports(&self) -> impl Iterator<Item = &ExportDirective>;

    /// Return an iterator over all rules.
    fn rules(&self) -> impl Iterator<Item = &Rule>;

    /// Return an iterator over all facts.
    fn facts(&self) -> impl Iterator<Item = &Fact>;

    /// Return an iterator over all outputs.
    fn outputs(&self) -> impl Iterator<Item = &Output>;

    /// Return an iterator over all parameter declarations.
    fn parameters(&self) -> impl Iterator<Item = &ParameterDeclaration>;

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
            } else {
                if !arities.contains_key(&predicate) {
                    report.add(
                        import,
                        ValidationError::UnknownArity {
                            predicate: predicate.to_string(),
                        },
                    );
                }
            }
        }

        for export in self.exports() {
            let predicate = export.predicate().clone();

            if let Some(arity) = export.expected_arity() {
                add(&mut arities, report, predicate, arity, export);
            } else {
                if !arities.contains_key(&predicate) {
                    report.add(
                        export,
                        ValidationError::UnknownArity {
                            predicate: predicate.to_string(),
                        },
                    );
                }
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

        for parameter in self.parameters() {
            if let Some(previous) = definitions.insert(parameter.variable().clone(), parameter) {
                report
                    .add(parameter, ValidationError::ParameterRedefinition)
                    .add_context(previous, Info::FirstDefinition);
                return;
            }

            if parameter.expression().is_none() {
                if !external.contains(parameter.variable()) {
                    report.add(parameter, ValidationError::ParameterMissingDefinition);
                }
            }
        }

        let mut valid = external;
        let mut valid_count: usize = valid.len();

        loop {
            for parameter in self.parameters() {
                if valid.contains(parameter.variable()) {
                    continue;
                }

                if let Some(expression) = parameter.expression() {
                    if expression.variables().all(|variable| {
                        if let Variable::Global(global) = variable {
                            valid.contains(global)
                        } else {
                            true
                        }
                    }) {
                        valid.insert(parameter.variable());
                    }
                }
            }

            if valid_count == valid.len() {
                break;
            }

            valid_count = valid.len();
        }

        for parameter in self.parameters() {
            if parameter.expression().is_some() {
                if !valid.contains(parameter.variable()) {
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
    }
}

impl ProgramRead for Program {
    fn imports(&self) -> impl Iterator<Item = &ImportDirective> {
        self.imports.iter()
    }

    fn exports(&self) -> impl Iterator<Item = &ExportDirective> {
        self.exports.iter()
    }

    fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter()
    }

    fn facts(&self) -> impl Iterator<Item = &Fact> {
        self.facts.iter()
    }

    fn outputs(&self) -> impl Iterator<Item = &Output> {
        self.outputs.iter()
    }

    fn parameters(&self) -> impl Iterator<Item = &ParameterDeclaration> {
        self.parameters.iter()
    }
}

impl Program {
    /// Return the rule at a particular index.
    ///
    /// # Panics
    /// Panics if there is no rule at this position.
    pub fn rule(&self, index: usize) -> &Rule {
        &self.rules[index]
    }

    /// Return an iterator over all imports.
    pub fn imports_mut(&mut self) -> impl Iterator<Item = &mut ImportDirective> {
        self.imports.iter_mut()
    }

    /// Return an iterator over all exports.
    pub fn exports_mut(&mut self) -> impl Iterator<Item = &mut ExportDirective> {
        self.exports.iter_mut()
    }

    /// Return an iterator over all rules.
    pub fn rules_mut(&mut self) -> impl Iterator<Item = &mut Rule> {
        self.rules.iter_mut()
    }

    /// Return an iterator over all facts.
    pub fn facts_mut(&mut self) -> impl Iterator<Item = &mut Fact> {
        self.facts.iter_mut()
    }

    /// Return an iterator over all outputs.
    pub fn outputs_mut(&mut self) -> impl Iterator<Item = &mut Output> {
        self.outputs.iter_mut()
    }

    /// Add new export statements to the program.
    pub fn add_exports<Iterator: IntoIterator<Item = ExportDirective>>(
        &mut self,
        exports: Iterator,
    ) {
        self.exports.extend(exports)
    }

    /// Remove all export statements
    pub fn clear_exports(&mut self) {
        self.exports.clear();
    }

    /// Remove all export statements
    pub fn clear_imports(&mut self) {
        self.imports.clear();
    }
}

impl ComponentBehavior for Program {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Program
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        self.validate_arity(&mut report);
        self.validate_stdin_imports(&mut report);

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for Program {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for Program {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for Program {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let rule_iterator = component_iterator(self.rules.iter());
        let fact_iterator = component_iterator(self.facts.iter());
        let output_iterator = component_iterator(self.outputs.iter());
        let import_iterator = component_iterator(self.imports.iter());
        let export_iterator = component_iterator(self.exports.iter());
        let parameter_iterator = component_iterator(self.parameters.iter());

        Box::new(
            rule_iterator
                .chain(fact_iterator)
                .chain(output_iterator)
                .chain(import_iterator)
                .chain(export_iterator)
                .chain(parameter_iterator),
        )
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let rule_iterator = component_iterator_mut(self.rules.iter_mut());
        let fact_iterator = component_iterator_mut(self.facts.iter_mut());
        let output_iterator = component_iterator_mut(self.outputs.iter_mut());
        let import_iterator = component_iterator_mut(self.imports.iter_mut());
        let export_iterator = component_iterator_mut(self.exports.iter_mut());
        let parameter_iterator = component_iterator_mut(self.parameters.iter_mut());

        Box::new(
            rule_iterator
                .chain(fact_iterator)
                .chain(output_iterator)
                .chain(import_iterator)
                .chain(export_iterator)
                .chain(parameter_iterator),
        )
    }
}

impl std::fmt::Display for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for parameter in self.parameters() {
            parameter.fmt(f)?;
            f.write_char('\n')?;
        }

        for import in self.imports() {
            import.fmt(f)?;
            f.write_char('\n')?;
        }

        for fact in self.facts() {
            fact.fmt(f)?;
            f.write_char('\n')?;
        }

        for rule in self.rules() {
            rule.fmt(f)?;
            f.write_char('\n')?;
        }

        for output in self.outputs() {
            output.fmt(f)?;
            f.write_char('\n')?;
        }

        for export in self.exports() {
            export.fmt(f)?;
            f.write_char('\n')?;
        }

        Ok(())
    }
}
