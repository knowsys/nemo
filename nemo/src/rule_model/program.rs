//! This module defines [Program].

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Write,
};

use super::{
    components::{
        fact::Fact,
        import_export::{ExportDirective, ImportDirective},
        literal::Literal,
        output::Output,
        parameter::ParameterDeclaration,
        rule::Rule,
        tag::Tag,
        term::{
            primitive::{ground::GroundTerm, variable::global::GlobalVariable, Primitive},
            Term,
        },
        ProgramComponent,
    },
    error::{
        info::Info, validation_error::ValidationErrorKind, ComplexErrorLabelKind,
        ValidationErrorBuilder,
    },
    origin::Origin,
    substitution::Substitution,
};

/// Representation of a nemo program
#[derive(Default, Clone)]
pub struct Program {
    /// Origin of this component
    origin: Origin,

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

    /// Expanded values of global parameters (filled upon validation)
    parameter_expansions: Substitution,

    /// Map of all predicate arities (filled upon validation)
    arities: HashMap<Tag, (usize, Origin)>,
}

impl std::fmt::Debug for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Program")
            .field("origin", &self.origin)
            .field("imports", &self.imports)
            .field("exports", &self.exports)
            .field("rules", &self.rules)
            .field("facts", &self.facts)
            .field("outputs", &self.outputs)
            .field("parameters", &self.parameters)
            .field("arities", &self.arities)
            .finish()
    }
}

impl Program {
    /// Return an iterator over all imports.
    pub fn imports(&self) -> impl Iterator<Item = &ImportDirective> {
        self.imports.iter()
    }

    /// Return an iterator over all exports.
    pub fn exports(&self) -> impl Iterator<Item = &ExportDirective> {
        self.exports.iter()
    }

    /// Return an iterator over all rules.
    pub fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter()
    }

    /// Return the rule at a particular index.
    ///
    /// # Panics
    /// Panics if there is no rule at this position.
    pub fn rule(&self, index: usize) -> &Rule {
        &self.rules[index]
    }

    /// Return an iterator over all facts.
    pub fn facts(&self) -> impl Iterator<Item = &Fact> {
        self.facts.iter()
    }

    /// Return an iterator over all outputs.
    pub fn outputs(&self) -> impl Iterator<Item = &Output> {
        self.outputs.iter()
    }

    /// Return the set of all predicates that are defined by import statements.
    pub fn import_predicates(&self) -> HashSet<Tag> {
        self.imports
            .iter()
            .map(|import| import.predicate().clone())
            .collect()
    }

    /// Return the set of all predicates that can be derived by applying rules.
    pub fn derived_predicates(&self) -> HashSet<Tag> {
        let rule_head = self
            .rules()
            .flat_map(|rule| rule.head())
            .map(|atom| atom.predicate().clone());
        let facts = self.facts().map(|fact| fact.predicate().clone());

        rule_head.chain(facts).collect()
    }

    /// Return the set of all predicates contained in this program.
    pub fn all_predicates(&self) -> HashSet<Tag> {
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

    /// Add a new export statement to the program.
    pub fn add_export(&mut self, directive: ExportDirective) {
        self.exports.push(directive);
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

    /// Add a new import statement to the program.
    pub fn add_import(&mut self, directive: ImportDirective) {
        self.imports.push(directive);
    }

    /// Mark a predicate as an output predicate.
    pub fn add_output(&mut self, predicate: Tag) {
        self.outputs.push(Output::new(predicate));
    }

    pub(crate) fn arities(&self) -> &HashMap<Tag, (usize, Origin)> {
        &self.arities
    }

    /// Check if a different arity was already used for the given predicate
    /// and report an error if this was the case.
    fn validate_arity(
        predicate_arity: &mut HashMap<Tag, (usize, Origin)>,
        tag: Tag,
        arity: usize,
        origin: Origin,
        builder: &mut ValidationErrorBuilder,
    ) {
        let predicate_string = tag.to_string();

        match predicate_arity.entry(tag) {
            Entry::Occupied(entry) => {
                let (previous_arity, previous_origin) = entry.get();

                if arity != *previous_arity {
                    builder
                        .report_error(
                            origin,
                            ValidationErrorKind::InconsistentArities {
                                predicate: predicate_string,
                                arity,
                            },
                        )
                        .add_label(
                            ComplexErrorLabelKind::Information,
                            *previous_origin,
                            Info::PredicateArity {
                                arity: *previous_arity,
                            },
                        );
                }
            }
            Entry::Vacant(entry) => {
                entry.insert((arity, origin));
            }
        }
    }

    /// Validate the global program properties without validating
    /// each program element.
    pub(crate) fn validate_global_properties(
        &mut self,
        builder: &mut ValidationErrorBuilder,
    ) -> Option<()> {
        let mut arities = self.arities.clone();

        for fact in self.facts() {
            let predicate = fact.predicate().clone();
            let arity = fact.subterms().count();
            let origin = *fact.origin();

            Self::validate_arity(&mut arities, predicate, arity, origin, builder);
        }

        for rule in self.rules() {
            for atom in rule.head() {
                let predicate = atom.predicate().clone();
                let arity = atom.arguments().count();
                let origin = *atom.origin();

                Self::validate_arity(&mut arities, predicate, arity, origin, builder);
            }

            for literal in rule.body() {
                match literal {
                    Literal::Positive(atom) | Literal::Negative(atom) => {
                        let predicate = atom.predicate().clone();
                        let arity = atom.arguments().count();
                        let origin = *atom.origin();

                        Self::validate_arity(&mut arities, predicate, arity, origin, builder);
                    }
                    Literal::Operation(_) => {
                        continue;
                    }
                }
            }
        }

        for import in &self.imports {
            if !arities.contains_key(import.predicate()) {
                builder.report_error(
                    *import.predicate().origin(),
                    ValidationErrorKind::UnknownArity {
                        predicate: import.predicate().to_string(),
                    },
                );
                return None;
            }
        }

        for export in &self.exports {
            if !arities.contains_key(export.predicate()) {
                builder.report_error(
                    *export.predicate().origin(),
                    ValidationErrorKind::UnknownArity {
                        predicate: export.predicate().to_string(),
                    },
                );
                return None;
            }
        }

        self.arities = arities;

        Some(())
    }
}

impl std::fmt::Display for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for import in &self.imports {
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
        for export in &self.exports {
            export.fmt(f)?;
            f.write_char('\n')?;
        }

        Ok(())
    }
}

impl Program {
    fn validate(
        &mut self,
        external_parameters: &HashMap<GlobalVariable, GroundTerm>,
        error_builder: &mut ValidationErrorBuilder,
    ) -> Option<()>
    where
        Self: Sized,
    {
        for decl in &self.parameters {
            let var = decl.variable();

            if let Some(expansion) = external_parameters.get(var) {
                self.parameter_expansions
                    .insert(var.clone(), expansion.clone());
            } else {
                let Some(term) = decl.expression() else {
                    error_builder.report_error(
                        *decl.origin(),
                        ValidationErrorKind::ParameterMissingDefinition,
                    );
                    return None;
                };

                let Term::Primitive(Primitive::Ground(expansion)) =
                    term.reduce_with_substitution(&self.parameter_expansions)
                else {
                    error_builder.report_error(
                        *term.origin(),
                        ValidationErrorKind::ParameterDeclarationReferencesUndefinedGlobal,
                    );
                    return None;
                };

                self.parameter_expansions
                    .insert(var.clone(), expansion.clone());
            }
        }

        let mut count_stdin = 0;
        for import in &mut self.imports {
            self.parameter_expansions.apply(import);
            let builder = import.validate(error_builder)?;
            let predicate = import.predicate().clone();
            let origin = *import.origin();

            if builder
                .resource()
                .is_some_and(|resource| resource.is_pipe())
            {
                count_stdin += 1;
                if count_stdin > 1 {
                    error_builder
                        .report_error(origin, ValidationErrorKind::ReachedStdinImportLimit);
                }
            }

            if let Some(arity) = builder.expected_arity() {
                Self::validate_arity(&mut self.arities, predicate, arity, origin, error_builder);
            }
        }

        for fact in &mut self.facts {
            self.parameter_expansions.apply(fact);
            let _ = fact.validate(error_builder);
        }

        for rule in &mut self.rules {
            self.parameter_expansions.apply(rule);
            let _ = rule.validate(error_builder);
        }

        for output in self.outputs() {
            let _ = output.validate(error_builder);
        }

        for export in &mut self.exports {
            self.parameter_expansions.apply(export);
            let builder = export.validate(error_builder)?;
            let predicate = export.predicate().clone();
            let origin = *export.origin();

            if let Some(arity) = builder.expected_arity() {
                Self::validate_arity(&mut self.arities, predicate, arity, origin, error_builder);
            }
        }

        self.validate_global_properties(error_builder)?;

        Some(())
    }
}

/// Builder for [Program]s
#[derive(Debug, Default)]
pub struct ProgramBuilder {
    /// The constructed program
    program: Program,
}

impl ProgramBuilder {
    /// Finish building and return a [Program].
    pub fn finalize(self) -> Program {
        self.program
    }

    /// Add a [Rule].
    pub fn add_rule(&mut self, rule: Rule) {
        self.program.rules.push(rule)
    }

    /// Add a [Fact].
    pub fn add_fact(&mut self, fact: Fact) {
        self.program.facts.push(fact);
    }

    /// Add a [ImportDirective].
    pub fn add_import(&mut self, import: ImportDirective) {
        self.program.imports.push(import);
    }

    /// Add a [ExportDirective].
    pub fn add_export(&mut self, export: ExportDirective) {
        self.program.exports.push(export);
    }

    /// Add a [Output].
    pub fn add_output(&mut self, output: Output) {
        self.program.outputs.push(output);
    }

    /// Add a parameter declaration to the program
    pub fn add_parameter_declaration(&mut self, decl: ParameterDeclaration) {
        self.program.parameters.push(decl);
    }

    /// Validate the program that is being built
    pub fn validate(
        &mut self,
        external_paramters: &HashMap<GlobalVariable, GroundTerm>,
        error_builder: &mut ValidationErrorBuilder,
    ) -> Option<()> {
        self.program.validate(external_paramters, error_builder)
    }
}
