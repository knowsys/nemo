//! This module defines [ProgramState].

use std::{fmt::Write, ops::Range};

use crate::rule_model::{
    components::{
        fact::Fact,
        import_export::{ExportDirective, ImportDirective},
        output::Output,
        parameter::ParameterDeclaration,
        rule::Rule,
        IterableComponent, ProgramComponent,
    },
    program::{Program, ProgramWrite},
};

use super::id::ProgramComponentId;

/// Trait implemented by by each instantiation of
/// [StatementSpan], which associates a nemo statement
/// with a span.
trait Spanned {
    /// Extend span to include the given commit.
    fn extend(&mut self, commit: usize);

    /// Restrict the span to end at the provided commit
    fn restrict(&mut self, commit: usize);

    /// Check whether this statement is valid for the given commid id.
    fn contains(&self, commit: usize) -> bool;

    /// Return a reference to the underlying statement as a [ProgramComponent].
    fn component(&self) -> &dyn ProgramComponent;

    /// Return a mutable reference to the underlying statement as a [ProgramComponent].
    fn component_mut(&mut self) -> &mut dyn ProgramComponent;
}

/// Accociates a [ProgramComponent] (top-level components, i.e. statements)
/// with a span -- a numerical range -- in which they are considered valid
#[derive(Debug)]
struct StatementSpan<Statement>
where
    Statement: ProgramComponent,
{
    /// The statement
    statement: Statement,
    /// Span indicating its validity
    span: Range<usize>,
}

impl<Statement> StatementSpan<Statement>
where
    Statement: ProgramComponent,
{
    /// Create a new [StatementSpan]
    /// that is valid for one step from the given starting point.
    pub fn new(statement: Statement, start: usize) -> Self {
        Self {
            statement,
            span: start..(start + 1),
        }
    }

    /// Given an iterator over statements and a target commit,
    /// return all statements that are valid in this commit.
    pub fn filter<'a, Iter>(iterator: Iter, commit: usize) -> impl Iterator<Item = &'a Statement>
    where
        Statement: 'a,
        Iter: Iterator<Item = &'a StatementSpan<Statement>>,
    {
        iterator
            .filter(move |element| element.contains(commit))
            .map(|element| &element.statement)
    }

    /// Given an iterator over statements and a target commit,
    /// return all statements that are valid in this commit.
    pub fn filter_owned<Iter>(iterator: Iter, commit: usize) -> impl Iterator<Item = Statement>
    where
        Iter: Iterator<Item = StatementSpan<Statement>>,
    {
        iterator
            .filter(move |element| element.contains(commit))
            .map(|element| element.statement)
    }
}

impl<Statement> Spanned for StatementSpan<Statement>
where
    Statement: ProgramComponent,
{
    fn extend(&mut self, commit: usize) {
        self.span.end = commit + 1;
    }

    fn restrict(&mut self, commit: usize) {
        self.span.end = commit;
    }

    fn contains(&self, commit: usize) -> bool {
        self.span.contains(&commit)
    }

    fn component(&self) -> &dyn ProgramComponent {
        &self.statement
    }

    fn component_mut(&mut self) -> &mut dyn ProgramComponent {
        &mut self.statement
    }
}

/// Types of statement (top-level [ProgramComponent]s)
/// whose validity can be extended within a [ProgramState]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ExtendStatementKind {
    /// Rules
    Rule,
    /// Facts
    Fact,
    /// Imports
    Import,
    /// Exports
    Export,
    /// Outputs
    Output,
    /// Parameters
    Parameter,
    /// All except the following
    Except(&'static Self),
    /// All from the provided list
    AllOf(&'static [Self]),
    /// All statements
    All,
}

impl ExtendStatementKind {
    /// Check whether a particular [ExtendStatementKind] is included.
    pub fn includes(&self, kind: Self) -> bool {
        if *self == kind || matches!(self, Self::All) {
            return true;
        }

        if let Self::Except(except) = self {
            return kind != **except;
        }

        if let Self::AllOf(list) = self {
            return list.contains(&kind);
        }

        false
    }
}

/// Used to decide which types of statements
/// to keep when commiting a new [ProgramState].
#[derive(Debug, Copy, Clone)]
pub enum ExtendStatementValidity {
    /// Only keep the provided types of statements,
    /// delete the rest
    Keep(ExtendStatementKind),
    /// Only delete the provided types of statements,
    /// keep the rest
    Delete(ExtendStatementKind),
}

impl Default for ExtendStatementValidity {
    fn default() -> Self {
        ExtendStatementValidity::Keep(ExtendStatementKind::All)
    }
}

impl ExtendStatementValidity {
    /// Check whether to keep a given [ExtendStatementKind].
    pub fn keep(&self, kind: ExtendStatementKind) -> bool {
        match self {
            ExtendStatementValidity::Keep(keep) => keep.includes(kind),
            ExtendStatementValidity::Delete(delete) => !delete.includes(kind),
        }
    }
}

/// Holds multiple states of a [crate::rule_model::program::Program]
/// where statements are only valid for a certain range of
/// transformation steps, called commits.
#[derive(Debug)]
pub struct ProgramState {
    /// Rules
    rules: Vec<StatementSpan<Rule>>,
    /// Facts
    facts: Vec<StatementSpan<Fact>>,
    /// Imports
    imports: Vec<StatementSpan<ImportDirective>>,
    /// Exports
    exports: Vec<StatementSpan<ExportDirective>>,
    /// Outputs
    outputs: Vec<StatementSpan<Output>>,
    /// Parameters
    parameters: Vec<StatementSpan<ParameterDeclaration>>,

    /// Number of the current commit
    current_commit: usize,
}

impl Default for ProgramState {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgramState {
    /// Create a new [ProgramState].
    pub fn new() -> Self {
        Self {
            rules: Vec::default(),
            facts: Vec::default(),
            imports: Vec::default(),
            exports: Vec::default(),
            outputs: Vec::default(),
            parameters: Vec::default(),
            current_commit: 0,
        }
    }

    /// Translate an iterator over any type implementing [Spanned]
    /// to an iterator over [Spanned] trait objects.
    fn iterator_spanned<'a, Component, Iter>(
        iterator: Iter,
    ) -> impl Iterator<Item = &'a dyn Spanned>
    where
        Component: ProgramComponent + 'a,
        Iter: Iterator<Item = &'a StatementSpan<Component>>,
    {
        iterator.map(|component| {
            let component: &dyn Spanned = component;
            component
        })
    }

    /// Translate an iterator over any type implementing [Spanned]
    /// to an iterator over [Spanned] trait objects.
    fn iterator_spanned_mut<'a, Component, Iter>(
        iterator: Iter,
    ) -> impl Iterator<Item = &'a mut dyn Spanned>
    where
        Component: ProgramComponent + 'a,
        Iter: Iterator<Item = &'a mut StatementSpan<Component>>,
    {
        iterator.map(|component| {
            let component: &mut dyn Spanned = component;
            component
        })
    }

    /// Return an iterator over every [Spanned] program component.
    fn spans(&self) -> impl Iterator<Item = &dyn Spanned> {
        let rules_iterator = Self::iterator_spanned(self.rules.iter());
        let facts_iterator = Self::iterator_spanned(self.facts.iter());
        let imports_iterator = Self::iterator_spanned(self.imports.iter());
        let exports_iterator = Self::iterator_spanned(self.exports.iter());
        let outputs_iterator = Self::iterator_spanned(self.outputs.iter());
        let parameters_iterator = Self::iterator_spanned(self.parameters.iter());

        rules_iterator
            .chain(facts_iterator)
            .chain(imports_iterator)
            .chain(exports_iterator)
            .chain(outputs_iterator)
            .chain(parameters_iterator)
    }

    /// Return an iterator over every [Spanned] program component.
    fn spans_mut(&mut self) -> impl Iterator<Item = &mut dyn Spanned> {
        let rules_iterator = Self::iterator_spanned_mut(self.rules.iter_mut());
        let facts_iterator = Self::iterator_spanned_mut(self.facts.iter_mut());
        let imports_iterator = Self::iterator_spanned_mut(self.imports.iter_mut());
        let exports_iterator = Self::iterator_spanned_mut(self.exports.iter_mut());
        let outputs_iterator = Self::iterator_spanned_mut(self.outputs.iter_mut());
        let parameters_iterator = Self::iterator_spanned_mut(self.parameters.iter_mut());

        rules_iterator
            .chain(facts_iterator)
            .chain(imports_iterator)
            .chain(exports_iterator)
            .chain(outputs_iterator)
            .chain(parameters_iterator)
    }

    /// Find a [Spanned] component from its id
    fn find_spanned_mut(&mut self, id: ProgramComponentId) -> Option<&mut dyn Spanned> {
        self.spans_mut()
            .find(|spanned| spanned.component().id() == id)
    }

    /// Prepare the next commit.
    pub fn prepare(&mut self, extend: ExtendStatementValidity) {
        let last_commit = self.current_commit;
        self.current_commit += 1;

        if extend.keep(ExtendStatementKind::Rule) {
            for rule in &mut self.rules {
                if rule.contains(last_commit) {
                    rule.extend(self.current_commit);
                }
            }
        }

        if extend.keep(ExtendStatementKind::Fact) {
            for fact in &mut self.facts {
                if fact.contains(last_commit) {
                    fact.extend(self.current_commit);
                }
            }
        }

        if extend.keep(ExtendStatementKind::Import) {
            for import in &mut self.imports {
                if import.contains(last_commit) {
                    import.extend(self.current_commit);
                }
            }
        }

        if extend.keep(ExtendStatementKind::Export) {
            for export in &mut self.exports {
                if export.contains(last_commit) {
                    export.extend(self.current_commit);
                }
            }
        }

        if extend.keep(ExtendStatementKind::Output) {
            for output in &mut self.outputs {
                if output.contains(last_commit) {
                    output.extend(self.current_commit);
                }
            }
        }

        if extend.keep(ExtendStatementKind::Parameter) {
            for parameter in &mut self.parameters {
                if parameter.contains(last_commit) {
                    parameter.extend(self.current_commit);
                }
            }
        }
    }
}

impl ProgramState {
    /// Delete a program component.
    pub fn delete(&mut self, id: ProgramComponentId) {
        let commit = self.current_commit;

        if let Some(component) = self.find_spanned_mut(id) {
            component.restrict(commit);
        }
    }

    /// Keep a program component.
    pub fn keep(&mut self, id: ProgramComponentId) {
        let commit = self.current_commit;

        if let Some(component) = self.find_spanned_mut(id) {
            component.extend(commit);
        }
    }

    /// Add a [Rule] valid for the current commit.
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules
            .push(StatementSpan::new(rule, self.current_commit));
    }

    /// Add a [Fact] valid for the current commit.
    pub fn add_fact(&mut self, fact: Fact) {
        self.facts
            .push(StatementSpan::new(fact, self.current_commit));
    }

    /// Add a [ImportDirective] valid for the current commit.
    pub fn add_import(&mut self, import: ImportDirective) {
        self.imports
            .push(StatementSpan::new(import, self.current_commit));
    }

    /// Add a [ExportDirective] valid for the current commit.
    pub fn add_export(&mut self, export: ExportDirective) {
        self.exports
            .push(StatementSpan::new(export, self.current_commit));
    }

    /// Add a [Output] valid for the current commit.
    pub fn add_output(&mut self, output: Output) {
        self.outputs
            .push(StatementSpan::new(output, self.current_commit));
    }

    /// Add a [ParameterDeclaration] valid for the current commit.
    pub fn add_parameter(&mut self, parameter: ParameterDeclaration) {
        self.parameters
            .push(StatementSpan::new(parameter, self.current_commit));
    }
}

impl ProgramState {
    /// Return an iterator over all active [Rule]s.
    pub fn rules(&self) -> impl Iterator<Item = &Rule> {
        StatementSpan::filter(self.rules.iter(), self.current_commit)
    }

    /// Return an iterator over all active [Fact]s.
    pub fn facts(&self) -> impl Iterator<Item = &Fact> {
        StatementSpan::filter(self.facts.iter(), self.current_commit)
    }

    /// Return an iterator over all active [ImportDirective]s.
    pub fn imports(&self) -> impl Iterator<Item = &ImportDirective> {
        StatementSpan::filter(self.imports.iter(), self.current_commit)
    }

    /// Return an iterator over all active [ExportDirective]s.
    pub fn exports(&self) -> impl Iterator<Item = &ExportDirective> {
        StatementSpan::filter(self.exports.iter(), self.current_commit)
    }

    /// Return an iterator over all active [Output]s.
    pub fn outputs(&self) -> impl Iterator<Item = &Output> {
        StatementSpan::filter(self.outputs.iter(), self.current_commit)
    }

    /// Return an iterator over all active [ParameterDeclaration]s.
    pub fn parameters(&self) -> impl Iterator<Item = &ParameterDeclaration> {
        StatementSpan::filter(self.parameters.iter(), self.current_commit)
    }
}

impl ProgramState {
    /// Return the current [Program].
    pub fn finalize(self) -> Program {
        let mut program = Program::default();

        let commit = self.current_commit;

        for rule in StatementSpan::filter_owned(self.rules.into_iter(), commit) {
            program.add_rule(rule);
        }

        for fact in StatementSpan::filter_owned(self.facts.into_iter(), commit) {
            program.add_fact(fact);
        }

        for import in StatementSpan::filter_owned(self.imports.into_iter(), commit) {
            program.add_import(import);
        }

        for export in StatementSpan::filter_owned(self.exports.into_iter(), commit) {
            program.add_export(export);
        }

        for output in StatementSpan::filter_owned(self.outputs.into_iter(), commit) {
            program.add_output(output);
        }

        for parameter in StatementSpan::filter_owned(self.parameters.into_iter(), commit) {
            program.add_parameter_declaration(parameter);
        }

        program
    }
}

impl IterableComponent for ProgramState {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let iterator = self.spans().map(|spanned| spanned.component());
        Box::new(iterator)
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let iterator = self.spans_mut().map(|spanned| spanned.component_mut());
        Box::new(iterator)
    }
}

impl std::fmt::Display for ProgramState {
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
