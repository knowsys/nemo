//! This module defines [ProgramState].

use std::{fmt::Display, ops::Range};

use crate::rule_model::{
    components::{
        component_iterator, component_iterator_mut,
        fact::Fact,
        import_export::{ExportDirective, ImportDirective},
        output::Output,
        rule::Rule,
        ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
        ProgramComponentKind,
    },
    origin::Origin,
};

use super::id::ProgramComponentId;

/// Trait implemented by by each instantiation of
/// [StatementSpan], which associates a nemo statement
/// with a span.
trait Spanned {
    /// Extend span by one.
    fn extend(&mut self);

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

    /// Return the underlying statement.
    pub fn statement(&self) -> &Statement {
        &self.statement
    }

    /// Given an itereator over statements and a target commit,
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

    /// Given a mutable itereator over statements and a target commit,
    /// return all statements that are valid in this commit.
    pub fn filter_mut<'a, Iter>(
        iterator: Iter,
        commit: usize,
    ) -> impl Iterator<Item = &'a mut Statement>
    where
        Statement: 'a,
        Iter: Iterator<Item = &'a mut StatementSpan<Statement>>,
    {
        iterator
            .filter(move |element| element.contains(commit))
            .map(|element| &mut element.statement)
    }
}

impl<Statement> Spanned for StatementSpan<Statement>
where
    Statement: ProgramComponent,
{
    fn extend(&mut self) {
        self.span.end += 1;
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
#[derive(Debug, Copy, Clone)]
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
    /// All from the provided list
    AllOf(&'static [Self]),
    /// All statements
    All,
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

/// Holds multiple states of a [crate::rule_model::program::Program]
/// where statements are only valid for a certain range of
/// transformation steps, called commits.
#[derive(Debug)]
pub struct ProgramState {
    /// Origin of this component
    /// (Only needed for the implementation of [ProgramComponent])
    origin: Origin,

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

    /// Number of the current commit
    current_commit: usize,
}

impl Display for ProgramState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProgramState {
    /// Create a new [ProgramState].
    pub fn new() -> Self {
        Self {
            origin: Origin::default(),
            rules: Vec::default(),
            facts: Vec::default(),
            imports: Vec::default(),
            exports: Vec::default(),
            outputs: Vec::default(),
            current_commit: 0,
        }
    }

    ///
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

    fn spans_mut(&mut self) -> impl Iterator<Item = &mut dyn Spanned> {
        self.rules
            .iter_mut()
            .map(|rule| {
                let rule: &mut dyn Spanned = rule;
                rule
            })
            .chain(self.facts.iter_mut().map(|fact| {
                let fact: &mut dyn Spanned = fact;
                fact
            }))
    }

    pub fn commit(&mut self, extend: bool) {
        self.current_commit += 1;

        if extend {
            for statement in self.spans_mut() {
                statement.extend();
            }
        }
    }
}

impl ProgramState {
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules
            .push(StatementSpan::new(rule, self.current_commit));
    }

    pub fn add_fact(&mut self, fact: Fact) {
        self.facts
            .push(StatementSpan::new(fact, self.current_commit));
    }
}

impl ProgramState {
    pub fn rules(&self) -> impl Iterator<Item = &Rule> {
        StatementSpan::filter(self.rules.iter(), self.current_commit)
    }

    fn rules_mut(&mut self) -> impl Iterator<Item = &mut Rule> {
        StatementSpan::filter_mut(self.rules.iter_mut(), self.current_commit)
    }

    pub fn facts(&self) -> impl Iterator<Item = &Fact> {
        StatementSpan::filter(self.facts.iter(), self.current_commit)
    }

    fn facts_mut(&mut self) -> impl Iterator<Item = &mut Fact> {
        StatementSpan::filter_mut(self.facts.iter_mut(), self.current_commit)
    }
}

impl ComponentBehavior for ProgramState {
    fn kind(&self) -> ProgramComponentKind {
        todo!()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        todo!()
    }

    fn validate(
        &self,
        builder: &mut crate::rule_model::error::ValidationErrorBuilder,
    ) -> Option<()> {
        todo!()
    }
}

impl ComponentIdentity for ProgramState {
    fn id(&self) -> ProgramComponentId {
        ProgramComponentId::unassigned()
    }

    fn set_id(&mut self, _id: ProgramComponentId) {}

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin
    }
}

impl IterableComponent for ProgramState {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let rules = component_iterator(self.rules());
        let facts = component_iterator(self.facts());

        Box::new(rules.chain(facts))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let rules = StatementSpan::filter_mut(self.rules.iter_mut(), self.current_commit);
        let facts = StatementSpan::filter_mut(self.facts.iter_mut(), self.current_commit);

        let rules = component_iterator_mut(rules);
        let facts = component_iterator_mut(facts);

        Box::new(rules.chain(facts))
    }
}
