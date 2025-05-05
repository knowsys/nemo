//! This module defines [ProgramState].

use std::{fmt::Display, ops::Range};

use crate::model::{
    components::{
        atom::Atom, component_iterator, component_iterator_mut, rule::Rule, ComponentBehavior,
        ComponentIdentity, IterableComponent, ProgramComponent,
    },
    origin::Origin,
};

use super::id::ProgramComponentId;

trait Spanned {
    fn extend(&mut self);

    fn contains(&self, commit: usize) -> bool;

    fn component(&self) -> &dyn ProgramComponent;

    fn component_mut(&mut self) -> &mut dyn ProgramComponent;
}

#[derive(Debug)]
pub struct StatementSpan<Statement>
where
    Statement: ProgramComponent,
{
    statement: Statement,
    span: Range<usize>,
}

impl<Statement> StatementSpan<Statement>
where
    Statement: ProgramComponent,
{
    pub fn new(statement: Statement, start: usize) -> Self {
        Self {
            statement,
            span: start..(start + 1),
        }
    }

    pub fn statement(&self) -> &Statement {
        &self.statement
    }

    pub fn filter<'a, Iter>(iterator: Iter, commit: usize) -> impl Iterator<Item = &'a Statement>
    where
        Statement: 'a,
        Iter: Iterator<Item = &'a StatementSpan<Statement>>,
    {
        iterator
            .filter(move |element| element.contains(commit))
            .map(|element| &element.statement)
    }

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

///
#[derive(Debug)]
pub struct ProgramState {
    origin: Origin,

    rules: Vec<StatementSpan<Rule>>,
    facts: Vec<StatementSpan<Atom>>,

    current_commit: usize,
}

impl Display for ProgramState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProgramState {
    pub fn new() -> Self {
        Self {
            origin: Origin::default(),
            rules: Vec::default(),
            facts: Vec::default(),
            current_commit: 0,
        }
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

    pub fn add_fact(&mut self, fact: Atom) {
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

    pub fn facts(&self) -> impl Iterator<Item = &Atom> {
        StatementSpan::filter(self.facts.iter(), self.current_commit)
    }

    fn facts_mut(&mut self) -> impl Iterator<Item = &mut Atom> {
        StatementSpan::filter_mut(self.facts.iter_mut(), self.current_commit)
    }
}

impl ComponentBehavior for ProgramState {
    fn kind(&self) -> crate::model::components::ProgramComponentKind {
        todo!()
    }

    fn validate(&self) -> Result<(), crate::model::components::NewValidationError> {
        todo!()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        todo!()
    }
}

impl ComponentIdentity for ProgramState {
    fn id(&self) -> ProgramComponentId {
        ProgramComponentId::UNASSIGNED
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
