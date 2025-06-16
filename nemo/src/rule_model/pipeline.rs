//! This module defines [ProgramPipeline].

use std::{cell::UnsafeCell, rc::Rc};

use id::ProgramComponentId;

use crate::rule_model::{
    components::statement::Statement,
    pipeline::revision::ProgramRevision,
    programs::{program::Program, ProgramWrite},
};

use super::components::{atom::Atom, rule::Rule, IterableComponent, ProgramComponent};

pub mod commit;
pub mod id;
pub mod revision;
pub mod transformations;

/// Program Manager
///
/// Contains different versions of nemo programs.
#[derive(Debug, Clone, Default)]
pub struct ProgramPipeline {
    /// All statements that have been constructed.
    /// Some may only be valid within certain revisions.
    statements: Vec<Statement>,

    /// Collection of all revisions, i.e. version of nemo prorams
    revisions: Vec<ProgramRevision>,
}

impl ProgramPipeline {
    /// Create a new managed [ProgramPipeline].
    pub fn new() -> Rc<UnsafeCell<Self>> {
        Rc::new(UnsafeCell::new(Self::default()))
    }

    /// Search for the [ProgramComponent] with a given [ProgramComponentId]
    /// within the child components of `component`.
    ///
    /// Returns `None` if there is no [ProgramComponent] could be found.
    fn find_child_component(
        component: &dyn IterableComponent,
        id: ProgramComponentId,
    ) -> Option<&dyn ProgramComponent> {
        let mut child_iterator = component.children();
        let mut previous_component = child_iterator.next()?;
        for current_component in child_iterator {
            if current_component.id() > id {
                break;
            }

            previous_component = current_component;
        }

        if previous_component.id() == id {
            Some(previous_component)
        } else if previous_component.id() < id {
            Self::find_child_component(previous_component, id)
        } else {
            None
        }
    }

    /// Search for the [ProgramComponent] with a given [ProgramComponentId].
    ///
    /// Returns `None` if there is no  [ProgramComponent] with that [ProgramComponentId].
    pub fn find_component(&self, id: ProgramComponentId) -> Option<&dyn ProgramComponent> {
        let component_statement: &dyn ProgramComponent = self.statements.get(id.statement())?;
        if id.is_statement() {
            return Some(component_statement);
        }

        Self::find_child_component(component_statement, id)
    }

    /// Given a [ProgramComponentId] return the corresponding [Statement],
    ///
    /// # Panics
    /// Pancis if no statement with this id exists.
    pub fn statement(&self, id: ProgramComponentId) -> &Statement {
        &self.statements[id.statement()]
    }

    /// Given a [ProgramComponentId] return the corresponding [Rule],
    /// if it exists.
    pub fn rule_by_id(&self, id: ProgramComponentId) -> Option<&Rule> {
        self.find_component(id)
            .and_then(|component| component.try_as_ref())
    }

    /// Given a [ProgramComponentId] return the corresponding [Atom],
    /// if it exists.
    pub fn atom_by_id(&self, id: ProgramComponentId) -> Option<&Atom> {
        self.find_component(id)
            .and_then(|component| component.try_as_ref())
    }

    /// Assigns a [ProgramComponentId] to every (sub) [ProgramComponent].
    fn register_component(component: &mut dyn ProgramComponent, parent_id: ProgramComponentId) {
        for child in component.children_mut() {
            let child_id = parent_id.increment_component();
            child.set_id(child_id);

            Self::register_component(child, child_id)
        }
    }

    /// Assign a [ProgramComponentId] to the given statement and its child components.
    fn register_statement<Component: ProgramComponent>(
        &self,
        statement: &mut Component,
    ) -> ProgramComponentId {
        let id = ProgramComponentId::new_statement(self.statements.len());

        Self::register_component(statement, id);
        statement.set_id(id);

        id
    }

    /// Add a [Statement] to the given revision.
    fn new_statement<S>(&mut self, statement: S) -> ProgramComponentId
    where
        S: Into<Statement>,
    {
        let mut statement: Statement = statement.into();

        let id = self.register_statement(&mut statement);
        self.statements.push(statement);

        id
    }

    /// Return a reference to the [ProgramRevision]
    /// with the given number.
    ///
    /// # Panics
    /// Panics if no revision exists at that index.
    pub fn revision(&self, revision: usize) -> &ProgramRevision {
        &self.revisions[revision]
    }

    /// Add a new [ProgramRevision] to the pipeline
    /// via a [ProgramCommit] and returns a [ProgramHandle]
    pub(crate) fn new_revision(&mut self, revision: ProgramRevision) -> usize {
        let num_revisions = self.revisions.len();
        self.revisions.push(revision);

        num_revisions
    }

    /// Turn the last revision into a standalone [Program].
    pub fn finalize(self) -> Program {
        let mut program = Program::default();

        for statement in self.statements {
            program.add_statement(statement);
        }

        program
    }
}
