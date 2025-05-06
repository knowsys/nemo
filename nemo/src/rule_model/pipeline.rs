//! This module defines [ProgramPipeline].

use commit::ProgramCommit;
use id::ProgramComponentId;
use state::{ExtendStatementValidity, ProgramState};

use super::components::{
    atom::Atom,
    fact::Fact,
    import_export::{ExportDirective, ImportDirective},
    output::Output,
    parameter::ParameterDeclaration,
    rule::Rule,
    IterableComponent, ProgramComponent,
};

pub mod commit;
pub mod id;
pub mod state;
pub mod transformation;

/// Big manager object
#[derive(Debug)]
pub struct ProgramPipeline {
    /// Programs given as strings
    _inputs: Vec<String>,

    /// Contains state of the program at every commit
    state: ProgramState,

    /// Current id
    current_id: ProgramComponentId,
}

impl ProgramPipeline {
    /// Create a new [ProgramPipeline].
    pub fn new() -> Self {
        Self {
            _inputs: Vec::default(),
            state: ProgramState::new(),
            current_id: ProgramComponentId::start(),
        }
    }
}

impl ProgramPipeline {
    /// Search for the [ProgramComponent] with a given [ProgramComponentId].
    ///
    /// Returns `None` if there is no  [ProgramComponent] with that [ProgramComponentId].
    fn find_child_component(
        component: &dyn IterableComponent,
        id: ProgramComponentId,
    ) -> Option<&dyn ProgramComponent> {
        let mut child_iterator = component.children();
        let mut previous_component = child_iterator.next()?;
        while let Some(current_component) = child_iterator.next() {
            if current_component.id() > id {
                break;
            }

            previous_component = current_component;
        }

        if previous_component.id() <= id {
            Self::find_child_component(previous_component, id)
        } else {
            None
        }
    }

    /// Search for the [ProgramComponent] with a given [ProgramComponentId].
    ///
    /// Returns `None` if there is no  [ProgramComponent] with that [ProgramComponentId].
    fn find_component(&self, id: ProgramComponentId) -> Option<&dyn ProgramComponent> {
        Self::find_child_component(&self.state, id)
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
}

impl ProgramPipeline {
    /// Assigns a [ProgramComponentId] to every (sub) [ProgramComponent].
    fn register_component(&mut self, component: &mut dyn ProgramComponent) -> ProgramComponentId {
        let id = self.current_id.increment();
        component.set_id(id);

        for sub in component.children_mut() {
            self.register_component(sub);
        }

        id
    }

    /// Add a [Rule] to the current program.
    pub fn add_rule(&mut self, mut rule: Rule) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut rule;
        let id = self.register_component(component);

        self.state.add_rule(rule);

        id
    }

    /// Add a [Fact] to the current program.
    pub fn add_fact(&mut self, mut fact: Fact) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut fact;
        let id = self.register_component(component);

        self.state.add_fact(fact);

        id
    }

    /// Add a [ImportDirective] to the current program.
    pub fn add_import(&mut self, mut import: ImportDirective) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut import;
        let id = self.register_component(component);

        self.state.add_import(import);

        id
    }

    /// Add a [ImportDirective] to the current program.
    pub fn add_export(&mut self, mut export: ExportDirective) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut export;
        let id = self.register_component(component);

        self.state.add_export(export);

        id
    }

    /// Add a [Output] to the current program.
    pub fn add_output(&mut self, mut output: Output) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut output;
        let id = self.register_component(component);

        self.state.add_output(output);

        id
    }

    /// Add a [ParameterDeclaration] to the current program.
    pub fn add_parameter(&mut self, mut parameter: ParameterDeclaration) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut parameter;
        let id = self.register_component(component);

        self.state.add_parameter(parameter);

        id
    }

    /// Prepare a new commit.
    pub fn prepare(&mut self, extend: ExtendStatementValidity) {
        self.state.prepare(extend);
    }

    /// Apply a commit.
    pub fn commit(&mut self, commit: ProgramCommit) {
        for id in commit.deleted {
            self.state.delete(id);
        }

        for id in commit.keep {
            self.state.keep(id);
        }

        for rule in commit.rules {
            self.add_rule(rule);
        }

        for fact in commit.facts {
            self.add_fact(fact);
        }

        for import in commit.imports {
            self.add_import(import);
        }

        for export in commit.exports {
            self.add_export(export);
        }

        for output in commit.outputs {
            self.add_output(output);
        }

        for parameter in commit.parameters {
            self.add_parameter(parameter);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::components::{
        atom::Atom, literal::Literal, rule::Rule, ComponentIdentity,
    };

    use super::ProgramPipeline;

    #[test]
    fn find_atom() {
        let mut pipeline = ProgramPipeline::new();

        let head_atom = Atom::new("head".into(), vec![]);
        let body_atom = Atom::new("body".into(), vec![]);

        let rule = Rule::new(vec![head_atom], vec![Literal::Positive(body_atom)]);
        let rule_id = pipeline.add_rule(rule);
        let rule = pipeline.rule_by_id(rule_id).unwrap();

        let head = &rule.head()[0];
        let head = pipeline.atom_by_id(head.id()).unwrap();

        assert_eq!(head.predicate().to_string(), "head".to_owned());
    }
}
