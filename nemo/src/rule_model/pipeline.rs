//! This module defines [ProgramPipeline].

use id::ProgramComponentId;
use state::ProgramState;

use crate::{error::Error, parser::Parser, rule_model::translation::ASTProgramTranslation};

use super::components::{atom::Atom, rule::Rule, ProgramComponent};

pub mod id;
pub mod state;

/// Big manager object
#[derive(Debug)]
pub struct ProgramPipeline {
    inputs: Vec<String>,

    state: ProgramState,

    current_id: ProgramComponentId,
}

impl ProgramPipeline {
    /// Create a new [ProgramPipeline].
    pub fn new() -> Self {
        Self {
            inputs: Vec::default(),
            state: ProgramState::new(),
            current_id: ProgramComponentId::start(),
        }
    }
}

impl ProgramPipeline {
    pub fn parse(&mut self, string: &str) -> Result<(), Error> {
        self.inputs.push(string.to_owned());

        let input = self.inputs.last().expect("msg");

        let ast = Parser::initialize(&input, String::default())
            .parse()
            .map_err(|_| Error::ProgramParseError)?;
        let program = ASTProgramTranslation::initialize(&input, String::default()).translate(&ast);

        todo!()
    }
}

impl ProgramPipeline {
    /// Search for the [ProgramComponent] with a given [ProgramComponentId]
    /// by traversing it's children.
    ///
    /// Returns `None` if there is no  [ProgramComponent] with that [ProgramComponentId].
    fn find_child_component(
        component: &dyn ProgramComponent,
        id: ProgramComponentId,
    ) -> Option<&dyn ProgramComponent> {
        if component.id() == id {
            return Some(component);
        }

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
