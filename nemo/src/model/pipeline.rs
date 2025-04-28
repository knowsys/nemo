//! This module defines [ProgramPipeline].

use std::collections::HashMap;

use id::ProgramComponentId;

use super::components::{atom::Atom, program::Program, rule::Rule, ProgramComponent};

pub mod id;

#[derive(Debug, Clone, Copy)]
struct StatementValidity {
    pub step: usize,
    pub count: usize,
}

#[derive(Debug)]
struct Statement<Component> {
    pub list: Vec<Component>,
    pub valid: StatementValidity,
}

/// Big manager object
#[derive(Debug)]
pub struct ProgramPipeline {
    program: Program,

    current_id: ProgramComponentId,
}

impl ProgramPipeline {
    /// Assigns a [ProgramComponentId] to every (sub) [ProgramComponent].
    fn register_component(&mut self, component: &mut dyn ProgramComponent) {
        component.set_id(self.current_id.increment());

        for sub in component.children_mut() {
            self.register_component(sub);
        }
    }

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
        let mut pass = false;
        while let Some(current_component) = child_iterator.next() {
            if current_component.id() > id {
                pass = true;
                break;
            }

            previous_component = current_component;
        }

        if pass {
            Self::find_child_component(previous_component, id)
        } else {
            None
        }
    }

    /// Search for the [ProgramComponent] with a given [ProgramComponentId].
    ///
    /// Returns `None` if there is no  [ProgramComponent] with that [ProgramComponentId].
    fn find_component(&self, id: ProgramComponentId) -> Option<&dyn ProgramComponent> {
        Self::find_child_component(&self.program, id)
    }

    // fn find_component(
    //     component: &dyn ProgramComponent,
    //     id: ProgramComponentId,
    // ) -> &dyn ProgramComponent {
    //     for sub in component.components(). {
    //         if sub.id() == id {
    //             return component;
    //         } else if sub.id() >
    //     }

    //     todo!()
    // }
}
