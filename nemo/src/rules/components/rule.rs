//! This module defines rules (example component)

use crate::rules::{
    manager::{BigManager, ProgramComponentId},
    origin::Origin,
};

/// Example of a program component
#[derive(Debug)]
pub struct Rule {
    origin: Origin,
    id: ProgramComponentId,

    head: Vec<usize>, // Placeholder
    body: Vec<usize>,
}

impl BigManager {
    pub fn assign_id(&mut self, rule: &mut Rule) {
        rule.id = ProgramComponentId(10);
    }
}
