//! This module will define the manager object

/// Usual id struct
#[derive(Debug, Copy, Clone)]
pub struct ProgramComponentId(pub usize);

/// Big manager object
#[derive(Debug)]
pub struct BigManager {
    x: Vec<usize>,
}

impl BigManager {
    pub fn begin_transformation(&mut self, name: &str) {
        todo!()
    }

    pub fn end_transformation(&mut self) {
        todo!()
    }

    pub fn current_program(&mut self) {
        // -> Program
        // return current_program
        todo!()
    }

    pub fn rules(&self) {
        // -> Iterator<Item = Rule>
        todo!()
    }
}
