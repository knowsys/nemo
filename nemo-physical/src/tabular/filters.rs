//! This module defines [FilterTransformPattern].

use std::collections::HashMap;

use crate::function::{evaluation::StackProgram, tree::FunctionTree};

use super::buffer::tuple_buffer::TransformPosition;

/// A pattern that can be used to filter and transform the tuples in a [super::buffer::TupleBuffer].
#[derive(Debug)]
pub struct FilterTransformPattern {
    pub(crate) filter: StackProgram,
    pub(crate) transformations: Vec<TransformPosition>,
}

impl FilterTransformPattern {
    /// Construct a new [FilterTransformPattern] from a filter and transformations.
    pub fn new(filter: FunctionTree<usize>, transformations: Vec<TransformPosition>) -> Self {
        let reference_map = filter
            .references()
            .into_iter()
            .map(|position| (position, position))
            .collect::<HashMap<_, _>>();
        let filter_program = StackProgram::from_function_tree(&filter, &reference_map, None);
        Self {
            filter: filter_program,
            transformations,
        }
    }
}
