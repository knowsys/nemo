//! This module defines [FilterTransformPattern].

use std::collections::HashMap;

use crate::function::{evaluation::StackProgram, tree::FunctionTree};

use super::operations::OperationColumnMarker;

/// A filter or transformation applied to a position
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformPosition {
    pub(crate) position: usize,
    pub(crate) program: StackProgram,
}

impl TransformPosition {
    /// Construct a new transformation.
    pub fn new(position: usize, value: FunctionTree<OperationColumnMarker>) -> Self {
        let reference_map = value
            .references()
            .into_iter()
            .map(|position| (position, position.0))
            .collect::<HashMap<_, _>>();
        let program = StackProgram::from_function_tree(&value, &reference_map, None);

        Self { position, program }
    }
}

/// A pattern that can be used to filter and transform the tuples in a tuple buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterTransformPattern {
    pub(crate) filter: StackProgram,
    pub(crate) transformations: Vec<TransformPosition>,
}

impl FilterTransformPattern {
    /// Construct a new [FilterTransformPattern] from a filter and transformations.
    pub fn new(
        filter: FunctionTree<OperationColumnMarker>,
        transformations: Vec<TransformPosition>,
    ) -> Self {
        let reference_map = filter
            .references()
            .into_iter()
            .map(|position| (position, position.0))
            .collect::<HashMap<_, _>>();
        let filter_program = StackProgram::from_function_tree(&filter, &reference_map, None);

        Self {
            filter: filter_program,
            transformations,
        }
    }
}
