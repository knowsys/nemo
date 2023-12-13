//! This module defines the structures for evaluating functions on column data.

use std::collections::HashMap;

use crate::{
    datavalues::{AnyDataValue, DataValue},
    error::Error,
    function::tree::FunctionLeaf,
    tabular::operations::OperationColumnMarker,
};

use super::{
    definitions::{BinaryFunction, BinaryFunctionEnum, UnaryFunction, UnaryFunctionEnum},
    tree::FunctionTree,
};

/// Upon calling the evaluation method of [StackProgram],
/// it will receive a vector of values.
/// This number indexes into that vector.
pub(crate) type StackReferenceIndex = usize;

/// A value pushed onto the evaluation stack of [StackProgram]
#[derive(Debug, Clone)]
pub(crate) enum StackValue {
    /// A constant value on the stack
    Constant(AnyDataValue),
    /// A reference to an outside value that will be provided upon calling the evaluate method of [StackProgram].
    /// Contains an index which identifies the value within a slice.
    Reference(StackReferenceIndex),
    /// A reference to an ouside value that will be provided upon calling the evaluate method of [StackProgram]
    /// In contrast to [StackValue::Reference] this value will be given directly.
    This,
}

/// Operation performed in a [StackProgram]
#[derive(Debug, Clone)]
pub(crate) enum StackOperation {
    /// Push the given value onto the stack.
    Push(StackValue),
    /// Evaluate the given unary function on the top element in the stack.
    UnaryFunction(UnaryFunctionEnum),
    /// Evaluate the given binary function on the top two elements in the stack.
    BinaryFunction(BinaryFunctionEnum),
}

/// TODO: Check doc reference
/// Representation of a [FunctionTree][super::tree::FunctionTree] as a stack program
#[derive(Debug, Clone)]
pub(crate) struct StackProgram {
    size: usize,
    instructions: Vec<StackOperation>,
}

impl StackProgram {
    /// Constructs a new [`StackProgram`] from a list of [`StackOperation`].
    /// Checks, that all stack operations have sufficient arguments, and the stack height
    /// at the end of the computation is exactly 1.
    /// Returns an error if these conditions are not met.
    fn new(instructions: Vec<StackOperation>) -> Result<Self, Error> {
        let mut max_height = 0;
        let mut current_height = 0;

        for instruction in instructions.iter() {
            match instruction {
                StackOperation::Push(_) => {
                    current_height += 1;
                }
                StackOperation::UnaryFunction(_) => {
                    if current_height == 0 {
                        return Err(Error::MalformedStackProgram);
                    }
                }
                StackOperation::BinaryFunction(_) => {
                    if current_height <= 1 {
                        return Err(Error::MalformedStackProgram);
                    }

                    current_height -= 1;
                }
            }

            max_height = std::cmp::max(current_height, max_height);
        }

        if current_height != 1 {
            return Err(Error::MalformedStackProgram);
        }

        Ok(Self {
            size: max_height,
            instructions,
        })
    }

    /// Construct a [StackProgram] from [FunctionTree].
    pub fn from_function_tree(
        tree: &FunctionTree<OperationColumnMarker>,
        reference_map: &HashMap<OperationColumnMarker, usize>,
        this: Option<OperationColumnMarker>,
    ) -> StackProgram {
        fn build_operations(
            term: &FunctionTree<OperationColumnMarker>,
            this: &Option<OperationColumnMarker>,
            reference_map: &HashMap<OperationColumnMarker, usize>,
            operations: &mut Vec<StackOperation>,
        ) {
            match term {
                FunctionTree::Leaf(leaf) => operations.push(StackOperation::Push(match leaf {
                    FunctionLeaf::Constant(constant) => StackValue::Constant(constant.clone()),
                    FunctionLeaf::Reference(reference) => {
                        if Some(*reference) == *this {
                            StackValue::This
                        } else {
                            StackValue::Reference(
                                *reference_map
                                    .get(reference)
                                    .expect("Referenced column must exist in the reference map"),
                            )
                        }
                    }
                })),
                FunctionTree::Unary(function, sub) => {
                    build_operations(sub, this, reference_map, operations);

                    operations.push(StackOperation::UnaryFunction(function.clone()));
                }
                FunctionTree::Binary {
                    function,
                    left,
                    right,
                } => {
                    build_operations(left, this, reference_map, operations);
                    build_operations(right, this, reference_map, operations);

                    operations.push(StackOperation::BinaryFunction(function.clone()));
                }
            }
        }

        let mut term_operations = Vec::new();
        build_operations(tree, &this, reference_map, &mut term_operations);
        Self::new(term_operations).expect("Compilation produces only valid stack programs.")
    }

    /// Evaluate the stack program and return the result.
    /// Returns `None` if some function could not be evaluated.
    fn evaluate(
        &self,
        referenced_values: &[AnyDataValue],
        this: Option<AnyDataValue>,
    ) -> Option<AnyDataValue> {
        let mut stack = Vec::<AnyDataValue>::with_capacity(self.size);

        for instruction in self.instructions.iter() {
            match instruction {
                StackOperation::Push(stack_value) => match stack_value {
                    StackValue::Constant(any_value) => stack.push(any_value.clone()),
                    StackValue::Reference(reference) => {
                        stack.push(referenced_values[*reference].clone())
                    }
                    StackValue::This => stack.push(this.clone()?),
                },
                StackOperation::UnaryFunction(function) => {
                    let input = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");

                    stack.push(function.evaluate(input)?);
                }
                StackOperation::BinaryFunction(function) => {
                    let second_input = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");
                    let first_input = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");

                    stack.push(function.evaluate(first_input, second_input)?);
                }
            }
        }

        Some(
            stack
                .pop()
                .expect("The final value is on the stack, since this program is valid."),
        )
    }

    /// Evaluate the stack program and return the result.
    /// Returns `None` if some function could not be evaluated.
    pub fn evaluate_data(&self, referenced_values: &[AnyDataValue]) -> Option<AnyDataValue> {
        self.evaluate(referenced_values, None)
    }

    /// Evaluate the stack program and return the result.
    /// This function assumes that the result will be a boolean.
    /// Returns `None` if some function could not be evaluated.
    pub fn evaluate_bool(
        &self,
        referenced_values: &[AnyDataValue],
        this: AnyDataValue,
    ) -> Option<bool> {
        let result = self.evaluate(referenced_values, Some(this))?;
        result.to_boolean()
    }
}
