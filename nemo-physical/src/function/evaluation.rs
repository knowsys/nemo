//! This module defines the structures for evaluating functions on column data.

use std::{collections::HashMap, fmt::Debug, hash::Hash};

use crate::{
    datatypes::storage_type_name::StorageTypeBitSet,
    datavalues::{AnyDataValue, DataValue},
    error::Error,
    function::tree::FunctionLeaf,
};

use super::{
    definitions::{
        BinaryFunction, BinaryFunctionEnum, FunctionTypePropagation, NaryFunction,
        NaryFunctionEnum, TernaryFunction, TernaryFunctionEnum, UnaryFunction, UnaryFunctionEnum,
    },
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
    /// Evaluate the given ternary function on the top three elements in the stack.
    TernaryFunction(TernaryFunctionEnum),
    /// Evaluate the given n-ary function on the top n elements in the stack.
    NaryFunction(NaryFunctionEnum, usize),
}

/// Representation of a [FunctionTree] as a stack program
#[derive(Debug, Clone)]
pub(crate) struct StackProgram {
    /// Maximmum size of the stack
    size: usize,
    /// List of instructions
    instructions: Vec<StackOperation>,
}

impl StackProgram {
    /// Constructs a new [StackProgram] from a list of [StackOperation].
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
                StackOperation::TernaryFunction(_) => {
                    if current_height <= 2 {
                        return Err(Error::MalformedStackProgram);
                    }

                    current_height -= 2;
                }
                StackOperation::NaryFunction(_, parameter_count) => {
                    if current_height < *parameter_count {
                        return Err(Error::MalformedStackProgram);
                    }

                    current_height -= parameter_count - 1;
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
    pub(crate) fn from_function_tree<ReferenceType: Hash + Eq + Debug + Clone>(
        tree: &FunctionTree<ReferenceType>,
        reference_map: &HashMap<ReferenceType, usize>,
        this: Option<ReferenceType>,
    ) -> StackProgram {
        fn build_operations<ReferenceType: Hash + Eq + Debug + Clone>(
            term: &FunctionTree<ReferenceType>,
            this: &Option<ReferenceType>,
            reference_map: &HashMap<ReferenceType, usize>,
            operations: &mut Vec<StackOperation>,
        ) {
            match term {
                FunctionTree::Leaf(leaf) => operations.push(StackOperation::Push(match leaf {
                    FunctionLeaf::Constant(constant) => StackValue::Constant(constant.clone()),
                    FunctionLeaf::Reference(reference) => {
                        if Some(reference) == this.as_ref() {
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

                    operations.push(StackOperation::UnaryFunction(*function));
                }
                FunctionTree::Binary {
                    function,
                    left,
                    right,
                } => {
                    build_operations(left, this, reference_map, operations);
                    build_operations(right, this, reference_map, operations);

                    operations.push(StackOperation::BinaryFunction(*function));
                }
                FunctionTree::Ternary {
                    function,
                    first,
                    second,
                    third,
                } => {
                    build_operations(first, this, reference_map, operations);
                    build_operations(second, this, reference_map, operations);
                    build_operations(third, this, reference_map, operations);

                    operations.push(StackOperation::TernaryFunction(*function));
                }
                FunctionTree::Nary {
                    function,
                    parameters,
                } => {
                    for parameter in parameters {
                        build_operations(parameter, this, reference_map, operations);
                    }

                    operations.push(StackOperation::NaryFunction(*function, parameters.len()))
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
                StackOperation::TernaryFunction(function) => {
                    let third_input = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");
                    let second_input = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");
                    let first_input = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");

                    stack.push(function.evaluate(first_input, second_input, third_input)?);
                }
                StackOperation::NaryFunction(function, parameter_count) => {
                    let mut inputs = Vec::new();
                    for _ in 0..*parameter_count {
                        inputs.push(
                            stack
                                .pop()
                                .expect("This program is valid, so the stack cannot be empty."),
                        );
                    }
                    inputs.reverse();

                    stack.push(function.evaluate(&inputs)?);
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
    ///
    /// Returns `None` if some function could not be evaluated.
    ///
    /// # Panics
    /// Panics if the [StackProgram] is not valid.
    pub(crate) fn evaluate_data(&self, referenced_values: &[AnyDataValue]) -> Option<AnyDataValue> {
        self.evaluate(referenced_values, None)
    }

    /// Evaluate the stack program and return the result.
    /// This function assumes that the result will be a boolean.
    ///
    /// Returns `None` if some function could not be evaluated.
    ///
    /// # Panics
    /// Panics if the [StackProgram] is not valid.
    pub(crate) fn evaluate_bool(
        &self,
        referenced_values: &[AnyDataValue],
        this: Option<AnyDataValue>,
    ) -> Option<bool> {
        let result = self.evaluate(referenced_values, this)?;
        result.to_boolean()
    }

    /// Computes the possible output type for a given set of possible input types.
    ///
    /// # Panics
    /// Panics if the [StackProgram] is not valid.
    pub(crate) fn type_propagation(
        &self,
        reference_types: &[StorageTypeBitSet],
        this: Option<StorageTypeBitSet>,
    ) -> StorageTypeBitSet {
        let mut stack = Vec::<StorageTypeBitSet>::with_capacity(self.size);

        for instruction in &self.instructions {
            let mut input_types = Vec::<StorageTypeBitSet>::new();

            let (pop, instruction_type_propagation) = match instruction {
                StackOperation::Push(stack_value) => (0, match stack_value {
                    StackValue::Constant(constant) => FunctionTypePropagation::KnownOutput(constant.value_domain().storage_type()),
                    StackValue::Reference(id) => FunctionTypePropagation::KnownOutput(reference_types[*id]),
                    StackValue::This => FunctionTypePropagation::KnownOutput(this.expect("If stackoperation references `this` it should be given as an argument to this function")),
                }),
                StackOperation::UnaryFunction(function) => {
                    (1, function.type_propagation())
                },
                StackOperation::BinaryFunction(function) => (2, function.type_propagation()),
                StackOperation::TernaryFunction(function) => (3, function.type_propagation()),
                StackOperation::NaryFunction(function, num_arguments) => (*num_arguments, function.type_propagation()),
            };

            for _ in 0..pop {
                input_types.push(
                    stack
                        .pop()
                        .expect("Well formed stack program should not pop from empty stack"),
                );
            }

            stack.push(instruction_type_propagation.propagate(&input_types));
        }

        stack
            .pop()
            .expect("The final value is on the stack, since this program is valid.")
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        datavalues::AnyDataValue, function::tree::FunctionTree,
        tabular::operations::OperationColumnMarker,
    };

    use super::StackProgram;

    type Function = FunctionTree<OperationColumnMarker>;

    fn any_string(string: &str) -> AnyDataValue {
        AnyDataValue::new_plain_string(String::from(string))
    }

    fn any_iri(string: &str) -> AnyDataValue {
        AnyDataValue::new_iri(String::from(string))
    }

    fn any_int(integer: i64) -> AnyDataValue {
        AnyDataValue::new_integer_from_i64(integer)
    }

    fn any_float(float: f32) -> AnyDataValue {
        AnyDataValue::new_float_from_f32(float).unwrap()
    }

    fn any_double(double: f64) -> AnyDataValue {
        AnyDataValue::new_double_from_f64(double).unwrap()
    }

    fn any_bool(boolean: bool) -> AnyDataValue {
        AnyDataValue::new_boolean(boolean)
    }

    fn evaluate_expect(tree: &Function, expected_value: Option<AnyDataValue>) {
        let program = StackProgram::from_function_tree(tree, &HashMap::new(), None);
        let result = program.evaluate(&[], None);

        assert_eq!(result, expected_value);
    }

    fn evaluate_bool_expect(tree: &Function, expected_value: bool) {
        let program = StackProgram::from_function_tree(tree, &HashMap::new(), None);
        let result = program.evaluate_bool(&[], None).unwrap();

        assert_eq!(result, expected_value);
    }

    #[test]
    fn evaluate_string() {
        let tree_length = Function::string_length(Function::constant(any_string("12345")));
        evaluate_expect(&tree_length, Some(AnyDataValue::new_integer_from_i64(5)));

        let tree_string_reverse =
            Function::string_reverse(Function::constant(any_string("Hello World")));
        evaluate_expect(&tree_string_reverse, Some(any_string("dlroW olleH")));

        let tree_lower_case = Function::string_lowercase(Function::constant(any_string("tEsT123")));
        evaluate_expect(&tree_lower_case, Some(any_string("test123")));

        let tree_upper_case = Function::string_uppercase(Function::constant(any_string("tEsT123")));
        evaluate_expect(&tree_upper_case, Some(any_string("TEST123")));

        let tree_compare_equal =
            Function::string_compare(tree_upper_case, Function::constant(any_string("TEST123")));
        evaluate_expect(
            &tree_compare_equal,
            Some(AnyDataValue::new_integer_from_i64(0)),
        );

        let tree_concat_contains = Function::string_contains(
            Function::string_concatenation(vec![
                Function::constant(any_string("algebra")),
                Function::constant(any_string("instruction")),
            ]),
            Function::constant(any_string("brain")),
        );
        evaluate_expect(&tree_concat_contains, Some(AnyDataValue::new_boolean(true)));

        let tree_not_contains = Function::string_contains(
            Function::constant(any_string("abc")),
            Function::constant(any_string("def")),
        );
        evaluate_expect(&tree_not_contains, Some(AnyDataValue::new_boolean(false)));

        let tree_regex = Function::string_regex(
            Function::constant(any_string("hello")),
            Function::constant(any_string("l+")),
        );
        evaluate_expect(&tree_regex, Some(AnyDataValue::new_boolean(true)));

        let tree_substring_length = Function::string_subtstring_length(
            Function::constant(any_string("Hello World")),
            Function::constant(AnyDataValue::new_integer_from_u64(7)),
            Function::constant(AnyDataValue::new_integer_from_u64(3)),
        );
        evaluate_expect(&tree_substring_length, Some(any_string("Wor")));

        let tree_substring = Function::string_subtstring(
            Function::constant(any_string("Hello World")),
            Function::constant(AnyDataValue::new_integer_from_u64(7)),
        );
        evaluate_expect(&tree_substring, Some(any_string("World")));

        let tree_starts = Function::string_starts(
            Function::constant(any_string("Hello World")),
            Function::constant(any_string("Hell")),
        );
        evaluate_expect(&tree_starts, Some(any_bool(true)));

        let tree_not_starts = Function::string_starts(
            Function::constant(any_string("Hello World")),
            Function::constant(any_string("World")),
        );
        evaluate_expect(&tree_not_starts, Some(any_bool(false)));

        let tree_ends = Function::string_ends(
            Function::constant(any_string("Hello World")),
            Function::constant(any_string(" World")),
        );
        evaluate_expect(&tree_ends, Some(any_bool(true)));

        let tree_not_ends = Function::string_ends(
            Function::constant(any_string("Hello World")),
            Function::constant(any_string("Hello")),
        );
        evaluate_expect(&tree_not_ends, Some(any_bool(false)));

        let tree_before = Function::string_before(
            Function::constant(any_string("Hello World")),
            Function::constant(any_string(" ")),
        );
        evaluate_expect(&tree_before, Some(any_string("Hello")));

        let tree_after = Function::string_after(
            Function::constant(any_string("Hello World")),
            Function::constant(any_string(" ")),
        );
        evaluate_expect(&tree_after, Some(any_string("World")));
    }

    #[test]
    fn evaluate_numeric_integer() {
        // log_2(((|sqrt(64) - 11| * -2 + 26) / 5) ^ 3) = 6
        let tree_arithmetic = Function::numeric_logarithm(
            Function::numeric_power(
                Function::numeric_division(
                    Function::numeric_addition(
                        Function::numeric_multiplication(
                            Function::numeric_absolute(Function::numeric_subtraction(
                                Function::numeric_squareroot(Function::constant(any_int(64))),
                                Function::constant(any_int(11)),
                            )),
                            Function::numeric_negation(Function::constant(any_int(2))),
                        ),
                        Function::constant(any_int(26)),
                    ),
                    Function::constant(any_int(5)),
                ),
                Function::constant(any_int(3)),
            ),
            Function::constant(any_int(2)),
        );
        evaluate_expect(&tree_arithmetic, Some(any_int(6)));

        let tree_less_less = Function::numeric_lessthan(
            Function::constant(any_int(-5)),
            Function::constant(any_int(10)),
        );
        let tree_less_eq = Function::numeric_lessthan(
            Function::constant(any_int(-5)),
            Function::constant(any_int(-5)),
        );
        let tree_less_greater = Function::numeric_lessthan(
            Function::constant(any_int(10)),
            Function::constant(any_int(-5)),
        );
        evaluate_expect(&tree_less_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_less_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_less_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_lesseq_less = Function::numeric_lessthaneq(
            Function::constant(any_int(-5)),
            Function::constant(any_int(10)),
        );
        let tree_lesseq_eq = Function::numeric_lessthaneq(
            Function::constant(any_int(-5)),
            Function::constant(any_int(-5)),
        );
        let tree_lesseq_greater = Function::numeric_lessthaneq(
            Function::constant(any_int(10)),
            Function::constant(any_int(-5)),
        );
        evaluate_expect(&tree_lesseq_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_greater_less = Function::numeric_greaterthan(
            Function::constant(any_int(-5)),
            Function::constant(any_int(10)),
        );
        let tree_greater_eq = Function::numeric_greaterthan(
            Function::constant(any_int(-5)),
            Function::constant(any_int(-5)),
        );
        let tree_greater_greater = Function::numeric_greaterthan(
            Function::constant(any_int(10)),
            Function::constant(any_int(-5)),
        );
        evaluate_expect(&tree_greater_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_greater, Some(AnyDataValue::new_boolean(true)));

        let tree_greatereq_less = Function::numeric_greaterthaneq(
            Function::constant(any_int(-5)),
            Function::constant(any_int(10)),
        );
        let tree_greatereq_eq = Function::numeric_greaterthaneq(
            Function::constant(any_int(-5)),
            Function::constant(any_int(-5)),
        );
        let tree_greatereq_greater = Function::numeric_greaterthaneq(
            Function::constant(any_int(10)),
            Function::constant(any_int(-5)),
        );
        evaluate_expect(&tree_greatereq_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greatereq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(
            &tree_greatereq_greater,
            Some(AnyDataValue::new_boolean(true)),
        );

        let tree_round = Function::numeric_round(Function::constant(any_int(-5)));
        let tree_ceil = Function::numeric_round(Function::constant(any_int(-5)));
        let tree_floor = Function::numeric_round(Function::constant(any_int(-5)));
        evaluate_expect(&tree_round, Some(AnyDataValue::new_integer_from_i64(-5)));
        evaluate_expect(&tree_ceil, Some(AnyDataValue::new_integer_from_i64(-5)));
        evaluate_expect(&tree_floor, Some(AnyDataValue::new_integer_from_i64(-5)));
    }

    #[test]
    fn evaluate_numeric_float() {
        // log_2(((|sqrt(64) - 11| * -2 + 26) / 5) ^ 3) = 6
        let tree_arithmetic = Function::numeric_logarithm(
            Function::numeric_power(
                Function::numeric_division(
                    Function::numeric_addition(
                        Function::numeric_multiplication(
                            Function::numeric_absolute(Function::numeric_subtraction(
                                Function::numeric_squareroot(Function::constant(any_float(64.0))),
                                Function::constant(any_float(11.0)),
                            )),
                            Function::numeric_negation(Function::constant(any_float(2.0))),
                        ),
                        Function::constant(any_float(26.0)),
                    ),
                    Function::constant(any_float(5.0)),
                ),
                Function::constant(any_float(3.0)),
            ),
            Function::constant(any_float(2.0)),
        );
        evaluate_expect(&tree_arithmetic, Some(any_float(6.0)));

        let tree_less_less = Function::numeric_lessthan(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(10.0)),
        );
        let tree_less_eq = Function::numeric_lessthan(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(-5.0)),
        );
        let tree_less_greater = Function::numeric_lessthan(
            Function::constant(any_float(10.0)),
            Function::constant(any_float(-5.0)),
        );
        evaluate_expect(&tree_less_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_less_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_less_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_lesseq_less = Function::numeric_lessthaneq(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(10.0)),
        );
        let tree_lesseq_eq = Function::numeric_lessthaneq(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(-5.0)),
        );
        let tree_lesseq_greater = Function::numeric_lessthaneq(
            Function::constant(any_float(10.0)),
            Function::constant(any_float(-5.0)),
        );
        evaluate_expect(&tree_lesseq_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_greater_less = Function::numeric_greaterthan(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(10.0)),
        );
        let tree_greater_eq = Function::numeric_greaterthan(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(-5.0)),
        );
        let tree_greater_greater = Function::numeric_greaterthan(
            Function::constant(any_float(10.0)),
            Function::constant(any_float(-5.0)),
        );
        evaluate_expect(&tree_greater_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_greater, Some(AnyDataValue::new_boolean(true)));

        let tree_greatereq_less = Function::numeric_greaterthaneq(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(10.0)),
        );
        let tree_greatereq_eq = Function::numeric_greaterthaneq(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(-5.0)),
        );
        let tree_greatereq_greater = Function::numeric_greaterthaneq(
            Function::constant(any_float(10.0)),
            Function::constant(any_float(-5.0)),
        );
        evaluate_expect(&tree_greatereq_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greatereq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(
            &tree_greatereq_greater,
            Some(AnyDataValue::new_boolean(true)),
        );

        let tree_sin = Function::numeric_sine(Function::constant(any_float(0.0)));
        evaluate_expect(&tree_sin, Some(any_float(0.0)));

        let tree_cos = Function::numeric_cosine(Function::constant(any_float(0.0)));
        evaluate_expect(&tree_cos, Some(any_float(1.0)));

        let tree_tan = Function::numeric_tangent(Function::constant(any_float(0.0)));
        evaluate_expect(&tree_tan, Some(any_float(0.0)));

        let tree_round = Function::numeric_round(Function::constant(any_float(-5.0)));
        evaluate_expect(&tree_round, Some(any_float(-5.0)));
        let tree_round = Function::numeric_round(Function::constant(any_float(-10.5)));
        evaluate_expect(&tree_round, Some(any_float(-11.0)));
        let tree_round = Function::numeric_round(Function::constant(any_float(-10.2)));
        evaluate_expect(&tree_round, Some(any_float(-10.0)));
        let tree_round = Function::numeric_round(Function::constant(any_float(-10.8)));
        evaluate_expect(&tree_round, Some(any_float(-11.0)));
        let tree_round = Function::numeric_round(Function::constant(any_float(10.5)));
        evaluate_expect(&tree_round, Some(any_float(11.0)));
        let tree_round = Function::numeric_round(Function::constant(any_float(10.2)));
        evaluate_expect(&tree_round, Some(any_float(10.0)));
        let tree_round = Function::numeric_round(Function::constant(any_float(10.8)));
        evaluate_expect(&tree_round, Some(any_float(11.0)));

        let tree_floor = Function::numeric_floor(Function::constant(any_float(-5.0)));
        evaluate_expect(&tree_floor, Some(any_float(-5.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_float(-10.5)));
        evaluate_expect(&tree_floor, Some(any_float(-11.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_float(-10.2)));
        evaluate_expect(&tree_floor, Some(any_float(-11.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_float(-10.8)));
        evaluate_expect(&tree_floor, Some(any_float(-11.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_float(10.5)));
        evaluate_expect(&tree_floor, Some(any_float(10.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_float(10.2)));
        evaluate_expect(&tree_floor, Some(any_float(10.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_float(10.8)));
        evaluate_expect(&tree_floor, Some(any_float(10.0)));

        let tree_ceil = Function::numeric_ceil(Function::constant(any_float(-5.0)));
        evaluate_expect(&tree_ceil, Some(any_float(-5.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_float(-10.5)));
        evaluate_expect(&tree_ceil, Some(any_float(-10.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_float(-10.2)));
        evaluate_expect(&tree_ceil, Some(any_float(-10.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_float(-10.8)));
        evaluate_expect(&tree_ceil, Some(any_float(-10.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_float(10.5)));
        evaluate_expect(&tree_ceil, Some(any_float(11.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_float(10.2)));
        evaluate_expect(&tree_ceil, Some(any_float(11.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_float(10.8)));
        evaluate_expect(&tree_ceil, Some(any_float(11.0)));
    }

    #[test]
    fn evaluate_numeric_double() {
        // log_2(((|sqrt(64) - 11| * -2 + 26) / 5) ^ 3) = 6
        let tree_arithmetic = Function::numeric_logarithm(
            Function::numeric_power(
                Function::numeric_division(
                    Function::numeric_addition(
                        Function::numeric_multiplication(
                            Function::numeric_absolute(Function::numeric_subtraction(
                                Function::numeric_squareroot(Function::constant(any_double(64.0))),
                                Function::constant(any_double(11.0)),
                            )),
                            Function::numeric_negation(Function::constant(any_double(2.0))),
                        ),
                        Function::constant(any_double(26.0)),
                    ),
                    Function::constant(any_double(5.0)),
                ),
                Function::constant(any_double(3.0)),
            ),
            Function::constant(any_double(2.0)),
        );
        evaluate_expect(&tree_arithmetic, Some(any_double(6.0)));

        let tree_less_less = Function::numeric_lessthan(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(10.0)),
        );
        let tree_less_eq = Function::numeric_lessthan(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(-5.0)),
        );
        let tree_less_greater = Function::numeric_lessthan(
            Function::constant(any_double(10.0)),
            Function::constant(any_double(-5.0)),
        );
        evaluate_expect(&tree_less_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_less_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_less_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_lesseq_less = Function::numeric_lessthaneq(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(10.0)),
        );
        let tree_lesseq_eq = Function::numeric_lessthaneq(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(-5.0)),
        );
        let tree_lesseq_greater = Function::numeric_lessthaneq(
            Function::constant(any_double(10.0)),
            Function::constant(any_double(-5.0)),
        );
        evaluate_expect(&tree_lesseq_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_greater_less = Function::numeric_greaterthan(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(10.0)),
        );
        let tree_greater_eq = Function::numeric_greaterthan(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(-5.0)),
        );
        let tree_greater_greater = Function::numeric_greaterthan(
            Function::constant(any_double(10.0)),
            Function::constant(any_double(-5.0)),
        );
        evaluate_expect(&tree_greater_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_greater, Some(AnyDataValue::new_boolean(true)));

        let tree_greatereq_less = Function::numeric_greaterthaneq(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(10.0)),
        );
        let tree_greatereq_eq = Function::numeric_greaterthaneq(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(-5.0)),
        );
        let tree_greatereq_greater = Function::numeric_greaterthaneq(
            Function::constant(any_double(10.0)),
            Function::constant(any_double(-5.0)),
        );
        evaluate_expect(&tree_greatereq_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greatereq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(
            &tree_greatereq_greater,
            Some(AnyDataValue::new_boolean(true)),
        );

        let tree_sin = Function::numeric_sine(Function::constant(any_double(0.0)));
        evaluate_expect(&tree_sin, Some(any_double(0.0)));

        let tree_cos = Function::numeric_cosine(Function::constant(any_double(0.0)));
        evaluate_expect(&tree_cos, Some(any_double(1.0)));

        let tree_tan = Function::numeric_tangent(Function::constant(any_double(0.0)));
        evaluate_expect(&tree_tan, Some(any_double(0.0)));

        let tree_round = Function::numeric_round(Function::constant(any_double(-5.0)));
        evaluate_expect(&tree_round, Some(any_double(-5.0)));
        let tree_round = Function::numeric_round(Function::constant(any_double(-10.5)));
        evaluate_expect(&tree_round, Some(any_double(-11.0)));
        let tree_round = Function::numeric_round(Function::constant(any_double(-10.2)));
        evaluate_expect(&tree_round, Some(any_double(-10.0)));
        let tree_round = Function::numeric_round(Function::constant(any_double(-10.8)));
        evaluate_expect(&tree_round, Some(any_double(-11.0)));
        let tree_round = Function::numeric_round(Function::constant(any_double(10.5)));
        evaluate_expect(&tree_round, Some(any_double(11.0)));
        let tree_round = Function::numeric_round(Function::constant(any_double(10.2)));
        evaluate_expect(&tree_round, Some(any_double(10.0)));
        let tree_round = Function::numeric_round(Function::constant(any_double(10.8)));
        evaluate_expect(&tree_round, Some(any_double(11.0)));

        let tree_floor = Function::numeric_floor(Function::constant(any_double(-5.0)));
        evaluate_expect(&tree_floor, Some(any_double(-5.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_double(-10.5)));
        evaluate_expect(&tree_floor, Some(any_double(-11.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_double(-10.2)));
        evaluate_expect(&tree_floor, Some(any_double(-11.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_double(-10.8)));
        evaluate_expect(&tree_floor, Some(any_double(-11.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_double(10.5)));
        evaluate_expect(&tree_floor, Some(any_double(10.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_double(10.2)));
        evaluate_expect(&tree_floor, Some(any_double(10.0)));
        let tree_floor = Function::numeric_floor(Function::constant(any_double(10.8)));
        evaluate_expect(&tree_floor, Some(any_double(10.0)));

        let tree_ceil = Function::numeric_ceil(Function::constant(any_double(-5.0)));
        evaluate_expect(&tree_ceil, Some(any_double(-5.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_double(-10.5)));
        evaluate_expect(&tree_ceil, Some(any_double(-10.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_double(-10.2)));
        evaluate_expect(&tree_ceil, Some(any_double(-10.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_double(-10.8)));
        evaluate_expect(&tree_ceil, Some(any_double(-10.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_double(10.5)));
        evaluate_expect(&tree_ceil, Some(any_double(11.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_double(10.2)));
        evaluate_expect(&tree_ceil, Some(any_double(11.0)));
        let tree_ceil = Function::numeric_ceil(Function::constant(any_double(10.8)));
        evaluate_expect(&tree_ceil, Some(any_double(11.0)));
    }

    #[test]
    fn evaluate_casting() {
        let tree_to_int = Function::casting_to_integer64(Function::constant(any_float(4.0)));
        evaluate_expect(&tree_to_int, Some(any_int(4)));

        let tree_to_float = Function::casting_to_float(Function::constant(any_int(4)));
        evaluate_expect(&tree_to_float, Some(any_float(4.0)));

        let tree_to_double = Function::casting_to_double(Function::constant(any_int(4)));
        evaluate_expect(&tree_to_double, Some(any_double(4.0)));
    }

    #[test]
    fn evaluate_boolean() {
        let tree_true = Function::numeric_greaterthan(
            Function::constant(any_int(10)),
            Function::constant(any_int(5)),
        );
        let tree_false = Function::numeric_greaterthan(
            Function::constant(any_int(-10)),
            Function::constant(any_int(5)),
        );

        let tree_conj_true = Function::boolean_conjunction(vec![
            Function::constant(any_bool(true)),
            tree_true.clone(),
        ]);
        evaluate_bool_expect(&tree_conj_true, true);

        let tree_conj_false = Function::boolean_conjunction(vec![
            tree_false.clone(),
            Function::constant(any_bool(true)),
        ]);
        evaluate_bool_expect(&tree_conj_false, false);

        let tree_disj_true =
            Function::boolean_disjunction(vec![Function::constant(any_bool(false)), tree_true]);
        evaluate_bool_expect(&tree_disj_true, true);

        let tree_disj_false =
            Function::boolean_disjunction(vec![tree_false, Function::constant(any_bool(false))]);
        evaluate_bool_expect(&tree_disj_false, false);

        let tree_neg_true = Function::boolean_negation(Function::constant(any_bool(false)));
        evaluate_bool_expect(&tree_neg_true, true);

        let tree_neg_false = Function::boolean_negation(Function::constant(any_bool(true)));
        evaluate_bool_expect(&tree_neg_false, false);
    }

    #[test]
    fn evaluate_generic() {
        let tree_integer = Function::constant(any_int(1));
        let tree_float = FunctionTree::constant(any_float(1.0));

        let tree_equals_same = Function::equals(tree_integer.clone(), tree_integer.clone());
        evaluate_bool_expect(&tree_equals_same, true);
        let tree_equals_different = Function::equals(tree_integer.clone(), tree_float.clone());
        evaluate_bool_expect(&tree_equals_different, false);

        let tree_unequals_same = Function::unequals(tree_integer.clone(), tree_integer.clone());
        evaluate_bool_expect(&tree_unequals_same, false);
        let tree_unequals_different = Function::unequals(tree_integer.clone(), tree_float.clone());
        evaluate_bool_expect(&tree_unequals_different, true);

        let tree_datatype = Function::datatype(tree_integer);
        evaluate_expect(
            &tree_datatype,
            Some(any_iri("http://www.w3.org/2001/XMLSchema#int")),
        );
    }

    #[test]
    fn evaluate_language() {
        let tree_tag = Function::languagetag(FunctionTree::constant(
            AnyDataValue::new_language_tagged_string(String::from("test"), String::from("en")),
        ));
        evaluate_expect(&tree_tag, Some(any_string("en")));
    }

    #[test]
    fn evaluate_check_type() {
        let tree_integer = Function::constant(any_int(1));
        let tree_float = Function::constant(any_float(1.0));
        let tree_double = Function::constant(any_double(1.0));
        let tree_string = Function::constant(any_string("test"));
        let tree_iri = Function::constant(AnyDataValue::new_iri(String::from("http://test.com")));

        let tree_is_integer = Function::check_is_integer(tree_integer.clone());
        evaluate_bool_expect(&tree_is_integer, true);
        let tree_not_integer = Function::check_is_integer(tree_double.clone());
        evaluate_bool_expect(&tree_not_integer, false);

        let tree_is_double = Function::check_is_double(tree_double.clone());
        evaluate_bool_expect(&tree_is_double, true);
        let tree_not_double = Function::check_is_double(tree_float.clone());
        evaluate_bool_expect(&tree_not_double, false);

        let tree_is_float = Function::check_is_float(tree_float.clone());
        evaluate_bool_expect(&tree_is_float, true);
        let tree_not_float = Function::check_is_float(tree_string.clone());
        evaluate_bool_expect(&tree_not_float, false);

        let tree_is_numeric = Function::check_is_numeric(tree_integer.clone());
        evaluate_bool_expect(&tree_is_numeric, true);
        let tree_not_numeric = Function::check_is_numeric(tree_string.clone());
        evaluate_bool_expect(&tree_not_numeric, false);

        let tree_is_iri = Function::check_is_iri(tree_iri.clone());
        evaluate_bool_expect(&tree_is_iri, true);
        let tree_not_iri = Function::check_is_iri(tree_string.clone());
        evaluate_bool_expect(&tree_not_iri, false);

        let tree_is_string = Function::check_is_string(tree_string.clone());
        evaluate_bool_expect(&tree_is_string, true);
        let tree_not_string = Function::check_is_string(tree_double.clone());
        evaluate_bool_expect(&tree_not_string, false);
    }
}
