//! This module defines [StackProgram].

use std::fmt::Debug;

use crate::function::new_tree::FunctionLeaf;

use super::{
    new_definitions::{Functions, Operable},
    new_tree::FunctionTree,
    stack::Stack,
};

/// Representation of a [FunctionTree] as a stack program
#[derive(Debug)]
pub(crate) struct StackProgram<T>
where
    T: Debug + Operable,
{
    /// Operations to be executed in order
    operations: Box<[Functions]>,
    /// Additional parameters
    operation_parameters: Box<[usize]>,
    /// Used constants
    constants: Box<[T]>,

    /// Stack
    stack: Stack<T>,
}

impl<T> StackProgram<T>
where
    T: Debug + Clone + Operable,
{
    fn build_operation<U, Reference>(
        term: &FunctionTree<U, Reference>,
        order: &[Reference],
        operations: &mut Vec<Functions>,
        operation_parameters: &mut Vec<usize>,
        constants: &mut Vec<T>,
    ) -> Option<usize>
    where
        U: Debug + Clone,
        Reference: Debug + Clone + Eq,
        T: TryFrom<U> + Debug + Clone + Default,
    {
        let max_height = match term {
            FunctionTree::Leaf(leaf) => match leaf {
                FunctionLeaf::Constant(constant) => {
                    operations.push(Functions::Constant);
                    operation_parameters.push(constants.len());
                    constants.push(T::try_from(constant.clone()).ok()?);

                    1
                }
                FunctionLeaf::Reference(reference) => {
                    let index = order
                        .iter()
                        .position(|r| r == reference)
                        .expect("each reference in the tree must occur in `order`");
                    operations.push(Functions::Constant);
                    operation_parameters.push(index);

                    1
                }
            },
            FunctionTree::Function(function, trees) => {
                let mut height: usize = 0;
                for (index, tree) in trees.iter().enumerate() {
                    height = height.max(
                        index
                            + Self::build_operation(
                                tree,
                                order,
                                operations,
                                operation_parameters,
                                constants,
                            )?,
                    );
                }

                operations.push(*function);
                operation_parameters.push(trees.len());

                height
            }
        };

        Some(max_height)
    }

    /// Create a new [StackProgram] from a [FunctionTree].
    pub fn new<U, Reference>(tree: &FunctionTree<U, Reference>, order: &[Reference]) -> Option<Self>
    where
        U: Debug + Clone,
        Reference: Debug + Clone + Eq,
        T: TryFrom<U> + Debug + Clone + Default,
    {
        let mut max_height: usize = 0;

        let mut operations = Vec::<Functions>::new();
        let mut operation_parameters = Vec::<usize>::new();
        let mut constants = vec![T::default(); order.len()];

        let max_height = Self::build_operation(
            tree,
            order,
            &mut operations,
            &mut operation_parameters,
            &mut constants,
        )?;

        Some(Self {
            operations: operations.into_boxed_slice(),
            operation_parameters: operation_parameters.into_boxed_slice(),
            constants: constants.into_boxed_slice(),
            stack: Stack::new(max_height),
        })
    }

    /// Evaluate the program.
    pub fn evaluate(&mut self) -> Option<T> {
        self.stack.reset();

        for (operation, parameter) in self.operations.iter().zip(self.operation_parameters.iter()) {
            match_function!(operation, self.stack, self.constants, *parameter);
        }

        Some(self.stack.top())
    }

    /// Evaluate the program and check whether it evaluates to `true`.
    pub fn check(&mut self) -> Option<bool> {
        let result = self.evaluate()?;

        if !T::is_boolean(&result) {
            return None;
        }

        Some(if result == T::boolean_true() {
            true
        } else {
            false
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        datavalues::AnyDataValue, function::new_tree::FunctionTree,
        tabular::operations::OperationColumnMarker,
    };

    use super::StackProgram;

    type Function = FunctionTree<AnyDataValue, OperationColumnMarker>;

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
        let mut program = StackProgram::<AnyDataValue>::new(tree, &[]).unwrap();
        let result = program.evaluate();

        assert_eq!(result, expected_value);
    }

    fn evaluate_check_expect(tree: &Function, expected_value: bool) {
        let mut program = StackProgram::<AnyDataValue>::new(tree, &[]).unwrap();
        let result = program.check().unwrap();

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

        let tree_substring_length = Function::string_substring_length(
            Function::constant(any_string("Hello World")),
            Function::constant(AnyDataValue::new_integer_from_u64(7)),
            Function::constant(AnyDataValue::new_integer_from_u64(3)),
        );
        evaluate_expect(&tree_substring_length, Some(any_string("Wor")));

        let tree_substring = Function::string_substring(
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

        let tree_less_less = Function::numeric_less_than(
            Function::constant(any_int(-5)),
            Function::constant(any_int(10)),
        );
        let tree_less_eq = Function::numeric_less_than(
            Function::constant(any_int(-5)),
            Function::constant(any_int(-5)),
        );
        let tree_less_greater = Function::numeric_less_than(
            Function::constant(any_int(10)),
            Function::constant(any_int(-5)),
        );
        evaluate_expect(&tree_less_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_less_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_less_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_lesseq_less = Function::numeric_less_than_eq(
            Function::constant(any_int(-5)),
            Function::constant(any_int(10)),
        );
        let tree_lesseq_eq = Function::numeric_less_than_eq(
            Function::constant(any_int(-5)),
            Function::constant(any_int(-5)),
        );
        let tree_lesseq_greater = Function::numeric_less_than_eq(
            Function::constant(any_int(10)),
            Function::constant(any_int(-5)),
        );
        evaluate_expect(&tree_lesseq_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_greater_less = Function::numeric_greater_than(
            Function::constant(any_int(-5)),
            Function::constant(any_int(10)),
        );
        let tree_greater_eq = Function::numeric_greater_than(
            Function::constant(any_int(-5)),
            Function::constant(any_int(-5)),
        );
        let tree_greater_greater = Function::numeric_greater_than(
            Function::constant(any_int(10)),
            Function::constant(any_int(-5)),
        );
        evaluate_expect(&tree_greater_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_greater, Some(AnyDataValue::new_boolean(true)));

        let tree_greatereq_less = Function::numeric_greater_than_eq(
            Function::constant(any_int(-5)),
            Function::constant(any_int(10)),
        );
        let tree_greatereq_eq = Function::numeric_greater_than_eq(
            Function::constant(any_int(-5)),
            Function::constant(any_int(-5)),
        );
        let tree_greatereq_greater = Function::numeric_greater_than_eq(
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

        let tree_less_less = Function::numeric_less_than(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(10.0)),
        );
        let tree_less_eq = Function::numeric_less_than(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(-5.0)),
        );
        let tree_less_greater = Function::numeric_less_than(
            Function::constant(any_float(10.0)),
            Function::constant(any_float(-5.0)),
        );
        evaluate_expect(&tree_less_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_less_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_less_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_lesseq_less = Function::numeric_less_than_eq(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(10.0)),
        );
        let tree_lesseq_eq = Function::numeric_less_than_eq(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(-5.0)),
        );
        let tree_lesseq_greater = Function::numeric_less_than_eq(
            Function::constant(any_float(10.0)),
            Function::constant(any_float(-5.0)),
        );
        evaluate_expect(&tree_lesseq_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_greater_less = Function::numeric_greater_than(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(10.0)),
        );
        let tree_greater_eq = Function::numeric_greater_than(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(-5.0)),
        );
        let tree_greater_greater = Function::numeric_greater_than(
            Function::constant(any_float(10.0)),
            Function::constant(any_float(-5.0)),
        );
        evaluate_expect(&tree_greater_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_greater, Some(AnyDataValue::new_boolean(true)));

        let tree_greatereq_less = Function::numeric_greater_than_eq(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(10.0)),
        );
        let tree_greatereq_eq = Function::numeric_greater_than_eq(
            Function::constant(any_float(-5.0)),
            Function::constant(any_float(-5.0)),
        );
        let tree_greatereq_greater = Function::numeric_greater_than_eq(
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

        let tree_less_less = Function::numeric_less_than(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(10.0)),
        );
        let tree_less_eq = Function::numeric_less_than(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(-5.0)),
        );
        let tree_less_greater = Function::numeric_less_than(
            Function::constant(any_double(10.0)),
            Function::constant(any_double(-5.0)),
        );
        evaluate_expect(&tree_less_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_less_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_less_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_lesseq_less = Function::numeric_less_than_eq(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(10.0)),
        );
        let tree_lesseq_eq = Function::numeric_less_than_eq(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(-5.0)),
        );
        let tree_lesseq_greater = Function::numeric_less_than_eq(
            Function::constant(any_double(10.0)),
            Function::constant(any_double(-5.0)),
        );
        evaluate_expect(&tree_lesseq_less, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_eq, Some(AnyDataValue::new_boolean(true)));
        evaluate_expect(&tree_lesseq_greater, Some(AnyDataValue::new_boolean(false)));

        let tree_greater_less = Function::numeric_greater_than(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(10.0)),
        );
        let tree_greater_eq = Function::numeric_greater_than(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(-5.0)),
        );
        let tree_greater_greater = Function::numeric_greater_than(
            Function::constant(any_double(10.0)),
            Function::constant(any_double(-5.0)),
        );
        evaluate_expect(&tree_greater_less, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_eq, Some(AnyDataValue::new_boolean(false)));
        evaluate_expect(&tree_greater_greater, Some(AnyDataValue::new_boolean(true)));

        let tree_greatereq_less = Function::numeric_greater_than_eq(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(10.0)),
        );
        let tree_greatereq_eq = Function::numeric_greater_than_eq(
            Function::constant(any_double(-5.0)),
            Function::constant(any_double(-5.0)),
        );
        let tree_greatereq_greater = Function::numeric_greater_than_eq(
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
        let tree_to_int = Function::casting_into_integer(Function::constant(any_float(4.0)));
        evaluate_expect(&tree_to_int, Some(any_int(4)));

        let tree_to_float = Function::casting_into_float(Function::constant(any_int(4)));
        evaluate_expect(&tree_to_float, Some(any_float(4.0)));

        let tree_to_double = Function::casting_into_double(Function::constant(any_int(4)));
        evaluate_expect(&tree_to_double, Some(any_double(4.0)));

        let tree_to_iri = Function::casting_into_iri(Function::constant(any_string("test")));
        evaluate_expect(&tree_to_iri, Some(any_iri("test")));
    }

    #[test]
    fn evaluate_boolean() {
        let tree_true = Function::numeric_greater_than(
            Function::constant(any_int(10)),
            Function::constant(any_int(5)),
        );
        let tree_false = Function::numeric_greater_than(
            Function::constant(any_int(-10)),
            Function::constant(any_int(5)),
        );

        let tree_conj_true = Function::boolean_conjunction(vec![
            Function::constant(any_bool(true)),
            tree_true.clone(),
        ]);
        evaluate_check_expect(&tree_conj_true, true);

        let tree_conj_false = Function::boolean_conjunction(vec![
            tree_false.clone(),
            Function::constant(any_bool(true)),
        ]);
        evaluate_check_expect(&tree_conj_false, false);

        let tree_disj_true =
            Function::boolean_disjunction(vec![Function::constant(any_bool(false)), tree_true]);
        evaluate_check_expect(&tree_disj_true, true);

        let tree_disj_false =
            Function::boolean_disjunction(vec![tree_false, Function::constant(any_bool(false))]);
        evaluate_check_expect(&tree_disj_false, false);

        let tree_neg_true = Function::boolean_negation(Function::constant(any_bool(false)));
        evaluate_check_expect(&tree_neg_true, true);

        let tree_neg_false = Function::boolean_negation(Function::constant(any_bool(true)));
        evaluate_check_expect(&tree_neg_false, false);
    }

    #[test]
    fn evaluate_generic() {
        let tree_integer = Function::constant(any_int(1));
        let tree_float = FunctionTree::constant(any_float(1.0));

        let tree_equals_same = Function::equals(tree_integer.clone(), tree_integer.clone());
        evaluate_check_expect(&tree_equals_same, true);
        let tree_equals_different = Function::equals(tree_integer.clone(), tree_float.clone());
        evaluate_check_expect(&tree_equals_different, false);

        let tree_unequals_same = Function::unequals(tree_integer.clone(), tree_integer.clone());
        evaluate_check_expect(&tree_unequals_same, false);
        let tree_unequals_different = Function::unequals(tree_integer.clone(), tree_float.clone());
        evaluate_check_expect(&tree_unequals_different, true);

        let tree_datatype = Function::datatype(tree_integer);
        evaluate_expect(
            &tree_datatype,
            Some(any_iri("http://www.w3.org/2001/XMLSchema#int")),
        );
    }

    #[test]
    fn evaluate_language() {
        let tree_tag = Function::language_tag(FunctionTree::constant(
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
        evaluate_check_expect(&tree_is_integer, true);
        let tree_not_integer = Function::check_is_integer(tree_double.clone());
        evaluate_check_expect(&tree_not_integer, false);

        let tree_is_double = Function::check_is_double(tree_double.clone());
        evaluate_check_expect(&tree_is_double, true);
        let tree_not_double = Function::check_is_double(tree_float.clone());
        evaluate_check_expect(&tree_not_double, false);

        let tree_is_float = Function::check_is_float(tree_float.clone());
        evaluate_check_expect(&tree_is_float, true);
        let tree_not_float = Function::check_is_float(tree_string.clone());
        evaluate_check_expect(&tree_not_float, false);

        let tree_is_numeric = Function::check_is_numeric(tree_integer.clone());
        evaluate_check_expect(&tree_is_numeric, true);
        let tree_not_numeric = Function::check_is_numeric(tree_string.clone());
        evaluate_check_expect(&tree_not_numeric, false);

        let tree_is_iri = Function::check_is_iri(tree_iri.clone());
        evaluate_check_expect(&tree_is_iri, true);
        let tree_not_iri = Function::check_is_iri(tree_string.clone());
        evaluate_check_expect(&tree_not_iri, false);

        let tree_is_string = Function::check_is_string(tree_string.clone());
        evaluate_check_expect(&tree_is_string, true);
        let tree_not_string = Function::check_is_string(tree_double.clone());
        evaluate_check_expect(&tree_not_string, false);
    }
}
