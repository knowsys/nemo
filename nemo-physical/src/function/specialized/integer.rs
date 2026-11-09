//! The integer form of a program.
//!
//! The integer fragment is the part of the instruction set
//! whose semantics on 64-bit integers and booleans agree with those of the generic interpreter.
//! It has the comparisons, the connectives,
//! and the checked sum, difference, product and negation.
//! A program within the fragment, with 64-bit integer constants only,
//! compiles into an [IntegerProgram].
//! The integer form evaluates on raw `i64`s, without constructing values,
//! dispatching on domains or reading the dictionary.
//! It applies to rows whose inputs all have the tag [Int](crate::storage::Tag::Int).
//!
//! On some rows the semantics would diverge,
//! if an intermediate result outgrows `i64` and becomes a wide integer for the generic interpreter,
//! or if the generic interpreter fails on a pair of operands.
//! There, the evaluation of the integer form *falls back*: it returns `None`,
//! and the row must be evaluated by the generic interpreter instead.

use crate::{
    function::opcode::OpCode,
    value::{DataValue, ValueRef},
};

/// One instruction of an [IntegerProgram].
///
/// A constant instruction contains the constant's value.
#[derive(Debug, Clone, Copy)]
enum IntOp {
    /// Pushes a constant.
    Constant(i64),
    /// Pushes the input with the given index.
    Input(u16),
    /// Pops two values of one kind and pushes whether they are equal.
    Equal,
    /// Pops two integers and pushes whether the first is smaller.
    Less,
    /// Pops two integers and pushes whether the first is smaller or equal.
    LessOrEqual,
    /// Pops two integers and pushes whether the first is greater.
    Greater,
    /// Pops two integers and pushes whether the first is greater or equal.
    GreaterOrEqual,
    /// Pops two integers and pushes their checked sum.
    Add,
    /// Pops two integers and pushes their checked difference.
    Subtract,
    /// Pops two integers and pushes their checked product.
    Multiply,
    /// Pops an integer and pushes its checked negation.
    Negate,
    /// Pops two values and pushes the conjunction of their effective boolean values.
    And,
    /// Pops two values and pushes the disjunction of their effective boolean values.
    Or,
    /// Pops a value and pushes the negation of its effective boolean value.
    Not,
}

/// A value on the integer evaluation stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntegerValue {
    /// An integer.
    Integer(i64),
    /// The result of a comparison or a connective.
    Boolean(bool),
}

impl IntegerValue {
    /// Returns the effective boolean value, which for an integer is false exactly at zero.
    fn truth(self) -> bool {
        match self {
            IntegerValue::Integer(value) => value != 0,
            IntegerValue::Boolean(value) => value,
        }
    }

    /// Returns the integer, or `None` for a boolean.
    ///
    /// The generic interpreter fails on a boolean used as a number,
    /// so the integer form falls back.
    fn integer(self) -> Option<i64> {
        match self {
            IntegerValue::Integer(value) => Some(value),
            IntegerValue::Boolean(_) => None,
        }
    }
}

/// The integer form of a stack program, which evaluates on 64-bit integers.
#[derive(Debug, Clone)]
pub(crate) struct IntegerProgram {
    /// The instructions, in evaluation order.
    operations: Vec<IntOp>,
    /// The number of inputs that the program reads from a row.
    inputs: usize,
}

impl IntegerProgram {
    /// Compiles the integer form of the program with the given instructions, constants
    /// and number of inputs.
    ///
    /// Returns `None` if an instruction or a constant lies outside the integer fragment.
    /// Division lies outside it,
    /// since the quotient of two integers is an exact number that may have a fraction.
    pub(crate) fn compile(
        operations: &[OpCode],
        constants: &[DataValue],
        inputs: usize,
    ) -> Option<Self> {
        let operations = operations
            .iter()
            .map(|&operation| {
                Some(match operation {
                    OpCode::Constant(index) => match constants[usize::from(index)].as_ref() {
                        ValueRef::Number(number) => IntOp::Constant(number.to_i64()?),
                        _ => return None,
                    },
                    OpCode::Input(index) => IntOp::Input(index),
                    // Within the fragment, identity and value equality coincide.
                    OpCode::Same | OpCode::Equal => IntOp::Equal,
                    OpCode::Less => IntOp::Less,
                    OpCode::LessOrEqual => IntOp::LessOrEqual,
                    OpCode::Greater => IntOp::Greater,
                    OpCode::GreaterOrEqual => IntOp::GreaterOrEqual,
                    OpCode::Add => IntOp::Add,
                    OpCode::Subtract => IntOp::Subtract,
                    OpCode::Multiply => IntOp::Multiply,
                    OpCode::Negate => IntOp::Negate,
                    OpCode::And => IntOp::And,
                    OpCode::Or => IntOp::Or,
                    OpCode::Not => IntOp::Not,
                    _ => return None,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { operations, inputs })
    }

    /// Evaluates the program on one row of integers, and returns its result.
    ///
    /// `stack` is cleared first, so that one stack can serve many evaluations.
    /// Returns `None` if the evaluation falls back,
    /// because an intermediate result outgrew `i64` or a pair of operands lies outside the
    /// fragment.
    pub(crate) fn evaluate(
        &self,
        stack: &mut Vec<IntegerValue>,
        inputs: &[i64],
    ) -> Option<IntegerValue> {
        debug_assert_eq!(inputs.len(), self.inputs, "the row must match the program");
        stack.clear();

        for &operation in &self.operations {
            let result = match operation {
                IntOp::Constant(value) => IntegerValue::Integer(value),
                IntOp::Input(index) => IntegerValue::Integer(inputs[usize::from(index)]),
                IntOp::Equal => {
                    let (left, right) = pop_pair(stack);
                    match (left, right) {
                        (IntegerValue::Integer(l), IntegerValue::Integer(r)) => {
                            IntegerValue::Boolean(l == r)
                        }
                        (IntegerValue::Boolean(l), IntegerValue::Boolean(r)) => {
                            IntegerValue::Boolean(l == r)
                        }
                        // An integer and a boolean are distinct terms but incomparable values,
                        // so the generic interpreter decides.
                        _ => return None,
                    }
                }
                IntOp::Less => comparison(stack, |l, r| l < r)?,
                IntOp::LessOrEqual => comparison(stack, |l, r| l <= r)?,
                IntOp::Greater => comparison(stack, |l, r| l > r)?,
                IntOp::GreaterOrEqual => comparison(stack, |l, r| l >= r)?,
                IntOp::Add => arithmetic(stack, i64::checked_add)?,
                IntOp::Subtract => arithmetic(stack, i64::checked_sub)?,
                IntOp::Multiply => arithmetic(stack, i64::checked_mul)?,
                IntOp::Negate => {
                    let value = stack.pop().expect("the program is validated");
                    IntegerValue::Integer(value.integer()?.checked_neg()?)
                }
                IntOp::And => {
                    let (left, right) = pop_pair(stack);
                    IntegerValue::Boolean(left.truth() && right.truth())
                }
                IntOp::Or => {
                    let (left, right) = pop_pair(stack);
                    IntegerValue::Boolean(left.truth() || right.truth())
                }
                IntOp::Not => {
                    let value = stack.pop().expect("the program is validated");
                    IntegerValue::Boolean(!value.truth())
                }
            };
            stack.push(result);
        }

        Some(stack.pop().expect("a validated program leaves one result"))
    }

    /// Evaluates the program as a condition,
    /// and returns the effective boolean value of its result.
    ///
    /// Returns `None` if the evaluation falls back, as for [evaluate](Self::evaluate).
    pub(crate) fn filter(&self, stack: &mut Vec<IntegerValue>, inputs: &[i64]) -> Option<bool> {
        self.evaluate(stack, inputs).map(IntegerValue::truth)
    }
}

/// Pops the two operands of a binary instruction.
fn pop_pair(stack: &mut Vec<IntegerValue>) -> (IntegerValue, IntegerValue) {
    let right = stack.pop().expect("the program is validated");
    let left = stack.pop().expect("the program is validated");
    (left, right)
}

/// Applies an integer comparison to the two operands on top of the stack,
/// or returns `None` for a boolean operand.
fn comparison(
    stack: &mut Vec<IntegerValue>,
    predicate: impl FnOnce(i64, i64) -> bool,
) -> Option<IntegerValue> {
    let (left, right) = pop_pair(stack);
    Some(IntegerValue::Boolean(predicate(
        left.integer()?,
        right.integer()?,
    )))
}

/// Applies checked integer arithmetic to the two operands on top of the stack,
/// or returns `None` on overflow or for a boolean operand.
fn arithmetic(
    stack: &mut Vec<IntegerValue>,
    operation: impl FnOnce(i64, i64) -> Option<i64>,
) -> Option<IntegerValue> {
    let (left, right) = pop_pair(stack);
    Some(IntegerValue::Integer(operation(
        left.integer()?,
        right.integer()?,
    )?))
}
