//! The integer form of a program.
//!
//! The integer fragment is the part of the instruction set whose semantics
//! over 64-bit integers and booleans coincide with the generic interpreter's:
//! the comparisons, the connectives, and the checked sum, difference, product and negation.
//! A program within the fragment, constants included, compiles into an [IntegerProgram],
//! which evaluates over raw `i64`s without constructing values, dispatching on domains,
//! or touching the dictionary.
//! It applies to a row whose inputs all read integer-tagged columns.
//!
//! If the semantics would diverge,
//! at an intermediate that outgrows `i64` and becomes a wide integer generically,
//! or at an operand pair the generic interpreter reports an error for,
//! evaluation reports a fallback and the caller re-runs the row generically.

use crate::{
    function::opcode::OpCode,
    value::{DataValue, ValueRef},
};

/// One instruction of an [IntegerProgram]; a constant carries its value.
#[derive(Debug, Clone, Copy)]
enum IntOp {
    /// Pushes a constant.
    Constant(i64),
    /// Pushes a row input.
    Input(u16),
    /// Pops two values of one kind; pushes whether they are equal.
    Equal,
    /// Pops two integers; pushes whether the first is smaller.
    Less,
    /// Pops two integers; pushes whether the first is smaller or equal.
    LessOrEqual,
    /// Pops two integers; pushes whether the first is greater.
    Greater,
    /// Pops two integers; pushes whether the first is greater or equal.
    GreaterOrEqual,
    /// Pops two integers; pushes their checked sum.
    Add,
    /// Pops two integers; pushes their checked difference.
    Subtract,
    /// Pops two integers; pushes their checked product.
    Multiply,
    /// Pops an integer; pushes its checked negation.
    Negate,
    /// Pops two values; pushes their conjunction.
    And,
    /// Pops two values; pushes their disjunction.
    Or,
    /// Pops a value; pushes its negated truth.
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
    /// Returns the effective boolean value; an integer is false exactly at zero.
    fn truth(self) -> bool {
        match self {
            IntegerValue::Integer(value) => value != 0,
            IntegerValue::Boolean(value) => value,
        }
    }

    /// Returns the integer, or `None` for a boolean,
    /// whose use as a number is an error generically and a fallback here.
    fn integer(self) -> Option<i64> {
        match self {
            IntegerValue::Integer(value) => Some(value),
            IntegerValue::Boolean(_) => None,
        }
    }
}

/// The integer form of a stack program; see the module documentation.
#[derive(Debug, Clone)]
pub(crate) struct IntegerProgram {
    /// The instructions, in evaluation order.
    operations: Vec<IntOp>,
    /// The number of row inputs the program reads.
    inputs: usize,
}

impl IntegerProgram {
    /// Compiles the integer form,
    /// or `None` if an instruction or a constant lies outside the integer fragment.
    ///
    /// Division is outside: exact integers divide into decimals.
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

    /// Evaluates the program over one row of integers.
    ///
    /// `None` is the fallback: the row must be re-run on the generic interpreter,
    /// because an intermediate outgrew `i64` or an operand pair lies outside the fragment.
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
                        // A comparison across the kinds is an error generically.
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

    /// Evaluates the program as a condition;
    /// `None` is the fallback, as for [evaluate](Self::evaluate).
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

/// Applies an integer comparison; `None` on a boolean operand.
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

/// Applies checked integer arithmetic; `None` on overflow or a boolean operand.
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
