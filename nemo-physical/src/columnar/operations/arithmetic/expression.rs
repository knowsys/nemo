//! Implements the functionality to represent and evaluate complex arithmetic expressions

use std::ops::Deref;

use crate::error::Error;

use super::traits::ArithmeticOperations;

/// Stack operation, transforming the top element.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnaryOperation {
    /// Computes the square root of a value.
    SquareRoot,
    /// Computes the negation of a value.
    Negation,
    /// Computes the absolute value of a value.
    Abs,
    /// Computes the signum of a value.
    Sign,
}

impl UnaryOperation {
    fn eval<T: ArithmeticOperations>(&self, value: T) -> Option<T> {
        match self {
            UnaryOperation::SquareRoot => value.checked_sqrt(),
            UnaryOperation::Negation => value.checked_neg(),
            UnaryOperation::Abs => {
                if value < T::zero() {
                    value.checked_neg()
                } else {
                    Some(value)
                }
            }
            UnaryOperation::Sign => {
                if value.is_zero() {
                    Some(value)
                } else if value < T::zero() {
                    T::one().checked_neg()
                } else {
                    Some(T::one())
                }
            }
        }
    }
}

/// Stack operation removing two elements and pushing the result.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BinaryOperation {
    /// Evaluates sum of the top two values.
    Addition,
    /// Evaluates to the difference between the first (lower on the stack) and second value.
    Subtraction,
    /// Evaluates to the product of the top two values.
    Multiplication,
    /// Evaluates to the quotient of the first (lower on the stack) and second value.
    Division,
    /// Evaluates to the value of the first (lower on the stack) value raised to the power of the second.
    Exponent,
}

impl BinaryOperation {
    fn eval<T: ArithmeticOperations>(&self, lhs: T, rhs: T) -> Option<T> {
        match self {
            BinaryOperation::Addition => lhs.checked_add(&rhs),
            BinaryOperation::Subtraction => lhs.checked_sub(&rhs),
            BinaryOperation::Multiplication => lhs.checked_mul(&rhs),
            BinaryOperation::Division => lhs.checked_div(&rhs),
            BinaryOperation::Exponent => lhs.checked_pow(rhs),
        }
    }
}

/// A value to be pushed onto the stack.
#[derive(Debug, Clone, Copy)]
pub enum StackValue<T> {
    /// A constant provided at compile time.
    Constant(T),
    /// A reference to a value scan at run time.
    Reference(usize),
}

/// Tree representing an arithmetic expression
#[derive(Debug, Clone, Copy)]
pub enum StackOperation<T> {
    /// Push the given value onto the stack.
    Push(StackValue<T>),
    /// Transform the top stack value
    UnaryOperation(UnaryOperation),
    /// Pop the top two values and push the result of the operation.
    BinaryOperation(BinaryOperation),
}

/// A list of stack operations, representing a numerical computation
#[derive(Debug, Clone)]
pub struct StackProgram<T> {
    stack_size: usize,
    instructions: Box<[StackOperation<T>]>,
}

impl<T> From<StackValue<T>> for StackProgram<T> {
    fn from(value: StackValue<T>) -> Self {
        StackProgram {
            stack_size: 1,
            instructions: Box::new([StackOperation::Push(value)]),
        }
    }
}

impl<T> StackProgram<T> {
    /// Constructs a new [`StackProgram`] from a list of [`StackOperation`].
    /// Checks, that all stack operations have sufficient arguments, and the stack height
    /// at the end of the computation is exactly 1.
    /// Returns an error if these conditions are not met.
    pub fn new<I>(iter: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = StackOperation<T>>,
    {
        let instructions: Box<_> = iter.into_iter().collect();
        let mut max_height = 0;
        let mut curr_height = 0;

        for instruction in instructions.iter() {
            match instruction {
                StackOperation::Push(_) => {
                    curr_height += 1;
                }
                StackOperation::UnaryOperation(_) => {
                    if curr_height == 0 {
                        return Err(Error::MalformedStackProgram);
                    }
                }
                StackOperation::BinaryOperation(_) => {
                    if curr_height <= 1 {
                        return Err(Error::MalformedStackProgram);
                    }

                    curr_height -= 1;
                }
            }

            max_height = std::cmp::max(curr_height, max_height);
        }

        if curr_height != 1 {
            return Err(Error::MalformedStackProgram);
        }

        Ok(StackProgram {
            stack_size: max_height,
            instructions,
        })
    }
}

impl<T> Deref for StackProgram<T> {
    type Target = [StackOperation<T>];

    fn deref(&self) -> &Self::Target {
        &self.instructions
    }
}

impl<T> StackProgram<T> {
    /// Iterator over all reference indeces in this program.
    pub fn references(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter().filter_map(|op| match op {
            StackOperation::Push(StackValue::Reference(index)) => Some(*index),
            _ => None,
        })
    }

    /// Mutable iterator over all reference indeces in this program. Can be used to remap the
    /// reference indeces.
    pub fn references_mut(&mut self) -> impl Iterator<Item = &mut usize> + '_ {
        self.instructions.iter_mut().filter_map(|op| match op {
            StackOperation::Push(StackValue::Reference(index)) => Some(index),
            _ => None,
        })
    }

    /// Creates a new [`StackProgram`], where all [`StackOperation::Push`] operations have been mapped with `f`.
    pub fn map_values<S>(&self, f: impl Fn(&StackValue<T>) -> StackValue<S>) -> StackProgram<S> {
        let instructions = self
            .instructions
            .iter()
            .map(|operation| match operation {
                StackOperation::Push(v) => StackOperation::Push(f(v)),
                StackOperation::UnaryOperation(op) => StackOperation::UnaryOperation(*op),
                StackOperation::BinaryOperation(op) => StackOperation::BinaryOperation(*op),
            })
            .collect();

        StackProgram {
            // needed stack size won't change
            stack_size: self.stack_size,
            instructions,
        }
    }

    /// Returns the maximum stack size needed to execute this program.
    pub fn space_requirement(&self) -> usize {
        self.stack_size
    }
}

impl<T: Clone> StackProgram<T> {
    /// If this Program is just a single [`StackValue`] being pushed onto the stack,
    /// then returns just that.
    pub fn trivial(&self) -> Option<StackValue<T>> {
        if self.instructions.len() != 1 {
            return None;
        }

        match &self.instructions[0] {
            StackOperation::Push(v) => Some(v.clone()),
            _ => panic!("malformed stack program"),
        }
    }
}

impl<T: ArithmeticOperations> StackProgram<T> {
    /// Executes the stack program and returns the top element at the end of execution.
    pub fn evaluate(&self, stack: &mut Vec<T>, referenced_values: &[T]) -> Option<T> {
        stack.clear();

        for instruction in self.instructions.iter() {
            match instruction {
                StackOperation::Push(v) => stack.push(match v {
                    StackValue::Constant(c) => *c,
                    StackValue::Reference(i) => referenced_values[*i],
                }),
                StackOperation::UnaryOperation(op) => {
                    let x = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");

                    stack.push(op.eval(x)?)
                }
                StackOperation::BinaryOperation(op) => {
                    let y = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");
                    let x = stack
                        .pop()
                        .expect("This program is valid, so the stack cannot be empty.");

                    stack.push(op.eval(x, y)?)
                }
            }
        }

        Some(
            stack
                .pop()
                .expect("The final value is on the stack, since this program is valid."),
        )
    }
}

impl<T> IntoIterator for StackProgram<T> {
    type Item = StackOperation<T>;
    type IntoIter = std::vec::IntoIter<StackOperation<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.instructions.into_vec().into_iter()
    }
}

#[cfg(test)]
mod test {
    use super::StackProgram;

    fn test_program() -> StackProgram<i32> {
        // (|5 - (-2 + 10)| * 4 / 2) ^ sqrt(16) = 1296
        // in stack code: 5 2 negate 10 + - abs 4 2 / * 16 sqrt ^
        use super::BinaryOperation::*;
        use super::StackOperation::*;
        use super::StackValue::*;
        use super::UnaryOperation::*;
        let stack_code = [
            Push(Constant(5)),
            Push(Constant(2)),
            UnaryOperation(Negation),
            Push(Constant(10)),
            BinaryOperation(Addition),
            BinaryOperation(Subtraction),
            UnaryOperation(Abs),
            Push(Constant(4)),
            Push(Constant(2)),
            BinaryOperation(Division),
            BinaryOperation(Multiplication),
            Push(Constant(16)),
            UnaryOperation(SquareRoot),
            BinaryOperation(Exponent),
        ];

        StackProgram::new(stack_code).unwrap()
    }

    #[test]
    fn stack_size() {
        assert_eq!(test_program().space_requirement(), 3);
    }

    #[test]
    fn evaluate_tree() {
        let program = test_program();
        let mut stack = Vec::with_capacity(program.space_requirement());
        assert_eq!(program.evaluate(&mut stack, &[]), Some(1296));
    }
}
