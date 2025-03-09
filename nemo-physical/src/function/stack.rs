//! This module defines [Stack].

/// Implementation of a stack,
/// particularly suited for the implementation of [super::program::StackProgram].
#[derive(Debug)]
pub(crate) struct Stack<T> {
    /// Content of the stack
    /// is stored in a fixed-sized array
    stack: Box<[T]>,
    /// Pointer to the next free element
    pointer: usize,
}

impl<T> Stack<T>
where
    T: Clone + Default,
{
    /// Create a new [Stack].
    pub fn new(capacity: usize) -> Self {
        Self {
            stack: vec![T::default(); capacity].into_boxed_slice(),
            pointer: 0,
        }
    }
}

impl<T> Stack<T>
where
    T: Clone + std::fmt::Debug,
{
    /// Push a new element onto the stack.
    ///
    /// # Panics
    /// When every slot is taken
    pub fn push(&mut self, value: T) {
        println!(
            "Push {:?} to stack (new pointer: {}, len: {})",
            value,
            self.pointer + 1,
            self.stack.len(),
        );

        self.stack[self.pointer] = value;
        self.pointer += 1;
    }

    /// Remove the top element from the stack.
    ///
    /// # Panics
    /// When called on an empty stack
    pub fn pop(&mut self) -> T {
        self.pointer -= 1;
        self.stack[self.pointer].clone()
    }

    /// Return the top-most element on the stack.
    ///
    /// # Panics
    /// When called on an empty stack
    pub fn top(&self) -> T {
        self.stack[self.pointer - 1].clone()
    }

    /// Return a mutable reference to the top-most element in the stack.
    ///
    /// # Panics
    /// When called on an empty stack
    pub fn top_mut(&mut self) -> &mut T {
        println!(
            "Access element at {}: {:?}",
            self.pointer - 1,
            self.stack[self.pointer - 1]
        );

        &mut self.stack[self.pointer - 1]
    }

    /// Return the top-most element.
    /// The state of the stack will be as if a unary function was called.
    ///
    /// # Panics
    /// When called on an empty stack
    pub fn unary(&mut self, _placeholder: usize) -> T {
        println!(
            "Unary at {}: {:?}",
            self.pointer - 1,
            self.stack[self.pointer - 1]
        );

        self.stack[self.pointer - 1].clone()
    }

    /// Return the two top-most elements on the stack.
    /// The state of the stack will be as if a binary function was called.
    ///
    /// # Panics
    /// When called on a stack with less than two elements.
    pub fn binary(&mut self, _placeholder: usize) -> (T, T) {
        self.pointer -= 1;

        println!(
            "Binary at ({}, {}): ({:?}, {:?}); new pointer: {}",
            self.pointer - 1,
            self.pointer,
            self.stack[self.pointer - 1],
            self.stack[self.pointer],
            self.pointer
        );

        (
            self.stack[self.pointer - 1].clone(),
            self.stack[self.pointer].clone(),
        )
    }

    /// Return the three top-most elements on the stack.
    /// The state of the stack will be as if a ternary function was called.
    ///
    /// # Panics
    /// When called on a stack with less than three elements.
    pub fn ternary(&mut self, _placeholder: usize) -> (T, T, T) {
        self.pointer -= 2;

        println!(
            "Ternary at ({}, {}, {}): ({:?}, {:?}, {:?}), new pointer: {}",
            self.pointer - 1,
            self.pointer,
            self.pointer + 1,
            self.stack[self.pointer - 1],
            self.stack[self.pointer],
            self.stack[self.pointer + 1],
            self.pointer
        );

        (
            self.stack[self.pointer - 1].clone(),
            self.stack[self.pointer].clone(),
            self.stack[self.pointer + 1].clone(),
        )
    }

    /// Return a reference to all stack elements,
    /// without reversing the order.
    pub fn nary(&mut self, amount: usize) -> &[T] {
        let end = self.pointer;
        self.pointer -= amount;
        self.pointer += 1;

        println!(
            "Nary at {}..{}, new pointer: {}",
            (self.pointer - 1),
            end,
            self.pointer,
        );

        &self.stack[(self.pointer - 1)..end]
    }

    /// Delete all elements from the stack.
    pub fn reset(&mut self) {
        println!("Reset\n");

        self.pointer = 0;
    }
}
