//! Implements the functionality to represent and evaluate complex arithmetic expressions

use std::fmt::{Debug, Display};

use super::traits::ArithmeticOperations;

/// Tree representing an arithmetic expression
#[derive(Clone)]
pub enum ArithmeticTree<T> {
    /// Evaluates to the given constant.
    Constant(T),
    /// Evaluates to the value referenced with the given index.
    Reference(usize),
    /// Evaluates sum of the values of the given subtrees.
    Addition(Vec<ArithmeticTree<T>>),
    /// Evaluates to the difference between the value of the first subtree and the second.
    Subtraction(Box<ArithmeticTree<T>>, Box<ArithmeticTree<T>>),
    /// Evaluates to the product of the values of the given subtrees.
    Multiplication(Vec<ArithmeticTree<T>>),
    /// Evaluates to the quotient of the value of the first subtree and the second.
    Division(Box<ArithmeticTree<T>>, Box<ArithmeticTree<T>>),
    /// Evaluates to the value of the first subtree raised to the power of the value of the second.
    Exponent(Box<ArithmeticTree<T>>, Box<ArithmeticTree<T>>),
    /// Evaluates to the square root of the value of the subtree.
    SquareRoot(Box<ArithmeticTree<T>>),
    /// Evaluates to negative one times the value of the subtree.
    Negation(Box<ArithmeticTree<T>>),
    /// Evaluates to the absolute value of the subtree.
    Abs(Box<ArithmeticTree<T>>),
}

/// Leaf node in an arithmetic tree
#[derive(Debug)]
pub enum ArithmeticTreeLeaf<T>
where
    T: Clone,
{
    /// Evaluates to the given constant.
    Constant(T),
    /// Evaluates to the value referenced with the given index.
    Reference(usize),
}

/// Mutable Leaf node in an arithmetic tree
#[derive(Debug)]
pub enum ArithmeticTreeLeafMut<'a, T> {
    /// Evaluates to the given constant.
    Constant(&'a mut T),
    /// Evaluates to the value referenced with the given index.
    Reference(&'a mut usize),
}

impl<T> ArithmeticTree<T> {
    /// Return whether this node is a leaf node.
    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            ArithmeticTree::Constant(_) | ArithmeticTree::Reference(_)
        )
    }

    /// Defines the relative priority between operations
    fn precedence(&self) -> usize {
        match self {
            ArithmeticTree::Constant(_) => 0,
            ArithmeticTree::Reference(_) => 0,
            ArithmeticTree::Addition(_) => 1,
            ArithmeticTree::Subtraction(_, _) => 1,
            ArithmeticTree::Multiplication(_) => 2,
            ArithmeticTree::Division(_, _) => 2,
            ArithmeticTree::Exponent(_, _) => 3,
            ArithmeticTree::SquareRoot(_) => 5,
            ArithmeticTree::Negation(_) => 5,
            ArithmeticTree::Abs(_) => 5,
        }
    }
}

impl<T: Clone> ArithmeticTree<T> {
    /// Return a reference to the values within the leaf nodes of this tree.
    pub fn leaves(&self) -> Vec<ArithmeticTreeLeaf<T>> {
        match self {
            ArithmeticTree::Constant(constant) => {
                vec![ArithmeticTreeLeaf::Constant(constant.clone())]
            }
            ArithmeticTree::Reference(index) => vec![ArithmeticTreeLeaf::Reference(*index)],
            ArithmeticTree::Addition(subs) | ArithmeticTree::Multiplication(subs) => {
                subs.iter().flat_map(Self::leaves).collect()
            }
            ArithmeticTree::Subtraction(left, right)
            | ArithmeticTree::Division(left, right)
            | ArithmeticTree::Exponent(left, right) => {
                let mut result = left.leaves();
                result.extend(right.leaves());

                result
            }
            ArithmeticTree::SquareRoot(sub)
            | ArithmeticTree::Negation(sub)
            | ArithmeticTree::Abs(sub) => sub.leaves(),
        }
    }

    /// Return a mutable reference to the values within the leaf nodes of this tree.
    pub fn leaves_mut(&mut self) -> Vec<ArithmeticTreeLeafMut<T>> {
        match self {
            ArithmeticTree::Constant(constant) => vec![ArithmeticTreeLeafMut::Constant(constant)],
            ArithmeticTree::Reference(index) => vec![ArithmeticTreeLeafMut::Reference(index)],
            ArithmeticTree::Addition(subs) | ArithmeticTree::Multiplication(subs) => {
                subs.iter_mut().flat_map(Self::leaves_mut).collect()
            }
            ArithmeticTree::Subtraction(left, right)
            | ArithmeticTree::Division(left, right)
            | ArithmeticTree::Exponent(left, right) => {
                let mut result = left.leaves_mut();
                result.extend(right.leaves_mut());

                result
            }
            ArithmeticTree::SquareRoot(sub)
            | ArithmeticTree::Negation(sub)
            | ArithmeticTree::Abs(sub) => sub.leaves_mut(),
        }
    }

    /// Apply `function` to every leaf node of the tree
    /// and return a new [`ArithmeticTree`] possibly of a different type.
    pub fn map<O>(
        &self,
        function: &dyn Fn(ArithmeticTreeLeaf<T>) -> ArithmeticTreeLeaf<O>,
    ) -> ArithmeticTree<O>
    where
        O: Clone,
    {
        match self {
            ArithmeticTree::Constant(constant) => {
                match function(ArithmeticTreeLeaf::Constant(constant.clone())) {
                    ArithmeticTreeLeaf::Constant(new_constant) => {
                        ArithmeticTree::Constant(new_constant.clone())
                    }
                    ArithmeticTreeLeaf::Reference(new_reference) => {
                        ArithmeticTree::Reference(new_reference)
                    }
                }
            }
            ArithmeticTree::Reference(index) => {
                match function(ArithmeticTreeLeaf::Reference(*index)) {
                    ArithmeticTreeLeaf::Constant(new_constant) => {
                        ArithmeticTree::Constant(new_constant.clone())
                    }
                    ArithmeticTreeLeaf::Reference(new_reference) => {
                        ArithmeticTree::Reference(new_reference)
                    }
                }
            }
            ArithmeticTree::Addition(sub) => {
                ArithmeticTree::Addition(sub.iter().map(|s| s.map(function)).collect())
            }
            ArithmeticTree::Multiplication(sub) => {
                ArithmeticTree::Multiplication(sub.iter().map(|s| s.map(function)).collect())
            }
            ArithmeticTree::Subtraction(left, right) => {
                let left_mapped = left.map(function);
                let right_mapped = right.map(function);

                ArithmeticTree::Subtraction(Box::new(left_mapped), Box::new(right_mapped))
            }
            ArithmeticTree::Division(left, right) => {
                let left_mapped = left.map(function);
                let right_mapped = right.map(function);

                ArithmeticTree::Division(Box::new(left_mapped), Box::new(right_mapped))
            }
            ArithmeticTree::Exponent(left, right) => {
                let left_mapped = left.map(function);
                let right_mapped = right.map(function);

                ArithmeticTree::Exponent(Box::new(left_mapped), Box::new(right_mapped))
            }
            ArithmeticTree::SquareRoot(sub) => {
                ArithmeticTree::SquareRoot(Box::new(sub.map(function)))
            }
            ArithmeticTree::Negation(sub) => ArithmeticTree::Negation(Box::new(sub.map(function))),
            ArithmeticTree::Abs(sub) => ArithmeticTree::Abs(Box::new(sub.map(function))),
        }
    }

    /// Return the maximum index that is referenced by a leaf node.
    /// Returns `None` if all the leaf nodes are constants.
    pub fn maximum_reference(&self) -> Option<usize> {
        let mut result = None;

        for leaf in self.leaves() {
            if let ArithmeticTreeLeaf::Reference(index) = leaf {
                result = Some(result.map_or(index, |r| std::cmp::max(r, index)));
            }
        }

        result
    }
}

impl<T> ArithmeticTree<T>
where
    T: ArithmeticOperations,
{
    /// Recursively evaluate the expression represented by this tree.
    /// Returns `None` if the result is not defined
    /// or cannot be represented within its type.
    pub fn evaluate(&self, referenced_values: &[T]) -> Option<T> {
        match self {
            ArithmeticTree::Constant(constant) => Some(*constant),
            ArithmeticTree::Reference(index) => Some(referenced_values[*index]),
            ArithmeticTree::Addition(sub) => {
                let mut sum = T::zero();

                for value in sub.iter().map(|s| s.evaluate(referenced_values)) {
                    sum = sum.checked_add(&value?)?;
                }

                Some(sum)
            }
            ArithmeticTree::Subtraction(left, right) => {
                let value_left = left.evaluate(referenced_values)?;
                let value_right = right.evaluate(referenced_values)?;

                value_left.checked_sub(&value_right)
            }
            ArithmeticTree::Multiplication(sub) => {
                let mut prod = T::one();

                for value in sub.iter().map(|s| s.evaluate(referenced_values)) {
                    prod = prod.checked_mul(&value?)?;
                }

                Some(prod)
            }
            ArithmeticTree::Division(left, right) => {
                let value_left = left.evaluate(referenced_values)?;
                let value_right = right.evaluate(referenced_values)?;

                value_left.checked_div(&value_right)
            }
            ArithmeticTree::Exponent(left, right) => {
                let value_left = left.evaluate(referenced_values)?;
                let value_right = right.evaluate(referenced_values)?;

                value_left.checked_pow(value_right)
            }
            ArithmeticTree::SquareRoot(sub) => {
                let value_sub = sub.evaluate(referenced_values)?;

                value_sub.checked_sqrt()
            }
            ArithmeticTree::Negation(sub) => {
                let value_sub = sub.evaluate(referenced_values)?;

                value_sub.checked_neg()
            }
            ArithmeticTree::Abs(sub) => {
                let value_sub = sub.evaluate(referenced_values)?;

                if value_sub < T::zero() {
                    return value_sub.checked_neg();
                }

                Some(value_sub)
            }
        }
    }
}

impl<T> ArithmeticTree<T>
where
    T: Debug,
{
    fn ascii_tree(&self) -> ascii_tree::Tree {
        match self {
            ArithmeticTree::Constant(constant) => {
                ascii_tree::Tree::Leaf(vec![format!("{:?}", constant)])
            }
            ArithmeticTree::Reference(index) => {
                ascii_tree::Tree::Leaf(vec![format!("Ref({index})")])
            }
            ArithmeticTree::Addition(sub) => ascii_tree::Tree::Node(
                "Addition".to_string(),
                sub.iter().map(Self::ascii_tree).collect(),
            ),
            ArithmeticTree::Subtraction(left, right) => ascii_tree::Tree::Node(
                "Subtraction".to_string(),
                vec![left.ascii_tree(), right.ascii_tree()],
            ),
            ArithmeticTree::Multiplication(sub) => ascii_tree::Tree::Node(
                "Multiplication".to_string(),
                sub.iter().map(Self::ascii_tree).collect(),
            ),
            ArithmeticTree::Division(left, right) => ascii_tree::Tree::Node(
                "Division".to_string(),
                vec![left.ascii_tree(), right.ascii_tree()],
            ),
            ArithmeticTree::Exponent(left, right) => ascii_tree::Tree::Node(
                "Exponent".to_string(),
                vec![left.ascii_tree(), right.ascii_tree()],
            ),
            ArithmeticTree::SquareRoot(sub) => {
                ascii_tree::Tree::Node("Squareroot".to_string(), vec![sub.ascii_tree()])
            }
            ArithmeticTree::Negation(sub) => {
                ascii_tree::Tree::Node("Negation".to_string(), vec![sub.ascii_tree()])
            }
            ArithmeticTree::Abs(sub) => {
                ascii_tree::Tree::Node("Abs".to_string(), vec![sub.ascii_tree()])
            }
        }
    }
}

impl<T> ArithmeticTree<T>
where
    T: Display,
{
    fn format_braces_priority(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        subtree: &ArithmeticTree<T>,
    ) -> std::fmt::Result {
        let need_braces = self.precedence() > subtree.precedence() && !subtree.is_leaf();

        if need_braces {
            self.format_braces(f, subtree)
        } else {
            subtree.fmt(f)
        }
    }

    fn format_braces(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        subtree: &ArithmeticTree<T>,
    ) -> std::fmt::Result {
        f.write_str("(")?;
        subtree.fmt(f)?;
        f.write_str(")")
    }

    fn format_subtree_list(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        sub: &[ArithmeticTree<T>],
        delimiter: &str,
    ) -> std::fmt::Result {
        for (index, subtree) in sub.iter().enumerate() {
            self.format_braces_priority(f, subtree)?;

            if index < sub.len() - 1 {
                f.write_str(delimiter)?;
            }
        }

        Ok(())
    }

    fn format_binary(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        left: &ArithmeticTree<T>,
        right: &ArithmeticTree<T>,
        delimiter: &str,
    ) -> std::fmt::Result {
        self.format_braces_priority(f, left)?;
        f.write_str(delimiter)?;

        if right.is_leaf() {
            right.fmt(f)
        } else {
            self.format_braces(f, right)
        }
    }
}

impl<T> Debug for ArithmeticTree<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ascii_tree::write_tree(f, &self.ascii_tree())
    }
}

impl<T> Display for ArithmeticTree<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArithmeticTree::Constant(constant) => constant.fmt(f),
            ArithmeticTree::Reference(index) => f.write_fmt(format_args!("&{index}")),
            ArithmeticTree::Addition(sub) => self.format_subtree_list(f, sub, " + "),
            ArithmeticTree::Subtraction(left, right) => self.format_binary(f, left, right, " - "),
            ArithmeticTree::Multiplication(sub) => self.format_subtree_list(f, sub, " * "),
            ArithmeticTree::Division(left, right) => self.format_binary(f, left, right, " / "),
            ArithmeticTree::Exponent(left, right) => self.format_binary(f, left, right, " ^ "),
            ArithmeticTree::SquareRoot(sub) => {
                f.write_str("sqrt(")?;
                sub.fmt(f)?;
                f.write_str(")")
            }
            ArithmeticTree::Negation(sub) => {
                f.write_str("-")?;

                let need_braces = self.precedence() > sub.precedence() && !sub.is_leaf();
                if need_braces {
                    f.write_str("(")?;
                }

                sub.fmt(f)?;

                if need_braces {
                    f.write_str(")")?;
                }

                Ok(())
            }
            ArithmeticTree::Abs(sub) => {
                f.write_str("|")?;
                sub.fmt(f)?;
                f.write_str("|")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::ArithmeticTree;

    fn representative_tree() -> ArithmeticTree<i32> {
        // (|5 - (-2 + 10)| * 4 / 2) ^ sqrt(16) = 1296
        ArithmeticTree::Exponent(
            Box::new(ArithmeticTree::Division(
                Box::new(ArithmeticTree::Multiplication(vec![
                    ArithmeticTree::Abs(Box::new(ArithmeticTree::Subtraction(
                        Box::new(ArithmeticTree::Constant(5)),
                        Box::new(ArithmeticTree::Addition(vec![
                            ArithmeticTree::Negation(Box::new(ArithmeticTree::Constant(2))),
                            ArithmeticTree::Constant(10),
                        ])),
                    ))),
                    ArithmeticTree::Constant(4),
                ])),
                Box::new(ArithmeticTree::Constant(2)),
            )),
            Box::new(ArithmeticTree::SquareRoot(Box::new(
                ArithmeticTree::Constant(16),
            ))),
        )
    }

    #[test]
    fn display_arithmetic_tree() {
        assert_eq!(
            representative_tree().to_string(),
            "(|5 - (-2 + 10)| * 4 / 2) ^ (sqrt(16))"
        );
    }

    #[test]
    fn evaluate_tree() {
        assert_eq!(representative_tree().evaluate(&[]), Some(1296))
    }
}
