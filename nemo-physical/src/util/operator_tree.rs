//! Module containing a data structure for representing mathematical terms.

use std::fmt::{Debug, Display};

/// Tree representing a mathematical term.
#[derive(Clone)]
pub enum OperationTree<TypeConstant, TypeVariable> {
    /// A variable in the computation
    Variable(TypeVariable),
    /// A constant in the computation
    Constant(TypeConstant),
    /// Value is the sum of the values of the two subtrees.
    Addition(
        Box<OperationTree<TypeConstant, TypeVariable>>,
        Box<OperationTree<TypeConstant, TypeVariable>>,
    ),
    /// Value is the difference of the value of the two subtrees.
    Subtraction(
        Box<OperationTree<TypeConstant, TypeVariable>>,
        Box<OperationTree<TypeConstant, TypeVariable>>,
    ),
    /// Value is the product of the values of the two subtrees.
    Multiplication(
        Box<OperationTree<TypeConstant, TypeVariable>>,
        Box<OperationTree<TypeConstant, TypeVariable>>,
    ),
    /// Value is the quotient of the values of the  two subtrees.
    Division(
        Box<OperationTree<TypeConstant, TypeVariable>>,
        Box<OperationTree<TypeConstant, TypeVariable>>,
    ),
}

impl<TypeConstant, TypeVariable> OperationTree<TypeConstant, TypeVariable> {
    /// Create a new [`OperationTree`] which evaluates to the given variable.
    pub fn variable(variable: TypeVariable) -> Self {
        Self::Variable(variable)
    }

    /// Create a new [`OperationTree`] which evaluates to the given constant.
    pub fn constant(constant: TypeConstant) -> Self {
        Self::Constant(constant)
    }

    /// Create a new [`OperationTree`] which evaluates
    /// to the sum of the given [`OperationTree`].
    pub fn addition(
        left: OperationTree<TypeConstant, TypeVariable>,
        right: OperationTree<TypeConstant, TypeVariable>,
    ) -> Self {
        Self::Addition(Box::new(left), Box::new(right))
    }

    /// Create a new [`OperationTree`] which evaluates
    /// to the difference of the given [`OperationTree`].
    pub fn subtraction(
        left: OperationTree<TypeConstant, TypeVariable>,
        right: OperationTree<TypeConstant, TypeVariable>,
    ) -> Self {
        Self::Subtraction(Box::new(left), Box::new(right))
    }

    /// Create a new [`OperationTree`] which evaluates
    /// to the product of the given [`OperationTree`].
    pub fn multiplication(
        left: OperationTree<TypeConstant, TypeVariable>,
        right: OperationTree<TypeConstant, TypeVariable>,
    ) -> Self {
        Self::Multiplication(Box::new(left), Box::new(right))
    }

    /// Create a new [`OperationTree`] which evaluates
    /// to the quotient of the given [`OperationTree`].
    pub fn division(
        left: OperationTree<TypeConstant, TypeVariable>,
        right: OperationTree<TypeConstant, TypeVariable>,
    ) -> Self {
        Self::Division(Box::new(left), Box::new(right))
    }
}

impl<TypeConstant, TypeVariable> Debug for OperationTree<TypeConstant, TypeVariable>
where
    TypeConstant: Debug,
    TypeVariable: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Variable(variable) => write!(f, "{variable:?}"),
            Self::Constant(constant) => write!(f, "{constant:?}"),
            Self::Addition(left, right) => write!(f, "({:?} + {:?})", left, right),
            Self::Subtraction(left, right) => write!(f, "({:?} - {:?})", left, right),
            Self::Multiplication(left, right) => write!(f, "({:?} * {:?})", left, right),
            Self::Division(left, right) => write!(f, "({:?} / {:?})", left, right),
        }
    }
}

impl<TypeConstant, TypeVariable> Display for OperationTree<TypeConstant, TypeVariable>
where
    TypeConstant: Display,
    TypeVariable: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Variable(variable) => write!(f, "{variable}"),
            Self::Constant(constant) => write!(f, "{constant}"),
            Self::Addition(left, right) => write!(f, "({} + {})", left, right),
            Self::Subtraction(left, right) => write!(f, "({} - {})", left, right),
            Self::Multiplication(left, right) => write!(f, "({} * {})", left, right),
            Self::Division(left, right) => write!(f, "({} / {})", left, right),
        }
    }
}

impl<TypeConstant, TypeVariable> OperationTree<TypeConstant, TypeVariable> {
    fn variables_recursive(tree: &OperationTree<TypeConstant, TypeVariable>) -> Vec<&TypeVariable> {
        let mut result = Vec::<&TypeVariable>::new();

        result.extend(match tree {
            OperationTree::Variable(variable) => vec![variable],
            OperationTree::Constant(_) => vec![],
            OperationTree::Addition(left, right)
            | OperationTree::Subtraction(left, right)
            | OperationTree::Multiplication(left, right)
            | OperationTree::Division(left, right) => {
                let mut sub_vector = Vec::<&TypeVariable>::new();
                sub_vector.extend(Self::variables_recursive(left));
                sub_vector.extend(Self::variables_recursive(right));

                sub_vector
            }
        });

        result
    }

    /// Return a list with references to all variables occuring in the tree.
    pub fn variables(&self) -> Vec<&TypeVariable> {
        Self::variables_recursive(self)
    }

    fn variables_mut_recursive(
        tree: &mut OperationTree<TypeConstant, TypeVariable>,
    ) -> Vec<&mut TypeVariable> {
        let mut result = Vec::<&mut TypeVariable>::new();

        result.extend(match tree {
            OperationTree::Variable(variable) => vec![variable],
            OperationTree::Constant(_) => vec![],
            OperationTree::Addition(left, right)
            | OperationTree::Subtraction(left, right)
            | OperationTree::Multiplication(left, right)
            | OperationTree::Division(left, right) => {
                let mut sub_vector = Vec::<&mut TypeVariable>::new();
                sub_vector.extend(Self::variables_mut_recursive(left));
                sub_vector.extend(Self::variables_mut_recursive(right));

                sub_vector
            }
        });

        result
    }

    /// Return a list with a mutable reference to all variables occuring in the tree.
    pub fn variables_mut(&mut self) -> Vec<&mut TypeVariable> {
        Self::variables_mut_recursive(self)
    }
}
