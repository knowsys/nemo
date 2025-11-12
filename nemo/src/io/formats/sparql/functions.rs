//! Mapping from [FunctionTree] to SPARQL functions.

use nemo_physical::{
    function::{
        definitions::{
            BinaryFunctionEnum, NaryFunctionEnum, TernaryFunctionEnum, UnaryFunctionEnum,
        },
        tree::{FunctionLeaf, FunctionTree},
    },
    tabular::operations::OperationColumnMarker,
};
use spargebra::{
    algebra::{Expression, Function},
    term::Variable,
};

type Marker = OperationColumnMarker;
type Tree = FunctionTree<Marker>;

/// Try to create a SPARQL FILTER expression from the given function tree.
pub(crate) fn try_expression_from_tree(
    variables: &[Variable],
    function: &Tree,
) -> Option<Expression> {
    // TODO(mam): actually check which functions we can support here
    match function {
        Tree::Leaf(leaf) => try_expression_from_leaf(variables, leaf),
        Tree::Unary(function, tree) => try_expression_from_unary(variables, function, tree),
        Tree::Binary {
            function,
            left,
            right,
        } => try_expression_from_binary(variables, function, left, right),
        Tree::Ternary {
            function,
            first,
            second,
            third,
        } => try_expression_from_ternary(variables, function, first, second, third),
        Tree::Nary {
            function,
            parameters,
        } => try_expression_from_nary(variables, function, parameters),
    }
}

fn try_expression_from_leaf(
    variables: &[Variable],
    leaf: &FunctionLeaf<Marker>,
) -> Option<Expression> {
    match leaf {
        FunctionLeaf::Constant(_) => None,
        FunctionLeaf::Reference(reference) => {
            Some(Expression::Variable(variables[reference.0].clone()))
        }
    }
}

fn try_expression_from_unary(
    variables: &[Variable],
    function: &UnaryFunctionEnum,
    tree: &Tree,
) -> Option<Expression> {
    let value = try_expression_from_tree(variables, tree)?;
    let function = match function {
        UnaryFunctionEnum::Datatype(_) => Some(Function::Datatype),
        UnaryFunctionEnum::LanguageTag(_) => Some(Function::Lang),
        // TODO(mam): actually check which functions we can support here
        _ => None,
    }?;

    Some(Expression::FunctionCall(function, vec![value]))
}

fn try_expression_from_binary(
    variables: &[Variable],
    function: &BinaryFunctionEnum,
    left: &Tree,
    right: &Tree,
) -> Option<Expression> {
    // TODO(mam): actually check which functions we can support here
    None
}

fn try_expression_from_ternary(
    variables: &[Variable],
    function: &TernaryFunctionEnum,
    first: &Tree,
    second: &Tree,
    third: &Tree,
) -> Option<Expression> {
    // TODO(mam): actually check which functions we can support here
    None
}

fn try_expression_from_nary(
    variables: &[Variable],
    function: &NaryFunctionEnum,
    parameters: &Vec<Tree>,
) -> Option<Expression> {
    // TODO(mam): actually check which functions we can support here
    None
}
