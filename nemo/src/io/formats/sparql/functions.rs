//! Mapping from [FunctionTree] to SPARQL functions.

use nemo_physical::{
    datavalues::{AnyDataValue, DataValue, ValueDomain},
    function::{
        definitions::{
            BinaryFunctionEnum, NaryFunctionEnum, TernaryFunctionEnum, UnaryFunctionEnum,
        },
        tree::{FunctionLeaf, FunctionTree},
    },
    tabular::operations::OperationColumnMarker,
};
use oxrdf::{Literal, NamedNode};
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

fn try_expression_from_value(value: &AnyDataValue) -> Option<Expression> {
    match value.value_domain() {
        ValueDomain::PlainString => Some(Expression::Literal(Literal::new_simple_literal(
            value.to_plain_string_unchecked(),
        ))),
        ValueDomain::LanguageTaggedString => {
            let (string, tag) = value.to_language_tagged_string_unchecked();
            Some(Expression::Literal(
                Literal::new_language_tagged_literal_unchecked(string, tag),
            ))
        }
        ValueDomain::Iri => Some(Expression::NamedNode(NamedNode::new_unchecked(
            value.to_iri_unchecked(),
        ))),
        ValueDomain::Float
        | ValueDomain::Double
        | ValueDomain::UnsignedLong
        | ValueDomain::NonNegativeLong
        | ValueDomain::UnsignedInt
        | ValueDomain::NonNegativeInt
        | ValueDomain::Long
        | ValueDomain::Int
        | ValueDomain::Boolean => Some(Expression::Literal(Literal::new_simple_literal(
            value.lexical_value(),
        ))),
        ValueDomain::Tuple | ValueDomain::Map | ValueDomain::Null | ValueDomain::Other => None,
    }
}

fn try_expression_from_leaf(
    variables: &[Variable],
    leaf: &FunctionLeaf<Marker>,
) -> Option<Expression> {
    match leaf {
        FunctionLeaf::Constant(value) => try_expression_from_value(value),
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
    let lhs = Box::new(try_expression_from_tree(variables, left)?);
    let rhs = Box::new(try_expression_from_tree(variables, right)?);

    match function {
        BinaryFunctionEnum::Equals(_) => Some(Expression::Equal(lhs, rhs)),
        BinaryFunctionEnum::Unequals(_) => {
            Some(Expression::Not(Box::new(Expression::Equal(lhs, rhs))))
        }
        // TODO(mam): actually check which functions we can support here
        _ => None,
    }
}

fn try_expression_from_ternary(
    _variables: &[Variable],
    _function: &TernaryFunctionEnum,
    _first: &Tree,
    _second: &Tree,
    _third: &Tree,
) -> Option<Expression> {
    // TODO(mam): actually check which functions we can support here
    None
}

fn try_expression_from_nary(
    _variables: &[Variable],
    _function: &NaryFunctionEnum,
    _parameters: &Vec<Tree>,
) -> Option<Expression> {
    // TODO(mam): actually check which functions we can support here
    None
}
