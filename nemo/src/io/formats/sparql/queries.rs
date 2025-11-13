//! Manipulation of SPARQL queries.

use nemo_physical::{
    datasources::bindings::Bindings,
    datavalues::{AnyDataValue, DataValue},
    tabular::filters::FilterTransformPattern,
};
use oxrdf::{Literal, NamedNode, Variable};
use spargebra::{
    algebra::{Expression, GraphPattern},
    term::GroundTerm,
};

use super::functions::try_expression_from_tree;

pub(crate) fn ground_term_from_datavalue(value: &AnyDataValue) -> Option<GroundTerm> {
    use nemo_physical::datavalues::ValueDomain;

    match value.value_domain() {
        ValueDomain::PlainString => Some(GroundTerm::Literal(Literal::new_simple_literal(
            value.to_plain_string_unchecked(),
        ))),
        ValueDomain::LanguageTaggedString => {
            let (content, language) = value.to_language_tagged_string_unchecked();
            Some(GroundTerm::Literal(
                Literal::new_language_tagged_literal(content, language).expect("should be valid"),
            ))
        }
        ValueDomain::Iri => Some(GroundTerm::NamedNode(NamedNode::new_unchecked(
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
        | ValueDomain::Boolean
        | ValueDomain::Other => Some(GroundTerm::Literal(Literal::new_typed_literal(
            value.lexical_value(),
            NamedNode::new_unchecked(value.datatype_iri()),
        ))),
        ValueDomain::Null => None,
        ValueDomain::Tuple | ValueDomain::Map => {
            unimplemented!("no support for complex values yet")
        }
    }
}

pub(crate) fn pattern_with_bindings(pattern: &GraphPattern, bindings: &[Bindings]) -> GraphPattern {
    modify_outermost_projection(pattern, |inner, variables| {
        let mut previous = inner.clone();

        for bindings_set in bindings {
            let bound_variables = bindings_set
                .positions()
                .iter()
                .map(|idx| variables[*idx].clone())
                .collect::<Vec<_>>();
            let bindings = bindings_set
                .bindings()
                .iter()
                .map(|row| row.iter().map(ground_term_from_datavalue).collect())
                .collect();

            let values = GraphPattern::Values {
                variables: bound_variables,
                bindings,
            };
            *previous = GraphPattern::Join {
                left: previous.clone(),
                right: Box::new(values),
            };
        }

        (previous, variables.clone())
    })
}

pub(crate) fn pattern_with_filters(
    pattern: &GraphPattern,
    patterns: &[FilterTransformPattern],
) -> (GraphPattern, Vec<FilterTransformPattern>) {
    let remaining_patterns = Vec::new();

    let pattern = modify_outermost_projection(pattern, |inner, variables| {
        let mut expressions = Vec::new();
        let mut remaining_patterns = Vec::new();

        for pattern in patterns {
            if let Some(expression) = try_expression_from_tree(variables, pattern.filter_function())
            {
                expressions.push(expression);
            } else {
                remaining_patterns.push(pattern.clone());
            }
        }

        let pattern = match expressions
            .into_iter()
            .reduce(|lhs, rhs| Expression::And(Box::new(lhs), Box::new(rhs)))
        {
            None => inner.clone(),
            Some(filter) => Box::new(GraphPattern::Filter {
                expr: filter,
                inner: inner.clone(),
            }),
        };

        (pattern, variables.to_vec())
    });

    (pattern, remaining_patterns)
}

pub(crate) fn modify_outermost_projection<'pattern, Modify>(
    pattern: &'pattern GraphPattern,
    modify: Modify,
) -> GraphPattern
where
    Modify: FnOnce(
        &'pattern Box<GraphPattern>,
        &'pattern Vec<Variable>,
    ) -> (Box<GraphPattern>, Vec<Variable>),
{
    match pattern {
        GraphPattern::Project { inner, variables } => {
            let (inner, variables) = modify(inner, variables);
            GraphPattern::Project { inner, variables }
        }
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(modify_outermost_projection(inner, modify)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(modify_outermost_projection(inner, modify)),
        },
        &GraphPattern::Slice {
            ref inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(modify_outermost_projection(inner, modify)),
            start,
            length,
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(modify_outermost_projection(inner, modify)),
            expression: expression.clone(),
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(modify_outermost_projection(inner, modify)),
            variables: variables.clone(),
            aggregates: aggregates.clone(),
        },
        _ => pattern.clone(),
    }
}
