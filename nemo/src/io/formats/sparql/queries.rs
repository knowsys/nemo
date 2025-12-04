//! Manipulation of SPARQL queries.

use std::{
    collections::{HashMap, HashSet},
    iter::empty,
};

use nemo_physical::{
    datasources::bindings::Bindings,
    datavalues::{AnyDataValue, DataValue},
    tabular::filters::FilterTransformPattern,
};
use oxrdf::{Literal, NamedNode, Variable};
use spargebra::{
    Query,
    algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression},
    term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern},
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
    if bindings.is_empty() {
        return pattern.clone();
    }

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

#[derive(Debug, Default, Clone)]
struct PatternVariables {
    in_scope: HashSet<Variable>,
    hidden: HashSet<Variable>,
}

impl PatternVariables {
    fn new<T, V>(in_scope: T, hidden: V) -> Self
    where
        T: IntoIterator<Item = Variable>,
        V: IntoIterator<Item = Variable>,
    {
        Self {
            in_scope: in_scope.into_iter().collect(),
            hidden: hidden.into_iter().collect(),
        }
    }

    fn project(mut self, other: &Self) -> Self {
        self.hidden.extend(self.in_scope.drain());
        self.union(other)
    }

    fn project_onto<'a, T: IntoIterator<Item = &'a Variable>>(self, variables: T) -> Self {
        self.project(&PatternVariables::new(
            variables.into_iter().cloned(),
            empty(),
        ))
    }

    fn union(mut self, other: &Self) -> Self {
        self.hidden.extend(other.hidden.iter().cloned());
        self.in_scope.extend(other.in_scope.iter().cloned());

        self
    }
}

fn variables_in_pattern(pattern: &GraphPattern) -> PatternVariables {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let mut variables = Vec::new();
            let mut seen_variables = HashSet::new();

            for pattern in patterns {
                if let TermPattern::Variable(subject) = &pattern.subject
                    && !seen_variables.contains(subject)
                {
                    variables.push(subject.clone());
                    seen_variables.insert(subject.clone());
                }
                if let NamedNodePattern::Variable(predicate) = &pattern.predicate
                    && !seen_variables.contains(predicate)
                {
                    variables.push(predicate.clone());
                    seen_variables.insert(predicate.clone());
                }
                if let TermPattern::Variable(object) = &pattern.object
                    && !seen_variables.contains(object)
                {
                    variables.push(object.clone());
                    seen_variables.insert(object.clone());
                }
            }

            PatternVariables::new(variables, empty())
        }
        GraphPattern::Path {
            subject,
            path: _,
            object,
        } => {
            let mut variables = Vec::new();

            if let TermPattern::Variable(subject) = subject {
                variables.push(subject.clone());
            }
            if let TermPattern::Variable(object) = object {
                variables.push(object.clone());
            }
            variables.dedup();

            PatternVariables::new(variables, empty())
        }
        GraphPattern::Values {
            variables,
            bindings: _,
        } => PatternVariables::new(variables.iter().cloned(), empty()),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right } => {
            variables_in_pattern(left).union(&variables_in_pattern(right))
        }
        GraphPattern::Graph { name, inner } => {
            let mut result = variables_in_pattern(inner);
            if let NamedNodePattern::Variable(variable) = name
            {
                result.in_scope.insert(variable.clone());
            }

            result
        }
        GraphPattern::Extend {
            inner, variable, ..
        } => {
            let mut result = variables_in_pattern(inner);
                result.in_scope.insert(variable.clone());

            result
        }
        GraphPattern::Minus { left, right } => {
            variables_in_pattern(right).project(&variables_in_pattern(left))
        }
        GraphPattern::Project { inner, variables } => {
            variables_in_pattern(inner).project_onto(variables)
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => variables_in_pattern(inner).project_onto(
            variables
                .iter()
                .chain(aggregates.iter().map(|(variable, _)| variable)),
        ),
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Service { inner, .. }
        | GraphPattern::Filter { inner, .. } => variables_in_pattern(inner),
    }
}

fn rename_variables<'a, T: IntoIterator<Item = &'a Variable>>(
    variables: T,
    mapping: &HashMap<Variable, Variable>,
) -> impl Iterator<Item = Variable> {
    variables.into_iter().map(|variable| {
        mapping
            .get(variable)
            .expect("mapping should be total")
            .clone()
    })
}

fn rename_aggregates<'a, T: IntoIterator<Item = &'a (Variable, AggregateExpression)>>(
    aggregates: T,
    mapping: &HashMap<Variable, Variable>,
) -> impl Iterator<Item = (Variable, AggregateExpression)> {
    aggregates.into_iter().map(|(variable, expression)| {
        (
            mapping
                .get(variable)
                .expect("mapping should be total")
                .clone(),
            match expression {
                AggregateExpression::CountSolutions { .. } => expression.clone(),
                AggregateExpression::FunctionCall {
                    name,
                    expr,
                    distinct,
                } => AggregateExpression::FunctionCall {
                    name: name.clone(),
                    expr: rename_in_expression(expr, mapping),
                    distinct: *distinct,
                },
            },
        )
    })
}

fn rename_in_term_pattern(
    term: &TermPattern,
    mapping: &HashMap<Variable, Variable>,
) -> TermPattern {
    match term {
        TermPattern::Variable(variable) => TermPattern::Variable(
            mapping
                .get(variable)
                .expect("mapping should be total")
                .clone(),
        ),
        _ => term.clone(),
    }
}

fn rename_in_named_node_pattern(
    pattern: &NamedNodePattern,
    mapping: &HashMap<Variable, Variable>,
) -> NamedNodePattern {
    match pattern {
        NamedNodePattern::Variable(variable) => NamedNodePattern::Variable(
            mapping
                .get(variable)
                .expect("mapping should be total")
                .clone(),
        ),
        _ => pattern.clone(),
    }
}

fn rename_in_expression(
    expression: &Expression,
    mapping: &HashMap<Variable, Variable>,
) -> Expression {
    match expression {
        Expression::NamedNode(_) | Expression::Literal(_) => expression.clone(),
        Expression::Variable(variable) => Expression::Variable(
            mapping
                .get(variable)
                .expect("mapping should be total")
                .clone(),
        ),
        Expression::Or(expression, expression1) => Expression::Or(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::And(expression, expression1) => Expression::And(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::Equal(expression, expression1) => Expression::Equal(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::SameTerm(expression, expression1) => Expression::SameTerm(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::Greater(expression, expression1) => Expression::Greater(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::GreaterOrEqual(expression, expression1) => Expression::GreaterOrEqual(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::Less(expression, expression1) => Expression::Less(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::LessOrEqual(expression, expression1) => Expression::LessOrEqual(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::In(expression, expressions) => Expression::In(
            Box::new(rename_in_expression(expression, mapping)),
            expressions
                .iter()
                .map(|expression| rename_in_expression(expression, mapping))
                .collect(),
        ),
        Expression::Add(expression, expression1) => Expression::Add(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::Subtract(expression, expression1) => Expression::Subtract(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::Multiply(expression, expression1) => Expression::Multiply(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::Divide(expression, expression1) => Expression::Divide(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
        ),
        Expression::UnaryPlus(expression) => {
            Expression::UnaryPlus(Box::new(rename_in_expression(expression, mapping)))
        }
        Expression::UnaryMinus(expression) => {
            Expression::UnaryMinus(Box::new(rename_in_expression(expression, mapping)))
        }
        Expression::Not(expression) => {
            Expression::Not(Box::new(rename_in_expression(expression, mapping)))
        }
        Expression::Exists(graph_pattern) => {
            Expression::Exists(Box::new(rename_in_graph_pattern(graph_pattern, mapping)))
        }
        Expression::Bound(variable) => Expression::Bound(
            mapping
                .get(variable)
                .expect("mapping should be total")
                .clone(),
        ),
        Expression::If(expression, expression1, expression2) => Expression::If(
            Box::new(rename_in_expression(expression, mapping)),
            Box::new(rename_in_expression(expression1, mapping)),
            Box::new(rename_in_expression(expression2, mapping)),
        ),
        Expression::Coalesce(expressions) => Expression::Coalesce(
            expressions
                .iter()
                .map(|expression| rename_in_expression(expression, mapping))
                .collect(),
        ),
        Expression::FunctionCall(function, expressions) => Expression::FunctionCall(
            function.clone(),
            expressions
                .iter()
                .map(|expression| rename_in_expression(expression, mapping))
                .collect(),
        ),
    }
}

fn rename_in_graph_pattern(
    pattern: &GraphPattern,
    mapping: &HashMap<Variable, Variable>,
) -> GraphPattern {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let mut result = Vec::new();

            for pattern in patterns {
                result.push(TriplePattern {
                    subject: rename_in_term_pattern(&pattern.subject, mapping),
                    predicate: rename_in_named_node_pattern(&pattern.predicate, mapping),
                    object: rename_in_term_pattern(&pattern.object, mapping),
                });
            }

            GraphPattern::Bgp { patterns: result }
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => GraphPattern::Path {
            subject: rename_in_term_pattern(subject, mapping),
            path: path.clone(),
            object: rename_in_term_pattern(object, mapping),
        },
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rename_in_graph_pattern(left, mapping)),
            right: Box::new(rename_in_graph_pattern(right, mapping)),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(rename_in_graph_pattern(left, mapping)),
            right: Box::new(rename_in_graph_pattern(right, mapping)),
            expression: expression
                .clone()
                .map(|expression| rename_in_expression(&expression, mapping)),
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr: rename_in_expression(expr, mapping),
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rename_in_graph_pattern(left, mapping)),
            right: Box::new(rename_in_graph_pattern(right, mapping)),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name: rename_in_named_node_pattern(name, mapping),
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
            variable: mapping
                .get(variable)
                .expect("mapping should be total")
                .clone(),
            expression: rename_in_expression(expression, mapping),
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(rename_in_graph_pattern(left, mapping)),
            right: Box::new(rename_in_graph_pattern(right, mapping)),
        },
        GraphPattern::Values {
            variables,
            bindings,
        } => GraphPattern::Values {
            variables: rename_variables(variables, mapping).collect(),
            bindings: bindings.clone(),
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
            expression: expression
                .iter()
                .map(|expression| match expression {
                    OrderExpression::Asc(expression) => {
                        OrderExpression::Asc(rename_in_expression(expression, mapping))
                    }
                    OrderExpression::Desc(expression) => {
                        OrderExpression::Desc(rename_in_expression(expression, mapping))
                    }
                })
                .collect(),
        },
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
            variables: rename_variables(variables, mapping).collect(),
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
            start: *start,
            length: *length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
            variables: rename_variables(variables, mapping).collect(),
            aggregates: rename_aggregates(aggregates, mapping).collect(),
        },
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => GraphPattern::Service {
            name: name.clone(),
            inner: Box::new(rename_in_graph_pattern(inner, mapping)),
            silent: *silent,
        },
    }
}

fn bind_constants(
    pattern: &GraphPattern,
    variables: &[Variable],
    constants: &HashMap<usize, GroundTerm>,
) -> GraphPattern {
    let mut constant_variables = Vec::new();
    let mut constant_terms = Vec::new();

    for (idx, term) in constants {
        constant_variables.push(variables[*idx].clone());
        constant_terms.push(Some(term.clone()));
    }

    if constant_terms.is_empty() {
        return pattern.clone();
    }

    GraphPattern::Join {
        left: Box::new(pattern.clone()),
        right: Box::new(GraphPattern::Values {
            variables: constant_variables,
            bindings: vec![constant_terms],
        }),
    }
}

fn rename_variables_in_project_pattern(
    pattern: &GraphPattern,
    mapping: &HashMap<Variable, Variable>,
    constants: &HashMap<usize, GroundTerm>,
) -> Option<(GraphPattern, Vec<Variable>)> {
    match pattern {
        GraphPattern::Project { inner, variables } => {
            let inner = rename_in_graph_pattern(inner, mapping);
            let renamed_variables = rename_variables(variables, mapping).collect::<Vec<_>>();

            Some((
                bind_constants(&inner, &renamed_variables, constants),
                renamed_variables
                    .into_iter()
                    .enumerate()
                    .filter_map(|(idx, variable)| {
                        if constants.contains_key(&idx) {
                            None
                        } else {
                            Some(variable)
                        }
                    })
                    .collect(),
            ))
        }
        _ => None,
    }
}

fn standardize_variables(
    prefix: &str,
    variables: &PatternVariables,
    join_variables: &[Variable],
) -> HashMap<Variable, Variable> {
    let mut result = HashMap::new();

    for (count, variable) in join_variables.iter().enumerate() {
        if result.contains_key(variable) {
            continue;
        }

        result.insert(
            variable.clone(),
            Variable::new_unchecked(format!("j{count}")),
        );
        // TODO: what if we hit the `Label`-suffixed version first?
        result.insert(
            Variable::new_unchecked(format!("{}Label", variable.as_str())),
            Variable::new_unchecked(format!("j{count}Label")),
        );
    }

    let mut count = 0;

    for variable in variables.in_scope.iter().chain(variables.hidden.iter()) {
        if result.contains_key(variable) {
            continue;
        }

        result.insert(
            variable.clone(),
            Variable::new_unchecked(format!("{prefix}{count}")),
        );
        // TODO: what if we hit the `Label`-suffixed version first?
        result.insert(
            Variable::new_unchecked(format!("{}Label", variable.as_str())),
            Variable::new_unchecked(format!("{prefix}{count}Label")),
        );
        count += 1;
    }

    result
}

pub(crate) fn merge_project_patterns(
    left: &GraphPattern,
    right: &GraphPattern,
    join_positions: &[(usize, usize)],
    left_constants: &HashMap<usize, GroundTerm>,
    right_constants: &HashMap<usize, GroundTerm>,
) -> Option<GraphPattern> {
    match (left, right) {
        (
            GraphPattern::Project {
                variables: left_project,
                ..
            },
            GraphPattern::Project {
                variables: right_project,
                ..
            },
        ) => {
            let left_variables = variables_in_pattern(left);
            let right_variables = variables_in_pattern(right);

            let mut left_join_variables = Vec::new();
            let mut right_join_variables = Vec::new();

            for (l, r) in join_positions {
                left_join_variables.push(left_project[*l].clone());
                right_join_variables.push(right_project[*r].clone());
            }

            let left_mapping = standardize_variables("l", &left_variables, &left_join_variables);
            let right_mapping = standardize_variables("r", &right_variables, &right_join_variables);

            let (left_pattern, left_variables) =
                rename_variables_in_project_pattern(left, &left_mapping, left_constants)?;
            let (right_pattern, right_variables) =
                rename_variables_in_project_pattern(right, &right_mapping, right_constants)?;

            let inner = Box::new(GraphPattern::Join {
                left: Box::new(left_pattern),
                right: Box::new(right_pattern),
            });

            let mut variables = left_variables;
            let join_variables =
                rename_variables(&left_join_variables, &left_mapping).collect::<HashSet<_>>();
            for variable in right_variables {
                if !join_variables.contains(&variable) {
                    variables.push(variable);
                }
            }

            Some(GraphPattern::Project { inner, variables })
        }
        _ => None,
    }
}

pub(crate) fn merge_queries(
    left: &Query,
    right: &Query,
    join_positions: &[(usize, usize)],
    left_constants: &HashMap<usize, GroundTerm>,
    right_constants: &HashMap<usize, GroundTerm>,
) -> Option<Query> {
    match (left, right) {
        (
            Query::Select {
                dataset: dataset_left,
                pattern: pattern_left,
                base_iri: base_left,
            },
            Query::Select {
                dataset: dataset_right,
                pattern: pattern_right,
                base_iri: base_right,
            },
        ) if dataset_left == dataset_right && base_left == base_right => Some(Query::Select {
            dataset: dataset_left.clone(),
            pattern: merge_project_patterns(
                pattern_left,
                pattern_right,
                join_positions,
                left_constants,
                right_constants,
            )?,
            base_iri: base_left.clone(),
        }),
        (
            Query::Ask {
                dataset: dataset_left,
                pattern: pattern_left,
                base_iri: base_left,
            },
            Query::Ask {
                dataset: dataset_right,
                pattern: pattern_right,
                base_iri: base_right,
            },
        ) if dataset_left == dataset_right && base_left == base_right => Some(Query::Ask {
            dataset: dataset_left.clone(),
            pattern: merge_project_patterns(
                pattern_left,
                pattern_right,
                join_positions,
                left_constants,
                right_constants,
            )?,
            base_iri: base_left.clone(),
        }),
        _ => None,
    }
}

pub(crate) fn push_constants_in_project_pattern(
    pattern: &GraphPattern,
    constants: &HashMap<usize, GroundTerm>,
) -> Option<GraphPattern> {
    match pattern {
        GraphPattern::Project { .. } => {
            let mapping = standardize_variables("v", &variables_in_pattern(pattern), &[]);
            let (inner, variables) =
                rename_variables_in_project_pattern(pattern, &mapping, constants)?;

            Some(GraphPattern::Project {
                inner: Box::new(inner),
                variables,
            })
        }
        _ => None,
    }
}

pub(crate) fn push_constants(query: &Query, constants: &HashMap<usize, GroundTerm>) -> Query {
    match query {
        Query::Select {
            dataset,
            pattern,
            base_iri,
        } => Query::Select {
            dataset: dataset.clone(),
            pattern: push_constants_in_project_pattern(pattern, constants)
                .unwrap_or_else(|| pattern.clone()),
            base_iri: base_iri.clone(),
        },
        Query::Ask {
            dataset,
            pattern,
            base_iri,
        } => Query::Ask {
            dataset: dataset.clone(),
            pattern: push_constants_in_project_pattern(pattern, constants)
                .unwrap_or_else(|| pattern.clone()),
            base_iri: base_iri.clone(),
        },
        _ => query.clone(),
    }
}


pub(crate) fn negate_project_pattern(pattern: &GraphPattern) -> GraphPattern {
    modify_outermost_projection(pattern, |pattern, variables| {
        (Box::new(GraphPattern::Filter { expr: Expression::Not(Box::new(Expression::Exists(pattern.clone()))), inner: Box::new(GraphPattern::default())}), variables.clone())
    })
}


pub(crate) fn negate_query(query: &Query) -> Query {
    match query {
        Query::Select {
            dataset,
            pattern,
            base_iri,
        } => Query::Select {
            dataset: dataset.clone(),
            pattern: negate_project_pattern(pattern),
            base_iri: base_iri.clone(),
        },
        Query::Ask {
            dataset,
            pattern,
            base_iri,
        } => Query::Ask {
            dataset: dataset.clone(),
            pattern: negate_project_pattern(pattern),
            base_iri: base_iri.clone(),
        },
        _ => query.clone(),
    }
}
