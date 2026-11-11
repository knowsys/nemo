//! Analyses of a query's algebra:
//! what its shape allows, and where values and filters are added to it.

use std::collections::HashSet;

use spargebra::{
    algebra::{AggregateExpression, AggregateFunction, Expression, Function, GraphPattern},
    term::{NamedNodePattern, TermPattern, TriplePattern, Variable},
};

/// The shape of a query, which says whether and where values and filters can be added to it.
///
/// A `VALUES` block or a `FILTER` added directly under the outermost projection
/// selects among the query's final solutions.
/// That is sound if the modifiers above the projection commute with selection,
/// as `DISTINCT`, `REDUCED` and `ORDER BY` do, and if the solutions are one determined set.
/// - A `LIMIT` or `OFFSET` bounds the *whole* result,
///   so a selection in each request changes what the query means.
/// - A `LIMIT` without `ORDER BY` is a nondeterministic choice,
///   which no union of restricted choices reproduces.
/// - A nondeterministic function or a `SAMPLE` aggregate has the same problem.
///
/// Such queries are [opaque](Self::Opaque): they can be neither restricted nor merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryShape {
    /// A `SELECT` over a plain group pattern, which can be inlined into a merged query.
    ///
    /// It has no aggregation, no subquery, no `LIMIT` or `OFFSET`,
    /// and no part that binds only some solutions.
    Pattern,
    /// A query with an aggregation, a subquery, or a part that binds only some solutions,
    /// such as an `OPTIONAL`, a `UNION` or a `MINUS`.
    ///
    /// Bindings and filters are added above what the query computes,
    /// and a merge includes the whole query as a subquery.
    Complex,
    /// A query for which no selection on the final solutions gives the result of a restricted
    /// request.
    ///
    /// The string names the construct that makes the query opaque.
    Opaque(&'static str),
}

/// Returns the shape of a `SELECT` query's pattern.
pub(super) fn shape(pattern: &GraphPattern) -> QueryShape {
    if let Some(reason) = opaque(pattern) {
        return QueryShape::Opaque(reason);
    }

    match strip_modifiers(pattern) {
        GraphPattern::Project { inner, .. } if !has_inner_scope(inner) => QueryShape::Pattern,
        _ => QueryShape::Complex,
    }
}

/// Returns the construct that makes a query opaque, or `None` if the query is not opaque.
fn opaque(pattern: &GraphPattern) -> Option<&'static str> {
    let mut reason = None;
    visit_patterns(pattern, &mut |node| {
        if reason.is_some() {
            return;
        }

        match node {
            GraphPattern::Slice { start, length, .. } => {
                reason = Some(match (start, length) {
                    (0, Some(_)) => "LIMIT",
                    (_, None) => "OFFSET",
                    _ => "LIMIT/OFFSET",
                });
            }
            GraphPattern::Group { aggregates, .. }
                if aggregates.iter().any(|(_, aggregate)| {
                    matches!(
                        aggregate,
                        AggregateExpression::FunctionCall {
                            name: AggregateFunction::Sample,
                            ..
                        }
                    )
                }) =>
            {
                reason = Some("SAMPLE");
            }
            _ => {}
        }
    });
    if reason.is_some() {
        return reason;
    }

    visit_expressions(pattern, &mut |expression| {
        if reason.is_some() {
            return;
        }
        if let Expression::FunctionCall(function, _) = expression {
            let name = match function {
                Function::Rand => "RAND()",
                Function::Now => "NOW()",
                Function::Uuid => "UUID()",
                Function::StrUuid => "STRUUID()",
                Function::BNode => "BNODE()",
                _ => return,
            };
            reason = Some(name);
        }
    });
    reason
}

/// Returns the pattern under the solution modifiers that commute with selection.
pub(super) fn strip_modifiers(pattern: &GraphPattern) -> &GraphPattern {
    match pattern {
        GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::OrderBy { inner, .. } => strip_modifiers(inner),
        other => other,
    }
}

/// Returns whether a `SELECT` query's pattern groups or has a subquery
/// under its outermost projection.
pub(super) fn computes_in_full(pattern: &GraphPattern) -> bool {
    let inner = match strip_modifiers(pattern) {
        GraphPattern::Project { inner, .. } => inner,
        other => other,
    };
    let mut found = false;
    visit_patterns(inner, &mut |node| {
        found |= matches!(
            node,
            GraphPattern::Project { .. } | GraphPattern::Group { .. }
        );
    });
    found
}

/// Returns whether a pattern contains, anywhere inside, a subquery, a grouping,
/// or a part whose variables only some solutions bind,
/// such as an `OPTIONAL`, a `UNION` or a `MINUS`.
///
/// Substituting a term into such a part, or sharing a variable with it,
/// would change what the query means,
/// so the whole query enters a merge as a subquery instead.
fn has_inner_scope(pattern: &GraphPattern) -> bool {
    let mut found = false;
    visit_patterns(pattern, &mut |node| {
        found |= matches!(
            node,
            GraphPattern::Project { .. }
                | GraphPattern::Group { .. }
                | GraphPattern::LeftJoin { .. }
                | GraphPattern::Union { .. }
                | GraphPattern::Minus { .. }
        );
    });
    found
}

/// Applies a rewrite directly under the outermost projection, keeping the modifiers above it.
///
/// The rewrite applies under the `ORDER BY` that the algebra keeps there,
/// and under the filters at the top of the pattern.
/// A selection commutes with these filters,
/// and their `NOT EXISTS` forms must see what a `VALUES` block binds.
/// A pattern without a projection is rewritten whole.
pub(super) fn under_projection(
    pattern: GraphPattern,
    rewrite: impl FnOnce(GraphPattern) -> GraphPattern,
) -> GraphPattern {
    /// Applies a rewrite under the filters at the top of a pattern.
    fn under_filters(
        pattern: GraphPattern,
        rewrite: impl FnOnce(GraphPattern) -> GraphPattern,
    ) -> GraphPattern {
        match pattern {
            GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
                expr,
                inner: Box::new(under_filters(*inner, rewrite)),
            },
            other => rewrite(other),
        }
    }
    match pattern {
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(match *inner {
                GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
                    inner: Box::new(under_filters(*inner, rewrite)),
                    expression,
                },
                other => under_filters(other, rewrite),
            }),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(under_projection(*inner, rewrite)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(under_projection(*inner, rewrite)),
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(under_projection(*inner, rewrite)),
            expression,
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(under_projection(*inner, rewrite)),
            start,
            length,
        },
        other => rewrite(other),
    }
}

/// Returns the variables that every solution of a pattern binds.
///
/// They are the variables that its mandatory parts match, and not those that an `OPTIONAL`,
/// one `UNION` branch, a `BIND` or a `VALUES` row may leave unbound.
pub(super) fn always_bound(pattern: &GraphPattern) -> HashSet<String> {
    match pattern {
        GraphPattern::Bgp { patterns } => triple_variables(patterns),
        GraphPattern::Path {
            subject, object, ..
        } => path_variables(subject, object),
        GraphPattern::Join { left, right } => {
            let mut bound = always_bound(left);
            bound.extend(always_bound(right));
            bound
        }
        GraphPattern::Union { left, right } => {
            let right = always_bound(right);
            always_bound(left)
                .into_iter()
                .filter(|variable| right.contains(variable))
                .collect()
        }
        GraphPattern::LeftJoin { left, .. } | GraphPattern::Minus { left, .. } => {
            always_bound(left)
        }
        GraphPattern::Graph { name, inner } => {
            let mut bound = always_bound(inner);
            if let NamedNodePattern::Variable(variable) = name {
                bound.insert(variable.as_str().to_owned());
            }

            bound
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => variables
            .iter()
            .enumerate()
            .filter(|(index, _)| bindings.iter().all(|row| row[*index].is_some()))
            .map(|(_, variable)| variable.as_str().to_owned())
            .collect(),
        GraphPattern::Project { inner, variables }
        | GraphPattern::Group {
            inner, variables, ..
        } => {
            let inner = always_bound(inner);
            variables
                .iter()
                .map(|variable| variable.as_str().to_owned())
                .filter(|variable| inner.contains(variable))
                .collect()
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Service { inner, .. } => always_bound(inner),
    }
}

/// Returns the variables of the triple patterns of a basic graph pattern.
fn triple_variables(patterns: &[TriplePattern]) -> HashSet<String> {
    patterns
        .iter()
        .flat_map(|triple| {
            let predicate = match &triple.predicate {
                NamedNodePattern::Variable(variable) => Some(variable.as_str().to_owned()),
                NamedNodePattern::NamedNode(_) => None,
            };

            term_variable(&triple.subject)
                .into_iter()
                .chain(predicate)
                .chain(term_variable(&triple.object))
        })
        .collect()
}

/// Returns the variables at the two ends of a property path.
fn path_variables(subject: &TermPattern, object: &TermPattern) -> HashSet<String> {
    term_variable(subject)
        .into_iter()
        .chain(term_variable(object))
        .collect()
}

/// Returns the variable in a term position, if there is one.
fn term_variable(term: &TermPattern) -> Option<String> {
    match term {
        TermPattern::Variable(variable) => Some(variable.as_str().to_owned()),
        _ => None,
    }
}

/// Returns the variables that every solution of a query's result binds.
///
/// They are the variables that its pattern always binds,
/// and those that the filters at its top require to be bound.
/// Only the result has passed these filters, so a block joined in under them,
/// where [under_projection] places one, still meets solutions that leave such a variable unbound.
pub(super) fn bound_in_results(pattern: &GraphPattern) -> HashSet<String> {
    match pattern {
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::OrderBy { inner, .. } => bound_in_results(inner),
        GraphPattern::Filter { inner, expr } => {
            let mut bound = bound_in_results(inner);
            bound.extend(required_bound(expr));
            bound
        }
        other => always_bound(other),
    }
}

/// Returns the variables that a filter condition requires to be bound:
/// those of its `BOUND` conjuncts.
fn required_bound(expression: &Expression) -> Vec<String> {
    match expression {
        Expression::Bound(variable) => vec![variable.as_str().to_owned()],
        Expression::And(left, right) => {
            let mut bound = required_bound(left);
            bound.extend(required_bound(right));
            bound
        }
        _ => Vec::new(),
    }
}

/// Returns the variables that some solution of a pattern may bind.
///
/// They are the variables that any part of it matches or defines,
/// and not those that only its filters mention, which no solution binds.
pub(super) fn possibly_bound(pattern: &GraphPattern) -> HashSet<String> {
    match pattern {
        GraphPattern::Bgp { patterns } => triple_variables(patterns),
        GraphPattern::Path {
            subject, object, ..
        } => path_variables(subject, object),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right } => {
            let mut bound = possibly_bound(left);
            bound.extend(possibly_bound(right));
            bound
        }
        GraphPattern::Minus { left, .. } => possibly_bound(left),
        GraphPattern::Graph { name, inner } => {
            let mut bound = possibly_bound(inner);
            if let NamedNodePattern::Variable(variable) = name {
                bound.insert(variable.as_str().to_owned());
            }

            bound
        }
        GraphPattern::Values { variables, .. } => variables
            .iter()
            .map(|variable| variable.as_str().to_owned())
            .collect(),
        GraphPattern::Project { inner, variables } => {
            let inner = possibly_bound(inner);
            variables
                .iter()
                .map(|variable| variable.as_str().to_owned())
                .filter(|variable| inner.contains(variable))
                .collect()
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let inner = possibly_bound(inner);
            variables
                .iter()
                .map(|variable| variable.as_str().to_owned())
                .filter(|variable| inner.contains(variable))
                .chain(
                    aggregates
                        .iter()
                        .map(|(variable, _)| variable.as_str().to_owned()),
                )
                .collect()
        }
        GraphPattern::Extend {
            inner, variable, ..
        } => {
            let mut bound = possibly_bound(inner);
            bound.insert(variable.as_str().to_owned());
            bound
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Service { inner, .. } => possibly_bound(inner),
    }
}

/// Returns a pattern as a group of its own: a subquery that projects every variable in scope.
///
/// A pattern joined with the group then applies to the group's solutions as a whole,
/// instead of merging into it.
/// A filter written into the same group would apply only after the join.
pub(super) fn grouped(pattern: GraphPattern) -> GraphPattern {
    let mut variables: Vec<Variable> = Vec::new();
    pattern.on_in_scope_variable(|variable| {
        if !variables.contains(variable) {
            variables.push(variable.clone());
        }
    });
    GraphPattern::Project {
        inner: Box::new(pattern),
        variables,
    }
}

/// Visits every pattern node, the root first,
/// including the patterns of `EXISTS` forms inside expressions.
pub(super) fn visit_patterns<'a>(
    pattern: &'a GraphPattern,
    visit: &mut impl FnMut(&'a GraphPattern),
) {
    visit(pattern);
    match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            visit_patterns(left, visit);
            visit_patterns(right, visit);
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            visit_patterns(left, visit);
            visit_patterns(right, visit);
            node_expressions(pattern, &mut |expression| {
                visit_expression_patterns(expression, visit);
            });
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Group { inner, .. } => {
            node_expressions(pattern, &mut |expression| {
                visit_expression_patterns(expression, visit);
            });
            visit_patterns(inner, visit);
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Service { inner, .. } => visit_patterns(inner, visit),
    }
}

/// Visits the expressions that a pattern node itself has,
/// without those of the patterns nested in it.
fn node_expressions<'a>(node: &'a GraphPattern, visit: &mut impl FnMut(&'a Expression)) {
    match node {
        GraphPattern::LeftJoin {
            expression: Some(expression),
            ..
        }
        | GraphPattern::Filter {
            expr: expression, ..
        }
        | GraphPattern::Extend { expression, .. } => visit(expression),
        GraphPattern::OrderBy { expression, .. } => {
            for order in expression {
                let (spargebra::algebra::OrderExpression::Asc(expression)
                | spargebra::algebra::OrderExpression::Desc(expression)) = order;
                visit(expression);
            }
        }
        GraphPattern::Group { aggregates, .. } => {
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    visit(expr);
                }
            }
        }
        _ => {}
    }
}

/// Visits the patterns inside an expression's `EXISTS` forms.
fn visit_expression_patterns<'a>(
    expression: &'a Expression,
    visit: &mut impl FnMut(&'a GraphPattern),
) {
    visit_subexpressions(expression, &mut |expression| {
        if let Expression::Exists(pattern) = expression {
            visit_patterns(pattern, visit);
        }
    });
}

/// Visits every expression node of a pattern,
/// including those in the patterns of `EXISTS` forms.
pub(super) fn visit_expressions<'a>(
    pattern: &'a GraphPattern,
    visit: &mut impl FnMut(&'a Expression),
) {
    visit_patterns(pattern, &mut |node| {
        node_expressions(node, &mut |expression| {
            visit_subexpressions(expression, visit);
        });
    });
}

/// Visits an expression and every expression nested in it,
/// but not the patterns inside `EXISTS`, which [visit_patterns] covers.
fn visit_subexpressions<'a>(expression: &'a Expression, visit: &mut impl FnMut(&'a Expression)) {
    visit(expression);
    match expression {
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_)
        | Expression::Exists(_) => {}
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            visit_subexpressions(left, visit);
            visit_subexpressions(right, visit);
        }
        Expression::In(subject, options) => {
            visit_subexpressions(subject, visit);
            for option in options {
                visit_subexpressions(option, visit);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            visit_subexpressions(inner, visit);
        }
        Expression::If(condition, then, otherwise) => {
            visit_subexpressions(condition, visit);
            visit_subexpressions(then, visit);
            visit_subexpressions(otherwise, visit);
        }
        Expression::Coalesce(options) | Expression::FunctionCall(_, options) => {
            for option in options {
                visit_subexpressions(option, visit);
            }
        }
    }
}
