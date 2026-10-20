//! The query algebra behind imports:
//! what a query's shape allows, and the places bindings and conditions enter it.

use std::collections::HashSet;

use spargebra::{
    algebra::{AggregateExpression, AggregateFunction, Expression, Function, GraphPattern},
    term::{GroundTerm, NamedNodePattern, TermPattern, Variable},
};

/// What may be pushed into a query, decided from its algebra.
///
/// A `VALUES` block or a `FILTER` entering directly under the outermost projection
/// selects among the query's final solutions.
/// That is sound if the modifiers above the projection commute with selection
/// (`DISTINCT`, `REDUCED` and `ORDER BY` do) and the solutions are one determined set.
/// A `LIMIT` or `OFFSET` bounds the *whole* result,
/// so a selection per request changes what the query means,
/// and a `LIMIT` without `ORDER BY` is a nondeterministic choice
/// that no union of restricted choices reproduces.
/// A nondeterministic function or a `SAMPLE` aggregate has the same problem.
/// Such queries are opaque: fetched whole, once, and never restricted or merged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryShape {
    /// A `SELECT` over a plain group pattern, which inlines into a merged query.
    ///
    /// It has no aggregation, no subquery, no slice, and no part that binds only some solutions.
    Pattern,
    /// Aggregation or a subquery inside: bindings and conditions enter
    /// above what the query computes, and a merge carries it whole, as
    /// a subquery.
    Complex,
    /// No selection on the final solutions reproduces a restricted request.
    ///
    /// The reason names the construct.
    Opaque(String),
}

/// Returns the shape of a `SELECT` query's pattern.
pub(super) fn shape(pattern: &GraphPattern) -> QueryShape {
    if let Some(reason) = opaque(pattern) {
        return QueryShape::Opaque(reason);
    }
    match strip_modifiers(pattern) {
        GraphPattern::Project { inner, .. } if !nests(inner) => QueryShape::Pattern,
        _ => QueryShape::Complex,
    }
}

/// Returns why a query is opaque, if it is.
fn opaque(pattern: &GraphPattern) -> Option<String> {
    let mut reason = None;
    visit_patterns(pattern, &mut |node| {
        if reason.is_some() {
            return;
        }
        match node {
            GraphPattern::Slice { start, length, .. } => {
                reason = Some(String::from(match (start, length) {
                    (0, Some(_)) => "LIMIT",
                    (_, None) => "OFFSET",
                    _ => "LIMIT/OFFSET",
                }));
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
                reason = Some(String::from("SAMPLE"));
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
            reason = Some(String::from(name));
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

/// Returns whether a pattern holds, anywhere inside,
/// a projection or an aggregation (a subquery or a grouping),
/// or a part whose variables only some solutions bind (an `OPTIONAL`, a `UNION`, a `MINUS`).
///
/// A term substituted into such a part, or a variable shared into it,
/// would change what the query means,
/// so the whole query enters a merge as a subquery instead.
fn nests(pattern: &GraphPattern) -> bool {
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
/// The rewrite applies under the `ORDER BY` the algebra keeps there,
/// and under the filters at the top of the pattern,
/// which a selection commutes with and whose `NOT EXISTS` forms must see what a `VALUES` block
/// binds.
/// A pattern without a projection is rewritten whole.
pub(super) fn under_projection(
    pattern: GraphPattern,
    rewrite: impl FnOnce(GraphPattern) -> GraphPattern,
) -> GraphPattern {
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

/// One `VALUES` block: rows of ground terms over some of a query's
/// projected columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuesBlock {
    /// The projected columns the rows bind, by position.
    pub positions: Vec<usize>,
    /// The rows, one term per position.
    pub rows: Vec<Vec<GroundTerm>>,
}

impl ValuesBlock {
    /// Returns the block as a pattern over the query's projected variables.
    pub(super) fn pattern(&self, variables: &[String]) -> GraphPattern {
        GraphPattern::Values {
            variables: self
                .positions
                .iter()
                .map(|&position| Variable::new_unchecked(variables[position].as_str()))
                .collect(),
            bindings: self
                .rows
                .iter()
                .map(|row| row.iter().cloned().map(Some).collect())
                .collect(),
        }
    }
}

/// Returns the variables every solution of a pattern binds:
/// what its mandatory parts match,
/// not what an `OPTIONAL`, one `UNION` branch, a `BIND` or a `VALUES` row may leave unbound.
pub(super) fn always_bound(pattern: &GraphPattern) -> HashSet<String> {
    fn term_variable(term: &TermPattern) -> Option<String> {
        match term {
            TermPattern::Variable(variable) => Some(variable.as_str().to_owned()),
            _ => None,
        }
    }
    match pattern {
        GraphPattern::Bgp { patterns } => patterns
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
            .collect(),
        GraphPattern::Path {
            subject, object, ..
        } => term_variable(subject)
            .into_iter()
            .chain(term_variable(object))
            .collect(),
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

/// Returns the variables some solution of a pattern may bind:
/// what any part of it matches or defines,
/// not what only its filters mention, which no solution binds.
pub(super) fn possibly_bound(pattern: &GraphPattern) -> HashSet<String> {
    fn term_variable(term: &TermPattern) -> Option<String> {
        match term {
            TermPattern::Variable(variable) => Some(variable.as_str().to_owned()),
            _ => None,
        }
    }
    match pattern {
        GraphPattern::Bgp { patterns } => patterns
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
            .collect(),
        GraphPattern::Path {
            subject, object, ..
        } => term_variable(subject)
            .into_iter()
            .chain(term_variable(object))
            .collect(),
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

/// Returns a pattern as a group of its own, a subquery projecting every variable in scope.
///
/// A pattern joined with the group then applies to its solutions as a whole,
/// instead of merging into it:
/// a filter written into the same group would apply after the join.
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

/// Visits every pattern node, the root first, `EXISTS` patterns inside
/// expressions included.
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
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            visit_patterns(left, visit);
            visit_patterns(right, visit);
            if let Some(expression) = expression {
                visit_expression_patterns(expression, visit);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            visit_expression_patterns(expr, visit);
            visit_patterns(inner, visit);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            visit_expression_patterns(expression, visit);
            visit_patterns(inner, visit);
        }
        GraphPattern::OrderBy { inner, expression } => {
            for order in expression {
                let (spargebra::algebra::OrderExpression::Asc(expression)
                | spargebra::algebra::OrderExpression::Desc(expression)) = order;
                visit_expression_patterns(expression, visit);
            }
            visit_patterns(inner, visit);
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    visit_expression_patterns(expr, visit);
                }
            }
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

/// Visits every expression node of a pattern, patterns inside `EXISTS`
/// forms included.
pub(super) fn visit_expressions<'a>(
    pattern: &'a GraphPattern,
    visit: &mut impl FnMut(&'a Expression),
) {
    visit_patterns(pattern, &mut |node| {
        let mut on = |expression: &'a Expression| visit_subexpressions(expression, visit);
        match node {
            GraphPattern::LeftJoin {
                expression: Some(expression),
                ..
            }
            | GraphPattern::Filter {
                expr: expression, ..
            }
            | GraphPattern::Extend { expression, .. } => on(expression),
            GraphPattern::OrderBy { expression, .. } => {
                for order in expression {
                    let (spargebra::algebra::OrderExpression::Asc(expression)
                    | spargebra::algebra::OrderExpression::Desc(expression)) = order;
                    on(expression);
                }
            }
            GraphPattern::Group { aggregates, .. } => {
                for (_, aggregate) in aggregates {
                    if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                        on(expr);
                    }
                }
            }
            _ => {}
        }
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
