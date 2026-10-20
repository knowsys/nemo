//! Composing queries: instantiating a query at an atom, joining several into one,
//! and projecting the result.
//!
//! An atom is a list of [bindings](Binding), one per projected variable of a query.
//! An instantiation renames the projected variables to the atom's variables,
//! substitutes the atom's constants if the pattern can hold a term,
//! and renames every other variable apart per instance,
//! so two instances of one query share nothing but what their atoms share.
//! - A [pattern-shaped](QueryShape::Pattern) query inlines.
//! - A [complex](QueryShape::Complex) one enters as a subquery,
//!   its constants joined in as single-row `VALUES` blocks,
//!   which SPARQL's bottom-up subquery semantics makes sound.
//! - An [opaque](QueryShape::Opaque) query is never instantiated.
//!
//! A query that [keeps its names](SparqlQuery::keeps_names) renames nothing:
//! its other variables keep their names, its constants bind through `VALUES` blocks,
//! and the caller binds its projected variables to their own names.
//! It cannot share a composition with another instance.

use std::collections::{HashMap, HashSet};

use spargebra::{
    Query,
    algebra::{
        AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
        QueryDataset,
    },
    term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern, Variable},
};

use crate::formats::sparql::{QueryShape, SparqlQuery, algebra};

/// What an instantiation binds a query's projected variable to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Binding {
    /// Another variable, by name.
    Variable(String),
    /// A constant.
    Constant(GroundTerm),
}

/// What a query variable becomes under an instantiation.
#[derive(Debug, Clone)]
enum Replacement {
    /// Another variable.
    Variable(Variable),
    /// A ground term, if the position can hold one.
    Term(GroundTerm),
}

impl SparqlQuery {
    /// Instantiates a query at an atom, given one binding per projected variable,
    /// in projection order.
    ///
    /// `suffix` tells this instance's private variables apart from other instances',
    /// and `reserved` names the variables they must avoid as well, the atom's own.
    /// A projected variable bound to a variable in `joined`, one the atom's context joins on,
    /// is required bound:
    /// a solution leaving it unbound (an `OPTIONAL` that did not match)
    /// would join with anything in SPARQL,
    /// while the null it becomes in the result joins with nothing.
    ///
    /// Returns the query's pattern under the projection (or the whole query as a subquery),
    /// ready to join,
    /// or `None` for a binding to a name that is not a valid SPARQL variable name.
    ///
    /// # Panics
    ///
    /// Panics for an [opaque](QueryShape::Opaque) query,
    /// and if the number of bindings is not the number of projected variables.
    pub fn instantiate(
        &self,
        bindings: &[Binding],
        suffix: &str,
        reserved: &HashSet<String>,
        joined: &HashSet<String>,
    ) -> Option<GraphPattern> {
        let shape = self.shape();
        if let QueryShape::Opaque(reason) = &shape {
            panic!("an opaque query cannot be instantiated: {reason}");
        }
        let projected = self.variables();
        assert_eq!(
            bindings.len(),
            projected.len(),
            "an instantiation binds every projected variable"
        );
        let GraphPattern::Project { inner, variables } =
            algebra::strip_modifiers(query_pattern(self))
        else {
            unreachable!("a SELECT query projects, `*` included")
        };
        // An ordering under the projection means nothing inside a join.
        let inner = match inner.as_ref() {
            GraphPattern::OrderBy { inner, .. } => inner,
            other => other,
        };

        let instance = Instance {
            suffix,
            reserved,
            keeps_names: self.keeps_names(),
        };
        let Rebinding {
            replacements,
            values,
            required,
        } = rebinding(projected, bindings, &shape, inner, &instance, joined)?;
        let mut map = |variable: &Variable| -> Replacement {
            replacements
                .get(variable.as_str())
                .cloned()
                .unwrap_or_else(|| Replacement::Variable(instance.private(variable.as_str())))
        };

        let mut pattern = inner.clone();
        rename_pattern(&mut pattern, &mut map);
        // The guards sit in a group of their own (inside the subquery a
        // complex query enters as, or one made for them), so they apply
        // to the solutions before anything joins them.
        let guard = conjunction(required.into_iter().map(Expression::Bound));
        if shape == QueryShape::Complex {
            if let Some(expr) = guard {
                pattern = GraphPattern::Filter {
                    expr,
                    inner: Box::new(pattern),
                };
            }
            pattern = GraphPattern::Project {
                inner: Box::new(pattern),
                variables: variables
                    .iter()
                    .map(|variable| match map(variable) {
                        Replacement::Variable(variable) => variable,
                        Replacement::Term(_) => unreachable!("a subquery keeps every variable"),
                    })
                    .collect(),
            };
        } else if let Some(expr) = guard {
            pattern = algebra::grouped(GraphPattern::Filter {
                expr,
                inner: Box::new(pattern),
            });
        }
        Some(join_values(pattern, values))
    }
}

/// Returns a pattern joined with one single-row `VALUES` block per constant,
/// each binding the variable that stands for it.
fn join_values(pattern: GraphPattern, values: Vec<(String, Variable, GroundTerm)>) -> GraphPattern {
    values
        .into_iter()
        .fold(pattern, |pattern, (_, variable, term)| GraphPattern::Join {
            left: Box::new(pattern),
            right: Box::new(GraphPattern::Values {
                variables: vec![variable],
                bindings: vec![vec![Some(term)]],
            }),
        })
}

/// Returns the conjunction of expressions, or `None` for none.
pub(crate) fn conjunction(expressions: impl IntoIterator<Item = Expression>) -> Option<Expression> {
    let mut expressions = expressions.into_iter();
    let first = expressions.next()?;
    Some(expressions.fold(first, |left, right| {
        Expression::And(Box::new(left), Box::new(right))
    }))
}

/// Returns the join of patterns, in order, or `None` for none.
fn join(patterns: Vec<GraphPattern>) -> Option<GraphPattern> {
    let mut patterns = patterns.into_iter();
    let first = patterns.next()?;
    Some(patterns.fold(first, |left, right| GraphPattern::Join {
        left: Box::new(left),
        right: Box::new(right),
    }))
}

/// Returns the condition that a pattern has no match.
fn not_exists(pattern: GraphPattern) -> Expression {
    Expression::Not(Box::new(Expression::Exists(Box::new(pattern))))
}

/// Returns the pattern filtered by every condition.
fn filter(pattern: GraphPattern, conditions: Vec<Expression>) -> GraphPattern {
    conditions
        .into_iter()
        .fold(pattern, |inner, expr| GraphPattern::Filter {
            expr,
            inner: Box::new(inner),
        })
}

/// A query under composition:
/// the patterns it joins, and the patterns that must have no match.
#[derive(Debug, Default)]
pub struct Composition {
    /// The patterns joined, in order.
    joined: Vec<GraphPattern>,
    /// The patterns that must have no match.
    absent: Vec<GraphPattern>,
}

impl Composition {
    /// Adds a pattern the query joins.
    pub fn join(&mut self, pattern: GraphPattern) {
        self.joined.push(pattern);
    }

    /// Adds a pattern that must have no match, a `FILTER NOT EXISTS`.
    pub fn exclude(&mut self, pattern: GraphPattern) {
        self.absent.push(pattern);
    }

    /// Returns whether no pattern is joined yet.
    pub fn joins_nothing(&self) -> bool {
        self.joined.is_empty()
    }

    /// Returns a `SELECT` of the given variables over the joined patterns,
    /// filtered by the absences,
    /// with a dataset clause if the one query it came from had one.
    ///
    /// Returns `None` if nothing is joined, or if a name is not a valid SPARQL variable name.
    pub fn select(
        self,
        variables: &[String],
        dataset: Option<QueryDataset>,
    ) -> Option<SparqlQuery> {
        let pattern = filter(
            join(self.joined)?,
            self.absent.into_iter().map(not_exists).collect(),
        );
        let variables = variables
            .iter()
            .map(|name| Variable::new(name).ok())
            .collect::<Option<Vec<_>>>()?;
        let query = SparqlQuery::from_select(Query::Select {
            dataset,
            pattern: GraphPattern::Project {
                inner: Box::new(pattern),
                variables,
            },
            base_iri: None,
        });
        Some(query.expect("a SELECT wraps"))
    }
}

/// How one instance names the variables its bindings do not name.
struct Instance<'a> {
    /// What tells this instance's variables apart from other instances'.
    suffix: &'a str,
    /// The names the instance's variables must avoid.
    reserved: &'a HashSet<String>,
    /// Whether the query [keeps its names](SparqlQuery::keeps_names).
    keeps_names: bool,
}

impl Instance<'_> {
    /// Returns a variable's name in this instance:
    /// its own if the query keeps its names, otherwise renamed [apart].
    fn private(&self, name: &str) -> Variable {
        if self.keeps_names {
            Variable::new_unchecked(name)
        } else {
            apart(name, self.suffix, self.reserved)
        }
    }
}

/// How an instance binds the projected variables: the replacement of
/// each, the `VALUES` rows binding constants outside the pattern (each
/// with the projected variable it stands for), and the variables a
/// solution must bind to count.
struct Rebinding {
    /// Per projected variable, what replaces it.
    replacements: HashMap<String, Replacement>,
    /// The constants bound by a single-row `VALUES` block outside.
    values: Vec<(String, Variable, GroundTerm)>,
    /// The variables a solution must bind to count.
    required: Vec<Variable>,
}

/// Decides how an instance binds each projected variable.
///
/// If a constant cannot replace the variable,
/// because the pattern defines it or a literal would sit in a predicate or graph position,
/// the instance keeps a variable and binds it by a single-row `VALUES` block outside.
/// A subquery always does, and so does a query keeping its names,
/// since a service may refer to a variable its pattern does not mention.
/// A constant bound that way must not meet a solution that leaves its variable unbound,
/// since the row would join with it and fabricate a match,
/// so such a variable is required, like a joined one the pattern may leave unbound.
///
/// Returns `None` for a binding to a name that is not a valid SPARQL variable name.
fn rebinding(
    projected: &[String],
    bindings: &[Binding],
    shape: &QueryShape,
    inner: &GraphPattern,
    instance: &Instance<'_>,
    joined: &HashSet<String>,
) -> Option<Rebinding> {
    let defined = defined_variables(inner);
    let node_positions = node_position_variables(inner);
    let assured = algebra::always_bound(inner);
    let maybe = algebra::possibly_bound(inner);
    let unassured = |name: &String| maybe.contains(name) && !assured.contains(name);
    let mut rebinding = Rebinding {
        replacements: HashMap::new(),
        values: Vec::new(),
        required: Vec::new(),
    };
    for (name, binding) in projected.iter().zip(bindings) {
        let replacement = match binding {
            Binding::Variable(target) => {
                let target = Variable::new(target).ok()?;
                if joined.contains(target.as_str()) && unassured(name) {
                    rebinding.required.push(target.clone());
                }
                Replacement::Variable(target)
            }
            Binding::Constant(term) => {
                let literal_in_node_position =
                    matches!(term, GroundTerm::Literal(_)) && node_positions.contains(name);
                if *shape == QueryShape::Complex
                    || instance.keeps_names
                    || defined.contains(name)
                    || literal_in_node_position
                {
                    let fresh = instance.private(name);
                    if unassured(name) {
                        rebinding.required.push(fresh.clone());
                    }
                    rebinding
                        .values
                        .push((name.clone(), fresh.clone(), term.clone()));
                    Replacement::Variable(fresh)
                } else {
                    Replacement::Term(term.clone())
                }
            }
        };
        rebinding.replacements.insert(name.clone(), replacement);
    }
    Some(rebinding)
}

/// Returns a query's selection pattern.
fn query_pattern(query: &SparqlQuery) -> &GraphPattern {
    match query.query() {
        Query::Select { pattern, .. } => pattern,
        _ => unreachable!("a SparqlQuery is a SELECT"),
    }
}

/// Returns a variable's name in one instance: the name with the instance's suffix,
/// which no source query uses.
///
/// The name is lengthened if it is a reserved name after all.
fn apart(name: &str, suffix: &str, reserved: &HashSet<String>) -> Variable {
    let mut apart = format!("{name}_{suffix}");
    while reserved.contains(&apart) {
        apart.push('_');
    }
    Variable::new_unchecked(apart)
}

/// Returns the variables a pattern defines rather than matches
/// (`BIND`, `VALUES`, grouping, and nested projections), which no term can replace.
fn defined_variables(pattern: &GraphPattern) -> HashSet<String> {
    let mut defined = HashSet::new();
    algebra::visit_patterns(pattern, &mut |node| match node {
        GraphPattern::Extend { variable, .. } => {
            defined.insert(variable.as_str().to_owned());
        }
        GraphPattern::Values { variables, .. } | GraphPattern::Project { variables, .. } => {
            defined.extend(
                variables
                    .iter()
                    .map(|variable| variable.as_str().to_owned()),
            );
        }
        GraphPattern::Group {
            variables,
            aggregates,
            ..
        } => {
            defined.extend(
                variables
                    .iter()
                    .map(|variable| variable.as_str().to_owned()),
            );
            defined.extend(
                aggregates
                    .iter()
                    .map(|(variable, _)| variable.as_str().to_owned()),
            );
        }
        _ => {}
    });
    defined
}

/// Returns the variables in a predicate or graph-name position,
/// which only an IRI can fill.
fn node_position_variables(pattern: &GraphPattern) -> HashSet<String> {
    let mut positions = HashSet::new();
    algebra::visit_patterns(pattern, &mut |node| match node {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                if let NamedNodePattern::Variable(variable) = &triple.predicate {
                    positions.insert(variable.as_str().to_owned());
                }
            }
        }
        GraphPattern::Graph {
            name: NamedNodePattern::Variable(variable),
            ..
        }
        | GraphPattern::Service {
            name: NamedNodePattern::Variable(variable),
            ..
        } => {
            positions.insert(variable.as_str().to_owned());
        }
        _ => {}
    });
    positions
}

/// Renames every variable of a pattern in place.
fn rename_pattern(pattern: &mut GraphPattern, map: &mut impl FnMut(&Variable) -> Replacement) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for TriplePattern {
                subject,
                predicate,
                object,
            } in patterns
            {
                rename_term(subject, map);
                rename_node(predicate, map);
                rename_term(object, map);
            }
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            rename_term(subject, map);
            let _: &PropertyPathExpression = path;
            rename_term(object, map);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            rename_pattern(left, map);
            rename_pattern(right, map);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            rename_pattern(left, map);
            rename_pattern(right, map);
            if let Some(expression) = expression {
                rename_expression(expression, map);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            rename_expression(expr, map);
            rename_pattern(inner, map);
        }
        GraphPattern::Graph { name, inner } | GraphPattern::Service { name, inner, .. } => {
            rename_node(name, map);
            rename_pattern(inner, map);
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            rename_pattern(inner, map);
            rename_variable(variable, map);
            rename_expression(expression, map);
        }
        GraphPattern::Values { variables, .. } | GraphPattern::Project { variables, .. } => {
            for variable in variables {
                rename_variable(variable, map);
            }
            if let GraphPattern::Project { inner, .. } = pattern {
                rename_pattern(inner, map);
            }
        }
        GraphPattern::OrderBy { inner, expression } => {
            rename_pattern(inner, map);
            for order in expression {
                let (OrderExpression::Asc(expression) | OrderExpression::Desc(expression)) = order;
                rename_expression(expression, map);
            }
        }
        GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => rename_pattern(inner, map),
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            rename_pattern(inner, map);
            for variable in variables {
                rename_variable(variable, map);
            }
            for (variable, aggregate) in aggregates {
                rename_variable(variable, map);
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    rename_expression(expr, map);
                }
            }
        }
    }
}

/// Renames a variable that must stay a variable.
///
/// [instantiate]'s check rules out a term replacement here, so such a replacement keeps the name.
fn rename_variable(variable: &mut Variable, map: &mut impl FnMut(&Variable) -> Replacement) {
    if let Replacement::Variable(renamed) = map(variable) {
        *variable = renamed;
    }
}

/// Renames a term position.
fn rename_term(term: &mut TermPattern, map: &mut impl FnMut(&Variable) -> Replacement) {
    if let TermPattern::Variable(variable) = term {
        *term = match map(variable) {
            Replacement::Variable(variable) => TermPattern::Variable(variable),
            Replacement::Term(GroundTerm::NamedNode(node)) => TermPattern::NamedNode(node),
            Replacement::Term(GroundTerm::Literal(literal)) => TermPattern::Literal(literal),
        };
    }
}

/// Renames a predicate or graph-name position.
fn rename_node(node: &mut NamedNodePattern, map: &mut impl FnMut(&Variable) -> Replacement) {
    if let NamedNodePattern::Variable(variable) = node {
        match map(variable) {
            Replacement::Variable(variable) => *node = NamedNodePattern::Variable(variable),
            Replacement::Term(GroundTerm::NamedNode(named)) => {
                *node = NamedNodePattern::NamedNode(named);
            }
            // Excluded by the pre-check: a literal cannot be a predicate or graph name.
            Replacement::Term(GroundTerm::Literal(_)) => {}
        }
    }
}

/// Renames every variable of an expression.
fn rename_expression(expression: &mut Expression, map: &mut impl FnMut(&Variable) -> Replacement) {
    match expression {
        Expression::NamedNode(_) | Expression::Literal(_) => {}
        Expression::Variable(variable) => {
            *expression = match map(variable) {
                Replacement::Variable(variable) => Expression::Variable(variable),
                Replacement::Term(GroundTerm::NamedNode(node)) => Expression::NamedNode(node),
                Replacement::Term(GroundTerm::Literal(literal)) => Expression::Literal(literal),
            };
        }
        Expression::Bound(variable) => {
            *expression = match map(variable) {
                Replacement::Variable(variable) => Expression::Bound(variable),
                // A constant is always bound.
                Replacement::Term(_) => Expression::Literal(spargebra::term::Literal::from(true)),
            };
        }
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
            rename_expression(left, map);
            rename_expression(right, map);
        }
        Expression::In(subject, options) => {
            rename_expression(subject, map);
            for option in options {
                rename_expression(option, map);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            rename_expression(inner, map);
        }
        Expression::Exists(pattern) => rename_pattern(pattern, map),
        Expression::If(condition, then, otherwise) => {
            rename_expression(condition, map);
            rename_expression(then, map);
            rename_expression(otherwise, map);
        }
        Expression::Coalesce(options) | Expression::FunctionCall(_, options) => {
            for option in options {
                rename_expression(option, map);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A named-node constant.
    fn iri(iri: &str) -> Binding {
        Binding::Constant(GroundTerm::NamedNode(
            spargebra::term::NamedNode::new_unchecked(iri),
        ))
    }

    /// Creates a binding to another variable.
    fn var(name: &str) -> Binding {
        Binding::Variable(String::from(name))
    }

    /// The parsed generic triple query.
    /// Returns the `SELECT` of the variables over the joined patterns, the absent ones excluded.
    fn composed(
        joined: Vec<GraphPattern>,
        absent: Vec<GraphPattern>,
        variables: &[&str],
    ) -> Option<SparqlQuery> {
        let mut composition = Composition::default();
        joined
            .into_iter()
            .for_each(|pattern| composition.join(pattern));
        absent
            .into_iter()
            .for_each(|pattern| composition.exclude(pattern));
        let variables: Vec<String> = variables.iter().map(|name| name.to_string()).collect();
        composition.select(&variables, None)
    }

    fn triples() -> SparqlQuery {
        SparqlQuery::parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", None, []).expect("valid")
    }

    #[test]
    fn instances_of_one_query_join_through_the_rule_variables() {
        let first = triples()
            .instantiate(
                &[var("e"), iri("urn:p1"), var("stmt")],
                "1",
                &HashSet::new(),
                &HashSet::new(),
            )
            .expect("instantiates");
        let second = triples()
            .instantiate(
                &[var("stmt"), iri("urn:p2"), var("next")],
                "2",
                &HashSet::new(),
                &HashSet::new(),
            )
            .expect("instantiates");
        let query = composed(vec![first, second], Vec::new(), &["e", "next"]).expect("selects");
        assert_eq!(
            query.text(),
            "SELECT ?e ?next WHERE { ?e <urn:p1> ?stmt . ?stmt <urn:p2> ?next . }"
        );
        assert_eq!(query.variables(), ["e", "next"]);
    }

    #[test]
    fn private_variables_rename_apart_and_constants_substitute() {
        let query = SparqlQuery::parse(
            "SELECT ?a ?c WHERE { ?a <urn:p> ?z . ?z <urn:q> ?c . FILTER(?z != ?a) }",
            None,
            [],
        )
        .expect("valid");
        let first = query
            .instantiate(&[var("x"), var("y")], "1", &HashSet::new(), &HashSet::new())
            .expect("instantiates");
        let second = query
            .instantiate(
                &[var("y"), iri("urn:end")],
                "2",
                &HashSet::new(),
                &HashSet::new(),
            )
            .expect("instantiates");
        let merged = composed(vec![first, second], Vec::new(), &["x"]).expect("selects");
        let text = merged.text();
        assert!(text.contains("?z_1") && text.contains("?z_2"), "{text}");
        assert!(text.contains("<urn:q> <urn:end>"), "{text}");
        assert!(text.contains("?z_2 = ?y"), "{text}");
    }

    #[test]
    fn complex_queries_enter_as_subqueries_with_values_for_constants() {
        let query = SparqlQuery::parse(
            "SELECT ?a (COUNT(?c) AS ?n) WHERE { ?a ?b ?c } GROUP BY ?a",
            None,
            [],
        )
        .expect("valid");
        let literal = Binding::Constant(GroundTerm::Literal(
            spargebra::term::Literal::new_typed_literal(
                "3",
                spargebra::term::NamedNode::new_unchecked(
                    "http://www.w3.org/2001/XMLSchema#integer",
                ),
            ),
        ));
        let instance = query
            .instantiate(&[var("x"), literal], "1", &HashSet::new(), &HashSet::new())
            .expect("instantiates");
        let merged = composed(vec![instance], Vec::new(), &["x"]).expect("selects");
        let text = merged.text();
        assert!(text.contains("GROUP BY ?x"), "{text}");
        assert!(text.contains("VALUES ( ?n_1 )"), "{text}");
        // The subquery is kept whole: re-parsing keeps the outer
        // projection and the aggregation inside.
        let reparsed = SparqlQuery::parse(text, None, []).expect("parses");
        assert_eq!(reparsed.variables(), ["x"]);
        assert_eq!(reparsed.shape(), QueryShape::Complex);
    }

    #[test]
    fn a_literal_predicate_is_bound_through_values() {
        let literal = Binding::Constant(GroundTerm::Literal(
            spargebra::term::Literal::new_simple_literal("p"),
        ));
        let instance = triples()
            .instantiate(
                &[var("s"), literal, var("o")],
                "1",
                &HashSet::new(),
                &HashSet::new(),
            )
            .expect("instantiates");
        let merged = composed(vec![instance], Vec::new(), &["s", "o"]).expect("selects");
        assert!(
            merged.text().contains("VALUES ( ?p_1 )"),
            "{}",
            merged.text()
        );
    }

    #[test]
    fn a_query_calling_a_service_renames_nothing() {
        let query = SparqlQuery::parse(
            "SELECT ?qid ?qidLabel WHERE { ?qid <urn:p> ?o . \
             SERVICE <http://wikiba.se/ontology#label> { \
             <http://www.bigdata.com/rdf#serviceParam> <http://wikiba.se/ontology#language> \"en\" } }",
            None,
            [],
        )
        .expect("valid");
        assert!(query.keeps_names());
        assert!(!triples().keeps_names());
        let instance = query
            .instantiate(
                &[iri("urn:q"), var("qidLabel")],
                "1",
                &HashSet::new(),
                &HashSet::new(),
            )
            .expect("instantiates");
        let merged = composed(vec![instance], Vec::new(), &["qidLabel"]).expect("selects");
        let text = merged.text();
        // The constant binds the variable the service labels, which keeps its name,
        // as does the private one.
        assert!(text.contains("?qid <urn:p> ?o ."), "{text}");
        assert!(text.contains("VALUES ( ?qid )"), "{text}");
    }

    #[test]
    #[should_panic(expected = "opaque query")]
    fn opaque_queries_do_not_instantiate() {
        let query =
            SparqlQuery::parse("SELECT ?a WHERE { ?a ?b ?c } LIMIT 5", None, []).expect("valid");
        query.instantiate(&[var("x")], "1", &HashSet::new(), &HashSet::new());
    }

    #[test]
    fn variables_with_names_invalid_in_sparql_do_not_compose() {
        let bindings = [var("x"), var("y"), var("z")];
        let none = HashSet::new();
        let invalid_names = [var("x-y"), var("y"), var("z")];
        assert_eq!(
            triples().instantiate(&invalid_names, "1", &none, &none),
            None
        );
        let pattern = triples()
            .instantiate(&bindings, "1", &none, &none)
            .expect("instantiates");
        assert!(composed(vec![pattern], Vec::new(), &["x-y"]).is_none());
    }

    #[test]
    fn negation_becomes_not_exists() {
        let instance = triples()
            .instantiate(
                &[var("x"), iri("urn:p3"), var("y")],
                "1",
                &HashSet::new(),
                &HashSet::new(),
            )
            .expect("instantiates");
        let positive = triples()
            .instantiate(
                &[var("y"), iri("urn:p1"), var("z")],
                "2",
                &HashSet::new(),
                &HashSet::new(),
            )
            .expect("instantiates");
        let merged = composed(vec![positive], vec![instance], &["y"]).expect("selects");
        assert!(
            merged
                .text()
                .contains("FILTER(NOT EXISTS { ?x <urn:p3> ?y . })"),
            "{}",
            merged.text()
        );
    }
}
