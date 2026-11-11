//! Composing queries: instantiating a query, joining several instances into one query,
//! and projecting the result.
//!
//! An *instance* of a query binds each of its projected variables
//! to a [binding](Binding): another variable or a constant.
//! Two instances of one query share nothing but what their bindings share.
//! A [Composition] joins instances into one query.

use std::collections::{HashMap, HashSet};

use spargebra::{
    Query,
    algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression, QueryDataset},
    term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern, Variable},
};

use super::{QueryShape, SparqlQuery, algebra};

/// What an instantiation binds a query's projected variable to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Binding {
    /// Another variable, by name.
    Variable(String),
    /// A constant, as the terms an endpoint may hold it as
    /// ([ground_terms](SparqlQuery::ground_terms)), any of which matches.
    ///
    /// There is usually one term.
    Constant(Vec<GroundTerm>),
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
    /// Instantiates the query, given one binding per projected variable, in projection order.
    ///
    /// The instance renames the projected variables to the variables they are bound to,
    /// substitutes the bound constants where the pattern can hold a term,
    /// and renames every other variable apart,
    /// so that it shares nothing with another instance but what their bindings share.
    /// - A [pattern-shaped](QueryShape::Pattern) query is inlined.
    /// - A [complex](QueryShape::Complex) query enters as a subquery,
    ///   with its constants joined in as `VALUES` blocks, one row per term.
    ///   SPARQL's bottom-up semantics of subqueries makes this sound.
    ///
    /// A query that [keeps its names](SparqlQuery::keeps_names) renames nothing:
    /// its other variables keep their names, and its constants are bound through `VALUES`
    /// blocks.
    /// Its projected variables must be bound to their own names,
    /// and it cannot share a composition with another instance.
    ///
    /// `suffix` distinguishes this instance's private variables from those of other instances,
    /// and `reserved` names the variables that they must avoid as well,
    /// such as those that the bindings of every instance use.
    /// A projected variable bound to a variable in `joined`,
    /// which the instance shares with the rest of the composition, is required to be bound.
    /// A solution that leaves it unbound, as an `OPTIONAL` that did not match does,
    /// would join with anything in SPARQL,
    /// while Nemo reads the unbound variable as a null, which joins with nothing.
    ///
    /// Returns the query's pattern under the projection, or the whole query as a subquery,
    /// ready to join.
    /// Returns `None` for a binding to a name that is not a valid SPARQL variable name.
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
        let GraphPattern::Project { inner, variables } = algebra::strip_modifiers(self.pattern())
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

        // The guards are placed in a group of their own, either inside the
        // subquery that a complex query becomes or in a group made for them,
        // so that they apply to the solutions before anything is joined with them.
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

/// Returns a pattern joined with one `VALUES` block per constant.
///
/// Each block binds the variable in the constant's place to the constant's terms,
/// one row per term.
fn join_values(pattern: GraphPattern, values: Vec<(Variable, Vec<GroundTerm>)>) -> GraphPattern {
    values
        .into_iter()
        .fold(pattern, |pattern, (variable, terms)| GraphPattern::Join {
            left: Box::new(pattern),
            right: Box::new(GraphPattern::Values {
                variables: vec![variable],
                bindings: terms.into_iter().map(|term| vec![Some(term)]).collect(),
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

/// A query being composed from patterns that it joins and patterns that must have no match.
#[derive(Debug, Default)]
pub struct Composition {
    /// The patterns joined, in order.
    joined: Vec<GraphPattern>,
    /// The patterns that must have no match.
    absent: Vec<GraphPattern>,
}

impl Composition {
    /// Adds a pattern to join.
    pub fn join(&mut self, pattern: GraphPattern) {
        self.joined.push(pattern);
    }

    /// Adds a pattern that must have no match, written as a `FILTER NOT EXISTS`.
    pub fn exclude(&mut self, pattern: GraphPattern) {
        self.absent.push(pattern);
    }

    /// Returns whether no pattern is joined yet.
    pub fn joins_nothing(&self) -> bool {
        self.joined.is_empty()
    }

    /// Returns a `SELECT` of the given variables over the joined patterns,
    /// filtered by the patterns that must have no match,
    /// with the given dataset clause if there is one.
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

/// How one instance names the variables that its bindings do not name.
struct Instance<'a> {
    /// The suffix that distinguishes this instance's variables from those of other instances.
    suffix: &'a str,
    /// The names that the instance's variables must avoid.
    reserved: &'a HashSet<String>,
    /// Whether the query [keeps its names](SparqlQuery::keeps_names).
    keeps_names: bool,
}

impl Instance<'_> {
    /// Returns a private variable's name in this instance:
    /// its own name if the query keeps its names, and otherwise the name that [apart] gives.
    fn private(&self, name: &str) -> Variable {
        if self.keeps_names {
            Variable::new_unchecked(name)
        } else {
            apart(name, self.suffix, self.reserved)
        }
    }
}

/// How an instance binds the projected variables.
struct Rebinding {
    /// For each projected variable, what replaces it.
    replacements: HashMap<String, Replacement>,
    /// The constants bound by a `VALUES` block outside the pattern:
    /// for each, the variable in its place and the terms.
    values: Vec<(Variable, Vec<GroundTerm>)>,
    /// The variables that a solution must bind to count.
    required: Vec<Variable>,
}

/// Decides how an instance binds each projected variable.
///
/// A constant replaces its variable, unless
/// - the pattern defines the variable,
/// - a literal would end up in a predicate or graph position,
/// - the constant has more than one term,
/// - the query is complex and enters as a subquery, or
/// - the query keeps its names, since a service may refer to a variable that its pattern does
///   not mention.
///
/// Then the instance keeps a variable, and binds it by a `VALUES` block outside the pattern,
/// with one row per term.
/// A constant bound that way must not meet a solution that leaves its variable unbound,
/// since the row would join with that solution and create a false match.
/// So such a variable is required to be bound,
/// like a joined variable that the pattern may leave unbound.
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
    let always = algebra::always_bound(inner);
    let possibly = algebra::possibly_bound(inner);
    let sometimes_bound = |name: &String| possibly.contains(name) && !always.contains(name);
    let mut rebinding = Rebinding {
        replacements: HashMap::new(),
        values: Vec::new(),
        required: Vec::new(),
    };
    for (name, binding) in projected.iter().zip(bindings) {
        let replacement = match binding {
            Binding::Variable(target) => {
                let target = Variable::new(target).ok()?;
                if joined.contains(target.as_str()) && sometimes_bound(name) {
                    rebinding.required.push(target.clone());
                }

                Replacement::Variable(target)
            }
            Binding::Constant(terms) => {
                let literal_in_node_position = node_positions.contains(name)
                    && terms
                        .iter()
                        .any(|term| matches!(term, GroundTerm::Literal(_)));
                let substitutable = *shape != QueryShape::Complex
                    && !instance.keeps_names
                    && !defined.contains(name)
                    && !literal_in_node_position;
                match terms.as_slice() {
                    [term] if substitutable => Replacement::Term(term.clone()),
                    _ => {
                        let fresh = instance.private(name);
                        if sometimes_bound(name) {
                            rebinding.required.push(fresh.clone());
                        }

                        rebinding.values.push((fresh.clone(), terms.clone()));
                        Replacement::Variable(fresh)
                    }
                }
            }
        };
        rebinding.replacements.insert(name.clone(), replacement);
    }

    Some(rebinding)
}

/// Returns a variable's name in one instance: the name followed by `_` and the instance's
/// suffix.
///
/// If that name is reserved, underscores are appended until it is not.
fn apart(name: &str, suffix: &str, reserved: &HashSet<String>) -> Variable {
    let mut apart = format!("{name}_{suffix}");
    while reserved.contains(&apart) {
        apart.push('_');
    }

    Variable::new_unchecked(apart)
}

/// Returns the variables that a pattern defines rather than matches, which no term can replace.
///
/// They are the variables of `BIND`, `VALUES`, grouping and nested projections.
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

/// Returns the variables in a predicate, graph-name or service-name position,
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

/// Replaces every variable of a pattern in place, as `map` says.
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

        // The path between the two ends has no variables.
        GraphPattern::Path {
            subject, object, ..
        } => {
            rename_term(subject, map);
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
        GraphPattern::Values { variables, .. } => {
            for variable in variables {
                rename_variable(variable, map);
            }
        }
        GraphPattern::Project { inner, variables } => {
            for variable in variables {
                rename_variable(variable, map);
            }

            rename_pattern(inner, map);
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

/// Renames a variable in a position that defines it, where it must stay a variable.
///
/// [rebinding] replaces no variable that the pattern defines by a term,
/// so a term replacement here leaves the name unchanged.
fn rename_variable(variable: &mut Variable, map: &mut impl FnMut(&Variable) -> Replacement) {
    if let Replacement::Variable(renamed) = map(variable) {
        *variable = renamed;
    }
}

/// Replaces the variable in a term position, if there is one.
fn rename_term(term: &mut TermPattern, map: &mut impl FnMut(&Variable) -> Replacement) {
    if let TermPattern::Variable(variable) = term {
        *term = match map(variable) {
            Replacement::Variable(variable) => TermPattern::Variable(variable),
            Replacement::Term(GroundTerm::NamedNode(node)) => TermPattern::NamedNode(node),
            Replacement::Term(GroundTerm::Literal(literal)) => TermPattern::Literal(literal),
        };
    }
}

/// Replaces the variable in a predicate or graph-name position, if there is one.
fn rename_node(node: &mut NamedNodePattern, map: &mut impl FnMut(&Variable) -> Replacement) {
    if let NamedNodePattern::Variable(variable) = node {
        match map(variable) {
            Replacement::Variable(variable) => *node = NamedNodePattern::Variable(variable),
            Replacement::Term(GroundTerm::NamedNode(named)) => {
                *node = NamedNodePattern::NamedNode(named);
            }

            // `rebinding` rules this out, since a literal cannot be a predicate or graph name.
            Replacement::Term(GroundTerm::Literal(_)) => {}
        }
    }
}

/// Replaces every variable of an expression, as `map` says.
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

    /// Returns a binding to a named-node constant.
    fn iri(iri: &str) -> Binding {
        Binding::Constant(vec![GroundTerm::NamedNode(
            spargebra::term::NamedNode::new_unchecked(iri),
        )])
    }

    /// Returns a binding to a variable.
    fn var(name: &str) -> Binding {
        Binding::Variable(String::from(name))
    }

    /// Returns the `SELECT` of the variables over the joined patterns,
    /// with the absent patterns excluded.
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

    /// Returns the parsed generic triple query.
    fn triples() -> SparqlQuery {
        SparqlQuery::parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", None, []).expect("valid")
    }

    /// Two instances of one query join through the variables their bindings share.
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

    /// Each instance renames its private variables apart, and substitutes its constants.
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

    /// A complex query enters as a subquery, with its constants bound through `VALUES` blocks.
    #[test]
    fn complex_queries_enter_as_subqueries_with_values_for_constants() {
        let query = SparqlQuery::parse(
            "SELECT ?a (COUNT(?c) AS ?n) WHERE { ?a ?b ?c } GROUP BY ?a",
            None,
            [],
        )
        .expect("valid");
        let literal = Binding::Constant(vec![GroundTerm::Literal(
            spargebra::term::Literal::new_typed_literal(
                "3",
                spargebra::term::NamedNode::new_unchecked(
                    "http://www.w3.org/2001/XMLSchema#integer",
                ),
            ),
        )]);
        let instance = query
            .instantiate(&[var("x"), literal], "1", &HashSet::new(), &HashSet::new())
            .expect("instantiates");
        let merged = composed(vec![instance], Vec::new(), &["x"]).expect("selects");
        let text = merged.text();
        assert!(text.contains("GROUP BY ?x"), "{text}");
        assert!(text.contains("VALUES ( ?n_1 )"), "{text}");

        // The subquery is kept whole: parsing the text again gives the outer
        // projection, with the aggregation inside.
        let reparsed = SparqlQuery::parse(text, None, []).expect("parses");
        assert_eq!(reparsed.variables(), ["x"]);
        assert_eq!(reparsed.shape(), QueryShape::Complex);
    }

    /// A literal bound to a predicate variable is bound through a `VALUES` block.
    #[test]
    fn a_literal_predicate_is_bound_through_values() {
        let literal = Binding::Constant(vec![GroundTerm::Literal(
            spargebra::term::Literal::new_simple_literal("p"),
        )]);
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

    /// A query calling a service keeps its variable names,
    /// and binds its constants through `VALUES` blocks.
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

        // The constant binds the variable that the service labels, which keeps its name,
        // and so does the private variable `?o`.
        assert!(text.contains("?qid <urn:p> ?o ."), "{text}");
        assert!(text.contains("VALUES ( ?qid )"), "{text}");
    }

    /// Instantiating an opaque query panics.
    #[test]
    #[should_panic(expected = "opaque query")]
    fn opaque_queries_do_not_instantiate() {
        let query =
            SparqlQuery::parse("SELECT ?a WHERE { ?a ?b ?c } LIMIT 5", None, []).expect("valid");
        query.instantiate(&[var("x")], "1", &HashSet::new(), &HashSet::new());
    }

    /// A binding to a variable name that SPARQL does not allow, or a selection of one,
    /// composes nothing.
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

    /// An excluded pattern becomes a `FILTER NOT EXISTS`.
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
