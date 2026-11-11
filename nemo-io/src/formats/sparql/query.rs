//! Validated `SELECT` queries: what a query projects, how it is sent,
//! and how values and filters are added to it.

use nemo_physical::value::{DataValue, NumberRef, ValueDomain, ValueRef};
use spargebra::{
    Query,
    algebra::{Expression, GraphPattern},
    term::{GroundTerm, Literal, NamedNode, Variable},
};

use crate::{file_format::FileFormat, remote::Request};

use super::{QueryError, QueryShape, ValuesBlock, algebra, compose};

/// The default query, which asks for every triple.
pub const DEFAULT_QUERY: &str = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";

/// The size of the longest request that is sent as a GET,
/// counted as the bytes of the endpoint and of the rendered query.
///
/// A longer request is sent as a POST form,
/// which is how the SPARQL protocol stays under URL length limits.
const GET_LIMIT: usize = 2_000;

/// A validated `SELECT` query, with its algebra, its text as sent,
/// and its projection, which is the result table.
///
/// # Example
///
/// ```
/// use nemo_io::sparql::{QueryShape, SparqlQuery};
///
/// let query = SparqlQuery::parse(
///     "SELECT ?city ?name WHERE { ?city ex:name ?name }",
///     None,
///     [("ex", "http://example.org/")],
/// )
/// .expect("the query parses");
/// assert_eq!(query.variables(), ["city", "name"]);
/// assert_eq!(query.shape(), QueryShape::Pattern);
///
/// // The request is plain data, which any HTTP client can send.
/// let request = query.request("https://example.org/sparql");
/// assert_eq!(request.method(), "GET");
/// ```
#[derive(Debug, Clone)]
pub struct SparqlQuery {
    /// The parsed query, with prefixes and base resolved.
    query: Query,
    /// The query text, as sent.
    text: String,
    /// The projected variables, in projection order: the columns of the result.
    variables: Vec<String>,
}

impl SparqlQuery {
    /// Parses and validates a `SELECT` query, with the given base and prefixes in scope.
    ///
    /// The prefixes let the query use prefixed names declared outside it,
    /// without a `PREFIX` of its own.
    /// A prefix name that the SPARQL grammar does not allow is left out of scope.
    ///
    /// # Errors
    ///
    /// Returns an error for a base that is not an IRI, a text that does not parse,
    /// or a query that is not a `SELECT`.
    pub fn parse<'a>(
        text: &str,
        base: Option<&str>,
        prefixes: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, QueryError> {
        let mut parser = spargebra::SparqlParser::new();
        if let Some(base) = base {
            parser = parser
                .with_base_iri(base)
                .map_err(|error| QueryError::Base {
                    base: base.to_owned(),
                    reason: error.to_string(),
                })?;
        }

        for (name, iri) in prefixes {
            // A prefix name that the SPARQL grammar does not allow stays
            // out of scope for the query. An empty name is allowed, but
            // not every name given is.
            if let Ok(seeded) = parser.clone().with_prefix(name, iri) {
                parser = seeded;
            }
        }

        let query = parser
            .parse_query(text)
            .map_err(|error| QueryError::Syntax {
                reason: error.to_string(),
            })?;

        Self::from_select(query).ok_or(QueryError::NotSelect)
    }

    /// Wraps a parsed query, or returns `None` for a query that is not a
    /// `SELECT`.
    pub(crate) fn from_select(query: Query) -> Option<Self> {
        let Query::Select { pattern, .. } = &query else {
            return None;
        };

        let mut variables: Vec<String> = Vec::new();
        pattern.on_in_scope_variable(|variable| {
            if !variables.iter().any(|seen| seen == variable.as_str()) {
                variables.push(variable.as_str().to_owned());
            }
        });

        Some(Self {
            // The parsed form is rendered with the prefixes and base
            // resolved, so the request needs no PREFIX declarations.
            text: query.to_string(),
            query,
            variables,
        })
    }

    /// Returns the number of columns of a solution row.
    pub fn width(&self) -> usize {
        self.variables.len()
    }

    /// Returns the projected variable names, in column order.
    pub fn variables(&self) -> &[String] {
        &self.variables
    }

    /// Returns the query text as sent, with prefixes and base resolved.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the parsed query.
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// Returns the query's dataset clause (`FROM`), if it has one.
    ///
    /// A dataset applies to a whole query,
    /// so a [composition](compose::Composition) has at most one.
    pub fn dataset(&self) -> Option<&spargebra::algebra::QueryDataset> {
        self.query.dataset()
    }

    /// Returns whether the query must keep its variable names when composed.
    ///
    /// That is the case if it calls a `SERVICE`, which may read or bind variables by name,
    /// as Wikidata's label service binds `?xLabel` for `?x`.
    pub fn keeps_names(&self) -> bool {
        let mut service = false;
        algebra::visit_patterns(self.pattern(), &mut |pattern| {
            service |= matches!(pattern, GraphPattern::Service { .. });
        });

        service
    }

    /// Returns the graph pattern of the query.
    pub(super) fn pattern(&self) -> &GraphPattern {
        match &self.query {
            Query::Select { pattern, .. } => pattern,
            _ => unreachable!("a SparqlQuery is a SELECT"),
        }
    }

    /// Returns the shape of the query, which says what can be joined or filtered into it.
    pub fn shape(&self) -> QueryShape {
        algebra::shape(self.pattern())
    }

    /// Returns whether the query groups its solutions or has a subquery.
    ///
    /// The endpoint computes such a part in full,
    /// before the values joined into the query select from it.
    pub fn computes_in_full(&self) -> bool {
        algebra::computes_in_full(self.pattern())
    }

    /// Returns whether the query may leave one of its projected variables unbound.
    ///
    /// Such a variable is
    /// - a variable that the query binds in some solutions but not in others,
    ///   such as a variable bound in an `OPTIONAL` that does not always match,
    ///   or a computed value that may fail, or
    /// - a variable that no pattern binds.
    ///
    /// In a query that calls a service, a variable that no pattern binds does not count,
    /// since the service may bind it, as a label service binds the label variables of the items
    /// that the query asks about.
    /// The result is conservative: a variable that the query computes counts as sometimes
    /// unbound.
    pub fn may_leave_unbound(&self) -> bool {
        let always = algebra::bound_in_results(self.pattern());
        let possibly = algebra::possibly_bound(self.pattern());
        let service = self.keeps_names();

        self.variables
            .iter()
            .any(|variable| !always.contains(variable) && (possibly.contains(variable) || !service))
    }

    /// Returns the projected variables that no pattern of the query binds, in projection order.
    ///
    /// Unless a service binds such a variable, every solution leaves it unbound.
    pub fn never_bound_variables(&self) -> Vec<&str> {
        let always = algebra::bound_in_results(self.pattern());
        let possibly = algebra::possibly_bound(self.pattern());

        self.variables
            .iter()
            .filter(|variable| !always.contains(*variable) && !possibly.contains(*variable))
            .map(String::as_str)
            .collect()
    }

    /// Returns the request for this query's solutions from an endpoint.
    pub fn request(&self, endpoint: &str) -> Request {
        Request::sparql(endpoint, &self.text)
    }

    /// Returns the query with the values joined in.
    ///
    /// There is one `VALUES` block per [ValuesBlock],
    /// over the projected variables that its positions name.
    /// The blocks are joined directly under the outermost projection,
    /// above everything the pattern computes,
    /// so every projected variable is in scope and the blocks select among the final solutions.
    /// A solution that leaves a bound variable unbound, as an `OPTIONAL` that did not match
    /// does, is filtered out below the join.
    /// Otherwise it would join with every row of the block,
    /// while Nemo reads an unbound variable as a null, which matches no value.
    ///
    /// Each value is sent as its [ground terms](Self::ground_terms), each in a row of its own:
    /// one term, or two for an integer.
    /// The endpoint matches them by its own notion of equal terms.
    /// - A value that the endpoint holds in another datatype or form,
    ///   such as `"3"^^xsd:int` for the integer 3, is not found.
    /// - An endpoint that matches more loosely than Nemo, as QLever matches a double for an
    ///   integer, delivers solutions that Nemo would not match.
    ///   A reader given the blocks ([SparqlReader::with_values](super::SparqlReader::with_values)) leaves those out.
    ///
    /// A row with a value that has no ground term, such as a null, is left out of its block.
    ///
    /// # Panics
    ///
    /// Panics for an [opaque](QueryShape::Opaque) query,
    /// since no part of such a query selects from its whole result.
    /// The query's [shape](Self::shape) says whether it is opaque.
    /// Panics also if a block has a position that is not one of the projected variables.
    pub fn with_values(&self, blocks: &[ValuesBlock]) -> Self {
        if let QueryShape::Opaque(reason) = self.shape() {
            panic!("values cannot be joined into an opaque query: {reason}");
        }

        let variables = &self.variables;
        let pattern = algebra::under_projection(self.pattern().clone(), |inner| {
            // Only a variable that the pattern binds in some solutions but not
            // all is guarded. A variable that the pattern never binds is bound by
            // the block itself. A filter above the block that requires the variable
            // to be bound would not guard it, since it sees the block's binding.
            let always = algebra::always_bound(&inner);
            let possibly = algebra::possibly_bound(&inner);
            let mut bound: Vec<usize> = blocks
                .iter()
                .flat_map(|block| block.positions.iter().copied())
                .collect();
            bound.sort_unstable();
            bound.dedup();
            let guard = compose::conjunction(
                bound
                    .into_iter()
                    .map(|position| &variables[position])
                    .filter(|name| possibly.contains(*name) && !always.contains(*name))
                    .map(|name| Expression::Bound(Variable::new_unchecked(name.as_str()))),
            );
            let mut inner = match guard {
                Some(expr) => algebra::grouped(GraphPattern::Filter {
                    expr,
                    inner: Box::new(inner),
                }),
                None => inner,
            };
            for block in blocks {
                inner = GraphPattern::Join {
                    left: Box::new(inner),
                    right: Box::new(block.pattern(variables)),
                };
            }

            inner
        });

        Self::from_select(self.with_pattern(pattern)).expect("the query stays a SELECT")
    }

    /// Returns the query with a filter on its projected variables,
    /// added directly under the outermost projection.
    ///
    /// # Panics
    ///
    /// Panics for an [opaque](QueryShape::Opaque) query.
    /// The query's [shape](Self::shape) says whether it is opaque.
    pub fn with_filter(&self, expression: Expression) -> Self {
        if let QueryShape::Opaque(reason) = self.shape() {
            panic!("a condition cannot be filtered into an opaque query: {reason}");
        }

        let pattern =
            algebra::under_projection(self.pattern().clone(), |inner| GraphPattern::Filter {
                expr: expression,
                inner: Box::new(inner),
            });

        Self::from_select(self.with_pattern(pattern)).expect("the query stays a SELECT")
    }

    /// Returns this query with another graph pattern, keeping its dataset and base.
    fn with_pattern(&self, pattern: GraphPattern) -> Query {
        Query::Select {
            dataset: self.query.dataset().cloned(),
            pattern,
            base_iri: self.query.base_iri().cloned(),
        }
    }

    /// Returns a value as a SPARQL ground term, in the value's canonical lexical form.
    ///
    /// - IRIs become named nodes.
    /// - Strings and language strings become literals.
    /// - Every other scalar becomes a typed literal of its domain's datatype.
    ///   For example, an integer becomes an `xsd:integer`, whatever datatype it was read from,
    ///   and a double is written in XSD's canonical form, such as `"2.5E0"^^xsd:double`.
    ///   The canonical form is the one most likely held by a store that keeps lexical forms as
    ///   written.
    ///
    /// An endpoint finds the value by this term only if it holds the value as this term,
    /// or canonicalizes values when loading them (see [with_values](Self::with_values)).
    ///
    /// Returns `None` for a null, since a query cannot ask for a blank node,
    /// and for tuples and maps, which have no RDF form.
    pub fn ground_term(value: &DataValue) -> Option<GroundTerm> {
        match crate::rdf::object_term(value)? {
            spargebra::term::Term::NamedNode(node) => Some(GroundTerm::NamedNode(node)),
            spargebra::term::Term::Literal(literal) => {
                let canonical = match value.as_ref() {
                    ValueRef::Number(NumberRef::Double(double)) => double.canonical_lexical(),
                    ValueRef::Number(NumberRef::Float(float)) => float.canonical_lexical(),
                    _ => return Some(GroundTerm::Literal(literal)),
                };
                Some(GroundTerm::Literal(Literal::new_typed_literal(
                    canonical,
                    literal.datatype().into_owned(),
                )))
            }
            spargebra::term::Term::BlankNode(_) => None,
            // `Term` has a variant for triple terms if a crate of the build turns on RDF 1.2.
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Returns the terms an endpoint may hold a value as, for asking for the value by term.
    ///
    /// They are the value's [ground term](Self::ground_term), and, for an integer, also the
    /// decimal that an endpoint holding quantities as decimals has,
    /// such as `"3.0"^^xsd:decimal` for the integer 3.
    /// Returns an empty list for a value without a ground term.
    pub fn ground_terms(value: &DataValue) -> Vec<GroundTerm> {
        let Some(term) = Self::ground_term(value) else {
            return Vec::new();
        };

        let decimal = match &term {
            GroundTerm::Literal(literal) => Self::integer_as_decimal(literal),
            _ => None,
        };
        match decimal {
            Some(decimal) => vec![term, GroundTerm::Literal(decimal)],
            None => vec![term],
        }
    }

    /// Returns an `xsd:integer` literal as an `xsd:decimal` with one zero after the point,
    /// such as `"3"` as `"3.0"^^xsd:decimal`, or `None` for any other literal.
    ///
    /// Stores keep datatypes apart, and some hold every quantity as a decimal,
    /// as Wikidata does, so a query for the integer does not find it.
    /// The decimal is written as such a store writes an integral quantity.
    fn integer_as_decimal(literal: &Literal) -> Option<Literal> {
        let decimal = ValueDomain::Decimal
            .datatype_iri()
            .expect("decimals have one datatype");

        (Some(literal.datatype().as_str()) == ValueDomain::Integer.datatype_iri()).then(|| {
            Literal::new_typed_literal(
                format!("{}.0", literal.value()),
                NamedNode::new_unchecked(decimal),
            )
        })
    }
}

impl Request {
    /// Creates the request for the solutions of a rendered query from an endpoint.
    ///
    /// The request is a GET with the query as a parameter,
    /// or a POST form if the URL would be too long.
    pub fn sparql(endpoint: &str, text: &str) -> Self {
        let mut request = Self::get(endpoint);
        request.accept = FileFormat::Sparql.media_type();
        if endpoint.len() + text.len() <= GET_LIMIT {
            request
                .parameters
                .push((String::from("query"), text.to_owned()));
        } else {
            request.form = Some(vec![(String::from("query"), text.to_owned())]);
        }

        request
    }
}

#[cfg(test)]
mod tests;
