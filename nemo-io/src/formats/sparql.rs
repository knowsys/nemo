//! SPARQL endpoints as sources: each `SELECT` query has one *result table*, its projection.
//!
//! A [SparqlQuery] is a validated query, which builds the [request](SparqlQuery::request) for
//! its solutions.
//! A query can also be asked with *bindings*, values for some of its projected variables,
//! which it sends as `VALUES` blocks ([SparqlQuery::with_values]).
//! A [SparqlReader] reads the endpoint's responses, which are in SPARQL TSV.
//! [compose] merges several queries into one.

pub mod compose;

mod algebra;
mod error;

use std::collections::HashSet;

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    value::{DataValue, NumberRef, ValueDomain, ValueRef},
};

use crate::{
    AboutResource,
    error::IoError,
    file_format::FileFormat,
    load::Loaded,
    selection::RowSelection,
    stream::remote::Request,
    term::{self, BlankNodes, LiteralSyntax},
};

/// The query algebra crate, for composing queries from expressions and patterns.
pub use spargebra;

pub use self::{
    algebra::{QueryShape, ValuesBlock},
    error::QueryError,
};

use spargebra::{
    Query,
    algebra::{Expression, GraphPattern},
    term::{GroundTerm, Literal, NamedNode, Variable},
};

/// The default query, which asks for every triple.
pub const DEFAULT_QUERY: &str = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";

/// The prefix of the name of each label scope in which an endpoint's blank nodes are named.
const BLANK_NODE_SCOPE: &str = "sparql ";

/// Returns the name of the label scope in which an endpoint's blank nodes are named.
fn blank_node_scope(endpoint: &str) -> String {
    format!("{BLANK_NODE_SCOPE}{endpoint}")
}

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
        match crate::formats::rdf::object_term(value)? {
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

    /// Returns the arity of the solution rows.
    pub fn arity(&self) -> usize {
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
    fn pattern(&self) -> &GraphPattern {
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
    /// before the bindings joined into the query select from it.
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
        let maybe = algebra::possibly_bound(self.pattern());
        let service = self.keeps_names();
        self.variables
            .iter()
            .any(|variable| !always.contains(variable) && (maybe.contains(variable) || !service))
    }

    /// Returns the projected variables that no pattern of the query binds, in projection order.
    ///
    /// Unless a service binds such a variable, every solution leaves it unbound.
    pub fn never_bound_variables(&self) -> Vec<&str> {
        let always = algebra::bound_in_results(self.pattern());
        let maybe = algebra::possibly_bound(self.pattern());
        self.variables
            .iter()
            .filter(|variable| !always.contains(*variable) && !maybe.contains(*variable))
            .map(String::as_str)
            .collect()
    }

    /// Returns the request for this query's solutions from an endpoint.
    pub fn request(&self, endpoint: &str) -> Request {
        Request::sparql(endpoint, &self.text)
    }

    /// Returns the query with the bindings joined in.
    ///
    /// There is one `VALUES` block per [ValuesBlock],
    /// over the projected variables that its positions name.
    /// The blocks are joined directly under the outermost projection,
    /// above everything the pattern computes,
    /// so every projected variable is in scope and the blocks select among the final solutions.
    /// A solution that leaves a bound variable unbound, as an `OPTIONAL` that did not match
    /// does, is filtered out below the join.
    /// Otherwise it would join with every row of the block,
    /// while Nemo reads an unbound variable as a null, which matches no binding.
    ///
    /// Each value is sent as its [ground terms](Self::ground_terms), each in a row of its own:
    /// one term, or two for an integer.
    /// The endpoint matches them by its own notion of equal terms.
    /// - A value that the endpoint holds in another datatype or form,
    ///   such as `"3"^^xsd:int` for the integer 3, is not found.
    /// - An endpoint that matches more loosely than Nemo, as QLever matches a double for an
    ///   integer, delivers solutions that Nemo would not match.
    ///   A reader given the blocks ([SparqlReader::with_bindings]) leaves those out.
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
            let assured = algebra::always_bound(&inner);
            let maybe = algebra::possibly_bound(&inner);
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
                    .filter(|name| maybe.contains(*name) && !assured.contains(*name))
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

/// One `VALUES` block in the form that a reader matches solutions against.
#[derive(Debug, Clone)]
struct ValuesLookup {
    /// The positions of the projected columns that the rows bind.
    positions: Vec<usize>,
    /// The rows, one value per position.
    rows: HashSet<Vec<DataValue>>,
}

/// A reader of an endpoint's responses, which contain solutions in SPARQL TSV.
#[derive(Debug, Clone)]
pub struct SparqlReader {
    /// The number of columns of a response row, which is the number of projected variables.
    arity: usize,
    /// The columns delivered and the rows admitted.
    selection: RowSelection,
    /// The bindings that a solution must match, one lookup per `VALUES` block.
    bindings: Vec<ValuesLookup>,
    /// Whether blank node labels name nulls in the endpoint's shared label scope,
    /// rather than in a separate scope for each response.
    endpoint_blank_nodes: bool,
}

impl SparqlReader {
    /// Creates a reader of responses with `arity` columns,
    /// which delivers the columns and rows of `selection`.
    ///
    /// Each response names its blank nodes in a scope of its own,
    /// unless [with_endpoint_blank_nodes](Self::with_endpoint_blank_nodes) shares the scope.
    pub fn new(arity: usize, selection: RowSelection) -> Self {
        Self {
            arity,
            selection,
            bindings: Vec::new(),
            endpoint_blank_nodes: false,
        }
    }

    /// Returns the reader with blank nodes named in the endpoint's shared label scope,
    /// so that a label names the same null in every response of the endpoint.
    ///
    /// SPARQL scopes a label to one result only,
    /// but a store that labels nodes by their internal ids, as Virtuoso does,
    /// keeps a label across results.
    pub fn with_endpoint_blank_nodes(mut self) -> Self {
        self.endpoint_blank_nodes = true;
        self
    }

    /// Returns whether the null `id` is a blank node that an endpoint returned to a reader
    /// [with_endpoint_blank_nodes](Self::with_endpoint_blank_nodes),
    /// rather than a null minted otherwise.
    pub fn is_endpoint_blank_node(dictionary: &Dictionary, id: u64) -> bool {
        dictionary
            .null_scope_name(id)
            .is_some_and(|scope| scope.starts_with(BLANK_NODE_SCOPE))
    }

    /// Returns the reader keeping only the solutions that match the bindings the query was
    /// asked with ([SparqlQuery::with_values]), as Nemo compares values.
    ///
    /// A solution matches if, for each block, its values in the block's columns form one of the
    /// block's rows.
    /// So a read never delivers a solution that its bindings do not justify,
    /// however loosely the endpoint matches terms.
    /// The reader reads a block's columns whether they are delivered or not,
    /// and a solution with an unbound value or a blank node in one of them matches no row.
    pub fn with_bindings(mut self, blocks: &[ValuesBlock]) -> Self {
        self.bindings = blocks
            .iter()
            .map(|block| ValuesLookup {
                positions: block.positions.clone(),
                rows: block.rows.iter().cloned().collect(),
            })
            .collect();
        self
    }

    /// Reads one response from `endpoint`: checks that it is whole, then reads its solutions.
    ///
    /// `context` is the evaluation context of the selection's condition.
    /// A response that the endpoint cut off is recognized here by its body.
    /// A partial response that the endpoint marks in its status or headers is a
    /// [Failure](crate::remote::Failure) of the fetch instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the response is cut off or is not SPARQL TSV.
    /// Every error names the endpoint.
    ///
    /// # Panics
    ///
    /// Panics if a block of the reader's bindings has a position that is not a column of the
    /// responses.
    pub fn read(
        &self,
        endpoint: &str,
        body: &[u8],
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        check_complete(endpoint, body)?;
        let name = blank_node_scope(endpoint);
        let shared = self
            .endpoint_blank_nodes
            .then(|| dictionary.shared_label_scope(&name));
        let blanks = BlankNodes::open(shared, &name, dictionary);
        let read = read_tsv(
            body,
            self.arity,
            &self.selection,
            &self.bindings,
            &blanks,
            dictionary,
            context,
        );
        blanks.close(dictionary);
        read.map_err(|diagnostic| diagnostic.with_resource(endpoint.to_owned()))
    }

    /// Returns whether an endpoint cut a response off partway,
    /// which [read](Self::read) reports as an error.
    pub fn is_cut_off(body: &[u8]) -> bool {
        cut_off(body).is_some()
    }
}

/// Checks that a response is whole, and returns an error naming `endpoint` otherwise.
///
/// An endpoint that hits its own time limit partway may respond with status 200
/// and append the exception to the rows it managed,
/// as Blazegraph does behind the Wikidata query service.
/// A connection closed early leaves the last row unfinished.
/// Either would otherwise be read as a partial result followed by a few malformed rows.
/// Only the lines after the last solution row, which contain the trace if there is one,
/// are searched for the known markers, since a row's cells may contain them too.
fn check_complete(endpoint: &str, body: &[u8]) -> Result<(), Diagnostic> {
    match cut_off(body) {
        Some(reason) => Err(IoError::SparqlResponseCut {
            endpoint: endpoint.to_owned(),
            reason,
        }
        .diagnostic()
        .with_resource(endpoint.to_owned())),
        None => Ok(()),
    }
}

/// Returns why a response was cut off, or `None` if it is whole (see [check_complete]).
///
/// The reason is the start of the line after the last row that says so,
/// or that the response ends partway through a row.
/// A response ends partway through a row if its last line has no line end and is not a whole
/// row, as when its connection closed early without an error message.
/// A whole last row needs no line end.
fn cut_off(body: &[u8]) -> Option<String> {
    /// Texts that mark a line as part of the error trace of a cut-off response.
    const MARKERS: [&str; 3] = [
        "TimeoutException",
        "upstream request timeout",
        "QueryTimeoutException",
    ];
    let text = String::from_utf8_lossy(body);
    let mut lines = text.lines();
    let columns = lines.next()?.split('\t').count();
    let traced = lines
        .rev()
        .take_while(|line| !is_row(line, columns))
        .find(|line| MARKERS.iter().any(|marker| line.contains(marker)))
        .map(|line| line.trim().chars().take(200).collect());
    if traced.is_some() {
        return traced;
    }
    let (_, last) = text.rsplit_once('\n')?;
    let last = last.strip_suffix('\r').unwrap_or(last);
    (!last.is_empty() && !is_whole_row(last, columns))
        .then(|| String::from("the response ends partway through a row"))
}

/// Reads a SPARQL TSV response into rows of the given arity.
///
/// The cells are terms in Turtle syntax:
/// `<iri>`, quoted literals with language tags and datatypes, and bare numbers and booleans.
/// - A blank node is the null that its label names in `blanks`.
/// - An *unbound* cell, as from an `OPTIONAL` that did not match, becomes a fresh null of its
///   own, since the value exists but is unknown.
/// - A cell that is not a term makes the row malformed, whichever columns are delivered.
///
/// In a response of one column, an empty line is a solution whose variable is unbound.
/// In a wider one, it is not a solution.
/// A solution that does not match the `bindings` is left out ([SparqlReader::with_bindings]).
/// `context` is the evaluation context of the selection's condition.
fn read_tsv(
    body: &[u8],
    arity: usize,
    selection: &RowSelection,
    bindings: &[ValuesLookup],
    blanks: &BlankNodes,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<Loaded, Diagnostic> {
    let mut loaded = Loaded::new(selection.arity(arity), context);
    let mut row = Vec::with_capacity(arity);
    let mut kept: Vec<DataValue> = Vec::with_capacity(arity);
    let mut values: Vec<Option<DataValue>> = vec![None; arity];
    // The columns a binding reads, whether delivered or not.
    let mut bound = vec![false; arity];
    for block in bindings {
        for &position in &block.positions {
            bound[position] = true;
        }
    }

    let text = std::str::from_utf8(body).map_err(|error| {
        IoError::SparqlResponseUnreadable {
            reason: error.to_string(),
        }
        .diagnostic()
    })?;
    // SPARQL TSV has no quoting convention, since quotes belong to the
    // terms, so a line is a row and a tab separates its cells. The header
    // row names the variables and is skipped.
    for (index, line) in text.split_terminator('\n').enumerate().skip(1) {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let number = index as u64 + 1;
        if line.is_empty() && arity != 1 {
            continue;
        }
        let cells: Vec<&str> = line.split('\t').collect();
        if cells.len() != arity {
            loaded.count_malformed(number);
            continue;
        }

        // Every cell is read, so that a cell that is no term makes its row malformed,
        // whichever columns are delivered.
        // An unbound cell and a blank node are left without a value here,
        // and get their nulls once the row is known to be kept.
        let mut complete = true;
        for (position, cell) in cells.iter().enumerate() {
            values[position] = None;
            if cell.is_empty() || cell.starts_with("_:") {
                continue;
            }
            match ground_cell(cell) {
                Some(value) => values[position] = Some(value),
                None => {
                    complete = false;
                    break;
                }
            }
        }
        if !complete {
            loaded.count_malformed(number);
            continue;
        }

        // An unbound value or a blank node in a bound column matches no binding,
        // so the row is left out, and no null is minted for it.
        let unmatched = bound
            .iter()
            .zip(&values)
            .any(|(bound, value)| *bound && value.is_none());
        if unmatched || !matches_bindings(&values, bindings) {
            continue;
        }

        kept.clear();
        for (position, cell) in cells.iter().enumerate() {
            if selection.keeps(position) {
                let value = values[position]
                    .take()
                    .or_else(|| term(cell, blanks, dictionary));
                kept.push(value.expect("every cell of the row is a term"));
            }
        }
        loaded.take(&kept, selection, dictionary, &mut row, number);
    }
    tracing::debug!(
        rows = loaded.rows.len(),
        malformed = loaded.malformed,
        excluded = loaded.excluded,
        "read a SPARQL response"
    );
    Ok(loaded)
}

/// Returns whether a solution's values match the bindings:
/// for each block, the values in the block's columns form one of its rows.
///
/// Every bound column must have a value.
fn matches_bindings(values: &[Option<DataValue>], bindings: &[ValuesLookup]) -> bool {
    bindings.iter().all(|block| {
        let key: Vec<DataValue> = block
            .positions
            .iter()
            .map(|&position| values[position].clone().expect("a bound cell is parsed"))
            .collect();
        block.rows.contains(&key)
    })
}

/// Returns whether a line of a response looks like a solution row with `columns` cells,
/// each of which [looks like a term](is_cell).
///
/// The lines of an error trace do not.
fn is_row(line: &str, columns: usize) -> bool {
    let cells: Vec<&str> = line.split('\t').collect();
    cells.len() == columns && cells.into_iter().all(is_cell)
}

/// Returns whether a cell looks like a term by its first characters:
/// an IRI, a literal, a blank node, a boolean, a number, or an unbound cell.
fn is_cell(cell: &str) -> bool {
    cell.is_empty()
        || (cell.starts_with('<') && cell.ends_with('>'))
        || cell.starts_with('"')
        || cell.starts_with("_:")
        || matches!(cell, "true" | "false")
        || starts_as_number(cell)
}

/// Returns whether a cell starts as a number does:
/// with a digit or a point, after an optional sign.
fn starts_as_number(cell: &str) -> bool {
    cell.strip_prefix(['+', '-'])
        .unwrap_or(cell)
        .starts_with(|character: char| character.is_ascii_digit() || character == '.')
}

/// Parses one TSV cell as a term in Turtle syntax,
/// or returns `None` for a cell that is not a term.
///
/// An unbound cell becomes a fresh null,
/// and a blank node becomes the null that its label names in `blanks`.
/// Quoted literals are read in [Turtle's literal syntax](LiteralSyntax::Turtle).
fn term(cell: &str, blanks: &BlankNodes, dictionary: &mut Dictionary) -> Option<DataValue> {
    if cell.is_empty() {
        // An unbound cell is a fresh null every time, since two unbound cells
        // say nothing about being equal.
        return Some(DataValue::null(dictionary.fresh_null()));
    }
    if let Some(label) = cell.strip_prefix("_:") {
        return Some(blanks.null(label, dictionary));
    }
    ground_cell(cell)
}

/// Returns whether a line is a whole solution row with `columns` cells,
/// each of them a [whole cell](is_whole_cell).
fn is_whole_row(line: &str, columns: usize) -> bool {
    let cells: Vec<&str> = line.split('\t').collect();
    cells.len() == columns && cells.into_iter().all(is_whole_cell)
}

/// Returns whether a cell is a whole term by its syntax alone.
///
/// A whole term is an unbound cell, a blank node, an IRI closed by `>`,
/// a literal closed by its quote and possibly followed by a language tag or a datatype IRI,
/// a boolean, or a number.
/// A literal whose datatype rejects its lexical form is whole, and is read as a malformed row.
/// An IRI or a literal that stops before its end is not whole.
fn is_whole_cell(cell: &str) -> bool {
    if let Some(label) = cell.strip_prefix("_:") {
        return !label.is_empty();
    }
    if cell.starts_with('<') {
        return cell.ends_with('>');
    }
    let Some(rest) = cell.strip_prefix('"') else {
        return is_cell(cell);
    };
    // The literal ends at its first unescaped quote, or its first `"""` if it is long.
    let (body, quote) = match rest.strip_prefix("\"\"") {
        Some(long) => (long, "\"\"\""),
        None => (rest, "\""),
    };
    let mut escaped = false;
    for (index, character) in body.char_indices() {
        match character {
            _ if escaped => escaped = false,
            '\\' => escaped = true,
            '"' if body[index..].starts_with(quote) => {
                let suffix = &body[index + quote.len()..];
                return suffix.is_empty()
                    || suffix.strip_prefix('@').is_some_and(|tag| {
                        !tag.is_empty()
                            && tag.chars().all(|character| {
                                character.is_ascii_alphanumeric() || character == '-'
                            })
                    })
                    || suffix
                        .strip_prefix("^^<")
                        .is_some_and(|datatype| datatype.ends_with('>'));
            }
            _ => {}
        }
    }
    false
}

/// Parses a TSV cell that is neither unbound nor a blank node as a term in Turtle syntax,
/// or returns `None` for a cell that is not a term.
fn ground_cell(cell: &str) -> Option<DataValue> {
    if let Some(iri) = term::iri(cell) {
        return Some(DataValue::iri(iri.as_ref()));
    }
    if let Some(rest) = cell.strip_prefix('"') {
        return term::literal(rest, LiteralSyntax::Turtle);
    }
    match cell {
        "true" => return Some(DataValue::boolean(true)),
        "false" => return Some(DataValue::boolean(false)),
        _ => {}
    }
    // Bare numbers have Turtle's three forms: a number with an exponent is a double,
    // and any other is an exact integer or decimal.
    if !starts_as_number(cell) {
        return None;
    }
    if cell.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
        return cell.parse::<f64>().ok().map(DataValue::double);
    }
    DataValue::decimal_from_lexical(cell.strip_prefix('+').unwrap_or(cell))
}

#[cfg(test)]
mod tests;
