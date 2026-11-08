//! SPARQL endpoint imports: one query, one result table.
//!
//! An import fetches the query's solutions over the SPARQL protocol:
//! a GET with the query as a URL parameter while it is short, a POST form beyond that.
//! The response is requested and parsed as SPARQL TSV, whose cells are Turtle-syntax terms.
//! A query asked once delivers its whole result.
//! A query asked with bindings carries them as `VALUES` blocks, a value as its terms
//! ([SparqlQuery::ground_terms]), and the reader keeps the solutions that match them
//! ([SparqlReader::with_bindings]).
//! [SparqlQuery::with_values] says what makes that sound,
//! and [compose] how several queries merge into one.
//!
//! A blank node in a response is a null, which its label names in a
//! [label scope](nemo_physical::dictionary::LabelScope): the endpoint's, shared by all its
//! responses ([SparqlReader::with_endpoint_blank_nodes]),
//! or, for a reader given none, the response's own.
//! A response cut short is an error, whether the endpoint says so in its body
//! ([SparqlReader::read]) or in its status and headers
//! ([Failure::of_partial_response](crate::remote::Failure::of_partial_response),
//! which the HTTP client checks).

pub mod compose;

mod algebra;
mod error;

use std::collections::HashSet;

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    value::{DataValue, NumberRef, ValueRef},
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

/// The datatype IRI of `xsd:integer`.
const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// The datatype IRI of `xsd:decimal`.
const DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";

/// The query an import without one asks: everything.
pub const DEFAULT_QUERY: &str = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";

/// The start of the name of every label scope an endpoint's blank nodes are named in.
const BLANK_NODE_SCOPE: &str = "sparql ";

/// Returns the name of the label scope an endpoint's blank nodes are named in.
fn blank_node_scope(endpoint: &str) -> String {
    format!("{BLANK_NODE_SCOPE}{endpoint}")
}

/// The longest rendered query that travels as a GET parameter.
///
/// A longer one travels as a POST form,
/// the SPARQL protocol's convention for staying under URL length limits.
const GET_LIMIT: usize = 2_000;

/// A validated `SELECT` query: its algebra, its text as sent, and the
/// projection that is the imported table.
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
    /// The parsed query, prefixes and base resolved.
    query: Query,
    /// The query text, as sent.
    text: String,
    /// The projected variables, in projection order: the columns of the result.
    variables: Vec<String>,
}

impl SparqlQuery {
    /// Returns a value as a SPARQL ground term, in the value's canonical lexical form.
    ///
    /// IRIs become named nodes, strings and language strings literals,
    /// and every other scalar a typed literal of its domain's datatype:
    /// an integer is an `xsd:integer`, whatever datatype it was read from,
    /// and a double is `"2.5E0"^^xsd:double`, in XSD's canonical form,
    /// the form a store keeping lexical forms as written most likely holds.
    /// An endpoint finds the value through this term only if it holds the value as this term,
    /// or canonicalises it on load (see [with_values](Self::with_values)).
    ///
    /// Returns `None` for a null, since a query cannot ask for a blank node,
    /// and for the structured values, which have no RDF form.
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
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Returns the terms a value goes out as where an endpoint compares it as a term,
    /// any of which the endpoint may hold it as: its [ground term](Self::ground_term),
    /// and for an integer also the decimal an endpoint holding quantities as decimals finds,
    /// `"3.0"^^xsd:decimal` for the integer 3.
    ///
    /// Empty for a value without a ground term.
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

    /// Returns an `xsd:integer` literal as the `xsd:decimal` with one zero after the point,
    /// `"3"` as `"3.0"^^xsd:decimal`, or `None` for any other literal.
    ///
    /// Stores keep datatypes apart, and some hold every quantity as a decimal, as Wikidata
    /// does, which an integer in its own datatype does not find.
    /// The decimal is written as such a store writes an integral quantity, `"3.0"`.
    fn integer_as_decimal(literal: &Literal) -> Option<Literal> {
        (literal.datatype().as_str() == INTEGER).then(|| {
            Literal::new_typed_literal(
                format!("{}.0", literal.value()),
                NamedNode::new_unchecked(DECIMAL),
            )
        })
    }

    /// Parses and validates a `SELECT` query, whose projection is the result table,
    /// with the given base and prefixes in scope.
    ///
    /// So the query can use prefixes declared outside it without a `PREFIX` of its own.
    ///
    /// # Errors
    ///
    /// Returns an error if the text is not a query an import can ask:
    /// a base that is not an IRI, a text that does not parse, or a query that is not a `SELECT`.
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
            // A prefix the SPARQL grammar cannot carry (empty names are
            // fine, but not every Nemo prefix is) simply stays out of
            // scope for the query.
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
            // The parsed form renders with the prefixes and base
            // resolved, so the request needs no PREFIX preamble.
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

    /// Returns the query text as sent, prefixes and base resolved.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the parsed query.
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// Returns the query's dataset clause (`FROM`), if it has one.
    ///
    /// A query with a dataset is never merged with another,
    /// since a dataset applies to a whole query.
    pub fn dataset(&self) -> Option<&spargebra::algebra::QueryDataset> {
        self.query.dataset()
    }

    /// Returns whether the query keeps its variable names when composed:
    /// it calls a `SERVICE`, which may read or bind variables by name,
    /// as Wikidata's label service binds `?xLabel` for `?x`.
    pub fn keeps_names(&self) -> bool {
        let mut service = false;
        algebra::visit_patterns(self.pattern(), &mut |pattern| {
            service |= matches!(pattern, GraphPattern::Service { .. });
        });
        service
    }

    /// Returns the query's selection pattern.
    fn pattern(&self) -> &GraphPattern {
        match &self.query {
            Query::Select { pattern, .. } => pattern,
            _ => unreachable!("a SparqlQuery is a SELECT"),
        }
    }

    /// Returns what may be pushed into the query, as [QueryShape] describes.
    pub fn shape(&self) -> QueryShape {
        algebra::shape(self.pattern())
    }

    /// Returns whether the query groups its solutions or holds a subquery,
    /// a part the endpoint computes in full before the bindings joined in select from it.
    pub fn computes_in_full(&self) -> bool {
        algebra::computes_in_full(self.pattern())
    }

    /// Returns whether the query may leave one of its projected variables unbound:
    /// a variable it binds in some solutions but not in others,
    /// as an `OPTIONAL` that does not match leaves it unbound, or a computed value that fails,
    /// or a variable no pattern binds.
    ///
    /// In a query calling a service, a variable no pattern binds does not count:
    /// the service may bind it, as a label service binds the label variables of the items
    /// the query is asked with.
    /// The result is conservative: a variable the query computes counts as sometimes unbound.
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
    /// Outside a service, nothing binds such a variable, so every solution leaves it unbound.
    pub fn never_bound_variables(&self) -> Vec<&str> {
        let always = algebra::bound_in_results(self.pattern());
        let maybe = algebra::possibly_bound(self.pattern());
        self.variables
            .iter()
            .filter(|variable| !always.contains(*variable) && !maybe.contains(*variable))
            .map(String::as_str)
            .collect()
    }

    /// Returns the request fetching this query's solutions from an endpoint.
    pub fn request(&self, endpoint: &str) -> Request {
        Request::sparql(endpoint, &self.text)
    }

    /// Returns the query text with the bindings joined in.
    ///
    /// There is one `VALUES` block per [ValuesBlock],
    /// over the projected variables its positions name.
    /// The blocks join directly under the outermost projection,
    /// above everything the pattern computes,
    /// so every projected variable is in scope and the blocks select among the final solutions.
    /// A solution leaving a bound variable unbound (an `OPTIONAL` that did not match)
    /// is filtered out below the join:
    /// it would join with every row of the block,
    /// and the null it becomes in the result stands for no binding.
    ///
    /// Each value goes out as its [ground terms](Self::ground_terms): one,
    /// or for an integer two, each a row of its own,
    /// which the endpoint matches by its own notion of equal terms.
    /// A value the endpoint holds in another datatype or form, such as `"3"^^xsd:int` for the
    /// integer 3, is not found.
    /// An endpoint matching more loosely than Nemo, as QLever finds a double for an integer,
    /// delivers solutions Nemo would not match.
    /// A reader told the blocks ([SparqlReader::with_bindings]) leaves those out.
    /// A row holding a value without a ground term, such as a null, is left out of its block.
    ///
    /// # Panics
    ///
    /// Panics for an [opaque](QueryShape::Opaque) query,
    /// since no injection point of such a query is a selection on the whole result.
    /// The [shape](Self::shape) tells.
    pub fn with_values(&self, blocks: &[ValuesBlock]) -> Self {
        if let QueryShape::Opaque(reason) = self.shape() {
            panic!("values cannot be joined into an opaque query: {reason}");
        }
        let variables = &self.variables;
        let pattern = algebra::under_projection(self.pattern().clone(), |inner| {
            // Only a variable the pattern binds in some solutions but not
            // all is guarded; one it never binds the block itself binds.
            // A filter above the block requiring it bound does not guard it,
            // since it sees the variable the block bound.
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

    /// Returns the query with a condition over its projected variables filtered in,
    /// directly under the outermost projection.
    ///
    /// # Panics
    ///
    /// Panics for an [opaque](QueryShape::Opaque) query.
    /// The [shape](Self::shape) tells.
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

    /// Returns this query with another selection pattern, its dataset and base kept.
    fn with_pattern(&self, pattern: GraphPattern) -> Query {
        Query::Select {
            dataset: self.query.dataset().cloned(),
            pattern,
            base_iri: self.query.base_iri().cloned(),
        }
    }
}

impl Request {
    /// Creates the request fetching a rendered query's solutions from an endpoint:
    /// a GET with the query as its parameter, or a POST form if the URL would grow too long.
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

/// The reader of an endpoint's responses, solutions in SPARQL TSV,
/// read like the other formats' resources.
#[derive(Debug, Clone)]
pub struct SparqlReader {
    /// The number of columns a response row has, the number of projected variables.
    arity: usize,
    /// The selection over the delivered columns.
    selection: RowSelection,
    /// The bindings a solution must match, per `VALUES` block:
    /// the block's columns, by position, and its rows.
    bindings: Vec<(Vec<usize>, HashSet<Vec<DataValue>>)>,
    /// Whether blank node labels name nulls in the endpoint's shared label scope,
    /// rather than in a scope of each response's own.
    endpoint_blank_nodes: bool,
}

impl SparqlReader {
    /// Creates a reader of responses of the given width, delivering the selection.
    ///
    /// Each response's blank node labels name nulls of its own,
    /// unless [with_endpoint_blank_nodes](Self::with_endpoint_blank_nodes) shares them.
    pub fn new(arity: usize, selection: RowSelection) -> Self {
        Self {
            arity,
            selection,
            bindings: Vec::new(),
            endpoint_blank_nodes: false,
        }
    }

    /// Returns the reader naming blank nodes in the endpoint's shared label scope,
    /// so that the endpoint's responses agree on them: a label names the same null in every
    /// response.
    ///
    /// SPARQL scopes a label to one result only, but stores labelling nodes by their internal
    /// ids, as in Jena's TSV results, Virtuoso, Oxigraph and QLever, keep it across results.
    pub fn with_endpoint_blank_nodes(mut self) -> Self {
        self.endpoint_blank_nodes = true;
        self
    }

    /// Returns whether a null is a blank node an endpoint returned to a reader
    /// [with_endpoint_blank_nodes](Self::with_endpoint_blank_nodes),
    /// rather than one a rule or another source made.
    pub fn is_endpoint_blank_node(dictionary: &Dictionary, id: u64) -> bool {
        dictionary
            .null_scope_name(id)
            .is_some_and(|scope| scope.starts_with(BLANK_NODE_SCOPE))
    }

    /// Returns the reader keeping only the solutions that match the bindings the query was
    /// asked with ([SparqlQuery::with_values]) as Nemo compares values:
    /// for each block, the solution's values in the block's columns form one of its rows.
    ///
    /// So an import asked with bindings never holds a solution its bindings don't justify,
    /// however loosely the endpoint matches terms.
    /// It reads a block's columns whether they are delivered or not.
    pub fn with_bindings(mut self, blocks: &[ValuesBlock]) -> Self {
        self.bindings = blocks
            .iter()
            .map(|block| {
                (
                    block.positions.clone(),
                    block.rows.iter().cloned().collect(),
                )
            })
            .collect();
        self
    }

    /// Reads one response: checks that it is whole, then reads its solutions.
    ///
    /// The selection's condition evaluates in the given context.
    ///
    /// # Errors
    ///
    /// Returns an error if the response is cut off or is not SPARQL TSV.
    /// Every error names the endpoint.
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

    /// Returns whether an endpoint cut a response off partway, as [read](Self::read) reports.
    pub fn is_cut_off(body: &[u8]) -> bool {
        cut_off(body).is_some()
    }
}

/// Checks that a response is whole.
///
/// An endpoint that hits its own time limit partway responds with `200`
/// and appends the exception to the rows it managed,
/// as Blazegraph does behind the Wikidata query service.
/// A connection closed early leaves the last row unfinished.
/// Either would read as a few malformed rows after a silently partial result.
/// Only the lines after the last solution row, a trace if there is one,
/// are searched for the known markers,
/// since a row's cells may contain them too.
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

/// Returns why a response was cut off, if it was (see [check_complete]):
/// the start of the line saying so after the last row,
/// or that the response ends partway through a row.
///
/// A response ends partway through a row if its last line is unterminated and not a whole
/// row, as when its connection closed early without a word.
/// A whole last row needs no line end.
fn cut_off(body: &[u8]) -> Option<String> {
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
/// Cells are Turtle-syntax terms:
/// `<iri>`, quoted literals with language tags and datatypes, and bare numbers and booleans.
/// A blank node is the null its label names in `blanks`.
/// An *unbound* cell (an `OPTIONAL` that did not match) mints a fresh null of its own:
/// the value exists, but is unknown.
/// A cell that is not a term makes the row malformed, and is counted,
/// if its column is delivered or bound.
/// In a response of one column, an empty line is a solution whose variable is unbound;
/// in a wider one, it is no solution.
/// A solution not matching the `bindings` is left out (see [SparqlReader::with_bindings]).
/// The selection's condition evaluates in `context`.
fn read_tsv(
    body: &[u8],
    arity: usize,
    selection: &RowSelection,
    bindings: &[(Vec<usize>, HashSet<Vec<DataValue>>)],
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
    for (positions, _) in bindings {
        for &position in positions {
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
        // An unbound value or a blank node in a bound column matches no binding:
        // the row is left out before anything is read, minting no null for it.
        if cells
            .iter()
            .zip(&bound)
            .any(|(cell, bound)| *bound && (cell.is_empty() || cell.starts_with("_:")))
        {
            continue;
        }
        // Each delivered or bound cell is parsed once:
        // a dropped column that no binding reads cannot make the row malformed.
        let mut complete = true;
        for (position, cell) in cells.iter().enumerate() {
            values[position] = None;
            if selection.keeps(position) || bound[position] {
                match term(cell, blanks, dictionary) {
                    Some(value) => values[position] = Some(value),
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
        }
        if !complete {
            loaded.count_malformed(number);
            continue;
        }
        if !matches_bindings(&values, bindings) {
            continue;
        }
        kept.clear();
        kept.extend(
            values
                .iter_mut()
                .enumerate()
                .filter(|(position, _)| selection.keeps(*position))
                .map(|(_, value)| value.take().expect("a delivered cell is parsed")),
        );
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

/// Returns whether a solution's values match the bindings: for each block,
/// the values in its columns form one of its rows.
///
/// Every bound column holds a value: [read_tsv] parsed it.
fn matches_bindings(
    values: &[Option<DataValue>],
    bindings: &[(Vec<usize>, HashSet<Vec<DataValue>>)],
) -> bool {
    bindings.iter().all(|(positions, rows)| {
        let key: Vec<DataValue> = positions
            .iter()
            .map(|&position| values[position].clone().expect("a bound cell is parsed"))
            .collect();
        rows.contains(&key)
    })
}

/// Returns whether a line of a response is a solution row of the given width:
/// as many cells as the header, each shaped like a term
/// (an IRI, a literal, a blank node, a boolean, a number, or unbound).
///
/// The lines of a trace are not.
fn is_row(line: &str, columns: usize) -> bool {
    let cells: Vec<&str> = line.split('\t').collect();
    cells.len() == columns && cells.into_iter().all(is_cell)
}

/// Returns whether a cell is shaped like a term, as [is_row] describes.
fn is_cell(cell: &str) -> bool {
    cell.is_empty()
        || (cell.starts_with('<') && cell.ends_with('>'))
        || cell.starts_with('"')
        || cell.starts_with("_:")
        || matches!(cell, "true" | "false")
        || cell
            .strip_prefix(['+', '-'])
            .unwrap_or(cell)
            .starts_with(|character: char| character.is_ascii_digit() || character == '.')
}

/// Parses one TSV cell as a Turtle-syntax term, or returns `None` for a cell that is not a term.
///
/// Quoted literals read in [Turtle's literal syntax](LiteralSyntax::Turtle),
/// and nulls are minted as [SparqlReader::read] describes.
fn term(cell: &str, blanks: &BlankNodes, dictionary: &mut Dictionary) -> Option<DataValue> {
    if cell.is_empty() {
        // Unbound: a fresh null every time, since two unbound cells
        // assert nothing about being equal.
        return Some(DataValue::null(dictionary.fresh_null()));
    }
    if let Some(label) = cell.strip_prefix("_:") {
        return Some(blanks.null(label, dictionary));
    }
    ground_cell(cell)
}

/// Returns whether a line is a whole solution row of the given width:
/// as many cells, each a [whole cell](is_whole_cell).
fn is_whole_row(line: &str, columns: usize) -> bool {
    let cells: Vec<&str> = line.split('\t').collect();
    cells.len() == columns && cells.into_iter().all(is_whole_cell)
}

/// Returns whether a cell is a whole term by its syntax alone:
/// unbound, a blank node, an IRI closed by `>`, a literal closed by its quote
/// (with a language tag or a datatype IRI after it), a boolean, or a number.
///
/// A literal whose datatype rejects its lexical form is whole, and reads as a malformed row;
/// an IRI or a literal that stops before its end is not.
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

/// Parses one TSV cell that is neither unbound nor a blank node as a Turtle-syntax term,
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
    // Bare numbers, in Turtle's three forms: exponents are doubles,
    // everything else takes the exact path.
    let numeric = cell
        .strip_prefix(['+', '-'])
        .unwrap_or(cell)
        .starts_with(|character: char| character.is_ascii_digit() || character == '.');
    if !numeric {
        return None;
    }
    if cell.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
        return cell.parse::<f64>().ok().map(DataValue::double);
    }
    DataValue::decimal_from_lexical(cell.strip_prefix('+').unwrap_or(cell))
}

#[cfg(test)]
mod tests;
