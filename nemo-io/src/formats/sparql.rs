//! SPARQL endpoint imports: one query, one result table.
//!
//! An import fetches the query's solutions over the SPARQL protocol:
//! a GET with the query as a URL parameter while it is short, a POST form beyond that.
//! The response is requested and parsed as SPARQL TSV, whose cells are Turtle-syntax terms.
//! A query asked once delivers its whole result.
//! A query asked with bindings carries them as `VALUES` blocks.
//! [SparqlQuery::with_values] says what makes that sound,
//! and [compose] how several queries merge into one.

pub mod compose;

mod algebra;
mod error;

use std::io::{Cursor, Read};

use csv::ReaderBuilder;

use nemo_physical::{
    diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext, value::DataValue,
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
    term::{GroundTerm, Variable},
};

/// The query an import without one asks: everything.
pub const DEFAULT_QUERY: &str = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";

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
    /// and every other scalar a typed literal.
    /// Returns `None` for a null, since a query cannot ask for a blank node,
    /// and for the structured values, which have no RDF form.
    pub fn ground_term(value: &DataValue) -> Option<GroundTerm> {
        match crate::formats::rdf::object_term(value)? {
            spargebra::term::Term::NamedNode(node) => Some(GroundTerm::NamedNode(node)),
            spargebra::term::Term::Literal(literal) => Some(GroundTerm::Literal(literal)),
            spargebra::term::Term::BlankNode(_) => None,
            #[allow(unreachable_patterns)]
            _ => None,
        }
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
    /// # Panics
    ///
    /// Panics for an [opaque](QueryShape::Opaque) query,
    /// since no injection point of such a query is a selection on the whole result.
    /// The [shape](Self::shape) tells.
    pub fn with_values(&self, blocks: &[ValuesBlock]) -> String {
        if let QueryShape::Opaque(reason) = self.shape() {
            panic!("values cannot be joined into an opaque query: {reason}");
        }
        let variables = &self.variables;
        let pattern = algebra::under_projection(self.pattern().clone(), |inner| {
            // Only a variable the pattern binds in some solutions but not
            // all is guarded; one it never binds the block itself binds.
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
        self.with_pattern(pattern).to_string()
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
}

impl SparqlReader {
    /// Creates a reader of responses of the given width, delivering the selection.
    pub fn new(arity: usize, selection: RowSelection) -> Self {
        Self { arity, selection }
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
        read_tsv(
            Cursor::new(body),
            self.arity,
            &self.selection,
            dictionary,
            context,
        )
        .map_err(|diagnostic| diagnostic.with_resource(endpoint.to_owned()))
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
/// That would read as a few malformed rows after a silently partial result.
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

/// Returns the start of the line saying why a response was cut off, if it was
/// (see [check_complete]).
fn cut_off(body: &[u8]) -> Option<String> {
    const MARKERS: [&str; 3] = [
        "TimeoutException",
        "upstream request timeout",
        "QueryTimeoutException",
    ];
    let text = String::from_utf8_lossy(body);
    let mut lines = text.lines();
    let columns = lines.next()?.split('\t').count();
    lines
        .rev()
        .take_while(|line| !is_row(line, columns))
        .find(|line| MARKERS.iter().any(|marker| line.contains(marker)))
        .map(|line| line.trim().chars().take(200).collect())
}

/// Reads a SPARQL TSV response into rows of the given arity.
///
/// Cells are Turtle-syntax terms:
/// `<iri>`, quoted literals with language tags and datatypes, and bare numbers and booleans.
/// A blank node mints a fresh labeled null, one per label per response.
/// An *unbound* cell (an `OPTIONAL` that did not match) mints a fresh null of its own:
/// the value exists, but is unknown.
/// A cell that is not a term makes the row malformed, and is counted.
/// The selection's condition evaluates in `context`.
fn read_tsv(
    source: impl Read,
    arity: usize,
    selection: &RowSelection,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<Loaded, Diagnostic> {
    let mut loaded = Loaded::new(selection.arity(arity), context);
    let mut blanks = BlankNodes::default();
    let mut row = Vec::with_capacity(arity);
    let mut kept: Vec<DataValue> = Vec::with_capacity(arity);

    // SPARQL TSV has no quoting convention, since quotes belong to the
    // terms. The header row names the variables and is skipped.
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .quoting(false)
        .has_headers(true)
        .flexible(true)
        .from_reader(source);
    let mut line = 1;
    for record in reader.records() {
        line += 1;
        let record = record.map_err(|error| {
            IoError::SparqlResponseUnreadable {
                reason: error.to_string(),
            }
            .diagnostic()
        })?;
        kept.clear();
        // Only delivered cells are parsed: a dropped column cannot
        // make the row malformed.
        let complete = record.len() == arity
            && record
                .iter()
                .enumerate()
                .filter(|(position, _)| selection.keeps(*position))
                .all(|(_, cell)| {
                    term(cell, &mut blanks, dictionary).is_some_and(|value| {
                        kept.push(value);
                        true
                    })
                });
        if complete {
            loaded.take(&kept, selection, dictionary, &mut row, line);
        } else {
            loaded.count_malformed(line);
        }
    }
    tracing::debug!(
        rows = loaded.rows.len(),
        malformed = loaded.malformed,
        excluded = loaded.excluded,
        "read a SPARQL response"
    );
    Ok(loaded)
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
fn term(cell: &str, blanks: &mut BlankNodes, dictionary: &mut Dictionary) -> Option<DataValue> {
    if cell.is_empty() {
        // Unbound: a fresh null every time, since two unbound cells
        // assert nothing about being equal.
        return Some(DataValue::null(dictionary.fresh_null()));
    }
    if let Some(iri) = term::iri(cell) {
        return Some(DataValue::iri(iri));
    }
    if let Some(label) = cell.strip_prefix("_:") {
        return Some(blanks.null(label, dictionary));
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
