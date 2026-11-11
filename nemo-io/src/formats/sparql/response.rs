//! Reading an endpoint's responses, which contain solutions in SPARQL TSV.

use std::str;

use nemo_physical::{
    diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext, value::DataValue,
};

use crate::{
    AboutResource,
    error::IoError,
    load::{BlankNodes, Loaded, RowIntake},
    selection::RowSelection,
};

use super::{ValuesBlock, cut_off, turtle, values::ValuesLookup};

/// The prefix of the name of each label scope in which an endpoint's blank nodes are named.
const BLANK_NODE_SCOPE: &str = "sparql ";

/// Returns the name of the label scope in which an endpoint's blank nodes are named.
fn blank_node_scope(endpoint: &str) -> String {
    format!("{BLANK_NODE_SCOPE}{endpoint}")
}

/// A reader of an endpoint's responses, which contain solutions in SPARQL TSV.
#[derive(Debug, Clone)]
pub struct SparqlReader {
    /// The number of columns of a response row, which is the number of projected variables.
    width: usize,
    /// The columns delivered and the rows admitted.
    selection: RowSelection,
    /// The values that a solution must match, one lookup per `VALUES` block.
    blocks: Vec<ValuesLookup>,
    /// Whether blank node labels name nulls in the endpoint's shared label scope,
    /// rather than in a separate scope for each response.
    endpoint_blank_nodes: bool,
}

impl SparqlReader {
    /// Creates a reader of responses with `width` columns,
    /// which delivers the columns and rows of `selection`.
    ///
    /// Each response names its blank nodes in a scope of its own,
    /// unless [with_endpoint_blank_nodes](Self::with_endpoint_blank_nodes) shares the scope.
    pub fn new(width: usize, selection: RowSelection) -> Self {
        Self {
            width,
            selection,
            blocks: Vec::new(),
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

    /// Returns the reader keeping only the solutions that match the values the query was
    /// asked with ([SparqlQuery::with_values](super::SparqlQuery::with_values)),
    /// as Nemo compares values.
    ///
    /// A solution matches if, for each block, its values in the block's columns form one of the
    /// block's rows.
    /// So a read never delivers a solution that the values do not justify,
    /// however loosely the endpoint matches terms.
    /// The reader reads a block's columns whether they are delivered or not,
    /// and a solution with an unbound value or a blank node in one of them matches no row.
    pub fn with_values(mut self, blocks: &[ValuesBlock]) -> Self {
        self.blocks = blocks.iter().map(ValuesLookup::new).collect();
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
    /// Panics if a block of the reader's values has a position that is not a column of the
    /// responses.
    pub fn read(
        &self,
        endpoint: &str,
        body: &[u8],
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        cut_off::check_complete(endpoint, body)?;
        let name = blank_node_scope(endpoint);
        let shared = self
            .endpoint_blank_nodes
            .then(|| dictionary.shared_label_scope(&name));
        let blanks = BlankNodes::open(shared, &name, dictionary);
        let read = read_tsv(
            body,
            self.width,
            &self.selection,
            &self.blocks,
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
        cut_off::reason(body).is_some()
    }
}

/// Reads a SPARQL TSV response whose rows have `width` columns.
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
/// A solution that does not match the `blocks` is left out ([SparqlReader::with_values]).
/// `context` is the evaluation context of the selection's condition.
fn read_tsv(
    body: &[u8],
    width: usize,
    selection: &RowSelection,
    blocks: &[ValuesLookup],
    blanks: &BlankNodes,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<Loaded, Diagnostic> {
    let mut intake = RowIntake::new(width, selection, context);
    let mut kept: Vec<DataValue> = Vec::with_capacity(width);
    let mut values: Vec<Option<DataValue>> = vec![None; width];

    // The columns that a block reads, whether delivered or not.
    let mut bound = vec![false; width];
    for block in blocks {
        for &position in block.positions() {
            bound[position] = true;
        }
    }

    let text = str::from_utf8(body).map_err(|error| {
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
        if line.is_empty() && width != 1 {
            continue;
        }
        let cells: Vec<&str> = line.split('\t').collect();
        if cells.len() != width {
            intake.count_malformed(number);
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
            intake.count_malformed(number);
            continue;
        }

        // An unbound value or a blank node in a bound column matches no row of a block,
        // so the row is left out, and no null is minted for it.
        let unmatched = bound
            .iter()
            .zip(&values)
            .any(|(bound, value)| *bound && value.is_none());
        if unmatched || !blocks.iter().all(|block| block.matches(&values)) {
            continue;
        }

        kept.clear();
        for (position, cell) in cells.iter().enumerate() {
            if selection.keeps(position) {
                let value = values[position]
                    .take()
                    .or_else(|| read_cell(cell, blanks, dictionary));
                kept.push(value.expect("every cell of the row is a term"));
            }
        }

        intake.take(&kept, dictionary, number);
    }

    let loaded = intake.finish();
    tracing::debug!(rows = loaded.rows.len(), counts = ?loaded.counts, "read a SPARQL response");

    Ok(loaded)
}

/// Parses one TSV cell as a term in Turtle syntax,
/// or returns `None` for a cell that is not a term.
///
/// An unbound cell becomes a fresh null,
/// and a blank node becomes the null that its label names in `blanks`.
/// Quoted literals are read in [Turtle's syntax](turtle::literal).
fn read_cell(cell: &str, blanks: &BlankNodes, dictionary: &mut Dictionary) -> Option<DataValue> {
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

/// Parses a TSV cell that is neither unbound nor a blank node as a term in Turtle syntax,
/// or returns `None` for a cell that is not a term.
fn ground_cell(cell: &str) -> Option<DataValue> {
    if let Some(iri) = turtle::iri(cell) {
        return Some(DataValue::iri(iri.as_ref()));
    }
    if let Some(rest) = cell.strip_prefix('"') {
        return turtle::literal(rest);
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

/// Returns whether a cell starts as a number does:
/// with a digit or a point, after an optional sign.
pub(super) fn starts_as_number(cell: &str) -> bool {
    cell.strip_prefix(['+', '-'])
        .unwrap_or(cell)
        .starts_with(|character: char| character.is_ascii_digit() || character == '.')
}

#[cfg(test)]
mod tests;
