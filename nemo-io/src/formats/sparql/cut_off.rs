//! Recognizing a response that its endpoint cut off partway.

use nemo_physical::diagnostics::Diagnostic;

use crate::{AboutResource, error::IoError};

use super::response::starts_as_number;

/// Checks that a response is whole, and returns an error naming `endpoint` otherwise.
///
/// An endpoint that hits its own time limit partway may respond with status 200
/// and append the exception to the rows it managed,
/// as Blazegraph does behind the Wikidata query service.
/// A connection closed early leaves the last row unfinished.
/// Either would otherwise be read as a partial result followed by a few malformed rows.
/// Only the lines after the last solution row, which contain the trace if there is one,
/// are searched for the known markers, since a row's cells may contain them too.
pub(super) fn check_complete(endpoint: &str, body: &[u8]) -> Result<(), Diagnostic> {
    match reason(body) {
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
pub(super) fn reason(body: &[u8]) -> Option<String> {
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

#[cfg(test)]
mod tests;
