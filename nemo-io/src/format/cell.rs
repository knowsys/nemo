//! A cell's value, borrowing the cell's text if the value is that text.

use nemo_physical::value::DataValue;

/// The value a [format](super::ValueFormat) reads from a cell,
/// borrowing the text of an IRI or a string that the cell holds as it is,
/// so a reader that only copies such text never makes an owned value.
#[derive(Debug)]
pub(crate) enum CellValue<'a> {
    /// An IRI, the cell's text or part of it.
    Iri(&'a str),
    /// A plain string, the cell's text or part of it.
    String(&'a str),
    /// Any other value, or an IRI or a string whose text differs from the cell's.
    Value(DataValue),
}

impl CellValue<'_> {
    /// Returns the value as an owned value.
    pub(crate) fn into_owned(self) -> DataValue {
        match self {
            CellValue::Iri(iri) => DataValue::iri(iri),
            CellValue::String(text) => DataValue::string(text),
            CellValue::Value(value) => value,
        }
    }
}
