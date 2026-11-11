//! JSON records: a resource is an array of records, or has one record per line,
//! and each record is one row.
//!
//! A [JsonReader] and a [JsonWriter] with the same options are inverses:
//! what one writes, the other reads back as the same rows, up to the names of the nulls.
//!
//! A record is an array with one element per column,
//! or an object whose keys are the names of the columns.
//! In a [Term](crate::value_format::ValueFormat::Term) column, each value says what it is:
//!
//! | JSON | value |
//! |---|---|
//! | string | string |
//! | `true`, `false` | boolean |
//! | number without a fraction or an exponent | integer, of any size |
//! | any other number | double |
//! | array | tuple without a label |
//! | object | map without a label, whose keys are strings |
//! | `null`, or an absent key | a missing value |
//!
//! Every other value is an object whose keys start with `@`.
//! Its first key names the value's format, and its value is the bare form as a string:
//! `{"@iri": "http://example.org/a"}`, `{"@decimal": "1.5"}`, `{"@date": "2026-10-18"}`.
//! The other forms are
//! - `{"@null": 5}` for a null,
//! - `{"@langstring": "hallo", "@language": "de"}` for a language string,
//! - `{"@literal": "5cm", "@datatype": "http://example.org/length"}` for a literal of a
//!   datatype without a format,
//! - `{"@tuple": [1, 2], "@label": "point"}` for a tuple with a label, and
//! - `{"@map": [[1, "one"]], "@label": "names"}` for a map with a label, with a key that is
//!   not a string, or with a string key that starts with `@`. The label is optional there.
//!
//! In a column with any other format, a cell holds the bare form of a value:
//! a JSON number under the numeric formats, `true` or `false` under `bool`,
//! and a string under every other format.
//! A language string has no bare form, so a `langstring` column holds its tagged form.

mod parse;
mod reader;
mod value;
mod writer;

pub use reader::{JsonReader, JsonReaderOptions};
pub use writer::{JsonSink, JsonWriter, JsonWriterOptions};

#[cfg(test)]
mod tests;
