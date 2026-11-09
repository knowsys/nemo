//! Structured values: tuples and maps of data values, which may have a label.

mod map;
mod tuple;

use std::fmt;

use crate::value::IriRef;

pub use map::{MapRef, MapValue};
pub use tuple::{TupleRef, TupleValue};

/// Writes the label of a tuple or a map in term syntax, as an IRI is written.
///
/// The label is written without angle brackets if `bare` is true
/// and the label [writes bare](IriRef::writes_bare), and in angle brackets otherwise.
fn write_label(f: &mut fmt::Formatter<'_>, label: &str, bare: bool) -> fmt::Result {
    if bare && IriRef::new(label).writes_bare() {
        f.write_str(label)
    } else {
        write!(f, "{}", IriRef::new(label).bracketed())
    }
}
