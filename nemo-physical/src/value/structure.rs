//! Structured values: tuples and maps of data values, optionally labeled.

mod map;
mod tuple;

use std::fmt;

use crate::value::IriRef;

pub use map::{MapRef, MapValue};
pub use tuple::{TupleRef, TupleValue};

/// Writes a structure's label in term syntax:
/// bare if `bare` allows it and it reads back as the label it spells, as IRIs are,
/// and in angle brackets otherwise.
fn write_label(f: &mut fmt::Formatter<'_>, label: &str, bare: bool) -> fmt::Result {
    if bare && IriRef::new(label).writes_bare() {
        f.write_str(label)
    } else {
        write!(f, "<{label}>")
    }
}
