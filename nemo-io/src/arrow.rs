//! The mapping of value columns onto Arrow arrays, for dataframes.
//!
//! A column whose values are all integers that fit in 64 bits, all floats, all doubles,
//! or all booleans becomes an Arrow array of that type.
//! Any other column becomes an array of text: strings as they are,
//! IRIs without angle brackets, and other values in canonical term syntax.
//!
//! The mapping loses information: for example, a string and an IRI with the same text
//! both become that text.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};

use nemo_physical::value::{NumberRef, ValueRef};

/// What one column of values becomes in Arrow.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnKind {
    /// 64-bit integers.
    Integer,
    /// 32-bit floats.
    Float,
    /// 64-bit floats.
    Double,
    /// Booleans.
    Boolean,
    /// Everything else, as text.
    Text,
}

impl ColumnKind {
    /// Returns the kind of one value.
    pub fn of(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Number(number) if number.to_i64().is_some() => ColumnKind::Integer,
            ValueRef::Number(NumberRef::Float(_)) => ColumnKind::Float,
            ValueRef::Number(NumberRef::Double(_)) => ColumnKind::Double,
            ValueRef::Boolean(_) => ColumnKind::Boolean,
            _ => ColumnKind::Text,
        }
    }

    /// Returns the kind of a column after one more value.
    ///
    /// `running` is the kind of the column's earlier values, or `None` before the first value.
    /// The result is the kind that all values share, or [Text](Self::Text) if they differ.
    pub fn unify(running: Option<Self>, value: ValueRef<'_>) -> Self {
        let of = Self::of(value);
        match running {
            None => of,
            Some(kind) if kind == of => kind,
            Some(_) => ColumnKind::Text,
        }
    }

    /// Returns the Arrow type of this kind.
    pub(crate) fn data_type(self) -> DataType {
        match self {
            ColumnKind::Integer => DataType::Int64,
            ColumnKind::Float => DataType::Float32,
            ColumnKind::Double => DataType::Float64,
            ColumnKind::Boolean => DataType::Boolean,
            ColumnKind::Text => DataType::Utf8,
        }
    }

    /// Returns the schema of columns of the given kinds,
    /// with fields named `column0`, `column1`, …
    pub fn schema(kinds: &[ColumnKind]) -> Arc<Schema> {
        Arc::new(Schema::new(
            kinds
                .iter()
                .enumerate()
                .map(|(index, kind)| Field::new(format!("column{index}"), kind.data_type(), false))
                .collect::<Vec<_>>(),
        ))
    }

    /// Builds one Arrow array of this kind from a column's values.
    ///
    /// # Panics
    ///
    /// Panics if the kind is not [Text](Self::Text) and a value is not of the kind.
    pub fn array<'a>(self, values: impl Iterator<Item = ValueRef<'a>>) -> ArrayRef {
        match self {
            ColumnKind::Integer => {
                Arc::new(Int64Array::from_iter_values(values.map(
                    |value| match value {
                        ValueRef::Number(number) => {
                            number.to_i64().expect("the column was inferred integer")
                        }
                        _ => unreachable!("the column was inferred integer"),
                    },
                )))
            }
            ColumnKind::Float => {
                Arc::new(Float32Array::from_iter_values(values.map(
                    |value| match value {
                        ValueRef::Number(NumberRef::Float(value)) => value.get(),
                        _ => unreachable!("the column was inferred float"),
                    },
                )))
            }
            ColumnKind::Double => {
                Arc::new(Float64Array::from_iter_values(values.map(
                    |value| match value {
                        ValueRef::Number(NumberRef::Double(value)) => value.get(),
                        _ => unreachable!("the column was inferred double"),
                    },
                )))
            }
            ColumnKind::Boolean => {
                Arc::new(BooleanArray::from_iter(values.map(|value| match value {
                    ValueRef::Boolean(value) => Some(value),
                    _ => unreachable!("the column was inferred boolean"),
                })))
            }
            ColumnKind::Text => Arc::new(StringArray::from_iter_values(values.map(text))),
        }
    }
}

/// Returns a value as text, for a column of kind [Text](ColumnKind::Text).
fn text(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::String(text) => text.to_owned(),
        ValueRef::Iri(iri) => iri.to_string(),
        other => other.to_string(),
    }
}
