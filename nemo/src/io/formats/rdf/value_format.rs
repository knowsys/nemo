//! This module defines the syntactic formats supported with values in Rdf files.
//! This includes reading (parsing) and writing (serialization)
//! for each supported format.

use enum_assoc::Assoc;
use nemo_physical::datavalues::{AnyDataValue, DataValue};

use crate::syntax::directive::value_formats;

/// Enum for the value formats that are supported for RDF. In many cases,
/// RDF defines how formatting should be done, so there is not much to select here.
///
/// Note that, irrespective of the format, RDF restricts the terms that are
/// allowed in subject, predicate, and graph name positions, and only such terms
/// will be handled there (others are dropped silently).
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn from_name(name: &str) -> Option<Self>)]
pub(crate) enum RdfValueFormat {
    /// General format that accepts any RDF term.
    #[assoc(name = value_formats::ANY)]
    #[assoc(from_name = value_formats::ANY)]
    Anything,
    /// Special format to indicate that the value should be skipped as if the whole
    /// column where not there.
    #[assoc(name = value_formats::SKIP)]
    #[assoc(from_name = value_formats::SKIP)]
    Skip,
}

/// Indicate what value parser should be used for each column.
#[derive(Debug, Clone)]
pub struct RdfValueFormats(Vec<RdfValueFormat>);

impl TryFrom<AnyDataValue> for RdfValueFormats {
    type Error = ();

    fn try_from(value: AnyDataValue) -> Result<Self, Self::Error> {
        let mut res = Vec::new();
        let mut idx = 0;
        while let Some(element) = value.tuple_element(idx) {
            idx += 1;
            res.push(RdfValueFormat::from_name(&element.to_string()).ok_or(())?);
        }

        Ok(Self(res))
    }
}

impl RdfValueFormats {
    /// Return a list of [RdfValueFormat]s with default entries.
    pub(crate) fn default(arity: usize) -> Self {
        Self((0..arity).map(|_| RdfValueFormat::Anything).collect())
    }

    /// Return the length of the format tuple.
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Return whether the tuple is empty.
    pub(crate) fn _is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the arity (ignoring the skipped columns)
    pub(crate) fn arity(&self) -> usize {
        self.0
            .iter()
            .filter(|format| **format != RdfValueFormat::Skip)
            .count()
    }

    /// Return an iterator over the [RdfValueFormat]s.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &RdfValueFormat> {
        self.0.iter()
    }
}
