//! This module defines the syntactic formats supported with values in Rdf files.
//! This includes reading (parsing) and writing (serialization)
//! for each supported format.

use enum_assoc::Assoc;

use crate::{
    rule_model::components::term::{primitive::Primitive, tuple::Tuple, Term},
    syntax::directive::value_formats,
};

/// Enum for the value formats that are supported for RDF. In many cases,
/// RDF defines how formatting should be done, so there is not much to select here.
///
/// Note that, irrespective of the format, RDF restricts the terms that are
/// allowed in subject, predicate, and graph name positions, and only such terms
/// will be handled there (others are dropped silently).
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn from_name(name: &str) -> Option<Self>)]
pub(super) enum RdfValueFormat {
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
pub(crate) struct RdfValueFormats(Vec<RdfValueFormat>);

impl RdfValueFormats {
    pub(crate) fn new(formats: Vec<RdfValueFormat>) -> Self {
        Self(formats)
    }

    /// Return a list of [RdfValueFormat]s with default entries.
    pub(crate) fn default(arity: usize) -> Self {
        Self((0..arity).map(|_| RdfValueFormat::Anything).collect())
    }

    /// Create a [DsvValueFormats] from a [Tuple].
    ///
    /// Returns `None` if tuple contains an unknown value.
    pub(crate) fn from_tuple(tuple: &Tuple) -> Option<Self> {
        let mut result = Vec::new();

        for value in tuple.arguments() {
            if let Term::Primitive(Primitive::Ground(ground)) = value {
                if let Some(format) = RdfValueFormat::from_name(&ground.to_string()) {
                    result.push(format);
                    continue;
                }
            }

            return None;
        }

        Some(Self::new(result))
    }

    /// Return the length of the format tuple.
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Return whether the tuple is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the arity (ignoring the skipped columns)
    pub(crate) fn arity(&self) -> usize {
        let mut arity = 0;

        for &format in &self.0 {
            if format != RdfValueFormat::Skip {
                arity += 1;
            }
        }

        arity
    }

    /// Return an iterator over the [RdfValueFormat]s.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &RdfValueFormat> {
        self.0.iter()
    }
}
