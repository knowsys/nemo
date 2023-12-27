//! This module provides an implementation for [`super::DataValue`]s that represent null values.

use super::{DataValue, ValueDomain};

/// Implementation of a null [`DataValue`].
///
/// Nulls of this type are not created explicitly but are provided by a dictionary.
/// Moreover, the public API of nulls offers no identifiable features: nulls can only
/// be compared to other nulls to see if they are equal or not.
///
/// The implementation still provides the common string representations of all [`DataValue`]s,
/// with the guarantee that the lexical value and the canonical string representation are unique
/// among all nulls that were created from a single dictionary. The canonical string representation
/// uses the blank node syntax of RDF Turtle (and N3). When re-creating nulls from a serialization,
/// each unique string ID should be associated with a fresh null, which is then used in all places
/// where the string ID occurred. However, the fresh null (freshly obtained from a dictionary) will
/// normally not have the original string ID as their canonical string representation. Indeed, when
/// loading data from several unrelated sources, the same string ID should not be assumed to refer
/// to the same null.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NullDataValue(u64);

impl NullDataValue {
    /// Constructor. This is private to the crate. External users must create nulls by using a dictionary.
    pub(crate) fn new(value: u64) -> Self {
        NullDataValue(value)
    }
}

impl DataValue for NullDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Null
    }

    fn canonical_string(&self) -> String {
        "_:".to_string() + &self.0.to_string()
    }
}
