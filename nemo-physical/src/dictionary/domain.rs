//! The domain stores, which keep the dictionary's entries of each kind of value.
//!
//! A domain store splits each of its values into the parts that the mechanisms of the
//! [store](super::store) module keep, and returns the dense index of the value's entry.

mod iri;
mod language_string;
mod other;
mod string;
mod structure;
mod wide_decimal;
mod wide_temporal;

pub(super) use self::{
    iri::IriStore,
    language_string::LanguageStringStore,
    other::OtherStore,
    string::StringStore,
    structure::StructureStore,
    wide_decimal::WideDecimalStore,
    wide_temporal::{WideDateStore, WideDateTimeStore, WideDurationStore, WideTimeStore},
};
