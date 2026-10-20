//! The domain stores, one per kind of dictionary-backed value.
//!
//! Each knows how its values split into stored bytes.
//! It hands those bytes to the [store](super::store) layer and returns dense indexes.

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
