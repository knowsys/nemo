//! Data values and the operations on them.
//!
//! A data value is a value as users of this crate see it,
//! such as an IRI, a string, a number, a dateTime or a structured value.
//! [DataValue] is the owned form.
//! Its constructors bring each value into canonical form, so that it has one representation.
//! [ValueRef] is the borrowed view, which is obtained without allocation
//! from an owned value or from the dictionary.
//!
//! Every value belongs to one [domain](ValueDomain).
//!
//! Equality, order and hashing of values follow *term identity*:
//! two values are equal exactly if they are the same term,
//! so `1`, `1.0d` and `"1"` are three distinct values.
//! The order of values under term identity is the *term order*.
//! It orders values of different domains by domain, and values of one domain by value.
//! An owned value and its borrowed view compare, hash and print the same way.
//! XSD's comparison by value, under which `1` and `1.0d` are equal, gives the *value order*.
//! It is computed by [value_eq](ValueRef::value_eq) and [value_cmp](ValueRef::value_cmp).
//!
//! A [Type] is an upper bound on the values that a column or an expression can hold.
//!
//! Values are written in *term syntax*, Nemo's notation for values,
//! by the [Display](std::fmt::Display) of [DataValue] and [ValueRef].
//! [NameKind] distinguishes the names that term syntax writes without quotes or brackets,
//! and [Escape] reads its escape sequences.

pub(crate) mod datatype;
mod domain;
mod error;
mod error_counts;
mod escape;
mod iri;
mod language_string;
mod literal;
mod name;
pub(crate) mod number;
mod owned;
mod reference;
mod structure;
pub(crate) mod temporal;
mod typing;

pub use domain::ValueDomain;
pub use error::EvaluationError;
pub use error_counts::ErrorCounts;
pub use escape::Escape;
pub use iri::{Iri, IriRef};
pub use language_string::{LanguageString, LanguageStringRef};
pub use literal::{OtherLiteral, OtherLiteralRef};
pub use name::NameKind;
pub use number::{Decimal, DecimalRef, Double, Float, Number, NumberRef};
pub use owned::DataValue;
pub use reference::ValueRef;
pub use structure::{MapRef, MapValue, TupleRef, TupleValue};
pub use temporal::{Date, DateTime, Duration, Time, TimezoneOffset};
pub use typing::{DomainSet, LabelBound, LabelTypes, MapShape, Shape, TupleShape, Type};
