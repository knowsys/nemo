//! Data values and operations.
//!
//! A data value is a value as users of this crate see it, e.g.
//! an IRI, a string, a number, a dateTime, or a structured value.
//! [DataValue] is the owned form,
//! whose constructors canonicalize so that each value has one representation.
//! [ValueRef] is the borrowed view,
//! obtained from an owned value or from the dictionary without allocation.
//!
//! Every value belongs to one [domain](ValueDomain),
//! its semantic classification.
//!
//! Equality, order and hashing of values are *term identity*:
//! two values are equal exactly if they are the same term,
//! so `1`, `1.0d` and `"1"` are three distinct values.
//! Values of different domains order by domain, and values of one domain by value.
//! An owned value and its borrowed view compare, hash and print alike.
//! XSD's *value* comparison, under which `1` and `1.0d` are equal,
//! is [value_eq](ValueRef::value_eq) and [value_cmp](ValueRef::value_cmp).
//!
//! A [Type] bounds the values a place can hold by a set of domains.

pub(crate) mod datatype;
mod domain;
mod error;
mod error_counts;
mod iri;
mod language_string;
mod literal;
pub(crate) mod number;
mod owned;
mod reference;
mod structure;
pub(crate) mod temporal;
mod typing;

pub use domain::ValueDomain;
pub use error::EvaluationError;
pub use error_counts::ErrorCounts;
pub use iri::{Iri, IriRef};
pub use language_string::{LanguageString, LanguageStringRef};
pub use literal::{OtherLiteral, OtherLiteralRef};
pub use number::{Decimal, DecimalRef, Double, Float, Number, NumberRef};
pub use owned::DataValue;
pub use reference::{Bracketed, ValueRef};
pub use structure::{MapRef, MapValue, TupleRef, TupleValue};
pub use temporal::{Date, DateTime, Duration, Time, TimezoneOffset};
pub use typing::{DomainSet, LabelBound, LabelTypes, MapShape, Shape, TupleShape, Type};
