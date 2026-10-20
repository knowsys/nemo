//! The value types of the engine's API.
//!
//! The types that name the values in [Engine](crate::engine::Engine) results
//! and in [ExportSpec](crate::engine::ExportSpec) fields are re-exported here,
//! so they can be used without depending on `nemo-physical` or `nemo-io`.
//!
//! ```
//! use nemo::values::{
//!     Compression, DataValue, FileFormat, NumberRef, RdfVariant, Type, ValueFormat, ValueRef,
//! };
//!
//! let value = DataValue::integer(42);
//! assert_eq!(value.to_string(), "42");
//! assert_eq!(value.as_ref(), ValueRef::Number(NumberRef::Decimal(42.into())));
//! let _ = (Compression::Auto, ValueFormat::Term, Type::EMPTY, RdfVariant::Turtle);
//! assert_eq!(FileFormat::from_name("turtle"), Some(FileFormat::Rdf(Some(RdfVariant::Turtle))));
//! ```

mod type_syntax;

pub use type_syntax::TypeSyntax;

pub use nemo_io::{
    compression::Compression,
    file_format::{FileFormat, RdfVariant},
    format::ValueFormat,
};
pub use nemo_physical::value::{
    DataValue, Date, DateTime, Decimal, DecimalRef, DomainSet, Double, Duration, Float, Iri,
    IriRef, LabelBound, LabelTypes, LanguageString, LanguageStringRef, MapRef, MapShape, MapValue,
    Number, NumberRef, OtherLiteral, OtherLiteralRef, Shape, Time, TimezoneOffset, TupleRef,
    TupleShape, TupleValue, Type, ValueDomain, ValueRef,
};
