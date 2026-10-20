//! Predicate type declarations.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, Span, fmt_list, span_ignoring_eq};

/// The base types of column declarations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BaseType {
    /// Any value.
    Any,
    /// A plain string.
    String,
    /// A language-tagged string.
    LanguageString,
    /// An IRI.
    Iri,
    /// An integer of any size, the `xsd:integer` value space.
    Int,
    /// A 64-bit floating point number.
    Double,
    /// A 32-bit floating point number.
    Float,
    /// A decimal number.
    Decimal,
    /// Any number: an integer, a decimal, a float, or a double.
    Number,
    /// A dateTime instant.
    DateTime,
    /// A calendar day.
    Date,
    /// A time of day.
    Time,
    /// Any duration: a dayTimeDuration, a yearMonthDuration, or one with months and seconds.
    Duration,
    /// A duration without months.
    DayTimeDuration,
    /// A duration of months only.
    YearMonthDuration,
    /// A boolean.
    Bool,
    /// An existential null, or a blank node read from RDF.
    Null,
    /// Any tuple, of any shape.
    Tuple,
    /// Any map, of any shape.
    Map,
}

impl BaseType {
    /// Returns the type's name in declarations.
    pub fn name(&self) -> &'static str {
        match self {
            BaseType::Any => "any",
            BaseType::String => "string",
            BaseType::LanguageString => "langstring",
            BaseType::Iri => "iri",
            BaseType::Int => "int",
            BaseType::Double => "double",
            BaseType::Float => "float",
            BaseType::Decimal => "decimal",
            BaseType::Number => "number",
            BaseType::DateTime => "datetime",
            BaseType::Date => "date",
            BaseType::Time => "time",
            BaseType::Duration => "duration",
            BaseType::DayTimeDuration => "daytimeduration",
            BaseType::YearMonthDuration => "yearmonthduration",
            BaseType::Bool => "bool",
            BaseType::Null => "null",
            BaseType::Tuple => "tuple",
            BaseType::Map => "map",
        }
    }

    /// Returns the base type a declaration keyword names, or `None` for any other name.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "any" => BaseType::Any,
            "string" => BaseType::String,
            "langstring" => BaseType::LanguageString,
            "iri" => BaseType::Iri,
            "int" => BaseType::Int,
            "double" => BaseType::Double,
            "float" => BaseType::Float,
            "decimal" => BaseType::Decimal,
            "number" => BaseType::Number,
            "datetime" => BaseType::DateTime,
            "date" => BaseType::Date,
            "time" => BaseType::Time,
            "duration" => BaseType::Duration,
            "daytimeduration" => BaseType::DayTimeDuration,
            "yearmonthduration" => BaseType::YearMonthDuration,
            "bool" => BaseType::Bool,
            "null" => BaseType::Null,
            "tuple" => BaseType::Tuple,
            "map" => BaseType::Map,
            _ => return None,
        })
    }
}

impl Display for BaseType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What a structured type says about its label.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeLabel {
    /// Exactly no label: `(int, int)`, `{ host: string }`.
    Unlabeled,
    /// Any label, or none: `_(int, int)`, `_{ host: string }`.
    Anonymous,
    /// Exactly this label: `point(int, int)`, `point{ x: int }`.
    Named(Identifier),
}

impl TypeLabel {
    /// Writes the label prefix a structured type carries.
    fn fmt_prefix(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TypeLabel::Unlabeled => Ok(()),
            TypeLabel::Anonymous => f.write_str("_"),
            TypeLabel::Named(name) => write!(f, "{name}"),
        }
    }
}

/// A type expression, mirroring the value syntax recursively.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeExpression {
    /// A base type like `string`.
    Base(BaseType),
    /// A reference to a type `@type` declares, by its label.
    Named(Identifier),
    /// A tuple type like `(string, int)`, `point(double, double)`, or
    /// `_(int, int)`.
    Tuple {
        /// What the type says about the label.
        label: TypeLabel,
        /// The field types.
        fields: Vec<TypeExpression>,
    },
    /// A map type like `{ host: string, port: int }`, labeled forms
    /// included.
    Map {
        /// What the type says about the label.
        label: TypeLabel,
        /// The fields by key name.
        fields: Vec<(String, TypeExpression)>,
    },
    /// A union type like `iri | null`: the values of any of its members,
    /// of which there are at least two.
    Union(Vec<TypeExpression>),
}

impl Display for TypeExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TypeExpression::Base(base) => write!(f, "{base}"),
            TypeExpression::Named(name) => write!(f, "{name}"),
            TypeExpression::Tuple { label, fields } => {
                label.fmt_prefix(f)?;
                f.write_str("(")?;
                fmt_list(f, fields)?;
                f.write_str(")")
            }
            TypeExpression::Map { label, fields } => {
                label.fmt_prefix(f)?;
                f.write_str("{ ")?;
                for (index, (name, field)) in fields.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{name}: {field}")?;
                }
                f.write_str(" }")
            }
            TypeExpression::Union(members) => {
                for (index, member) in members.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" | ")?;
                    }
                    write!(f, "{member}")?;
                }
                Ok(())
            }
        }
    }
}

/// One declared column: an optional name and its type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDeclaration {
    /// The column's name, if declared.
    pub name: Option<String>,
    /// The column's type.
    pub datatype: TypeExpression,
}

impl Display for ColumnDeclaration {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{name}: ")?;
        }
        write!(f, "{}", self.datatype)
    }
}

/// A type declaration: `@type point(double, double) .`.
///
/// It declares the element types of every `point(…)` value,
/// and makes `point` usable as a type.
#[derive(Debug, Clone)]
pub struct TypeDirective {
    /// The declared type: a tuple or map type whose label is the declared name.
    pub definition: TypeExpression,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(TypeDirective: definition);

impl Display for TypeDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@type {} .", self.definition)
    }
}

/// A predicate declaration:
/// `@declare person(name: string, birth: datetime) .`.
#[derive(Debug, Clone)]
pub struct DeclareDirective {
    /// The declared predicate.
    pub predicate: Identifier,
    /// The columns, fixing the arity.
    pub columns: Vec<ColumnDeclaration>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(DeclareDirective: predicate, columns);

impl Display for DeclareDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@declare {}(", self.predicate)?;
        fmt_list(f, &self.columns)?;
        f.write_str(") .")
    }
}
