//! Types in the type syntax of Nemo declarations.
//!
//! [TypeSyntax] displays a database [Type] the way a `@declare` writes it.

use std::fmt::{self, Display, Formatter};

use nemo_physical::value::{
    DomainSet, LabelBound, MapShape, Shape, TupleShape, Type, ValueDomain, ValueRef,
};

use crate::model::BaseType;

/// A [Type] displayed in the type syntax of Nemo declarations.
///
/// - A type is written as its domains, separated by `|`.
/// - A tuple or map domain with shapes is written as those shapes
///   instead of the word `tuple` or `map`,
///   a declared label as its name.
/// - A type with all four numeric domains is written as `number`.
/// - Integers are a subset of the decimals,
///   so a type with both the integer and the decimal domain is written as `decimal`.
/// - A type with all three duration domains is written as `duration`.
#[derive(Debug, Clone, Copy)]
pub struct TypeSyntax<'a>(&'a Type);

impl<'a> TypeSyntax<'a> {
    /// Wraps a type for display.
    pub fn of(datatype: &'a Type) -> Self {
        Self(datatype)
    }
}

impl Display for TypeSyntax<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let datatype = self.0;
        let domains = datatype.domains();
        if datatype.tuple_shapes().is_none() && datatype.map_shapes().is_none() {
            return fmt_domains(f, domains);
        }
        let mut first = true;
        let mut separate = |f: &mut Formatter<'_>| -> fmt::Result {
            if !first {
                f.write_str(" | ")?;
            }
            first = false;
            Ok(())
        };
        let plain = domains
            .without(ValueDomain::Tuple)
            .without(ValueDomain::Map);
        if !plain.is_empty() {
            separate(f)?;
            fmt_domains(f, plain)?;
        }
        if domains.contains(ValueDomain::Tuple) {
            match datatype.tuple_shapes() {
                Some(shapes) => {
                    for shape in shapes {
                        separate(f)?;
                        match shape {
                            Shape::Structural(shape) => fmt_tuple(f, shape)?,
                            Shape::Declared(label) => f.write_str(label)?,
                        }
                    }
                }
                None => {
                    separate(f)?;
                    f.write_str(BaseType::Tuple.name())?;
                }
            }
        }
        if domains.contains(ValueDomain::Map) {
            match datatype.map_shapes() {
                Some(shapes) => {
                    for shape in shapes {
                        separate(f)?;
                        match shape {
                            Shape::Structural(shape) => fmt_map(f, shape)?,
                            Shape::Declared(label) => f.write_str(label)?,
                        }
                    }
                }
                None => {
                    separate(f)?;
                    f.write_str(BaseType::Map.name())?;
                }
            }
        }
        Ok(())
    }
}

/// Returns the word a domain is written as.
///
/// A domain that can be declared is written as its declaration keyword.
/// Literals of other datatypes have no keyword, and are written as `other literal`.
fn keyword(domain: ValueDomain) -> &'static str {
    match domain {
        ValueDomain::Iri => BaseType::Iri.name(),
        ValueDomain::String => BaseType::String.name(),
        ValueDomain::LanguageString => BaseType::LanguageString.name(),
        ValueDomain::Boolean => BaseType::Bool.name(),
        ValueDomain::Integer => BaseType::Int.name(),
        ValueDomain::Decimal => BaseType::Decimal.name(),
        ValueDomain::Float => BaseType::Float.name(),
        ValueDomain::Double => BaseType::Double.name(),
        ValueDomain::DateTime => BaseType::DateTime.name(),
        ValueDomain::Date => BaseType::Date.name(),
        ValueDomain::Time => BaseType::Time.name(),
        ValueDomain::DayTimeDuration => BaseType::DayTimeDuration.name(),
        ValueDomain::YearMonthDuration => BaseType::YearMonthDuration.name(),
        ValueDomain::Duration => BaseType::Duration.name(),
        ValueDomain::Tuple => BaseType::Tuple.name(),
        ValueDomain::Map => BaseType::Map.name(),
        ValueDomain::Null => BaseType::Null.name(),
        ValueDomain::Other => "other literal",
    }
}

/// Writes a domain set by its keywords, separated by `|`.
///
/// The four numeric domains together are written as one `number`,
/// the integer and the decimal domain together as one `decimal`,
/// and the three duration domains together as one `duration`.
fn fmt_domains(f: &mut Formatter<'_>, domains: DomainSet) -> fmt::Result {
    if domains.is_empty() {
        return f.write_str("no value");
    }
    if domains == DomainSet::ANY {
        return f.write_str(BaseType::Any.name());
    }
    let full_number = DomainSet::NUMERIC.is_subset(domains);
    let full_decimal = DomainSet::EXACT.is_subset(domains);
    let full_duration = DomainSet::DURATION.is_subset(domains);
    let folded = |domain| match domain {
        ValueDomain::Float | ValueDomain::Double => full_number,
        ValueDomain::Decimal => full_decimal,
        ValueDomain::YearMonthDuration | ValueDomain::Duration => full_duration,
        _ => false,
    };
    let mut first = true;
    for domain in ValueDomain::ALL {
        if !domains.contains(domain) || folded(domain) {
            continue;
        }
        if !first {
            f.write_str(" | ")?;
        }
        first = false;
        f.write_str(match domain {
            ValueDomain::Integer if full_number => BaseType::Number.name(),
            ValueDomain::Integer if full_decimal => BaseType::Decimal.name(),
            ValueDomain::DayTimeDuration if full_duration => BaseType::Duration.name(),
            other => keyword(other),
        })?;
    }
    Ok(())
}

/// Writes the label prefix of a structured shape, if it is exactly known.
fn fmt_label(f: &mut Formatter<'_>, label: &LabelBound) -> fmt::Result {
    if let LabelBound::Exactly(Some(label)) = label {
        f.write_str(label)?;
    }
    Ok(())
}

/// Writes a tuple shape as `label(t1, t2)`, a single element trailed by a comma.
fn fmt_tuple(f: &mut Formatter<'_>, shape: &TupleShape) -> fmt::Result {
    fmt_label(f, &shape.label)?;
    f.write_str("(")?;
    for (index, element) in shape.elements.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        TypeSyntax(element).fmt(f)?;
    }
    if shape.elements.len() == 1 {
        f.write_str(",")?;
    }
    f.write_str(")")
}

/// Writes a map shape as `label{key: t, ...}`.
///
/// Keys are written briefly, for messages rather than for parsing back:
/// - IRIs without angle brackets,
/// - strings quoted,
/// - integers as numbers,
/// - other keys by their domain.
fn fmt_map(f: &mut Formatter<'_>, shape: &MapShape) -> fmt::Result {
    fmt_label(f, &shape.label)?;
    f.write_str("{")?;
    for (index, (key, value)) in shape.pairs().iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        match key.as_ref() {
            ValueRef::Iri(iri) => write!(f, "{iri}")?,
            ValueRef::String(text) => write!(f, "{text:?}")?,
            ValueRef::Number(number) if number.is_integral() => write!(f, "{number}")?,
            other => fmt_domains(f, DomainSet::of(other.domain()))?,
        }
        write!(f, ": {}", TypeSyntax(value))?;
    }
    f.write_str("}")
}

#[cfg(test)]
mod tests {
    use super::*;

    use nemo_physical::value::{DataValue, TupleValue};

    /// Returns the type syntax of a set of domains.
    fn render(domains: DomainSet) -> String {
        TypeSyntax::of(&Type::of_domains(domains)).to_string()
    }

    #[test]
    fn domains_read_by_their_keywords() {
        assert_eq!(render(DomainSet::EMPTY), "no value");
        assert_eq!(render(DomainSet::ANY), "any");
        assert_eq!(
            render(DomainSet::INTEGER.with(ValueDomain::String)),
            "string | int"
        );
        assert_eq!(render(DomainSet::of(ValueDomain::Null)), "null");
    }

    #[test]
    fn the_decimal_halves_read_folded() {
        // A type with both the integer and the decimal domain is written as `decimal`.
        assert_eq!(render(DomainSet::EXACT), "decimal");
        assert_eq!(
            render(
                DomainSet::STRING
                    .with(ValueDomain::Integer)
                    .with(ValueDomain::Decimal)
            ),
            "string | decimal"
        );
        // Either domain alone is written as its own keyword.
        assert_eq!(render(DomainSet::INTEGER), "int");
        assert_eq!(render(DomainSet::DECIMAL), "decimal");
    }

    #[test]
    fn the_duration_domains_read_folded() {
        assert_eq!(render(DomainSet::DURATION), "duration");
        assert_eq!(
            render(DomainSet::DURATION.with(ValueDomain::Date)),
            "date | duration"
        );
        assert_eq!(render(DomainSet::DAY_TIME_DURATION), "daytimeduration");
    }

    #[test]
    fn the_numeric_domains_read_folded() {
        assert_eq!(render(DomainSet::NUMERIC), "number");
        assert_eq!(
            render(DomainSet::NUMERIC.with(ValueDomain::String)),
            "string | number"
        );
        // Short of all four, the decimal halves still fold.
        assert_eq!(
            render(DomainSet::EXACT.with(ValueDomain::Double)),
            "decimal | double"
        );
    }

    #[test]
    fn shapes_read_as_declarations() {
        let point = DataValue::tuple(TupleValue::labeled(
            "point",
            vec![DataValue::integer(1), DataValue::string("x")],
        ));
        let typed = Type::of_value(point.as_ref());
        assert_eq!(TypeSyntax::of(&typed).to_string(), "point(int, string)");

        let single =
            Type::of_value(DataValue::tuple(TupleValue::new([DataValue::integer(1)])).as_ref());
        assert_eq!(TypeSyntax::of(&single).to_string(), "(int,)");

        let mixed = Type::of(ValueDomain::String).union(&typed);
        assert_eq!(
            TypeSyntax::of(&mixed).to_string(),
            "string | point(int, string)"
        );
        assert_eq!(
            TypeSyntax::of(&Type::of(ValueDomain::Map)).to_string(),
            "map"
        );
    }
}
