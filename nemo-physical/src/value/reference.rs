//! Borrowed views of data values.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::{Add, Div, Mul, Neg, Rem, Sub},
};

use crate::{
    dictionary::{TermId, TermLane},
    value::{
        DataValue, Date, DateTime, Duration, EvaluationError, IriRef, LanguageStringRef, MapRef,
        NumberRef, OtherLiteralRef, Time, TupleRef, ValueDomain, datatype::xsd,
    },
};

/// A borrowed data value, e.g., an IRI, a string, a number, a dateTime, a structured value,
/// or a value of any other [domain](ValueDomain).
/// Its owned form is [DataValue].
#[derive(Debug, Clone, Copy)]
pub enum ValueRef<'a> {
    /// An [IRI](https://www.w3.org/TR/rdf11-concepts/#section-IRIs), an RDF resource.
    Iri(IriRef<'a>),
    /// A plain string, the value of an [`xsd:string`](https://www.w3.org/TR/xmlschema11-2/#string) literal.
    String(&'a str),
    /// A [language-tagged string](https://www.w3.org/TR/rdf11-concepts/#dfn-language-tagged-string),
    /// the value of an `rdf:langString` literal.
    LanguageString(LanguageStringRef<'a>),
    /// A boolean, the value of an [`xsd:boolean`](https://www.w3.org/TR/xmlschema11-2/#boolean) literal.
    Boolean(bool),
    /// A number of any numeric domain.
    Number(NumberRef<'a>),
    /// An instant with an optional timezone,
    /// the value of an [`xsd:dateTime`](https://www.w3.org/TR/xmlschema11-2/#dateTime) literal.
    DateTime(DateTime),
    /// A calendar day with an optional timezone,
    /// the value of an [`xsd:date`](https://www.w3.org/TR/xmlschema11-2/#date) literal.
    Date(Date),
    /// A time of day with an optional timezone,
    /// the value of an [`xsd:time`](https://www.w3.org/TR/xmlschema11-2/#time) literal.
    Time(Time),
    /// A duration of any of the three duration domains,
    /// the value of an [`xsd:duration`](https://www.w3.org/TR/xmlschema11-2/#duration) literal
    /// or of one of its derived types.
    Duration(Duration),
    /// A labeled null, identified by its id.
    Null(u64),
    /// A tuple, a structured value with no RDF counterpart.
    Tuple(TupleRef<'a>),
    /// A map, a structured value with no RDF counterpart.
    Map(MapRef<'a>),
    /// A [literal](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal)
    /// of a datatype this crate has no native representation for.
    Other(OtherLiteralRef<'a>),
}

impl<'a> ValueRef<'a> {
    /// Returns the semantic domain of this value.
    pub fn domain(self) -> ValueDomain {
        match self {
            ValueRef::Iri(_) => ValueDomain::Iri,
            ValueRef::String(_) => ValueDomain::String,
            ValueRef::LanguageString { .. } => ValueDomain::LanguageString,
            ValueRef::Boolean(_) => ValueDomain::Boolean,
            ValueRef::Number(value) => value.domain(),
            ValueRef::DateTime(_) => ValueDomain::DateTime,
            ValueRef::Date(_) => ValueDomain::Date,
            ValueRef::Time(_) => ValueDomain::Time,
            ValueRef::Duration(value) => value.domain(),
            ValueRef::Null(_) => ValueDomain::Null,
            ValueRef::Tuple(_) => ValueDomain::Tuple,
            ValueRef::Map(_) => ValueDomain::Map,
            ValueRef::Other { .. } => ValueDomain::Other,
        }
    }

    /// Returns the datatype IRI of this value,
    /// or `None` for the non-literal domains (see [ValueDomain::datatype_iri]).
    pub fn datatype_iri(self) -> Option<&'a str> {
        match self {
            ValueRef::Other(literal) => Some(literal.datatype()),
            _ => self.domain().datatype_iri(),
        }
    }

    /// Returns the number of a null, counting the nulls its dictionary minted from 0,
    /// as `_:0` writes it, or `None` for any other value.
    ///
    /// A null id the dictionary did not mint numbers itself.
    pub fn null_number(self) -> Option<u64> {
        let ValueRef::Null(id) = self else {
            return None;
        };
        Some(match TermId::from_raw(id).decompose() {
            Some((TermLane::Null, index)) => index,
            _ => id,
        })
    }

    /// Returns the effective boolean value of this value,
    /// SPARQL's [EBV](https://www.w3.org/TR/sparql12-query/#ebv).
    ///
    /// A boolean is itself, and a number is false exactly at zero and the NaN.
    /// A plain or language-tagged string is false exactly if it is empty.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) for every other value.
    pub fn effective_boolean_value(self) -> Result<bool, EvaluationError> {
        match self {
            ValueRef::Boolean(value) => Ok(value),
            ValueRef::Number(NumberRef::Float(value)) => {
                Ok(!(value.is_nan() || value.get() == 0.0))
            }
            ValueRef::Number(NumberRef::Double(value)) => {
                Ok(!(value.is_nan() || value.get() == 0.0))
            }
            ValueRef::Number(value) => Ok(!value.is_zero()),
            ValueRef::String(value) => Ok(!value.is_empty()),
            ValueRef::LanguageString(value) => Ok(!value.text().is_empty()),
            _ => Err(EvaluationError::TypeMismatch),
        }
    }

    /// Clones the viewed value into an owned [DataValue].
    ///
    /// This allocates for the borrowed content.
    /// Cloning a [DataValue] is cheaper if one exists.
    pub fn to_owned(self) -> DataValue {
        DataValue::from_ref(self)
    }

    /// Returns whether two values are equal.
    ///
    /// Following SPARQL's `sameValue`, values of different domains
    /// that cannot be compared numerically are unequal if at least one is an IRI or a null.
    ///
    /// # Errors
    ///
    /// Returns [Incomparable](EvaluationError::Incomparable)
    /// for other values of different domains that cannot be compared numerically,
    /// and for two distinct literals of unknown datatypes.
    pub fn value_eq(self, other: Self) -> Result<bool, EvaluationError> {
        match (self, other) {
            (ValueRef::Number(left), ValueRef::Number(right)) => return Ok(left.value_eq(right)),
            (ValueRef::DateTime(left), ValueRef::DateTime(right)) => {
                return Ok(left.value_eq(right));
            }
            (ValueRef::Date(left), ValueRef::Date(right)) => return Ok(left.value_eq(right)),
            (ValueRef::Time(left), ValueRef::Time(right)) => return Ok(left.value_eq(right)),
            // A duration is its own value, whichever of the three domains it is in.
            (ValueRef::Duration(left), ValueRef::Duration(right)) => return Ok(left == right),
            _ => {}
        }

        let (left, right) = (self.domain(), other.domain());
        if left == right {
            if self == other {
                return Ok(true);
            }
            return if left == ValueDomain::Other {
                Err(EvaluationError::Incomparable)
            } else {
                Ok(false)
            };
        }

        let non_literal = |domain| matches!(domain, ValueDomain::Iri | ValueDomain::Null);
        if non_literal(left) || non_literal(right) {
            return Ok(false);
        }
        Err(EvaluationError::Incomparable)
    }

    /// Returns the order of two values, `op:numeric-less-than` and its kin,
    /// or `None` if the domain leaves it undefined:
    /// for a NaN operand, and for a zoned beside an unzoned dateTime, date or time.
    ///
    /// Numbers of any numeric domain order by value, strings by code point,
    /// and booleans with `false` first.
    /// DateTimes, dates and times order by XSD's partial order,
    /// and durations by length if they [share a kind](Duration::shares_kind).
    /// Tuples of one shape, with the same label and arity, order elementwise.
    ///
    /// # Errors
    ///
    /// Returns [Incomparable](EvaluationError::Incomparable) for every other pair of values.
    pub fn value_cmp(self, other: Self) -> Result<Option<Ordering>, EvaluationError> {
        match (self, other) {
            (ValueRef::Number(left), ValueRef::Number(right)) => Ok(left.value_cmp(right)),
            (ValueRef::DateTime(left), ValueRef::DateTime(right)) => Ok(left.value_cmp(right)),
            (ValueRef::Date(left), ValueRef::Date(right)) => Ok(left.value_cmp(right)),
            (ValueRef::Time(left), ValueRef::Time(right)) => Ok(left.value_cmp(right)),
            (ValueRef::Duration(left), ValueRef::Duration(right)) if left.shares_kind(right) => {
                Ok(left.value_cmp(right))
            }
            (ValueRef::Boolean(left), ValueRef::Boolean(right)) => Ok(Some(left.cmp(&right))),
            (ValueRef::String(left), ValueRef::String(right)) => Ok(Some(left.cmp(right))),
            (ValueRef::Tuple(left), ValueRef::Tuple(right)) => {
                if left.label() != right.label() || left.len() != right.len() {
                    return Err(EvaluationError::Incomparable);
                }
                for (left, right) in left.elements().zip(right.elements()) {
                    match left.value_cmp(right)? {
                        Some(Ordering::Equal) => continue,
                        decided => return Ok(decided),
                    }
                }
                Ok(Some(Ordering::Equal))
            }
            _ => Err(EvaluationError::Incomparable),
        }
    }
}

/// Returns the numbers two values hold, or a type mismatch.
fn numbers<'a>(
    left: ValueRef<'a>,
    right: ValueRef<'a>,
) -> Result<(NumberRef<'a>, NumberRef<'a>), EvaluationError> {
    match (left, right) {
        (ValueRef::Number(left), ValueRef::Number(right)) => Ok((left, right)),
        _ => Err(EvaluationError::TypeMismatch),
    }
}

impl Add for ValueRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the sum of two values:
    ///
    /// - of two numbers, `op:numeric-add`;
    /// - of two durations that [share a kind](Duration::shares_kind),
    ///   `op:add-dayTimeDurations` and `op:add-yearMonthDurations`;
    /// - of a dateTime, date or time and a duration, in either order,
    ///   `op:add-dayTimeDuration-to-dateTime` and its kin.
    ///
    /// A time takes a dayTimeDuration only.
    /// A dateTime or date also takes a duration with both months and seconds,
    /// which XSD's addition defines and XPath leaves out.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) for any other pair,
    /// [Overflow](EvaluationError::Overflow) for a temporal result beyond the value space,
    /// and the errors of the [sum of numbers](NumberRef::add).
    fn add(self, other: Self) -> Self::Output {
        match (self, other) {
            (ValueRef::Number(left), ValueRef::Number(right)) => left + right,
            (ValueRef::Duration(left), ValueRef::Duration(right)) => {
                durations(left, right, Duration::checked_add)
            }
            (point, ValueRef::Duration(duration)) | (ValueRef::Duration(duration), point) => {
                shifted(point, duration)
            }
            _ => Err(EvaluationError::TypeMismatch),
        }
    }
}

impl Sub for ValueRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the difference of two values:
    ///
    /// - of two numbers, `op:numeric-subtract`;
    /// - of two durations that share a kind, `op:subtract-dayTimeDurations` and its kin;
    /// - of a dateTime, date or time and a duration,
    ///   `op:subtract-dayTimeDuration-from-dateTime` and its kin;
    /// - of two dateTimes, two dates or two times, a dayTimeDuration,
    ///   `op:subtract-dateTimes` and its kin.
    ///
    /// # Errors
    ///
    /// Returns the errors of [addition](ValueRef::add),
    /// and [Incomparable](EvaluationError::Incomparable)
    /// for a zoned and an unzoned point in time, which have no distance.
    fn sub(self, other: Self) -> Self::Output {
        let distance = |duration: Option<Duration>| {
            duration
                .map(DataValue::duration)
                .ok_or(EvaluationError::Incomparable)
        };

        match (self, other) {
            (ValueRef::Number(left), ValueRef::Number(right)) => left - right,
            (ValueRef::Duration(left), ValueRef::Duration(right)) => {
                durations(left, right, Duration::checked_sub)
            }
            (point, ValueRef::Duration(duration)) => shifted(
                point,
                duration.checked_neg().ok_or(EvaluationError::Overflow)?,
            ),
            (ValueRef::DateTime(left), ValueRef::DateTime(right)) => {
                distance(left.duration_since(right))
            }
            (ValueRef::Date(left), ValueRef::Date(right)) => distance(left.duration_since(right)),
            (ValueRef::Time(left), ValueRef::Time(right)) => distance(left.duration_since(right)),
            _ => Err(EvaluationError::TypeMismatch),
        }
    }
}

impl Mul for ValueRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the product of two numbers, `op:numeric-multiply`,
    /// or of a duration and a number, in either order,
    /// `op:multiply-dayTimeDuration` and `op:multiply-yearMonthDuration`.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) for any other pair,
    /// and the errors of the [product of numbers](NumberRef::mul)
    /// or of the [product of a duration](Duration::mul).
    fn mul(self, other: Self) -> Self::Output {
        match (self, other) {
            (ValueRef::Number(left), ValueRef::Number(right)) => left * right,
            (ValueRef::Duration(duration), ValueRef::Number(factor))
            | (ValueRef::Number(factor), ValueRef::Duration(duration)) => {
                (duration * factor).map(DataValue::duration)
            }
            _ => Err(EvaluationError::TypeMismatch),
        }
    }
}

impl Div for ValueRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the quotient of two values:
    ///
    /// - of two numbers, `op:numeric-divide`;
    /// - of a duration and a number, `op:divide-dayTimeDuration` and its kin;
    /// - of two durations that share a kind, a decimal,
    ///   `op:divide-dayTimeDuration-by-dayTimeDuration` and its kin.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) for any other pair,
    /// and the errors of the [quotient of numbers](NumberRef::div)
    /// or of the [quotient of durations](Duration::div).
    fn div(self, other: Self) -> Self::Output {
        match (self, other) {
            (ValueRef::Number(left), ValueRef::Number(right)) => left / right,
            (ValueRef::Duration(duration), ValueRef::Number(divisor)) => {
                (duration / divisor).map(DataValue::duration)
            }
            (ValueRef::Duration(left), ValueRef::Duration(right)) => left / right,
            _ => Err(EvaluationError::TypeMismatch),
        }
    }
}

impl Rem for ValueRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the remainder of integer division, `op:numeric-mod`.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) for a value that is not a number,
    /// and the errors of the [remainder of numbers](NumberRef::rem).
    fn rem(self, other: Self) -> Self::Output {
        let (left, right) = numbers(self, other)?;
        left % right
    }
}

impl Neg for ValueRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the negated number, `op:numeric-unary-minus`.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) for a value that is not a number.
    fn neg(self) -> Self::Output {
        match self {
            ValueRef::Number(value) => Ok(-value),
            _ => Err(EvaluationError::TypeMismatch),
        }
    }
}

impl fmt::Display for ValueRef<'_> {
    /// Writes the value in canonical Nemo term syntax:
    /// IRIs in angle brackets,
    /// strings quoted with `\`, `"` and line breaks escaped,
    /// floats and doubles with their `f` and `d` suffixes,
    /// or as typed literals when not finite,
    /// dates, times, durations and unsupported datatypes as typed literals,
    /// and structured values recursively.
    ///
    /// One value has one rendering, and distinct values render distinctly.
    /// Every rendering re-parses as a term denoting this value,
    /// except a null, which renders as `_:id` in the syntax of data, not of programs.
    ///
    /// An IRI that reads back bare is written bare;
    /// [bracketed](Self::bracketed) writes every IRI in angle brackets.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_term(f, true)
    }
}

/// A value in term syntax with every IRI in angle brackets; see [ValueRef::bracketed].
#[derive(Debug, Clone, Copy)]
pub struct Bracketed<'a>(ValueRef<'a>);

impl fmt::Display for Bracketed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.write_term(f, false)
    }
}

impl<'a> ValueRef<'a> {
    /// Returns the value in term syntax with every IRI in angle brackets,
    /// structure labels and map keys included,
    /// the text a program reads back as written when it declares a base IRI,
    /// under which a bare name is relative to the base.
    pub fn bracketed(self) -> Bracketed<'a> {
        Bracketed(self)
    }
}

impl ValueRef<'_> {
    /// Writes the value in term syntax, the IRIs that read back bare written bare if `bare`.
    pub(super) fn write_term(self, f: &mut fmt::Formatter<'_>, bare: bool) -> fmt::Result {
        match self {
            ValueRef::Iri(iri) if bare && iri.writes_bare() => write!(f, "{iri}"),
            ValueRef::Iri(iri) => write!(f, "<{iri}>"),
            ValueRef::String(text) => write_quoted(f, text),
            ValueRef::LanguageString(value) => write!(f, "{value}"),
            ValueRef::Boolean(value) => write!(f, "{value}"),
            ValueRef::Number(NumberRef::Float(value)) if value.is_finite() => write!(f, "{value}f"),
            ValueRef::Number(NumberRef::Float(value)) => {
                write_quoted(f, &value.to_string())?;
                write!(f, "^^<{}>", xsd::FLOAT)
            }
            ValueRef::Number(NumberRef::Double(value)) if value.is_finite() => {
                write!(f, "{value}d")
            }
            ValueRef::Number(NumberRef::Double(value)) => {
                write_quoted(f, &value.to_string())?;
                write!(f, "^^<{}>", xsd::DOUBLE)
            }
            ValueRef::Number(value) => write!(f, "{value}"),
            ValueRef::DateTime(value) => self.write_typed(f, value),
            ValueRef::Date(value) => self.write_typed(f, value),
            ValueRef::Time(value) => self.write_typed(f, value),
            ValueRef::Duration(value) => self.write_typed(f, value),
            ValueRef::Null(_) => write!(f, "_:{}", self.null_number().unwrap_or_default()),
            ValueRef::Tuple(tuple) => tuple.write_term(f, bare),
            ValueRef::Map(map) => map.write_term(f, bare),
            ValueRef::Other(literal) => write!(f, "{literal}"),
        }
    }

    /// Writes a value of a native datatype as a typed literal:
    /// its canonical lexical form, quoted, and the datatype of its domain.
    fn write_typed(self, f: &mut fmt::Formatter<'_>, lexical: impl fmt::Display) -> fmt::Result {
        let datatype = self
            .datatype_iri()
            .expect("a value written as a typed literal has a datatype");
        write_quoted(f, &lexical.to_string())?;
        write!(f, "^^<{datatype}>")
    }
}

/// Returns the sum or the difference of two durations that share a kind,
/// the error of a pair that does not,
/// or [Overflow](EvaluationError::Overflow) beyond the value space.
fn durations(
    left: Duration,
    right: Duration,
    combine: fn(Duration, Duration) -> Option<Duration>,
) -> Result<DataValue, EvaluationError> {
    if !left.shares_kind(right) {
        return Err(EvaluationError::TypeMismatch);
    }
    combine(left, right)
        .map(DataValue::duration)
        .ok_or(EvaluationError::Overflow)
}

/// Returns a dateTime, date or time moved by a duration;
/// a time moves by a dayTimeDuration only.
///
/// Fails with [TypeMismatch](EvaluationError::TypeMismatch) for any other operand,
/// and with [Overflow](EvaluationError::Overflow) for a result beyond the value space.
fn shifted(point: ValueRef<'_>, duration: Duration) -> Result<DataValue, EvaluationError> {
    let shifted = match point {
        ValueRef::DateTime(value) => value.checked_add(duration).map(DataValue::datetime),
        ValueRef::Date(value) => value.checked_add(duration).map(DataValue::date),
        ValueRef::Time(value) if duration.is_day_time() => {
            value.checked_add(duration).map(DataValue::time)
        }
        _ => return Err(EvaluationError::TypeMismatch),
    };
    shifted.ok_or(EvaluationError::Overflow)
}

/// Writes a string in quoted form,
/// escaping the quote and the escape character themselves, and the line breaks,
/// which the parser would read back normalized.
pub(super) fn write_quoted(f: &mut fmt::Formatter<'_>, text: &str) -> fmt::Result {
    f.write_str("\"")?;
    for character in text.chars() {
        match character {
            '\\' => f.write_str("\\\\")?,
            '"' => f.write_str("\\\"")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            other => write!(f, "{other}")?,
        }
    }
    f.write_str("\"")
}

impl Ord for ValueRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ValueRef::Iri(left), ValueRef::Iri(right)) => left.cmp(right),
            (ValueRef::String(left), ValueRef::String(right)) => left.cmp(right),
            (ValueRef::LanguageString(left), ValueRef::LanguageString(right)) => left.cmp(right),
            (ValueRef::Boolean(left), ValueRef::Boolean(right)) => left.cmp(right),
            (ValueRef::Number(left), ValueRef::Number(right)) => left.cmp(right),
            (ValueRef::DateTime(left), ValueRef::DateTime(right)) => left.cmp(right),
            (ValueRef::Date(left), ValueRef::Date(right)) => left.cmp(right),
            (ValueRef::Time(left), ValueRef::Time(right)) => left.cmp(right),
            (ValueRef::Duration(left), ValueRef::Duration(right)) => left.cmp(right),
            (ValueRef::Null(left), ValueRef::Null(right)) => left.cmp(right),
            (ValueRef::Tuple(left), ValueRef::Tuple(right)) => left.cmp(right),
            (ValueRef::Map(left), ValueRef::Map(right)) => left.cmp(right),
            (ValueRef::Other(left), ValueRef::Other(right)) => left.cmp(right),
            // The term order across domains is their declaration order.
            _ => self.domain().cmp(&other.domain()),
        }
    }
}

impl PartialOrd for ValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ValueRef<'_> {
    /// Compares by term identity, agreeing with [Ord].
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueRef::Iri(left), ValueRef::Iri(right)) => left == right,
            (ValueRef::String(left), ValueRef::String(right)) => left == right,
            (ValueRef::LanguageString(left), ValueRef::LanguageString(right)) => left == right,
            (ValueRef::Boolean(left), ValueRef::Boolean(right)) => left == right,
            (ValueRef::Number(left), ValueRef::Number(right)) => left == right,
            (ValueRef::DateTime(left), ValueRef::DateTime(right)) => left == right,
            (ValueRef::Date(left), ValueRef::Date(right)) => left == right,
            (ValueRef::Time(left), ValueRef::Time(right)) => left == right,
            (ValueRef::Duration(left), ValueRef::Duration(right)) => left == right,
            (ValueRef::Null(left), ValueRef::Null(right)) => left == right,
            (ValueRef::Tuple(left), ValueRef::Tuple(right)) => left == right,
            (ValueRef::Map(left), ValueRef::Map(right)) => left == right,
            (ValueRef::Other(left), ValueRef::Other(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for ValueRef<'_> {}

impl Hash for ValueRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.domain().hash(state);
        match self {
            ValueRef::Iri(iri) => iri.hash(state),
            ValueRef::String(string) => string.hash(state),
            ValueRef::LanguageString(value) => value.hash(state),
            ValueRef::Boolean(value) => value.hash(state),
            ValueRef::Number(value) => value.hash(state),
            ValueRef::DateTime(value) => value.hash(state),
            ValueRef::Date(value) => value.hash(state),
            ValueRef::Time(value) => value.hash(state),
            ValueRef::Duration(value) => value.hash(state),
            ValueRef::Null(id) => id.hash(state),
            ValueRef::Tuple(tuple) => tuple.hash(state),
            ValueRef::Map(map) => map.hash(state),
            ValueRef::Other(literal) => literal.hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::number;

    use EvaluationError::{DivisionByZero, Incomparable, Malformed, Overflow, TypeMismatch};

    /// Returns the value of a typed literal in the XSD namespace;
    /// panics on a lexical the datatype does not accept.
    fn typed(datatype: &str, lexical: &str) -> DataValue {
        DataValue::typed_literal(&format!("{}{datatype}", xsd::NAMESPACE), lexical).expect(lexical)
    }

    /// Returns a dateTime value.
    fn datetime(lexical: &str) -> DataValue {
        typed("dateTime", lexical)
    }

    /// Returns a date value.
    fn date(lexical: &str) -> DataValue {
        typed("date", lexical)
    }

    /// Returns a time value.
    fn time(lexical: &str) -> DataValue {
        typed("time", lexical)
    }

    /// Returns a duration value.
    fn duration(lexical: &str) -> DataValue {
        typed("duration", lexical)
    }

    #[test]
    fn bracketed_values_write_every_iri_in_brackets() {
        use crate::value::{MapValue, TupleValue};

        let map = MapValue::new([(DataValue::iri("k"), DataValue::iri("v"))]).expect("one key");
        let value = DataValue::tuple(TupleValue::labeled(
            "pt",
            vec![
                DataValue::iri("a"),
                DataValue::map(map),
                DataValue::integer(1),
            ],
        ));
        assert_eq!(value.to_string(), "pt(a, {k = v}, 1)");
        assert_eq!(
            value.as_ref().bracketed().to_string(),
            "<pt>(<a>, {<k> = <v>}, 1)"
        );
    }

    #[test]
    fn points_in_time_move_by_durations() {
        // The examples of XPath's operators.
        assert_eq!(
            &datetime("2000-10-30T11:12:00") + &duration("P1Y2M"),
            Ok(datetime("2001-12-30T11:12:00"))
        );
        assert_eq!(
            &datetime("2000-10-30T11:12:00") + &duration("P3DT1H15M"),
            Ok(datetime("2000-11-02T12:27:00"))
        );
        assert_eq!(
            &datetime("2000-10-30T11:12:00") - &duration("P1Y2M"),
            Ok(datetime("1999-08-30T11:12:00"))
        );
        assert_eq!(
            &datetime("2000-02-29T11:12:00") - &duration("P1Y"),
            Ok(datetime("1999-02-28T11:12:00"))
        );
        assert_eq!(
            &date("2000-10-30") + &duration("P1Y2M"),
            Ok(date("2001-12-30"))
        );
        assert_eq!(
            &date("2004-10-30Z") + &duration("P2DT2H30M0S"),
            Ok(date("2004-11-01Z"))
        );
        assert_eq!(
            &date("2000-10-30") - &duration("P3DT1H15M"),
            Ok(date("2000-10-26"))
        );
        assert_eq!(
            &time("11:12:00") + &duration("P3DT1H15M"),
            Ok(time("12:27:00"))
        );
        assert_eq!(
            &time("23:12:00+03:00") + &duration("P1DT3H15M"),
            Ok(time("02:27:00+03:00"))
        );
        assert_eq!(
            &time("08:20:00-05:00") - &duration("P23DT10H10M"),
            Ok(time("22:10:00-05:00"))
        );

        // Addition commutes; a time moves by no months.
        assert_eq!(
            &duration("P1Y2M") + &date("2000-10-30"),
            Ok(date("2001-12-30"))
        );
        assert_eq!(&time("11:12:00") + &duration("P1M"), Err(TypeMismatch));
        assert_eq!(&duration("P1M") - &date("2000-10-30"), Err(TypeMismatch));
    }

    #[test]
    fn points_in_time_are_durations_apart() {
        assert_eq!(
            &datetime("2000-10-30T06:12:00-05:00") - &datetime("1999-11-28T09:00:00Z"),
            Ok(duration("P337DT2H12M"))
        );
        assert_eq!(
            &date("2000-10-30") - &date("1999-11-28"),
            Ok(duration("P337D"))
        );
        assert_eq!(
            &date("2000-10-15-05:00") - &date("2000-10-10+02:00"),
            Ok(duration("P5DT7H"))
        );
        assert_eq!(
            &time("11:12:00Z") - &time("04:00:00-05:00"),
            Ok(duration("PT2H12M"))
        );
        assert_eq!(
            &time("17:00:00-06:00") - &time("08:00:00+09:00"),
            Ok(duration("P1D"))
        );
        assert_eq!(
            &time("24:00:00") - &time("23:59:59"),
            Ok(duration("-PT23H59M59S"))
        );

        // Without an implicit timezone, a zoned and an unzoned value have no distance.
        assert_eq!(
            &date("2000-10-30Z") - &date("1999-11-28"),
            Err(Incomparable)
        );
        assert_eq!(
            &date("2000-10-30") - &datetime("1999-11-28T00:00:00"),
            Err(TypeMismatch)
        );
    }

    #[test]
    fn durations_combine_within_a_kind() {
        // The examples of XPath's operators.
        assert_eq!(
            &duration("P2Y11M") + &duration("P3Y3M"),
            Ok(duration("P6Y2M"))
        );
        assert_eq!(
            &duration("P2Y11M") - &duration("P3Y3M"),
            Ok(duration("-P4M"))
        );
        assert_eq!(
            &duration("P2DT12H5M") + &duration("P5DT12H"),
            Ok(duration("P8DT5M"))
        );
        assert_eq!(
            &duration("P2DT12H") - &duration("P1DT10H30M"),
            Ok(duration("P1DT1H30M"))
        );
        assert_eq!(&duration("P2Y11M") * &number("2.3"), Ok(duration("P6Y9M")));
        assert_eq!(&duration("P2Y11M") / &number("1.5"), Ok(duration("P1Y11M")));
        assert_eq!(&duration("P3Y4M") / &duration("-P1Y4M"), Ok(number("-2.5")));
        assert_eq!(
            &duration("PT2H10M") * &number("2.1"),
            Ok(duration("PT4H33M"))
        );
        assert_eq!(
            &duration("P1DT2H30M10.5S") / &number("1.5"),
            Ok(duration("PT17H40M7S"))
        );

        // The zero duration is of either kind, and the result of a sum to zero.
        assert_eq!(&duration("P1M") - &duration("P1M"), Ok(duration("PT0S")));
        assert_eq!(&duration("PT0S") + &duration("P1M"), Ok(duration("P1M")));
        assert_eq!(&duration("P1M") + &duration("PT1S"), Err(TypeMismatch));
        assert_eq!(&duration("P1M") / &duration("PT1S"), Err(TypeMismatch));
        assert_eq!(&duration("PT1S") / &duration("PT0S"), Err(DivisionByZero));
    }

    #[test]
    fn durations_scale_exactly_and_round_halves_up() {
        // A double counts as its shortest decimal, 2.1, not the binary value near it.
        assert_eq!(
            &DataValue::double(2.1) * &duration("PT2H10M"),
            Ok(duration("PT4H33M"))
        );
        assert_eq!(&duration("P1M") * &number("2.5"), Ok(duration("P3M")));
        assert_eq!(&duration("-P1M") * &number("2.5"), Ok(duration("-P2M")));
        assert_eq!(
            &duration("PT1S") / &number("3"),
            Ok(duration("PT0.333333333333333333S"))
        );

        assert_eq!(&duration("P1MT1S") * &number("2"), Err(TypeMismatch));
        assert_eq!(&duration("P1M") / &number("0"), Err(DivisionByZero));
        assert_eq!(
            &duration("P1M") / &DataValue::double(f64::INFINITY),
            Ok(duration("PT0S"))
        );
        assert_eq!(
            &duration("P1M") * &DataValue::double(f64::NAN),
            Err(Malformed)
        );
        assert_eq!(
            &duration("P1M") * &DataValue::double(f64::INFINITY),
            Err(Overflow)
        );
        assert_eq!(
            &duration("P1M") * &number("1000000000000000000000000000000"),
            Err(Overflow)
        );
    }

    #[test]
    fn dates_times_and_durations_compare_by_value() {
        // The examples of XPath's equality operators.
        assert_eq!(
            date("2004-12-25Z").value_eq(&date("2004-12-25+07:00")),
            Ok(false)
        );
        assert_eq!(
            date("2004-12-25-12:00").value_eq(&date("2004-12-26+12:00")),
            Ok(true)
        );
        assert_eq!(
            time("08:00:00+09:00").value_eq(&time("17:00:00-06:00")),
            Ok(false)
        );
        assert_eq!(
            time("21:30:00+10:30").value_eq(&time("06:00:00-05:00")),
            Ok(true)
        );
        assert_eq!(
            duration("P1Y").value_eq(&typed("yearMonthDuration", "P12M")),
            Ok(true)
        );
        assert_eq!(duration("P1Y").value_eq(&duration("P365D")), Ok(false));

        assert_eq!(
            date("2004-12-24Z").value_cmp(&date("2004-12-25Z")),
            Ok(Some(Ordering::Less))
        );
        assert_eq!(
            duration("P1Y").value_cmp(&duration("P11M")),
            Ok(Some(Ordering::Greater))
        );
        assert_eq!(
            duration("PT0S").value_cmp(&duration("P1M")),
            Ok(Some(Ordering::Less))
        );
        assert_eq!(
            duration("P1M").value_cmp(&duration("PT1S")),
            Err(Incomparable)
        );
    }

    #[test]
    fn arithmetic_names_its_errors() {
        let int = DataValue::integer;
        assert_eq!(&int(1) + &int(2), Ok(int(3)));
        assert_eq!(&int(7) - &number("0.5"), Ok(number("6.5")));
        assert_eq!(&int(6) * &int(7), Ok(int(42)));
        assert_eq!(&int(7) / &int(2), Ok(number("3.5")));
        assert_eq!(&int(-7) % &int(3), Ok(int(-1)));
        assert_eq!(-&int(7), Ok(int(-7)));

        assert_eq!(&int(1) + &DataValue::string("1"), Err(TypeMismatch));
        assert_eq!(-&DataValue::boolean(true), Err(TypeMismatch));
        assert_eq!(&int(1) / &int(0), Err(DivisionByZero));
        assert_eq!(&int(1) % &int(0), Err(DivisionByZero));
        assert_eq!(&number("1.5") % &int(1), Err(TypeMismatch));
        assert_eq!(&int(3) * &int(i64::MAX), Ok(number("27670116110564327421")));
    }

    #[test]
    fn floating_arithmetic_stays_in_its_value_space() {
        // A zero divisor and an overflow are values, as XPath's `op:numeric-*` define them.
        assert_eq!(
            &DataValue::double(1.0) / &DataValue::float(0.0),
            Ok(DataValue::double(f64::INFINITY))
        );
        assert_eq!(
            &DataValue::double(1e300) * &DataValue::double(1e300),
            Ok(DataValue::double(f64::INFINITY))
        );
        assert_eq!(
            -&DataValue::double(f64::INFINITY),
            Ok(DataValue::double(f64::NEG_INFINITY))
        );
        assert_eq!(
            &DataValue::float(0.0) / &DataValue::integer(0),
            Ok(DataValue::float(f32::NAN))
        );
    }

    #[test]
    fn equality_follows_rdf_term_equal() {
        let int = DataValue::integer;
        let datetime =
            |lexical| DataValue::datetime(DateTime::from_lexical(lexical).expect("well-formed"));

        assert_eq!(int(1).value_eq(&number("1.0")), Ok(true));
        assert_eq!(int(1).value_eq(&DataValue::double(1.0)), Ok(true));
        assert_eq!(
            datetime("2000-01-01T12:00:00Z").value_eq(&datetime("2000-01-01T13:00:00+01:00")),
            Ok(true)
        );
        assert_eq!(
            DataValue::string("a").value_eq(&DataValue::string("b")),
            Ok(false)
        );
        assert_eq!(DataValue::iri("a").value_eq(&int(1)), Ok(false));
        assert_eq!(DataValue::string("1").value_eq(&int(1)), Err(Incomparable));
        // Literals of unknown datatypes: equal as terms, or undecided.
        let other = |lexical| DataValue::other("http://example.org/t", lexical).expect("other");
        assert_eq!(other("a").value_eq(&other("a")), Ok(true));
        assert_eq!(other("a").value_eq(&other("b")), Err(Incomparable));
    }

    #[test]
    fn order_is_defined_within_domains() {
        let int = DataValue::integer;
        assert_eq!(int(1).value_cmp(&number("1.5")), Ok(Some(Ordering::Less)));
        assert_eq!(
            DataValue::string("b").value_cmp(&DataValue::string("a")),
            Ok(Some(Ordering::Greater))
        );
        assert_eq!(
            DataValue::boolean(false).value_cmp(&DataValue::boolean(true)),
            Ok(Some(Ordering::Less))
        );
        assert_eq!(DataValue::double(f64::NAN).value_cmp(&int(1)), Ok(None));
        assert_eq!(int(1).value_cmp(&DataValue::string("1")), Err(Incomparable));
    }

    #[test]
    fn values_name_their_datatype() {
        assert_eq!(
            ValueRef::Number(NumberRef::Decimal(42.into())).datatype_iri(),
            Some("http://www.w3.org/2001/XMLSchema#integer")
        );
        assert_eq!(
            ValueRef::Iri(IriRef::new("http://example.org")).datatype_iri(),
            None
        );

        let literal = OtherLiteralRef::new("http://example.org/unit", "5cm").expect("not native");
        assert_eq!(
            ValueRef::Other(literal).datatype_iri(),
            Some("http://example.org/unit")
        );
    }
}
