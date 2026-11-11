//! The mapping between JSON values and Nemo's values.
//!
//! In a `term` column, each value says what it is.
//! The kinds that JSON has are written as they are:
//! a string, a boolean, an integer of any size, a double (a number with a fraction or an
//! exponent), a tuple without a label (an array),
//! and a map without a label whose keys are strings (an object).
//! Every other value is an object whose keys start with `@`,
//! the first of which names the value's format, as in `{"@iri": "http://example.org/a"}`.
//! In a column with any other format, a cell holds the value's bare form.

use std::fmt::Write;

use nemo_physical::value::{DataValue, MapValue, NumberRef, TupleValue, ValueDomain, ValueRef};

use crate::value_format::{ColumnFormat, ValueFormat};

use super::parse::Json;

/// What a column's format reads from one cell of a record.
#[derive(Debug)]
pub(super) enum JsonCell {
    /// The value that the cell holds.
    Value(DataValue),
    /// A missing value, which the column admits.
    Missing,
    /// A missing value, which the column does not admit.
    Unadmitted,
    /// A JSON value that the column's format does not read.
    Malformed,
}

/// Reads one cell of a record by its column's format.
///
/// `cell` is `None` for a key that the record does not have, which is a missing value,
/// as `null` is.
/// `null` returns the null that a label names in the document.
pub(super) fn read(
    cell: Option<&Json<'_>>,
    format: ColumnFormat,
    null: &mut dyn FnMut(&str) -> DataValue,
) -> JsonCell {
    let Some(cell) = cell.filter(|cell| **cell != Json::Null) else {
        return if format.optional {
            JsonCell::Missing
        } else {
            JsonCell::Unadmitted
        };
    };

    match bare(cell, format.value, null) {
        Some(value) => JsonCell::Value(value),
        None => JsonCell::Malformed,
    }
}

/// Reads a JSON value as the bare form of a format, or as a term under [ValueFormat::Term].
fn bare(
    cell: &Json<'_>,
    format: ValueFormat,
    null: &mut dyn FnMut(&str) -> DataValue,
) -> Option<DataValue> {
    let mut no_null = |_: &str| DataValue::null(0);
    match (format, cell) {
        (ValueFormat::Term, cell) => term(cell, null),

        // A language string has no bare form in JSON, so its column holds the tagged form.
        (ValueFormat::LanguageString, Json::Object(_)) => {
            term(cell, null).filter(|value| value.domain() == ValueDomain::LanguageString)
        }
        (ValueFormat::Boolean, Json::Boolean(value)) => Some(DataValue::boolean(*value)),
        (
            ValueFormat::Integer
            | ValueFormat::Double
            | ValueFormat::Decimal
            | ValueFormat::Number
            | ValueFormat::Float,
            Json::Number(text),
        ) => format.parse(text, &mut no_null),
        (
            ValueFormat::String
            | ValueFormat::Iri
            | ValueFormat::DateTime
            | ValueFormat::Date
            | ValueFormat::Time
            | ValueFormat::Duration
            | ValueFormat::DayTimeDuration
            | ValueFormat::YearMonthDuration,
            Json::String(text),
        ) => format.parse(text, &mut no_null),
        _ => None,
    }
}

/// Reads a JSON value that says what it is, or returns `None` for a value that is no term:
/// `null`, a number beyond the doubles, and an object with `@` keys that are no tag.
fn term(cell: &Json<'_>, null: &mut dyn FnMut(&str) -> DataValue) -> Option<DataValue> {
    match cell {
        Json::Null => None,
        Json::Boolean(value) => Some(DataValue::boolean(*value)),
        Json::Number(text) if text.contains(['.', 'e', 'E']) => text
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(DataValue::double),
        Json::Number(text) => DataValue::decimal_from_lexical(text),
        Json::String(text) => Some(DataValue::string(text.as_ref())),
        Json::Array(elements) => {
            let elements = terms(elements, null)?;

            Some(DataValue::tuple(TupleValue::new(elements)))
        }
        Json::Object(members) if members.iter().any(|(key, _)| key.starts_with('@')) => {
            tagged(cell, members.len(), null)
        }
        Json::Object(members) => {
            let pairs = members
                .iter()
                .map(|(key, value)| Some((DataValue::string(key.as_ref()), term(value, null)?)))
                .collect::<Option<Vec<_>>>()?;

            MapValue::new(pairs).map(DataValue::map)
        }
    }
}

/// Reads each JSON value of a list as a term.
fn terms(cells: &[Json<'_>], null: &mut dyn FnMut(&str) -> DataValue) -> Option<Vec<DataValue>> {
    cells.iter().map(|cell| term(cell, null)).collect()
}

/// Reads an object with `members` members whose keys start with `@`.
///
/// The object is one of the tagged forms, with exactly the keys of that form.
fn tagged(
    object: &Json<'_>,
    members: usize,
    null: &mut dyn FnMut(&str) -> DataValue,
) -> Option<DataValue> {
    let string = |key: &str| match object.member(key) {
        Some(Json::String(text)) => Some(text.as_ref()),
        _ => None,
    };
    let label = string("@label");
    let labeled = members == 1 + usize::from(label.is_some());

    if let Some(iri) = string("@iri").filter(|_| members == 1) {
        return Some(DataValue::iri(iri));
    }
    if let Some(label) = object.member("@null").filter(|_| members == 1) {
        return match label {
            Json::Number(label) => Some(null(label)),
            Json::String(label) => Some(null(label)),
            _ => None,
        };
    }
    if let (Some(text), Some(language), 2) = (string("@langstring"), string("@language"), members) {
        return DataValue::language_string(text, language);
    }
    if let (Some(lexical), Some(datatype), 2) = (string("@literal"), string("@datatype"), members) {
        return DataValue::typed_literal(datatype, lexical);
    }
    if let Some(Json::Array(elements)) = object.member("@tuple").filter(|_| labeled) {
        let elements = terms(elements, null)?;

        return Some(DataValue::tuple(match label {
            Some(label) => TupleValue::labeled(label, elements),
            None => TupleValue::new(elements),
        }));
    }
    if let Some(Json::Array(pairs)) = object.member("@map").filter(|_| labeled) {
        let pairs = pairs
            .iter()
            .map(|pair| match pair {
                Json::Array(pair) if pair.len() == 2 => {
                    Some((term(&pair[0], null)?, term(&pair[1], null)?))
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let map = match label {
            Some(label) => MapValue::labeled(label, pairs),
            None => MapValue::new(pairs),
        };

        return map.map(DataValue::map);
    }

    // Any other tag is the name of a format, with the value's bare form as a string.
    let Json::Object(pairs) = object else {
        return None;
    };
    let [(tag, Json::String(lexical))] = pairs.as_slice() else {
        return None;
    };
    let format = ValueFormat::from_name(tag.strip_prefix('@')?)?;
    let typed = !matches!(
        format,
        ValueFormat::Term | ValueFormat::Text | ValueFormat::Skip | ValueFormat::LanguageString
    );

    typed
        .then(|| format.parse(lexical, &mut |_| DataValue::null(0)))
        .flatten()
}

/// Appends the JSON form of a value in a column of the given format to `out`,
/// and returns whether the value has a form in the column.
///
/// A column that admits a missing value writes a null as `null`.
/// Nothing is appended for a value without a form.
pub(super) fn write(value: ValueRef<'_>, format: ColumnFormat, out: &mut String) -> bool {
    if format.optional && matches!(value, ValueRef::Null(_)) {
        out.push_str("null");
        return true;
    }

    let start = out.len();
    match format.value {
        ValueFormat::Term => {
            write_term(value, out);
            true
        }
        ValueFormat::LanguageString => {
            let fits = matches!(value, ValueRef::LanguageString(_));
            if fits {
                write_term(value, out);
            }

            fits
        }
        ValueFormat::Boolean => match value {
            ValueRef::Boolean(value) => {
                out.push_str(if value { "true" } else { "false" });
                true
            }
            _ => false,
        },

        // A number is written bare, unless its text is no JSON number, as `INF` is not.
        ValueFormat::Integer
        | ValueFormat::Double
        | ValueFormat::Decimal
        | ValueFormat::Number
        | ValueFormat::Float => {
            let fits = format.value.write_into(value, out) && is_number(&out[start..]);
            if !fits {
                out.truncate(start);
            }

            fits
        }
        ValueFormat::Skip => false,

        // Every other format writes a string that holds the bare form.
        other => {
            let mut text = String::new();
            let fits = other.write_into(value, &mut text);
            if fits {
                write_string(&text, out);
            }

            fits
        }
    }
}

/// Returns whether a text is a JSON number: digits with an optional sign, fraction and
/// exponent, without a leading `+` and without leading zeros.
fn is_number(text: &str) -> bool {
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (unsigned, None),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (mantissa, None),
    };
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());

    digits(whole)
        && (whole == "0" || !whole.starts_with('0'))
        && fraction.is_none_or(digits)
        && exponent
            .is_none_or(|exponent| digits(exponent.strip_prefix(['+', '-']).unwrap_or(exponent)))
}

/// Appends the JSON form of a value that says what the value is.
fn write_term(value: ValueRef<'_>, out: &mut String) {
    let tagged = |tag: &str, format: ValueFormat, out: &mut String| {
        let mut lexical = String::new();
        format.write_into(value, &mut lexical);
        write!(out, "{{\"@{tag}\": ").expect("writing to a string does not fail");
        write_string(&lexical, out);
        out.push('}');
    };

    match value {
        ValueRef::String(text) => write_string(text, out),
        ValueRef::Boolean(value) => out.push_str(if value { "true" } else { "false" }),
        ValueRef::Number(number) if number.domain() == ValueDomain::Integer => {
            write!(out, "{number}").expect("writing to a string does not fail");
        }

        // A double has a fraction or an exponent, which distinguishes it from an integer.
        ValueRef::Number(NumberRef::Double(double)) if double.is_finite() => {
            write!(out, "{:?}", double.get()).expect("writing to a string does not fail");
        }
        ValueRef::Number(NumberRef::Double(_)) => tagged("double", ValueFormat::Double, out),
        ValueRef::Number(NumberRef::Float(_)) => tagged("float", ValueFormat::Float, out),
        ValueRef::Number(_) => tagged("decimal", ValueFormat::Decimal, out),
        ValueRef::Iri(_) => tagged("iri", ValueFormat::Iri, out),
        ValueRef::DateTime(_) => tagged("datetime", ValueFormat::DateTime, out),
        ValueRef::Date(_) => tagged("date", ValueFormat::Date, out),
        ValueRef::Time(_) => tagged("time", ValueFormat::Time, out),
        ValueRef::Duration(_) => tagged("duration", ValueFormat::Duration, out),
        ValueRef::LanguageString(value) => {
            out.push_str("{\"@langstring\": ");
            write_string(value.text(), out);
            out.push_str(", \"@language\": ");
            write_string(value.language(), out);
            out.push('}');
        }
        ValueRef::Other(literal) => {
            out.push_str("{\"@literal\": ");
            write_string(literal.lexical(), out);
            out.push_str(", \"@datatype\": ");
            write_string(literal.datatype(), out);
            out.push('}');
        }
        ValueRef::Null(_) => {
            let number = value.null_number().unwrap_or_default();
            write!(out, "{{\"@null\": {number}}}").expect("writing to a string does not fail");
        }
        ValueRef::Tuple(tuple) => {
            if tuple.label().is_some() {
                out.push_str("{\"@tuple\": ");
            }

            out.push('[');
            for (index, element) in tuple.elements().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }

                write_term(element, out);
            }
            out.push(']');

            if let Some(label) = tuple.label() {
                out.push_str(", \"@label\": ");
                write_string(label, out);
                out.push('}');
            }
        }
        ValueRef::Map(map) => {
            // An object has the keys of a map only if they are strings that are no tags.
            let plain = map.label().is_none()
                && map
                    .pairs()
                    .all(|(key, _)| matches!(key, ValueRef::String(key) if !key.starts_with('@')));
            out.push_str(if plain { "{" } else { "{\"@map\": [" });
            for (index, (key, value)) in map.pairs().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }

                if plain {
                    write_term(key, out);
                    out.push_str(": ");
                    write_term(value, out);
                } else {
                    out.push('[');
                    write_term(key, out);
                    out.push_str(", ");
                    write_term(value, out);
                    out.push(']');
                }
            }

            if !plain {
                out.push(']');
            }
            if let Some(label) = map.label() {
                out.push_str(", \"@label\": ");
                write_string(label, out);
            }
            out.push('}');
        }
    }
}

/// Appends a text as a JSON string, with the quote, the backslash and the control characters
/// escaped.
pub(super) fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if u32::from(control) < 0x20 => {
                write!(out, "\\u{:04x}", u32::from(control))
                    .expect("writing to a string does not fail");
            }
            other => out.push(other),
        }
    }
    out.push('"');
}
