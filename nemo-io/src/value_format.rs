//! Cell formats: how the text of a cell is read as a value, and how a value is written as a cell.
//!
//! A [ValueFormat] reads and writes the cells that hold a value.
//! A [ColumnFormat] is the format of one column:
//! a value format, and whether the column admits a *missing value*,
//! which is a place in a row where the resource has no value, such as an empty cell.

use std::fmt::{self, Display, Formatter};

use nemo_physical::value::{
    DataValue, DomainSet, Iri, NumberRef, ReadValue, ValueDomain, ValueRef,
};

/// The way the cells of one column are read and written.
///
/// [Term](Self::Term) reads a cell as a constant in term syntax, which says what its value is,
/// and [Skip](Self::Skip) does not read it.
/// Every other format is *typed*: it reads only values of one type,
/// and a cell that does not contain such a value makes its row malformed.
/// The [name](Self::name) of a typed format is the name of its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueFormat {
    /// One constant in term syntax, as a program writes it ([ReadValue::from_term_syntax]).
    ///
    /// Examples are `alice`, `<http://example.org/a>`, `42`, `2.5d`, `true`, `"plain"`,
    /// `"hallo"@de`, the null `_:5`, a tuple and a map.
    /// A cell with any other text is not read:
    /// a full IRI needs its angle brackets, and a text with spaces needs its quotes.
    /// The cell is read without a base IRI and without prefixes,
    /// so that a resource means the same to every program.
    ///
    /// A value is written in canonical term syntax, which is its
    /// [Display], and is read back as the same value.
    /// A null is read back as a null of the resource that it is read from.
    Term,
    /// The cell as it is, as a plain string.
    String,
    /// The cell as it is, as an IRI, if it is a [valid](Iri::is_valid) one.
    Iri,
    /// An integer of any size.
    Integer,
    /// A 64-bit floating point number in XSD's lexical form, as
    /// [DataValue::typed_literal] reads an `xsd:double`.
    ///
    /// The special values are spelled `INF`, `-INF` and `NaN` only.
    Double,
    /// A decimal number.
    ///
    /// An integral value is read as an integer.
    Decimal,
    /// A number, read as an integer, a decimal or a double, depending on its XSD lexical form.
    Number,
    /// A 32-bit floating point number in XSD's lexical form.
    Float,
    /// A boolean in XSD's lexical form: `true`, `false`, `1` or `0`.
    Boolean,
    /// A language-tagged string in term syntax, `"…"@lang`.
    LanguageString,
    /// A dateTime in XSD's lexical form.
    DateTime,
    /// A date in XSD's lexical form.
    Date,
    /// A time in XSD's lexical form.
    Time,
    /// A duration of any kind in XSD's lexical form.
    Duration,
    /// A duration without months in XSD's lexical form.
    DayTimeDuration,
    /// A duration of months only in XSD's lexical form.
    ///
    /// The zero duration is a dayTimeDuration, so `P0M` is not read as a yearMonthDuration.
    YearMonthDuration,
    /// A column that is not read.
    ///
    /// Its cells are never parsed and deliver no value.
    /// The column only counts toward the number of columns in the file.
    Skip,
}

impl ValueFormat {
    /// The names that [from_name](Self::from_name) accepts, for diagnostics.
    pub const NAMES: &str = "int, double, decimal, number, float, bool, string, langstring, iri, \
        datetime, date, time, duration, daytimeduration, yearmonthduration, term, skip";

    /// Every format.
    pub const ALL: [Self; 17] = [
        ValueFormat::Term,
        ValueFormat::String,
        ValueFormat::Iri,
        ValueFormat::Integer,
        ValueFormat::Double,
        ValueFormat::Decimal,
        ValueFormat::Number,
        ValueFormat::Float,
        ValueFormat::Boolean,
        ValueFormat::LanguageString,
        ValueFormat::DateTime,
        ValueFormat::Date,
        ValueFormat::Time,
        ValueFormat::Duration,
        ValueFormat::DayTimeDuration,
        ValueFormat::YearMonthDuration,
        ValueFormat::Skip,
    ];

    /// Returns the format with the given name, or `None` for an unknown name.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "int" => ValueFormat::Integer,
            "double" => ValueFormat::Double,
            "decimal" => ValueFormat::Decimal,
            "number" => ValueFormat::Number,
            "float" => ValueFormat::Float,
            "bool" => ValueFormat::Boolean,
            "string" => ValueFormat::String,
            "langstring" => ValueFormat::LanguageString,
            "iri" => ValueFormat::Iri,
            "datetime" => ValueFormat::DateTime,
            "date" => ValueFormat::Date,
            "time" => ValueFormat::Time,
            "duration" => ValueFormat::Duration,
            "daytimeduration" => ValueFormat::DayTimeDuration,
            "yearmonthduration" => ValueFormat::YearMonthDuration,
            "term" => ValueFormat::Term,
            "skip" => ValueFormat::Skip,
            _ => return None,
        })
    }

    /// Returns the format's name, which [from_name](Self::from_name) maps back to the format.
    pub fn name(&self) -> &'static str {
        match self {
            ValueFormat::Integer => "int",
            ValueFormat::Double => "double",
            ValueFormat::Decimal => "decimal",
            ValueFormat::Number => "number",
            ValueFormat::Float => "float",
            ValueFormat::Boolean => "bool",
            ValueFormat::String => "string",
            ValueFormat::LanguageString => "langstring",
            ValueFormat::Iri => "iri",
            ValueFormat::DateTime => "datetime",
            ValueFormat::Date => "date",
            ValueFormat::Time => "time",
            ValueFormat::Duration => "duration",
            ValueFormat::DayTimeDuration => "daytimeduration",
            ValueFormat::YearMonthDuration => "yearmonthduration",
            ValueFormat::Term => "term",
            ValueFormat::Skip => "skip",
        }
    }

    /// Returns the domains of the values the format reads.
    ///
    /// A typed format rejects a cell whose value would be of another domain,
    /// so every value it reads is of these domains without a further check.
    pub fn domains(&self) -> DomainSet {
        match self {
            ValueFormat::Term => DomainSet::ANY,
            ValueFormat::String => DomainSet::STRING,
            ValueFormat::Iri => DomainSet::IRI,
            ValueFormat::Integer => DomainSet::INTEGER,
            ValueFormat::Double => DomainSet::DOUBLE,
            // An integral decimal is read as an integer.
            ValueFormat::Decimal => DomainSet::EXACT,
            ValueFormat::Number => DomainSet::NUMERIC,
            ValueFormat::Float => DomainSet::FLOAT,
            ValueFormat::Boolean => DomainSet::BOOLEAN,
            ValueFormat::LanguageString => DomainSet::LANGUAGE_STRING,
            ValueFormat::DateTime => DomainSet::DATE_TIME,
            ValueFormat::Date => DomainSet::DATE,
            ValueFormat::Time => DomainSet::TIME,
            ValueFormat::Duration => DomainSet::DURATION,
            ValueFormat::DayTimeDuration => DomainSet::DAY_TIME_DURATION,
            ValueFormat::YearMonthDuration => DomainSet::YEAR_MONTH_DURATION,
            // A skipped column stores nothing.
            ValueFormat::Skip => DomainSet::EMPTY,
        }
    }

    /// Returns the XSD datatype whose lexical forms the format reads,
    /// or `None` for a format that reads cells another way.
    fn datatype(&self) -> Option<&'static str> {
        let domain = match self {
            ValueFormat::Integer => ValueDomain::Integer,
            ValueFormat::Double => ValueDomain::Double,
            ValueFormat::Float => ValueDomain::Float,
            ValueFormat::Boolean => ValueDomain::Boolean,
            ValueFormat::DateTime => ValueDomain::DateTime,
            ValueFormat::Date => ValueDomain::Date,
            ValueFormat::Time => ValueDomain::Time,
            ValueFormat::Duration => ValueDomain::Duration,
            ValueFormat::DayTimeDuration => ValueDomain::DayTimeDuration,
            ValueFormat::YearMonthDuration => ValueDomain::YearMonthDuration,
            _ => return None,
        };

        domain.datatype_iri()
    }

    /// Parses one cell, or returns `None` if it does not match the format.
    ///
    /// `null` returns the null that a label names in the resource of the cell.
    /// It is called for each null of a [Term](Self::Term) cell, and by no other format.
    pub fn parse(&self, cell: &str, null: &mut dyn FnMut(&str) -> DataValue) -> Option<DataValue> {
        self.parse_cell(cell, null).map(ReadValue::into_owned)
    }

    /// Parses one cell as [parse](Self::parse) does,
    /// without copying an IRI or a string that appears in the cell unchanged.
    pub(crate) fn parse_cell<'a>(
        &self,
        cell: &'a str,
        null: &mut dyn FnMut(&str) -> DataValue,
    ) -> Option<ReadValue<'a>> {
        let value = match self {
            ValueFormat::Term => return ReadValue::from_term_syntax(cell, null),
            ValueFormat::String => return Some(ReadValue::String(cell)),
            ValueFormat::Iri => return Iri::is_valid(cell).then_some(ReadValue::Iri(cell)),
            ValueFormat::Skip => return None,
            ValueFormat::Decimal => DataValue::decimal_from_lexical(cell)?,
            ValueFormat::Number => {
                let integer = ValueFormat::Integer.datatype()?;
                let double = ValueFormat::Double.datatype()?;
                DataValue::typed_literal(integer, cell)
                    .or_else(|| DataValue::decimal_from_lexical(cell))
                    .or_else(|| DataValue::typed_literal(double, cell))?
            }
            // A null is no language string, so none is named here.
            ValueFormat::LanguageString => {
                ReadValue::from_term_syntax(cell, &mut |_| DataValue::null(0))?.into_owned()
            }
            typed => DataValue::typed_literal(typed.datatype()?, cell)?,
        };

        self.domains()
            .contains(value.domain())
            .then_some(ReadValue::Value(value))
    }

    /// Returns the text of a value as a cell, or `None` if the value does not fit the format.
    ///
    /// - [Term](Self::Term) writes any value, in canonical term syntax.
    /// - A typed format writes only values of its own domains,
    ///   as their lexical forms without a datatype.
    /// - No value fits [Skip](Self::Skip).
    pub fn write(&self, value: ValueRef<'_>) -> Option<String> {
        match (self, value) {
            (ValueFormat::Term, value) => Some(value.to_string()),
            (ValueFormat::String, ValueRef::String(text)) => Some(text.to_owned()),
            (ValueFormat::Iri, ValueRef::Iri(iri)) => Some(iri.to_string()),
            (ValueFormat::Integer, ValueRef::Number(value)) if value.is_integral() => {
                Some(value.to_string())
            }
            (ValueFormat::Double, ValueRef::Number(NumberRef::Double(value))) => {
                Some(value.to_string())
            }
            (ValueFormat::Decimal, ValueRef::Number(value))
                if matches!(value.domain(), ValueDomain::Integer | ValueDomain::Decimal) =>
            {
                Some(value.to_string())
            }

            // A double is written in exponent form, so that it is not read back as a decimal.
            (ValueFormat::Number, ValueRef::Number(NumberRef::Double(value))) => {
                let value = value.get();
                Some(if value.is_nan() {
                    String::from("NaN")
                } else if value.is_infinite() {
                    String::from(if value > 0.0 { "INF" } else { "-INF" })
                } else {
                    format!("{value:E}")
                })
            }
            (ValueFormat::Number, ValueRef::Number(value))
                if value.domain() != ValueDomain::Float =>
            {
                Some(value.to_string())
            }
            (ValueFormat::Float, ValueRef::Number(NumberRef::Float(value))) => {
                Some(value.to_string())
            }
            (ValueFormat::Boolean, ValueRef::Boolean(value)) => Some(value.to_string()),
            (ValueFormat::LanguageString, ValueRef::LanguageString(_)) => Some(value.to_string()),

            // The canonical lexical form of a temporal value is its `Display`.
            (ValueFormat::DateTime, ValueRef::DateTime(value)) => Some(value.to_string()),
            (ValueFormat::Date, ValueRef::Date(value)) => Some(value.to_string()),
            (ValueFormat::Time, ValueRef::Time(value)) => Some(value.to_string()),
            (
                ValueFormat::Duration
                | ValueFormat::DayTimeDuration
                | ValueFormat::YearMonthDuration,
                ValueRef::Duration(duration),
            ) if self.domains().contains(value.domain()) => Some(duration.to_string()),
            _ => None,
        }
    }
}

impl Display for ValueFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The format of one column: how its cells are read and written,
/// and whether the column admits a missing value.
///
/// A column admits a missing value if its format has the marker `?`, as in `int?`.
/// - Read under the marker, a missing value is a fresh null, so the column also holds nulls.
///   Without the marker, a row with a missing value is dropped and counted.
/// - Written under the marker, a null is a missing value,
///   and a value whose cell would be empty has no form.
///
/// In delimiter-separated values the missing value is the empty cell.
/// Without the marker, a format that reads the empty text, as `string` does,
/// reads the empty cell as that value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnFormat {
    /// How a cell that holds a value is read and written.
    pub value: ValueFormat,
    /// Whether the column admits a missing value.
    pub optional: bool,
}

/// What a column's format reads from one cell.
#[derive(Debug)]
pub(crate) enum CellRead<'a> {
    /// The value that the cell holds.
    Value(ReadValue<'a>),
    /// A missing value, which the column admits.
    Missing,
    /// A missing value, which the column does not admit.
    Unadmitted,
    /// A text that the column's format does not read.
    Malformed,
}

impl ColumnFormat {
    /// The marker of a column that admits a missing value, written after the format's name.
    pub const MARKER: char = '?';

    /// Creates the format of a column that admits no missing value.
    pub const fn new(value: ValueFormat) -> Self {
        Self {
            value,
            optional: false,
        }
    }

    /// Creates the format of a column that admits a missing value.
    pub const fn optional(value: ValueFormat) -> Self {
        Self {
            value,
            optional: true,
        }
    }

    /// Returns the format with the given name, such as `int` or `int?`,
    /// or `None` for an unknown name.
    ///
    /// `skip?` names no format, since a column that is not read has no values to miss.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.strip_suffix(Self::MARKER) {
            Some(name) => ValueFormat::from_name(name)
                .filter(|&value| value != ValueFormat::Skip)
                .map(Self::optional),
            None => ValueFormat::from_name(name).map(Self::new),
        }
    }

    /// Returns the domains of the values that the column holds when it is read:
    /// those of its [value format](ValueFormat::domains), and the nulls if it admits a missing
    /// value.
    pub fn domains(&self) -> DomainSet {
        if self.optional {
            self.value.domains().union(DomainSet::NULL)
        } else {
            self.value.domains()
        }
    }

    /// Reads one cell of a delimiter-separated resource.
    ///
    /// `null` returns the null that a label names in the resource, as for
    /// [ValueFormat::parse].
    pub(crate) fn read<'a>(
        &self,
        cell: &'a str,
        null: &mut dyn FnMut(&str) -> DataValue,
    ) -> CellRead<'a> {
        if cell.is_empty() && self.optional {
            return CellRead::Missing;
        }

        match self.value.parse_cell(cell, null) {
            Some(value) => CellRead::Value(value),
            None if cell.is_empty() => CellRead::Unadmitted,
            None => CellRead::Malformed,
        }
    }

    /// Returns the text of a value as a cell of a delimiter-separated resource,
    /// or `None` if the value has no form in the column.
    ///
    /// A column that admits a missing value writes a null as the empty cell.
    /// A value whose cell would be empty, such as the empty string under `string?`,
    /// has no form there, since it would be read back as a missing value.
    pub fn write(&self, value: ValueRef<'_>) -> Option<String> {
        if !self.optional {
            return self.value.write(value);
        }
        if matches!(value, ValueRef::Null(_)) {
            return Some(String::new());
        }

        self.value.write(value).filter(|cell| !cell.is_empty())
    }
}

impl From<ValueFormat> for ColumnFormat {
    fn from(value: ValueFormat) -> Self {
        Self::new(value)
    }
}

impl Display for ColumnFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.value.name())?;
        if self.optional {
            write!(f, "{}", Self::MARKER)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nemo_physical::value::TupleValue;

    use super::*;

    /// Reads a cell by a format, with every null read as the null 0.
    fn read(format: ValueFormat, cell: &str) -> Option<DataValue> {
        format.parse(cell, &mut |_| DataValue::null(0))
    }

    /// Every format's name maps back to the format and is among the accepted names,
    /// while `any` and `integer` name no format.
    #[test]
    fn names_round_trip_and_any_names_no_format() {
        for format in ValueFormat::ALL {
            assert_eq!(ValueFormat::from_name(format.name()), Some(format));
            assert!(ValueFormat::NAMES.contains(format.name()));
        }

        assert_eq!(ValueFormat::from_name("any"), None);
        assert_eq!(ValueFormat::from_name("integer"), None);
        assert_eq!(ValueFormat::Integer.to_string(), "int");
    }

    /// A format's name with the marker names the format of a column that admits a missing
    /// value, except for `skip`.
    #[test]
    fn the_marker_names_a_column_that_admits_a_missing_value() {
        let optional = ColumnFormat::from_name("int?").expect("the format exists");
        assert_eq!(optional, ColumnFormat::optional(ValueFormat::Integer));
        assert_eq!(optional.to_string(), "int?");
        assert!(optional.domains().contains(ValueDomain::Null));
        assert!(
            !ColumnFormat::new(ValueFormat::Integer)
                .domains()
                .contains(ValueDomain::Null)
        );

        assert_eq!(
            ColumnFormat::from_name("int"),
            Some(ValueFormat::Integer.into())
        );
        assert_eq!(ColumnFormat::from_name("skip?"), None);
        assert_eq!(ColumnFormat::from_name("int??"), None);
        assert_eq!(ColumnFormat::from_name("?"), None);
    }

    /// The term format writes a value in canonical term syntax and reads it back,
    /// and does not read a cell that is not one constant.
    #[test]
    fn term_cells_hold_one_constant_in_term_syntax() {
        let pair = DataValue::tuple(TupleValue::new(vec![
            DataValue::integer(1),
            DataValue::string("a"),
        ]));
        for (value, cell) in [
            (DataValue::iri("alice"), "alice"),
            (
                DataValue::iri("http://example.org/a"),
                "<http://example.org/a>",
            ),
            (DataValue::iri("42"), "<42>"),
            (DataValue::string("plain"), "\"plain\""),
            (DataValue::integer(42), "42"),
            (DataValue::double(2.5), "2.5d"),
            (DataValue::boolean(true), "true"),
            (DataValue::null(0), "_:0"),
            (pair, "(1, \"a\")"),
        ] {
            let written = ValueFormat::Term.write(value.as_ref());
            assert_eq!(written.as_deref(), Some(cell));
            assert_eq!(read(ValueFormat::Term, cell), Some(value), "cell {cell:?}");
        }

        // A full IRI needs its brackets, a text with spaces its quotes, and a prefixed name
        // a declaration that a cell does not have.
        for cell in ["", "http://example.org/a", "Alice Smith", "ex:a", "1 + 2"] {
            assert_eq!(read(ValueFormat::Term, cell), None, "cell {cell:?}");
        }
    }

    /// The IRI format reads the cell as it is, if it is a valid IRI.
    #[test]
    fn the_iri_format_reads_only_valid_iris() {
        let iri = |text: &str| Some(DataValue::iri(text));
        assert_eq!(read(ValueFormat::Iri, "http://e/a"), iri("http://e/a"));
        assert_eq!(read(ValueFormat::Iri, "a{b}"), None);
        assert_eq!(read(ValueFormat::Iri, "<http://e/a>"), None);
    }

    /// A typed format reads every lexical form of its type, canonical or not,
    /// and nothing else.
    #[test]
    fn typed_formats_parse_leniently() {
        assert_eq!(
            read(ValueFormat::Integer, "42"),
            Some(DataValue::integer(42))
        );
        assert_eq!(read(ValueFormat::Integer, "abc"), None);
        assert_eq!(
            read(ValueFormat::Double, "2.5"),
            Some(DataValue::double(2.5))
        );
        assert!(read(ValueFormat::Double, "NaN").is_some());
        assert_eq!(
            read(ValueFormat::Double, "-INF"),
            Some(DataValue::double(f64::NEG_INFINITY))
        );

        // Only XSD's spellings of the special values are read, and surrounding whitespace is
        // rejected.
        for cell in ["inf", "nan", "Infinity", " 2.5"] {
            assert_eq!(read(ValueFormat::Double, cell), None, "cell {cell:?}");
        }

        // A typed column stores values, not lexical forms, so a
        // non-canonical lexical form is normalized rather than rejected.
        assert_eq!(
            read(ValueFormat::Decimal, "1.50"),
            Some(DataValue::decimal(15, -1).expect("representable"))
        );
        assert_eq!(read(ValueFormat::Decimal, "x"), None);

        // Strings and IRIs are read verbatim, not as numbers.
        assert_eq!(read(ValueFormat::String, "5"), Some(DataValue::string("5")));
        assert_eq!(
            read(ValueFormat::Iri, "http://example.org/a"),
            Some(DataValue::iri("http://example.org/a"))
        );
    }

    /// Every scalar type has a format that reads its lexical forms and rejects other text.
    #[test]
    fn every_scalar_type_has_a_strict_reading() {
        assert_eq!(
            read(ValueFormat::Boolean, "true"),
            Some(DataValue::boolean(true))
        );
        assert_eq!(
            read(ValueFormat::Boolean, "0"),
            Some(DataValue::boolean(false))
        );
        assert_eq!(read(ValueFormat::Boolean, "yes"), None);
        assert_eq!(
            read(ValueFormat::Float, "1.5").map(|value| value.domain()),
            Some(ValueDomain::Float)
        );
        for (format, cell, domain) in [
            (
                ValueFormat::DateTime,
                "2026-10-18T12:00:00Z",
                ValueDomain::DateTime,
            ),
            (ValueFormat::Date, "2026-10-18", ValueDomain::Date),
            (ValueFormat::Time, "12:00:00", ValueDomain::Time),
            (ValueFormat::Duration, "P1Y1D", ValueDomain::Duration),
            (
                ValueFormat::DayTimeDuration,
                "PT1H",
                ValueDomain::DayTimeDuration,
            ),
            (
                ValueFormat::YearMonthDuration,
                "P1Y",
                ValueDomain::YearMonthDuration,
            ),
        ] {
            assert_eq!(
                read(format, cell).map(|value| value.domain()),
                Some(domain),
                "{format} reads {cell}"
            );
            assert_eq!(read(format, "x"), None, "{format} rejects x");
        }

        // The zero duration is a dayTimeDuration, never a yearMonthDuration.
        assert_eq!(read(ValueFormat::YearMonthDuration, "P0M"), None);

        // A number is an integer, a decimal, or a double, as it is written.
        assert_eq!(read(ValueFormat::Number, "5"), Some(DataValue::integer(5)));
        assert_eq!(
            read(ValueFormat::Number, "1.5").map(|value| value.domain()),
            Some(ValueDomain::Decimal)
        );
        assert_eq!(
            read(ValueFormat::Number, "1.5E0").map(|value| value.domain()),
            Some(ValueDomain::Double)
        );
        assert_eq!(read(ValueFormat::Number, "five"), None);

        // The language string format reads a language-tagged string, and nothing else.
        assert_eq!(
            read(ValueFormat::LanguageString, "\"hello\"@en").map(|value| value.domain()),
            Some(ValueDomain::LanguageString)
        );
        assert_eq!(read(ValueFormat::LanguageString, "\"hello\""), None);
        assert_eq!(read(ValueFormat::LanguageString, "hello"), None);
    }

    /// Writing a value that a format has read gives a cell that the format reads back as that
    /// value.
    #[test]
    fn a_reading_reads_back_what_it_writes() {
        for (format, cell) in [
            (ValueFormat::Boolean, "true"),
            (ValueFormat::Float, "1.5"),
            (ValueFormat::DateTime, "2026-10-18T12:00:00Z"),
            (ValueFormat::Date, "2026-10-18"),
            (ValueFormat::Time, "12:00:00"),
            (ValueFormat::Duration, "P1Y1D"),
            (ValueFormat::DayTimeDuration, "PT1H"),
            (ValueFormat::YearMonthDuration, "P1Y"),
            (ValueFormat::Number, "1.5"),
            (ValueFormat::Number, "2.5E0"),
            (ValueFormat::Number, "1E300"),
            (ValueFormat::Number, "-0E0"),
            (ValueFormat::Number, "INF"),
            (ValueFormat::Number, "NaN"),
            (ValueFormat::Number, "123456789012345678901"),
            (ValueFormat::LanguageString, "\"hello\"@en"),
        ] {
            let value = read(format, cell).expect("the cell reads");
            let written = format.write(value.as_ref()).expect("the value writes");
            assert_eq!(
                read(format, &written),
                Some(value),
                "{format} through {written}"
            );
        }
    }
}
