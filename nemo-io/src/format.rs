//! Cell formats: how a textual field becomes a value.

mod cell;

pub(crate) use cell::CellValue;

use std::fmt::{self, Display, Formatter};

use nemo_physical::value::{DataValue, DomainSet, Iri, NumberRef, ValueDomain};

use crate::term::{self, LiteralSyntax};

/// The interpretation of one input column's cells.
///
/// [Term](Self::Term) reads a cell in term syntax.
/// The typed formats parse strictly: a cell that does not match makes its row malformed.
///
/// A format's name ([from_name](Self::from_name), [name](Self::name)) is the name of the type
/// it reads, for the typed formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueFormat {
    /// The cell in term syntax:
    /// numbers in every spelling of the syntax (`+5`, `007`, `1E3`, `2.5f`, `0x1F`),
    /// `"…"` as a plain string (with `@lang` and `^^<datatype>` typed forms and the escapes of the
    /// syntax), `<…>` as an IRI, and anything else as a bare IRI constant,
    /// or as a string where no IRI could hold the text.
    Term,
    /// The cell verbatim as a plain string.
    String,
    /// The cell verbatim as an IRI, if it is a [valid](Iri::is_valid) one.
    Iri,
    /// An integer, compact or wide.
    Integer,
    /// A 64-bit floating point number in XSD's lexical form, as the
    /// `DOUBLE` cast reads it: `INF`, `-INF` and `NaN` included, other
    /// spellings of them not.
    Double,
    /// A decimal (integral values canonicalize to integers, oversized
    /// ones to wide numbers).
    Decimal,
    /// A number: an integer, a decimal, or a double, by its XSD lexical form.
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
    /// The zero duration is a dayTimeDuration, so `P0M` does not read as one.
    YearMonthDuration,
    /// The column is dropped.
    ///
    /// Its cells are never parsed and deliver no value.
    /// It counts toward the file's width only.
    Skip,
}

impl ValueFormat {
    /// The accepted names, for diagnostics.
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

    /// Returns the format a name names, or `None` for an unknown name.
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

    /// Returns the format's canonical name, which [from_name](Self::from_name) reads back.
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

    /// Returns the domains a cell of the format can become: the type of what it reads.
    ///
    /// A typed format reads only values of its domains, a cell that becomes another one
    /// being malformed, so the type holds without a check of the values read.
    pub fn domains(&self) -> DomainSet {
        match self {
            ValueFormat::Term => DomainSet::ANY,
            ValueFormat::String => DomainSet::STRING,
            ValueFormat::Iri => DomainSet::IRI,
            ValueFormat::Integer => DomainSet::INTEGER,
            ValueFormat::Double => DomainSet::DOUBLE,
            // Integral decimals canonicalize to integers.
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

    /// Returns the XSD datatype a typed format reads the lexical forms of,
    /// or `None` for a format that reads otherwise.
    fn datatype(&self) -> Option<&'static str> {
        Some(match self {
            ValueFormat::Integer => "http://www.w3.org/2001/XMLSchema#integer",
            ValueFormat::Double => "http://www.w3.org/2001/XMLSchema#double",
            ValueFormat::Float => "http://www.w3.org/2001/XMLSchema#float",
            ValueFormat::Boolean => "http://www.w3.org/2001/XMLSchema#boolean",
            ValueFormat::DateTime => "http://www.w3.org/2001/XMLSchema#dateTime",
            ValueFormat::Date => "http://www.w3.org/2001/XMLSchema#date",
            ValueFormat::Time => "http://www.w3.org/2001/XMLSchema#time",
            ValueFormat::Duration => "http://www.w3.org/2001/XMLSchema#duration",
            ValueFormat::DayTimeDuration => "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
            ValueFormat::YearMonthDuration => "http://www.w3.org/2001/XMLSchema#yearMonthDuration",
            _ => return None,
        })
    }

    /// Parses one cell, or `None` if it does not match the format.
    pub fn parse(&self, cell: &str) -> Option<DataValue> {
        self.parse_cell(cell).map(CellValue::into_owned)
    }

    /// Parses one cell as [parse](Self::parse) does, borrowing the text
    /// of an IRI or a string that the cell holds as it is.
    pub(crate) fn parse_cell<'a>(&self, cell: &'a str) -> Option<CellValue<'a>> {
        let value = match self {
            ValueFormat::Term => return Self::term(cell),
            ValueFormat::String => return Some(CellValue::String(cell)),
            ValueFormat::Iri => return Iri::is_valid(cell).then_some(CellValue::Iri(cell)),
            ValueFormat::Skip => return None,
            ValueFormat::Decimal => DataValue::decimal_from_lexical(cell)?,
            ValueFormat::Number => {
                let integer = ValueFormat::Integer.datatype()?;
                let double = ValueFormat::Double.datatype()?;
                DataValue::typed_literal(integer, cell)
                    .or_else(|| DataValue::decimal_from_lexical(cell))
                    .or_else(|| DataValue::typed_literal(double, cell))?
            }
            ValueFormat::LanguageString => match Self::term(cell)? {
                CellValue::Value(value) => value,
                _ => return None,
            },
            typed => DataValue::typed_literal(typed.datatype()?, cell)?,
        };
        self.domains()
            .contains(value.domain())
            .then_some(CellValue::Value(value))
    }

    /// Serializes one cell for writing, or returns `None` if the value does not fit the format.
    ///
    /// A writer drops (and counts) a row with a value that does not fit.
    /// [Term](Self::Term) writes the cell text of [any_cell](Self::any_cell).
    /// The typed formats write bare lexicals of their own domain only.
    pub fn write(&self, value: nemo_physical::value::ValueRef<'_>) -> Option<String> {
        use nemo_physical::value::ValueRef;
        match (self, value) {
            (ValueFormat::Term, value) => Some(Self::any_cell(value)),
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
            // A double takes the exponent form, which the reading tells from a decimal.
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
            (ValueFormat::LanguageString, ValueRef::LanguageString(_)) => {
                Some(Self::any_cell(value))
            }
            // The canonical lexical of a temporal value is its `Display`.
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

    /// Reads a cell in term syntax, or returns `None` for an invalid literal.
    ///
    /// - Numbers parse in every spelling of the
    ///   [term syntax](DataValue::number_from_term_syntax).
    /// - Quoted cells are strings, with `@lang` and `^^<datatype>` typed forms,
    ///   their escapes resolved as the [term syntax](LiteralSyntax::TermSyntax) defines them.
    /// - Angle-bracketed cells are IRIs, if [valid](Iri::is_valid).
    /// - Bare cells are IRI constants rather than strings, if they could be IRIs.
    ///
    /// The cell is trimmed first, which no other format does.
    /// An invalid literal is a typed form that does not parse, an unknown escape,
    /// a quote that never closes, or an invalid IRI in angle brackets.
    fn term(cell: &str) -> Option<CellValue<'_>> {
        let cell = cell.trim();
        if let Some(rest) = cell.strip_prefix('"') {
            return term::literal(rest, LiteralSyntax::TermSyntax).map(CellValue::Value);
        }
        if let Some(iri) = term::iri(cell) {
            return Iri::is_valid(iri).then_some(CellValue::Iri(iri));
        }
        if let Some(number) = DataValue::number_from_term_syntax(cell) {
            return Some(CellValue::Value(number));
        }
        // Bare text is an IRI constant only if it could be one, so empty
        // cells and cells holding what no IRI holds, as whitespace, stay strings.
        Some(
            if cell.is_empty() || cell.chars().any(char::is_whitespace) || !Iri::is_valid(cell) {
                CellValue::String(cell)
            } else {
                CellValue::Iri(cell)
            },
        )
    }

    /// Returns the cell text the `term` column format writes for a value,
    /// before CSV-level quoting.
    ///
    /// - Integers and IRIs are bare.
    /// - Strings are quoted, with escapes, and language strings tagged.
    /// - Nulls are written `_:id`.
    /// - Everything else is in canonical typed form.
    ///
    /// An IRI whose bare text would read back as something else
    /// (a number, a quoted literal, a string with whitespace) is written in angle brackets.
    pub fn any_cell(value: nemo_physical::value::ValueRef<'_>) -> String {
        use nemo_physical::value::ValueRef;
        let quote = |text: &str| {
            let mut quoted = String::with_capacity(text.len() + 2);
            quoted.push('"');
            for character in text.chars() {
                match character {
                    '\\' => quoted.push_str("\\\\"),
                    '"' => quoted.push_str("\\\""),
                    '\n' => quoted.push_str("\\n"),
                    '\r' => quoted.push_str("\\r"),
                    other => quoted.push(other),
                }
            }
            quoted.push('"');
            quoted
        };
        let typed = |lexical: &str| {
            let datatype = value
                .datatype_iri()
                .expect("a value of a native datatype has one");
            format!("{}^^<{datatype}>", quote(lexical))
        };
        match value {
            ValueRef::Iri(iri) => {
                let text = iri.to_string();
                if Self::reads_as_iri(&text) {
                    text
                } else {
                    format!("<{text}>")
                }
            }
            ValueRef::String(text) => quote(text),
            ValueRef::LanguageString(value) => {
                format!("{}@{}", quote(value.text()), value.language())
            }
            ValueRef::Boolean(value) => typed(&value.to_string()),
            ValueRef::Number(value) if value.domain() == ValueDomain::Integer => value.to_string(),
            ValueRef::Number(value) => typed(&value.to_string()),
            // The canonical lexical of a temporal value is its `Display`.
            ValueRef::DateTime(lexical) => typed(&lexical.to_string()),
            ValueRef::Date(lexical) => typed(&lexical.to_string()),
            ValueRef::Time(lexical) => typed(&lexical.to_string()),
            ValueRef::Duration(lexical) => typed(&lexical.to_string()),
            ValueRef::Null(_) => format!("_:{}", value.null_number().unwrap_or_default()),
            ValueRef::Other(literal) => {
                format!("{}^^<{}>", quote(literal.lexical()), literal.datatype())
            }
            // Structured values write the syntax they are written in,
            // recursively: `(1, 2)`, `point(1, 2)`, `{a = 1}`.
            ValueRef::Tuple(tuple) => {
                let elements: Vec<String> = tuple.elements().map(Self::any_cell).collect();
                let label = tuple.label().map(Self::label_cell).unwrap_or_default();
                // A one-element tuple keeps the comma that tells it from
                // a parenthesized term.
                let trailing = if elements.len() == 1 && tuple.label().is_none() {
                    ","
                } else {
                    ""
                };
                format!("{label}({}{trailing})", elements.join(", "))
            }
            ValueRef::Map(map) => {
                let pairs: Vec<String> = map
                    .pairs()
                    .map(|(key, value)| {
                        format!("{} = {}", Self::any_cell(key), Self::any_cell(value))
                    })
                    .collect();
                format!(
                    "{}{{{}}}",
                    map.label().map(Self::label_cell).unwrap_or_default(),
                    pairs.join(", ")
                )
            }
        }
    }

    /// Returns the text of a structure's label, as term syntax writes it:
    /// bare if it reads back as the label, in angle brackets otherwise.
    fn label_cell(label: &str) -> String {
        if nemo_physical::value::IriRef::new(label).writes_bare() {
            label.to_string()
        } else {
            format!("<{label}>")
        }
    }

    /// Returns whether a bare cell holding the text reads back as an IRI with that text
    /// under the [Term](Self::Term) interpretation.
    ///
    /// An IRI whose text does is written without angle brackets.
    fn reads_as_iri(text: &str) -> bool {
        !text.is_empty()
            && !text.chars().any(char::is_whitespace)
            && Iri::is_valid(text)
            && !text.starts_with('"')
            && term::iri(text).is_none()
            && DataValue::number_from_term_syntax(text).is_none()
    }
}

impl Display for ValueFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iris_that_would_not_read_back_bare_are_bracketed() {
        for (text, written) in [
            ("http://example.org/a", "http://example.org/a"),
            ("123", "<123>"),
            ("1.5", "<1.5>"),
            ("+5", "<+5>"),
            ("1E3", "<1E3>"),
            ("2.5f", "<2.5f>"),
            ("0x1F", "<0x1F>"),
            ("+", "+"),
            ("5.", "5."),
            ("", "<>"),
        ] {
            let iri = DataValue::iri(text);
            assert_eq!(ValueFormat::any_cell(iri.as_ref()), written, "{text:?}");
            assert_eq!(ValueFormat::Term.parse(written), Some(iri), "{text:?}");
        }
    }

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

    #[test]
    fn structured_values_write_the_syntax_they_are_written_in() {
        use nemo_physical::value::{MapValue, TupleValue};

        let rendered = |value: DataValue| ValueFormat::any_cell(value.as_ref());
        let tuple = |elements: Vec<DataValue>| DataValue::tuple(TupleValue::new(elements));

        assert_eq!(
            rendered(tuple(vec![DataValue::integer(1), DataValue::integer(2)])),
            "(1, 2)"
        );
        // The comma is what tells a one-element tuple from a
        // parenthesized term.
        assert_eq!(rendered(tuple(vec![DataValue::integer(1)])), "(1,)");
        assert_eq!(
            rendered(DataValue::tuple(TupleValue::labeled(
                "point",
                vec![DataValue::integer(1), DataValue::integer(2)],
            ))),
            "point(1, 2)"
        );
        assert_eq!(
            rendered(DataValue::map(
                MapValue::new([
                    (DataValue::iri("port"), DataValue::integer(8080)),
                    (DataValue::iri("host"), DataValue::string("localhost")),
                ])
                .expect("the keys differ")
            )),
            // Pairs are stored sorted by key, so the rendering is canonical.
            "{host = \"localhost\", port = 8080}"
        );
        // Nesting recurses.
        assert_eq!(
            rendered(tuple(vec![
                DataValue::integer(1),
                tuple(vec![DataValue::integer(2), DataValue::integer(3)]),
            ])),
            "(1, (2, 3))"
        );
    }

    #[test]
    fn iris_read_only_where_valid() {
        let read = |format: ValueFormat, cell: &str| format.parse(cell);
        let iri = |text: &str| Some(DataValue::iri(text));
        assert_eq!(read(ValueFormat::Term, "<http://e/a>"), iri("http://e/a"));
        assert_eq!(read(ValueFormat::Term, "<a b>"), None);
        assert_eq!(read(ValueFormat::Term, "<a>b>"), None);
        assert_eq!(read(ValueFormat::Iri, "http://e/a"), iri("http://e/a"));
        assert_eq!(read(ValueFormat::Iri, "a{b}"), None);
        // Bare text no IRI could hold stays a string.
        assert_eq!(
            read(ValueFormat::Term, "a{b}"),
            Some(DataValue::string("a{b}"))
        );
        assert_eq!(
            read(ValueFormat::Term, "a|b"),
            Some(DataValue::string("a|b"))
        );
    }

    #[test]
    fn terms_read_numbers_as_programs_do() {
        let term = |cell: &str| ValueFormat::Term.parse(cell);
        for (cell, value) in [
            ("7", DataValue::integer(7)),
            ("+7", DataValue::integer(7)),
            ("007", DataValue::integer(7)),
            ("-0x1F", DataValue::integer(-31)),
            ("1.50", DataValue::decimal(15, -1).expect("representable")),
            ("2.5e0", DataValue::double(2.5)),
            ("2.5E0", DataValue::double(2.5)),
            ("1E3", DataValue::double(1000.0)),
            ("2.5d", DataValue::double(2.5)),
            ("2.5f", DataValue::float(2.5)),
            (" 2.5f ", DataValue::float(2.5)),
        ] {
            assert_eq!(term(cell), Some(value), "cell {cell:?}");
        }
        // What a program reads as no number stays an IRI constant.
        for cell in [
            "+", "-.", "5.", ".5", "1e", "0x", "INF", "nan", "1.5.5", "2.5ff",
        ] {
            assert_eq!(term(cell), Some(DataValue::iri(cell)), "cell {cell:?}");
        }
    }

    #[test]
    fn terms_resolve_the_escapes_of_quoted_literals() {
        let term = |cell: &str| ValueFormat::Term.parse(cell);
        assert_eq!(
            term(r#""say \"hi\"""#),
            Some(DataValue::string(r#"say "hi""#))
        );
        assert_eq!(term(r#""a\\b""#), Some(DataValue::string(r"a\b")));
        assert_eq!(term(r#""x\ny\r\t""#), Some(DataValue::string("x\ny\r\t")));
        assert_eq!(
            term(r#""he said \"x\""@en"#),
            DataValue::language_string(r#"he said "x""#, "en")
        );
        assert_eq!(
            term(r#""q\"uote"^^<http://example.org/t>"#),
            DataValue::other("http://example.org/t", r#"q"uote"#)
        );
        // Character escapes resolve, and an unknown escape keeps its backslash, as in programs.
        assert_eq!(
            term(r#""\u0041\U0001F600""#),
            Some(DataValue::string("A\u{1F600}"))
        );
        assert_eq!(term(r#""a\qb""#), Some(DataValue::string(r"a\qb")));
        assert_eq!(
            term(r#""\d+ \user""#),
            Some(DataValue::string(r"\d+ \user"))
        );
        // An unclosed literal and a tail that is neither a language tag nor a datatype are
        // invalid.
        assert_eq!(term(r#""abc"#), None);
        assert_eq!(term(r#""a"b"#), None);
    }

    #[test]
    fn typed_formats_parse_leniently() {
        assert_eq!(
            ValueFormat::Integer.parse("42"),
            Some(DataValue::integer(42))
        );
        assert_eq!(ValueFormat::Integer.parse("abc"), None);
        assert_eq!(
            ValueFormat::Double.parse("2.5"),
            Some(DataValue::double(2.5))
        );
        assert!(ValueFormat::Double.parse("NaN").is_some());
        assert_eq!(
            ValueFormat::Double.parse("-INF"),
            Some(DataValue::double(f64::NEG_INFINITY))
        );
        // Only XSD's spellings of the special values, as the cast takes them.
        for cell in ["inf", "nan", "Infinity", " 2.5"] {
            assert_eq!(ValueFormat::Double.parse(cell), None, "cell {cell:?}");
        }
        // A declared column identifies by value, so non-canonical
        // lexicals normalize instead of failing.
        assert_eq!(
            ValueFormat::Decimal.parse("1.50"),
            Some(DataValue::decimal(15, -1).expect("representable"))
        );
        assert_eq!(ValueFormat::Decimal.parse("x"), None);
        // Strings and IRIs are verbatim and distinct from numbers.
        assert_eq!(ValueFormat::String.parse("5"), Some(DataValue::string("5")));
        assert_eq!(
            ValueFormat::Iri.parse("http://example.org/a"),
            Some(DataValue::iri("http://example.org/a"))
        );
    }

    #[test]
    fn every_scalar_type_has_a_strict_reading() {
        let reads = |format: ValueFormat, cell: &str| format.parse(cell);
        assert_eq!(
            reads(ValueFormat::Boolean, "true"),
            Some(DataValue::boolean(true))
        );
        assert_eq!(
            reads(ValueFormat::Boolean, "0"),
            Some(DataValue::boolean(false))
        );
        assert_eq!(reads(ValueFormat::Boolean, "yes"), None);
        assert_eq!(
            reads(ValueFormat::Float, "1.5").map(|value| value.domain()),
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
                reads(format, cell).map(|value| value.domain()),
                Some(domain),
                "{format} reads {cell}"
            );
            assert_eq!(reads(format, "x"), None, "{format} rejects x");
        }
        // The zero duration is a dayTimeDuration, never a yearMonthDuration.
        assert_eq!(reads(ValueFormat::YearMonthDuration, "P0M"), None);
        // A number is an integer, a decimal, or a double, as it is written.
        assert_eq!(reads(ValueFormat::Number, "5"), Some(DataValue::integer(5)));
        assert_eq!(
            reads(ValueFormat::Number, "1.5").map(|value| value.domain()),
            Some(ValueDomain::Decimal)
        );
        assert_eq!(
            reads(ValueFormat::Number, "1.5E0").map(|value| value.domain()),
            Some(ValueDomain::Double)
        );
        assert_eq!(reads(ValueFormat::Number, "five"), None);
        // A language-tagged string, and nothing else.
        assert_eq!(
            reads(ValueFormat::LanguageString, "\"hello\"@en").map(|value| value.domain()),
            Some(ValueDomain::LanguageString)
        );
        assert_eq!(reads(ValueFormat::LanguageString, "\"hello\""), None);
        assert_eq!(reads(ValueFormat::LanguageString, "hello"), None);
    }

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
            let value = format.parse(cell).expect("the cell reads");
            let written = format.write(value.as_ref()).expect("the value writes");
            assert_eq!(
                format.parse(&written),
                Some(value),
                "{format} through {written}"
            );
        }
    }
}
