//! Reading values from term syntax.

use std::borrow::Cow;

use super::{DataValue, Escape, Iri, MapValue, NameKind, Number, TupleValue, is_builtin_name};

/// The deepest nesting of tuples and maps that is read.
const MAX_NESTING: usize = 128;

/// A value read from a text, which borrows the text where the text is the value.
///
/// A reader that only copies the text of IRIs and strings,
/// as a reader of a file does when it interns them, then never creates an owned value for them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadValue<'a> {
    /// An IRI whose text is a part of the text read.
    Iri(&'a str),
    /// A plain string whose text is a part of the text read.
    String(&'a str),
    /// Any other value, or an IRI or a string whose text differs from the text read.
    Value(DataValue),
}

impl<'a> ReadValue<'a> {
    /// Reads a text that holds one constant in term syntax, or returns `None` for any other
    /// text.
    ///
    /// A constant is a term that denotes a value by itself:
    /// - a bare name such as `alice`, or an IRI in angle brackets;
    /// - a number literal, with an optional sign ([Number::from_term_syntax]);
    /// - `true` or `false`;
    /// - a string in quotes, also in the forms `"…"@lang` and `"…"^^datatype`,
    ///   where the datatype is an IRI in angle brackets or a bare name;
    /// - a null, written `_:` followed by its label;
    /// - a tuple such as `(1, 2)` or `point(1, 2)`, or a map such as `{a = 1}`, of constants.
    ///   A label is a bare name that is not the [name of a builtin](is_builtin_name),
    ///   or an IRI in angle brackets.
    ///
    /// These are the constants of a program, read as a program without a base IRI reads them.
    /// A prefixed name is not read, since the text declares no prefix.
    /// A term that is computed, such as `1 + 2` or `STR(5)`, is not a constant.
    /// Whitespace may surround the constant and the parts of a tuple or a map.
    ///
    /// `null` returns the null that a label names.
    /// It is called once for each null of the text, in the order of the text.
    ///
    /// The text that [Display](std::fmt::Display) writes for a value is read back as that value,
    /// with each null replaced by the one that `null` returns for its number.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::{DataValue, ReadValue};
    ///
    /// let read = |text| ReadValue::from_term_syntax(text, &mut |_| DataValue::null(0));
    /// assert_eq!(read("alice"), Some(ReadValue::Iri("alice")));
    /// assert_eq!(read("<http://example.org/a>"), Some(ReadValue::Iri("http://example.org/a")));
    /// assert_eq!(read(r#""plain""#), Some(ReadValue::String("plain")));
    /// assert_eq!(read("2.5d"), Some(ReadValue::Value(DataValue::double(2.5))));
    /// assert_eq!(read("true"), Some(ReadValue::Value(DataValue::boolean(true))));
    /// assert_eq!(read("http://example.org/a"), None);
    /// assert_eq!(read("Alice Smith"), None);
    /// ```
    pub fn from_term_syntax(
        text: &'a str,
        null: &mut dyn FnMut(&str) -> DataValue,
    ) -> Option<Self> {
        let mut reader = Reader {
            text,
            position: 0,
            depth: 0,
            null,
        };

        reader.skip_whitespace();
        let value = reader.constant()?;
        reader.skip_whitespace();

        (reader.position == text.len()).then_some(value)
    }

    /// Returns the value as an owned value.
    pub fn into_owned(self) -> DataValue {
        match self {
            ReadValue::Iri(iri) => DataValue::iri(iri),
            ReadValue::String(text) => DataValue::string(text),
            ReadValue::Value(value) => value,
        }
    }
}

impl DataValue {
    /// Reads a text that holds one constant in term syntax, or returns `None` for any other
    /// text.
    ///
    /// [ReadValue::from_term_syntax] describes the constants, and what `null` is called with.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::DataValue;
    ///
    /// let read = |text| DataValue::from_term_syntax(text, &mut |_| DataValue::null(7));
    /// let value = read(r#"point(1, "a"@en, _:x)"#).expect("the text is a constant");
    /// assert_eq!(value.to_string(), r#"point(1, "a"@en, _:7)"#);
    /// assert_eq!(read("?x"), None);
    /// ```
    pub fn from_term_syntax(text: &str, null: &mut dyn FnMut(&str) -> DataValue) -> Option<Self> {
        ReadValue::from_term_syntax(text, null).map(ReadValue::into_owned)
    }
}

/// A reader of one text, at a position in it.
struct Reader<'a, 'n> {
    /// The text read.
    text: &'a str,
    /// The position of the next byte to read.
    position: usize,
    /// The number of tuples and maps that enclose the position.
    depth: usize,
    /// Returns the null that a label names.
    null: &'n mut dyn FnMut(&str) -> DataValue,
}

impl<'a> Reader<'a, '_> {
    /// Returns the text from the position on.
    fn rest(&self) -> &'a str {
        &self.text[self.position..]
    }

    /// Returns the byte at the position.
    fn peek(&self) -> Option<u8> {
        self.peek_at(0)
    }

    /// Returns the byte `offset` places after the position.
    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.text.as_bytes().get(self.position + offset).copied()
    }

    /// Consumes the given byte if it is next, and returns whether it was.
    fn eat(&mut self, byte: u8) -> bool {
        let next = self.peek() == Some(byte);
        if next {
            self.position += 1;
        }

        next
    }

    /// Consumes bytes as long as they satisfy the predicate.
    fn skip_while(&mut self, predicate: impl Fn(u8) -> bool) {
        while self.peek().is_some_and(&predicate) {
            self.position += 1;
        }
    }

    /// Consumes whitespace.
    fn skip_whitespace(&mut self) {
        self.skip_while(|byte| byte.is_ascii_whitespace());
    }

    /// Reads one constant at the position.
    fn constant(&mut self) -> Option<ReadValue<'a>> {
        let rest = self.rest();
        match *rest.as_bytes().first()? {
            b'"' => self.string(),
            b'<' => {
                let iri = self.iri()?;
                self.named(iri, false)
            }
            b'0'..=b'9' => self.number().map(|number| ReadValue::Value(number.into())),
            sign @ (b'+' | b'-') => {
                self.position += 1;
                self.skip_whitespace();
                if !self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                    return None;
                }

                let number = self.number()?;
                Some(ReadValue::Value(if sign == b'-' {
                    -number
                } else {
                    number.into()
                }))
            }
            b'(' => {
                self.position += 1;
                self.parenthesized()
            }
            b'{' => {
                self.position += 1;
                self.map(None).map(ReadValue::Value)
            }
            b'_' if rest[1..].starts_with(':') && NameKind::Local.length(&rest[2..]) > 0 => {
                let length = NameKind::Local.length(&rest[2..]);
                self.position += 2 + length;

                Some(ReadValue::Value((self.null)(&rest[2..2 + length])))
            }
            // `f"…"` is a format string, which is computed.
            b'f' if rest[1..].starts_with('"') => None,
            _ => {
                let length = NameKind::Bare.length(rest);
                let name = &rest[..length];
                self.position += length;

                // A lone `_` is a variable, and a name before a `:` needs a declared prefix
                // or a module.
                if length == 0 || name == "_" || self.peek() == Some(b':') {
                    return None;
                }

                self.named(Cow::Borrowed(name), true)
            }
        }
    }

    /// Reads one constant at the position as an owned value.
    fn value(&mut self) -> Option<DataValue> {
        self.constant().map(ReadValue::into_owned)
    }

    /// Reads the rest of a constant that starts with a name: a labeled tuple, a labeled map,
    /// a boolean, or the IRI that the name is.
    ///
    /// `bare` says whether the name is written without angle brackets.
    /// The bare name of a builtin labels nothing: before a `(` it makes a call,
    /// which is computed, and before a `{` it makes no term.
    fn named(&mut self, name: Cow<'a, str>, bare: bool) -> Option<ReadValue<'a>> {
        let reserved = bare && is_builtin_name(&name);
        let end = self.position;
        self.skip_whitespace();
        if self.eat(b'(') {
            if reserved {
                return None;
            }

            let elements = self.elements(b')')?;
            let tuple = TupleValue::labeled(name.into_owned(), elements);

            return Some(ReadValue::Value(DataValue::tuple(tuple)));
        }

        self.position = end;
        match name {
            Cow::Borrowed("true") if bare => Some(ReadValue::Value(DataValue::boolean(true))),
            Cow::Borrowed("false") if bare => Some(ReadValue::Value(DataValue::boolean(false))),
            // A `{` labels a map only directly after the name.
            _ if self.peek() == Some(b'{') && reserved => None,
            name if self.eat(b'{') => self.map(Some(name.into_owned())).map(ReadValue::Value),
            Cow::Borrowed(name) => Some(ReadValue::Iri(name)),
            Cow::Owned(name) => Some(ReadValue::Value(DataValue::iri(name))),
        }
    }

    /// Reads an IRI in angle brackets at the position, and returns its text.
    ///
    /// The IRI ends at the first `>`, which must come before any whitespace.
    /// An IRI with the character U+0000 is not read.
    fn iri(&mut self) -> Option<Cow<'a, str>> {
        let rest = &self.rest()[1..];
        let length = rest
            .bytes()
            .take_while(|byte| !byte.is_ascii_whitespace())
            .position(|byte| byte == b'>')?;
        let iri = Iri::unescape(&rest[..length]);
        self.position += length + 2;

        (!iri.contains('\u{0}')).then_some(iri)
    }

    /// Reads a number literal without a sign at the position.
    ///
    /// The literal is a binary, octal or hexadecimal number after `0b`, `0o` or `0x`,
    /// or digits with an optional fraction, exponent and type suffix.
    fn number(&mut self) -> Option<Number> {
        let start = self.position;
        let radix_digit = |marker: u8, byte: u8| match marker {
            b'b' => matches!(byte, b'0' | b'1'),
            b'o' => matches!(byte, b'0'..=b'7'),
            _ => byte.is_ascii_hexdigit(),
        };
        match (self.peek(), self.peek_at(1), self.peek_at(2)) {
            (Some(b'0'), Some(marker @ (b'b' | b'o' | b'x')), Some(digit))
                if radix_digit(marker, digit) =>
            {
                self.position += 2;
                self.skip_while(|byte| radix_digit(marker, byte));
            }
            _ => {
                self.skip_while(|byte| byte.is_ascii_digit());
                if self.peek() == Some(b'.')
                    && self.peek_at(1).is_some_and(|byte| byte.is_ascii_digit())
                {
                    self.position += 1;
                    self.skip_while(|byte| byte.is_ascii_digit());
                }

                if matches!(self.peek(), Some(b'e' | b'E')) {
                    let sign = usize::from(matches!(self.peek_at(1), Some(b'+' | b'-')));
                    if self
                        .peek_at(1 + sign)
                        .is_some_and(|byte| byte.is_ascii_digit())
                    {
                        self.position += 1 + sign;
                        self.skip_while(|byte| byte.is_ascii_digit());
                    }
                }

                // A suffix marks a float or a double, unless a longer name starts there.
                if matches!(self.peek(), Some(b'f' | b'F' | b'd' | b'D'))
                    && !self.rest()[1..]
                        .chars()
                        .next()
                        .is_some_and(NameKind::continues)
                {
                    self.position += 1;
                }
            }
        }

        Number::from_term_syntax(&self.text[start..self.position])
    }

    /// Reads a string in quotes at the position, with its language tag or datatype.
    fn string(&mut self) -> Option<ReadValue<'a>> {
        let text = self.quoted()?;

        // A language tag follows the closing quote directly.
        if self.eat(b'@') {
            let start = self.position;
            let name = NameKind::Marked.length(self.rest());
            if name == 0 {
                return None;
            }

            self.position += name;
            while self.peek() == Some(b'-')
                && self
                    .peek_at(1)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric())
            {
                self.position += 1;
                self.skip_while(|byte| byte.is_ascii_alphanumeric());
            }

            let language = self.text[start..self.position].to_lowercase();

            return DataValue::language_string(text, &language).map(ReadValue::Value);
        }

        let end = self.position;
        self.skip_whitespace();
        if self.rest().starts_with("^^") {
            self.position += 2;
            self.skip_whitespace();
            let datatype = self.datatype()?;

            return DataValue::typed_literal(&datatype, &text).map(ReadValue::Value);
        }

        self.position = end;
        Some(match text {
            Cow::Borrowed(text) => ReadValue::String(text),
            Cow::Owned(text) => ReadValue::Value(DataValue::string(text)),
        })
    }

    /// Reads the text between the quotes of a string at the position, with its escapes resolved.
    ///
    /// Three quotes open a string that closes with three quotes, and may contain quotes.
    /// A backslash that starts no [escape of a string](Escape::in_string) stands for itself,
    /// and a line break written `\r\n` is read as `\n`.
    /// Returns `None` if the string never closes.
    fn quoted(&mut self) -> Option<Cow<'a, str>> {
        self.position += 1;
        let multiline = self.rest().starts_with("\"\"");
        if multiline {
            self.position += 2;
        }

        // The text is copied only from the first place where it differs from what is written.
        let mut resolved: Option<String> = None;
        let mut segment = self.position;
        loop {
            let special = self.rest().find(['"', '\\', '\r'])?;
            self.position += special;
            match self.peek()? {
                b'"' if multiline && !self.rest().starts_with("\"\"\"") => self.position += 1,
                b'"' => {
                    let last = &self.text[segment..self.position];
                    self.position += if multiline { 3 } else { 1 };

                    return Some(match resolved {
                        Some(mut text) => {
                            text.push_str(last);
                            Cow::Owned(text)
                        }
                        None => Cow::Borrowed(last),
                    });
                }
                b'\r' if self.peek_at(1) != Some(b'\n') => self.position += 1,
                special => {
                    let text = resolved.get_or_insert_with(String::new);
                    text.push_str(&self.text[segment..self.position]);
                    self.position += 1;
                    if special == b'\\' {
                        match Escape::in_string(self.rest()) {
                            Some(escape) => {
                                text.push(escape.character);
                                self.position += escape.length;
                            }
                            None => text.push('\\'),
                        }
                    }

                    segment = self.position;
                }
            }
        }
    }

    /// Reads the datatype of a literal at the position: an IRI in angle brackets,
    /// or a bare name, which is the IRI with its text.
    fn datatype(&mut self) -> Option<Cow<'a, str>> {
        if self.peek() == Some(b'<') {
            return self.iri();
        }

        let rest = self.rest();
        let length = NameKind::Bare.length(rest);
        self.position += length;

        (length > 0 && self.peek() != Some(b':')).then_some(Cow::Borrowed(&rest[..length]))
    }

    /// Reads the rest of a constant after its `(`: a tuple, or a constant in parentheses.
    ///
    /// `()` is the empty tuple, and a comma after the first element makes a tuple,
    /// so `(1,)` is a tuple of one element and `(1)` is the number.
    fn parenthesized(&mut self) -> Option<ReadValue<'a>> {
        self.skip_whitespace();
        if self.eat(b')') {
            return Some(ReadValue::Value(DataValue::tuple(TupleValue::new([]))));
        }

        let first = self.nested(Self::constant)?;
        self.skip_whitespace();
        if self.eat(b')') {
            return Some(first);
        }
        if !self.eat(b',') {
            return None;
        }

        let mut elements = vec![first.into_owned()];
        elements.extend(self.elements(b')')?);

        Some(ReadValue::Value(DataValue::tuple(TupleValue::new(
            elements,
        ))))
    }

    /// Reads constants separated by commas, possibly none, through the closing byte `close`.
    ///
    /// A comma after the last constant is not read.
    fn elements(&mut self, close: u8) -> Option<Vec<DataValue>> {
        let mut elements = Vec::new();
        self.skip_whitespace();
        if self.eat(close) {
            return Some(elements);
        }

        loop {
            elements.push(self.nested(Self::value)?);
            self.skip_whitespace();
            if self.eat(close) {
                return Some(elements);
            }
            if !self.eat(b',') {
                return None;
            }

            self.skip_whitespace();
        }
    }

    /// Reads the rest of a map after its `{`: pairs `key = value` separated by commas,
    /// through the closing `}`.
    ///
    /// A map with two pairs of the same key is not read.
    fn map(&mut self, label: Option<String>) -> Option<DataValue> {
        let mut pairs = Vec::new();
        self.skip_whitespace();
        if !self.eat(b'}') {
            loop {
                let key = self.nested(Self::value)?;
                self.skip_whitespace();
                if !self.eat(b'=') {
                    return None;
                }

                self.skip_whitespace();
                let value = self.nested(Self::value)?;
                pairs.push((key, value));
                self.skip_whitespace();
                if self.eat(b'}') {
                    break;
                }
                if !self.eat(b',') {
                    return None;
                }

                self.skip_whitespace();
            }
        }

        let map = match label {
            Some(label) => MapValue::labeled(label, pairs),
            None => MapValue::new(pairs),
        };

        map.map(DataValue::map)
    }

    /// Reads a part of a tuple or a map with `read`, one level of nesting deeper,
    /// or returns `None` beyond the [deepest nesting](MAX_NESTING).
    fn nested<T>(&mut self, read: impl FnOnce(&mut Self) -> Option<T>) -> Option<T> {
        if self.depth >= MAX_NESTING {
            return None;
        }

        self.depth += 1;
        let part = read(self);
        self.depth -= 1;

        part
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads a text whose nulls are numbered in the order of their first use.
    fn read(text: &str) -> Option<DataValue> {
        let mut labels: Vec<String> = Vec::new();

        DataValue::from_term_syntax(text, &mut |label| {
            let known = labels.iter().position(|known| known == label);
            let id = known.unwrap_or_else(|| {
                labels.push(label.to_owned());
                labels.len() - 1
            });

            DataValue::null(id as u64)
        })
    }

    /// Every kind of value is read back from the text that `Display` writes for it.
    #[test]
    fn written_values_are_read_back() {
        let typed = |datatype: &str, lexical: &str| {
            DataValue::typed_literal(
                &format!("http://www.w3.org/2001/XMLSchema#{datatype}"),
                lexical,
            )
            .expect("the literal is valid")
        };
        let tuple = |elements: Vec<DataValue>| DataValue::tuple(TupleValue::new(elements));
        let values = [
            DataValue::iri("alice"),
            DataValue::iri("item-1"),
            DataValue::iri("http://example.org/a"),
            DataValue::iri("a b>c"),
            DataValue::iri(r"C:\data\u1"),
            DataValue::iri("true"),
            DataValue::iri("_"),
            DataValue::iri("42"),
            DataValue::iri(""),
            DataValue::string(""),
            DataValue::string("plain"),
            DataValue::string("say \"hi\"\\\n\r\tend"),
            DataValue::string("42"),
            DataValue::language_string("hallo", "de").expect("the tag is valid"),
            DataValue::language_string("colour", "en-gb").expect("the tag is valid"),
            DataValue::boolean(true),
            DataValue::boolean(false),
            DataValue::integer(0),
            DataValue::integer(-42),
            DataValue::decimal_from_lexical("123456789012345678901234567890").expect("valid"),
            DataValue::decimal_from_lexical("-1.50").expect("valid"),
            DataValue::double(2.5),
            DataValue::double(-1e300),
            DataValue::double(f64::INFINITY),
            DataValue::double(f64::NAN),
            DataValue::float(2.5),
            DataValue::float(f32::NEG_INFINITY),
            typed("dateTime", "2024-02-29T12:00:00Z"),
            typed("date", "2024-02-29"),
            typed("time", "12:00:00"),
            typed("duration", "P1Y2M3DT4H"),
            DataValue::other("http://example.org/datatype", "lexical \"form\"")
                .expect("the datatype is not native"),
            DataValue::null(0),
            tuple(vec![]),
            tuple(vec![DataValue::integer(1)]),
            tuple(vec![
                DataValue::integer(1),
                DataValue::string("a"),
                DataValue::null(0),
            ]),
            DataValue::tuple(TupleValue::labeled(
                "point",
                vec![DataValue::integer(1), tuple(vec![DataValue::iri("x")])],
            )),
            DataValue::tuple(TupleValue::labeled("http://example.org/point", vec![])),
            DataValue::map(
                MapValue::new([
                    (DataValue::iri("a"), DataValue::integer(1)),
                    (
                        DataValue::string("b"),
                        tuple(vec![DataValue::boolean(true)]),
                    ),
                ])
                .expect("the keys differ"),
            ),
            DataValue::map(MapValue::labeled("box", []).expect("the map is empty")),
        ];

        for value in values {
            let written = value.to_string();
            assert_eq!(read(&written), Some(value), "{written}");
        }
    }

    /// The spellings of a program's constants are read, with whitespace around their parts.
    #[test]
    fn the_spellings_of_constants_are_read() {
        let cases = [
            ("  alice\t", DataValue::iri("alice")),
            ("<alice>", DataValue::iri("alice")),
            (
                r"<http://e.org/caf\u00E9>",
                DataValue::iri("http://e.org/café"),
            ),
            ("+5", DataValue::integer(5)),
            ("- 5", DataValue::integer(-5)),
            ("007", DataValue::integer(7)),
            ("-0x1F", DataValue::integer(-31)),
            ("1.50", DataValue::decimal(15, -1).expect("valid")),
            ("1E3", DataValue::double(1000.0)),
            ("2.5f", DataValue::float(2.5)),
            ("2D", DataValue::double(2.0)),
            ("INF", DataValue::iri("INF")),
            (r#""a\u00E9\d""#, DataValue::string(r"aé\d")),
            (
                "\"\"\"two \"quoted\"\r\nlines\"\"\"",
                DataValue::string("two \"quoted\"\nlines"),
            ),
            (
                r#""Hallo"@DE"#,
                DataValue::language_string("Hallo", "de").expect("valid"),
            ),
            (
                r#""5" ^^ <http://www.w3.org/2001/XMLSchema#integer>"#,
                DataValue::integer(5),
            ),
            (
                r#""5"^^meters"#,
                DataValue::other("meters", "5").expect("valid"),
            ),
            ("( 1 , (2,) , ( 3 ) )", {
                let inner = DataValue::tuple(TupleValue::new(vec![DataValue::integer(2)]));
                DataValue::tuple(TupleValue::new(vec![
                    DataValue::integer(1),
                    inner,
                    DataValue::integer(3),
                ]))
            }),
            ("(alice)", DataValue::iri("alice")),
            (
                "point (1)",
                DataValue::tuple(TupleValue::labeled("point", vec![DataValue::integer(1)])),
            ),
            (
                "<http://e.org/p>{ a = 1 }",
                DataValue::map(
                    MapValue::labeled(
                        "http://e.org/p",
                        [(DataValue::iri("a"), DataValue::integer(1))],
                    )
                    .expect("one key"),
                ),
            ),
            ("true(1)", {
                DataValue::tuple(TupleValue::labeled("true", vec![DataValue::integer(1)]))
            }),
            (
                "<strlen>(1)",
                DataValue::tuple(TupleValue::labeled("strlen", vec![DataValue::integer(1)])),
            ),
        ];

        for (text, value) in cases {
            assert_eq!(read(text), Some(value), "{text}");
        }
    }

    /// A text that is not one constant is not read.
    #[test]
    fn other_texts_are_not_read() {
        let texts = [
            "",
            " ",
            "http://example.org/a",
            "Alice Smith",
            "alice bob",
            "ex:alice",
            ":alice",
            "lib::alice",
            "_",
            "?x",
            "!x",
            "#count(1)",
            "f\"text\"",
            "1 + 2",
            "--5",
            "-alice",
            "5.",
            ".5",
            "1e",
            "0x",
            "<a b>",
            "<a",
            "<a\\u0000b>",
            "\"open",
            "\"\"\"open\"\"",
            "\"a\"@",
            "\"a\" @en",
            "\"a\"@en^^<b>",
            "\"a\"^<b>",
            "\"a\"^^",
            "\"a\"^^ex:b",
            "\"five\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "_:",
            "_:a b",
            "(1, 2",
            "(1, 2,)",
            "(,)",
            "(1 2)",
            "point(1,)",
            "{a = 1,}",
            "{a = 1, a = 2}",
            "{a}",
            "{a = }",
            "label {a = 1}",
            // The bare name of a builtin labels nothing, in any case.
            "STRLEN(\"abc\")",
            "strlen (\"abc\")",
            "Year{a = 1}",
            "(1, sum(2, 3))",
            "alice,",
            "alice)",
            "% comment",
            "alice % comment",
        ];

        for text in texts {
            assert_eq!(read(text), None, "{text}");
        }
    }

    /// A label names one null wherever it appears in a text, as `null` decides,
    /// and the labels are passed in the order of the text.
    #[test]
    fn nulls_are_resolved_by_their_labels() {
        assert_eq!(read("_:b"), Some(DataValue::null(0)));
        assert_eq!(
            read("(_:x, _:y.1, _:x)"),
            Some(DataValue::tuple(TupleValue::new(vec![
                DataValue::null(0),
                DataValue::null(1),
                DataValue::null(0),
            ])))
        );
    }

    /// An IRI or a string that is written as its text borrows it, and any other is owned.
    #[test]
    fn plain_text_is_borrowed() {
        let read = |text| ReadValue::from_term_syntax(text, &mut |_| DataValue::null(0));

        assert_eq!(read(" alice "), Some(ReadValue::Iri("alice")));
        assert_eq!(read("<urn:a>"), Some(ReadValue::Iri("urn:a")));
        assert_eq!(read(r#""a b""#), Some(ReadValue::String("a b")));
        assert_eq!(
            read(r#""a\tb""#),
            Some(ReadValue::Value(DataValue::string("a\tb")))
        );
        assert_eq!(
            read(r"<urn:\u0041>"),
            Some(ReadValue::Value(DataValue::iri("urn:A")))
        );
    }

    /// Structures nested beyond the limit are not read, and deep ones up to it are.
    #[test]
    fn nesting_is_bounded() {
        let nested = |depth: usize| format!("{}1{}", "(".repeat(depth), ",)".repeat(depth));

        assert!(read(&nested(MAX_NESTING)).is_some());
        assert_eq!(read(&nested(MAX_NESTING + 1)), None);
    }
}
