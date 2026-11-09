//! JSON: reading documents as node/edge triples, and writing rows as arrays.
//!
//! A document is read as a table of triples `(node, key, value)`,
//! with one node for every JSON value in it.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    io::Read,
};

use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use nemo_physical::{
    diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext,
    storage::StorageValue, value::DataValue,
};

use crate::{
    error::IoError,
    load::{Loaded, Reader},
    selection::RowSelection,
    stream::compression::Compression,
    written::{Writer, Written},
};

/// A reader of JSON documents, which decodes a document into node/edge triples.
///
/// Every JSON value at any depth, scalar, array or object, is a node with an integer id.
/// The root is `0`, and the other ids are unique, but which node gets which id is unspecified.
/// - Every node has a triple `(id, <type>, "…")` that names its kind:
///   `null`, `bool`, `number`, `string`, `array` or `object`.
/// - A scalar other than `null` has a triple `(id, <value>, v)`.
/// - An array has an edge to each element, with the element's integer index as the key.
/// - An object has an edge to each member, with the member's key as a plain string.
///
/// The keys `<type>` and `<value>` are IRIs,
/// so they never collide with a JSON key such as `type`, which is read as a plain string.
/// An integer is read exactly, even beyond 64 bits,
/// and a number with a fraction or an exponent is read as a double.
///
/// # Example
///
/// ```
/// use nemo_io::{json::JsonReader, load::Reader};
/// use nemo_physical::{dictionary::Dictionary, function::EvaluationContext};
///
/// let mut dictionary = Dictionary::new();
/// let document = r#"{"name": "Alice", "age": 42}"#;
/// let loaded = JsonReader::new()
///     .read(document.as_bytes(), &mut dictionary, EvaluationContext::now())
///     .expect("the document reads");
///
/// // The object has an edge to each of its two members, and each member has a type
/// // and a value triple: 2 + 2 * 2 triples, plus the object's own type triple.
/// assert_eq!(loaded.rows.len(), 7);
/// ```
#[derive(Debug, Clone, Default)]
pub struct JsonReader {
    /// The columns delivered and the rows admitted.
    selection: RowSelection,
}

impl JsonReader {
    /// Creates a reader that delivers every triple with all its columns.
    pub fn new() -> Self {
        Self {
            selection: RowSelection::default(),
        }
    }

    /// Creates a reader that delivers the columns and rows of a selection.
    pub fn with_selection(selection: RowSelection) -> Self {
        Self { selection }
    }
}

impl Reader for JsonReader {
    fn compression(&self) -> Compression {
        Compression::Auto
    }

    /// Reads a whole document, interning triples as they are produced.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream fails or the document is not valid JSON.
    /// A syntax error fails the resource as a whole.
    fn read(
        &self,
        mut source: impl Read,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        let mut document = Vec::new();
        source
            .read_to_end(&mut document)
            .map_err(IoError::stream_unreadable)?;
        let document = Node::parse(&document).map_err(|error| {
            IoError::NotJson {
                reason: error.to_string(),
            }
            .diagnostic()
        })?;

        let mut triples = Triples::new(&self.selection, context);
        triples.walk(document, dictionary);
        let loaded = triples.loaded;
        tracing::debug!(
            rows = loaded.rows.len(),
            excluded = loaded.excluded,
            errors = ?loaded.errors,
            "read a JSON document"
        );
        Ok(loaded)
    }
}

/// The triples of a document's nodes, loaded as they are produced.
struct Triples<'a> {
    /// The selection that the reader applies to each triple.
    selection: &'a RowSelection,
    /// The rows loaded so far.
    loaded: Loaded,
    /// The delivered values of the current triple.
    kept: Vec<DataValue>,
    /// A scratch buffer for the encoded values of the current triple.
    encoded: Vec<StorageValue>,
}

impl<'a> Triples<'a> {
    /// Creates an empty collection of triples under a selection.
    ///
    /// `context` is the evaluation context of the selection's condition.
    fn new(selection: &'a RowSelection, context: EvaluationContext) -> Self {
        Self {
            selection,
            loaded: Loaded::new(selection.arity(3), context),
            kept: Vec::with_capacity(3),
            encoded: Vec::with_capacity(3),
        }
    }

    /// Loads one triple.
    ///
    /// A triple with a value that the dictionary cannot hold, such as a very long string,
    /// is counted as malformed.
    fn push(&mut self, dictionary: &mut Dictionary, triple: [&DataValue; 3]) {
        self.kept.clear();
        self.kept.extend(
            triple
                .into_iter()
                .enumerate()
                .filter(|(position, _)| self.selection.keeps(*position))
                .map(|(_, value)| value.clone()),
        );
        self.loaded
            .take(&self.kept, self.selection, dictionary, &mut self.encoded, 0);
    }

    /// Loads the triples of a document, breadth first.
    ///
    /// For each node, it loads the node's type, its value if it is a scalar other than `null`,
    /// and an edge to each child, which gets the next free id.
    fn walk(&mut self, document: Node, dictionary: &mut Dictionary) {
        let key_type = DataValue::iri("type");
        let key_value = DataValue::iri("value");
        let tag = |kind: &str| DataValue::string(kind);
        let mut next_id = 1u64;
        let mut pending: VecDeque<(u64, Node)> = VecDeque::new();
        pending.push_back((0, document));
        while let Some((id, node)) = pending.pop_front() {
            let id_value = DataValue::integer(i64::try_from(id).expect("ids stay in range"));
            let mut child = |pending: &mut VecDeque<(u64, Node)>, node: Node| {
                let child = next_id;
                next_id += 1;
                pending.push_back((child, node));
                DataValue::integer(i64::try_from(child).expect("ids stay in range"))
            };
            match node {
                // A null has a type but no value.
                Node::Null => self.push(dictionary, [&id_value, &key_type, &tag("null")]),
                Node::Bool(value) => {
                    self.push(dictionary, [&id_value, &key_type, &tag("bool")]);
                    self.push(
                        dictionary,
                        [&id_value, &key_value, &DataValue::boolean(value)],
                    );
                }
                Node::Number(value) => {
                    self.push(dictionary, [&id_value, &key_type, &tag("number")]);
                    self.push(dictionary, [&id_value, &key_value, &value.value()]);
                }
                Node::String(value) => {
                    self.push(dictionary, [&id_value, &key_type, &tag("string")]);
                    self.push(
                        dictionary,
                        [&id_value, &key_value, &DataValue::string(value)],
                    );
                }
                Node::Array(elements) => {
                    self.push(dictionary, [&id_value, &key_type, &tag("array")]);
                    for (index, element) in elements.into_iter().enumerate() {
                        let index = DataValue::integer(
                            i64::try_from(index).expect("indices stay in range"),
                        );
                        let child = child(&mut pending, element);
                        self.push(dictionary, [&id_value, &index, &child]);
                    }
                }
                Node::Object(members) => {
                    self.push(dictionary, [&id_value, &key_type, &tag("object")]);
                    for (key, member) in members {
                        let child = child(&mut pending, member);
                        self.push(dictionary, [&id_value, &DataValue::string(key), &child]);
                    }
                }
            }
        }
    }
}

/// A parsed JSON value, which is a node of a document.
///
/// `serde_json`'s own tree converts every number as it parses,
/// so an integer beyond 64 bits would arrive as a double.
/// The reader builds this tree instead, which keeps such integers as written.
#[derive(Debug)]
enum Node {
    /// `null`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A number.
    Number(Number),
    /// A string.
    String(String),
    /// An array, with its elements in order.
    Array(Vec<Node>),
    /// An object, with its members sorted by key, which is the order in which they get their
    /// ids.
    ///
    /// If a key repeats, its last value counts.
    Object(BTreeMap<String, Node>),
}

impl Node {
    /// Parses a document.
    fn parse(document: &[u8]) -> Result<Self, serde_json::Error> {
        let mut literals = NumberLiterals::new(document);
        let mut deserializer = serde_json::Deserializer::from_slice(document);
        let node = NodeSeed {
            literals: &mut literals,
        }
        .deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(node)
    }
}

/// A parsed JSON number.
#[derive(Debug)]
enum Number {
    /// An integer within 64 bits.
    Integer(i64),
    /// An integer beyond 64 bits, as written.
    Big(String),
    /// A number with a fraction or an exponent.
    Double(f64),
}

impl Number {
    /// Returns the number's value:
    /// the exact integer, or a double for a number with a fraction or an exponent.
    fn value(&self) -> DataValue {
        match self {
            Number::Integer(value) => DataValue::integer(*value),
            Number::Big(lexical) => {
                DataValue::decimal_from_lexical(lexical).expect("an integer lexical is well-formed")
            }
            Number::Double(value) => DataValue::double(*value),
        }
    }
}

/// The number literals of a document, in document order, found by a scan that skips strings.
///
/// The parser hands a number over already converted.
/// The text of a number that does not fit the parser's integer types is taken from this scan
/// when the parser reaches the number, since the parser reads the document in order.
struct NumberLiterals<'a> {
    /// The document.
    document: &'a [u8],
    /// Where the scan continues.
    position: usize,
}

impl<'a> NumberLiterals<'a> {
    /// Creates a scan over a document, positioned at its start.
    fn new(document: &'a [u8]) -> Self {
        Self {
            document,
            position: 0,
        }
    }

    /// Skips the string that opens at the current quote, including its escapes.
    fn skip_string(&mut self) {
        self.position += 1;
        while let Some(&byte) = self.document.get(self.position) {
            self.position += match byte {
                b'\\' => 2,
                b'"' => break,
                _ => 1,
            };
        }
        self.position += 1;
    }
}

impl<'a> Iterator for NumberLiterals<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        while let Some(&byte) = self.document.get(self.position) {
            match byte {
                b'"' => self.skip_string(),
                b'-' | b'0'..=b'9' => {
                    let start = self.position;
                    while self.document.get(self.position).is_some_and(|byte| {
                        matches!(byte, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                    }) {
                        self.position += 1;
                    }
                    // A number literal is ASCII.
                    return std::str::from_utf8(&self.document[start..self.position]).ok();
                }
                _ => self.position += 1,
            }
        }
        None
    }
}

/// The deserializer of one node, which takes the text of a number beyond the
/// parser's integer types from the document's literals.
struct NodeSeed<'a, 'b> {
    /// The document's number literals, positioned at the next one the
    /// parser will hand over.
    literals: &'b mut NumberLiterals<'a>,
}

impl<'de> DeserializeSeed<'de> for NodeSeed<'_, '_> {
    type Value = Node;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Node, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for NodeSeed<'_, '_> {
    type Value = Node;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Node, E> {
        Ok(Node::Null)
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Node, E> {
        Ok(Node::Bool(value))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Node, E> {
        self.literals.next();
        Ok(Node::Number(Number::Integer(value)))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Node, E> {
        self.literals.next();
        Ok(Node::Number(match i64::try_from(value) {
            Ok(value) => Number::Integer(value),
            Err(_) => Number::Big(value.to_string()),
        }))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Node, E> {
        // An integer literal that the parser can only hand over as a double
        // keeps its text if the text denotes this number. The check guards
        // against the scan getting out of step with the parse.
        // The parser does not round a long literal correctly,
        // so the two may differ in the last places.
        let literal = self.literals.next().filter(|literal| {
            !literal
                .bytes()
                .any(|byte| matches!(byte, b'.' | b'e' | b'E'))
                && literal
                    .parse::<f64>()
                    .is_ok_and(|parsed| (parsed - value).abs() <= 4.0 * f64::EPSILON * parsed.abs())
        });
        Ok(Node::Number(match literal {
            Some(literal) => Number::Big(literal.to_owned()),
            None => Number::Double(value),
        }))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Node, E> {
        Ok(Node::String(value.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Node, E> {
        Ok(Node::String(value))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut elements: A) -> Result<Node, A::Error> {
        let mut nodes = Vec::new();
        while let Some(node) = elements.next_element_seed(NodeSeed {
            literals: &mut *self.literals,
        })? {
            nodes.push(node);
        }
        Ok(Node::Array(nodes))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut members: A) -> Result<Node, A::Error> {
        let mut nodes = BTreeMap::new();
        while let Some(key) = members.next_key::<String>()? {
            let node = members.next_value_seed(NodeSeed {
                literals: &mut *self.literals,
            })?;
            nodes.insert(key, node);
        }
        Ok(Node::Object(nodes))
    }
}

/// A writer of rows as a JSON array of row arrays.
///
/// A value with a matching JSON type is written as that type:
/// - integers within 64 bits and finite doubles as numbers,
/// - booleans as booleans,
/// - plain strings as strings.
///
/// Every other value is written as a string,
/// the text that [any_cell](crate::format::ValueFormat::any_cell) gives for it.
/// The output is meant for other programs, and is not read back as rows:
/// the [JsonReader] reads any document as node/edge triples.
#[derive(Debug, Clone, Default)]
pub struct JsonWriter {
    /// The stream compression.
    pub compression: Compression,
}

impl JsonWriter {
    /// Creates a writer with auto-detected compression.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Writer for JsonWriter {
    fn compression(&self) -> Compression {
        self.compression
    }

    /// Writes rows to a target.
    ///
    /// # Errors
    ///
    /// Returns an error if the target cannot be written.
    fn write<'a>(
        &self,
        target: impl std::io::Write,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        let mut target = target;
        let mut written = Written::default();
        target.write_all(b"[").map_err(IoError::stream_unwritable)?;
        for row in rows {
            let cells: Vec<serde_json::Value> = row.iter().map(cell).collect();
            let separator: &[u8] = if written.rows == 0 { b"\n" } else { b",\n" };
            target
                .write_all(separator)
                .map_err(IoError::stream_unwritable)?;
            serde_json::to_writer(&mut target, &cells).map_err(IoError::stream_unwritable)?;
            written.rows += 1;
        }
        target
            .write_all(b"\n]\n")
            .map_err(IoError::stream_unwritable)?;
        target.flush().map_err(IoError::stream_unwritable)?;
        Ok(written)
    }
}

/// Returns the JSON form of one value, as [JsonWriter] describes it.
fn cell(value: &DataValue) -> serde_json::Value {
    use nemo_physical::value::{NumberRef, ValueRef};
    let value = value.as_ref();
    if let ValueRef::Number(number) = value
        && let Some(integer) = number.to_i64()
    {
        return serde_json::Value::from(integer);
    }
    match value {
        ValueRef::Number(NumberRef::Double(value)) if value.is_finite() => {
            serde_json::Value::from(value.get())
        }
        ValueRef::Boolean(value) => serde_json::Value::from(value),
        ValueRef::String(text) => serde_json::Value::from(text),
        other => serde_json::Value::from(crate::format::ValueFormat::any_cell(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns the `<value>` of every scalar node of a document, by node id.
    fn values(text: &str) -> BTreeMap<i64, DataValue> {
        let mut dictionary = Dictionary::new();
        let loaded = JsonReader::new()
            .read(text.as_bytes(), &mut dictionary, EvaluationContext::now())
            .expect("the document reads");
        let key_value = DataValue::iri("value");
        (0..loaded.rows.len())
            .map(|index| loaded.rows.row(index))
            .filter(|row| dictionary.decode(row[1]).expect("decodes") == key_value.as_ref())
            .map(|row| {
                let id = match dictionary.decode(row[0]).expect("decodes") {
                    nemo_physical::value::ValueRef::Number(id) => id.to_i64().expect("small"),
                    other => panic!("ids are integers, not {other:?}"),
                };
                (id, dictionary.decode(row[2]).expect("decodes").to_owned())
            })
            .collect()
    }

    /// A document's scalars are read as values of their JSON types,
    /// and a document cut short is an error.
    #[test]
    fn documents_read_as_typed_scalars() {
        let text = r#"{"name": "alice", "age": 42, "half": 0.5, "on": true, "none": null}"#;
        let values = values(text);
        // The members' ids follow the key order: age, half, name, none, on.
        assert_eq!(values[&1], DataValue::integer(42));
        assert_eq!(values[&2], DataValue::double(0.5));
        assert_eq!(values[&3], DataValue::string("alice"));
        assert!(!values.contains_key(&4), "a null has no value");
        assert_eq!(values[&5], DataValue::boolean(true));
        assert!(
            JsonReader::new()
                .read(
                    "{\"a\": ".as_bytes(),
                    &mut Dictionary::new(),
                    EvaluationContext::now()
                )
                .is_err()
        );
    }

    /// An integer beyond 64 bits is read exactly,
    /// and a number with a fraction or an exponent as a double.
    #[test]
    fn integers_beyond_sixty_four_bits_stay_exact() {
        let big = |lexical: &str| DataValue::decimal_from_lexical(lexical).expect("well-formed");
        let text = r#"[-9223372036854775809, 9223372036854775809, 18446744073709551617,
            -9223372036854775808, 1e19, -0, 2.5, "1e400", {"k\"e\\y": 100000000000000000000}]"#;
        let values = values(text);
        assert_eq!(values[&1], big("-9223372036854775809"));
        assert_eq!(values[&2], big("9223372036854775809"));
        assert_eq!(values[&3], big("18446744073709551617"));
        assert_eq!(values[&4], DataValue::integer(i64::MIN));
        // Exponent and fraction forms are doubles, whatever they denote.
        assert_eq!(values[&5], DataValue::double(1e19));
        assert_eq!(values[&6], DataValue::integer(0));
        assert_eq!(values[&7], DataValue::double(2.5));
        assert_eq!(values[&8], DataValue::string("1e400"));
        // After an escaped string, the scan is still in step with the parse.
        assert_eq!(values[&10], big("100000000000000000000"));
    }

    /// A long integer is read exactly,
    /// even though the parser's double for it is off in the last places.
    #[test]
    fn long_integers_stay_exact_whatever_the_parser_rounds() {
        // The parser converts these to doubles a few units off the correctly rounded value.
        for lexical in [
            "123456789012345678901234567890123456789",
            "98765432109876543210987654321098765432109876543210",
            "-31415926535897932384626433832795028841971693993751",
        ] {
            let values = values(&format!("[{lexical}]"));
            assert_eq!(
                values[&1],
                DataValue::decimal_from_lexical(lexical).expect("well-formed"),
                "{lexical}"
            );
        }
    }
}
