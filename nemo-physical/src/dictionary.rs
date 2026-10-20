//! The dictionary: the stores of values that do not fit inline.
//!
//! A value whose payload encoding cannot be stored in a trie directly
//! is represented by an entry in one of the stores.
//! A store numbers its entries densely in insertion order; that number is the entry's *index*.
//! Usually the [tag](Tag) of the storage value identifies the store and the payload is the index.
//! The *terms*, which are IRIs, strings, language-tagged strings, other literals and nulls,
//! share the one [Term](Tag::Term) tag instead, since they meet in the same columns of real data.
//! Their payload is a *term id*, which packs the term's *lane*, the kind of term, with its index.
//!
//! Two levels sit beneath the [Dictionary]:
//! the domain stores, which know how each kind of value splits into stored bytes,
//! and the store mechanisms, arenas, interning pools and blob encodings,
//! that keep those bytes.
//!
//! Reads borrow from the stores and allocate nothing:
//! a structured value is read in place, each element decoded when it is read.
//! Reads may share the dictionary across threads, writes take it exclusively.

mod domain;
mod store;
mod stored_structure;
mod term_id;

pub(crate) use self::{
    stored_structure::StoredStructure,
    term_id::{TermId, TermLane},
};

use std::fmt;

use crate::{
    dictionary::{
        domain::{
            IriStore, LanguageStringStore, OtherStore, StringStore, StructureStore, WideDateStore,
            WideDateTimeStore, WideDecimalStore, WideDurationStore, WideTimeStore,
        },
        store::{
            Arena,
            content::{self, Blobs},
        },
    },
    storage::{StorageValue, Tag},
    value::{
        DecimalRef, LanguageStringRef, MapRef, NumberRef, OtherLiteralRef, TupleRef, ValueRef,
        number::decimal,
    },
};

/// The dictionary of one database instance.
///
/// [encode](Self::encode) and [decode](Self::decode) translate between values and [StorageValue]s,
/// choosing the inline payload whenever one exists and a store entry otherwise,
/// so that no value has two representations.
/// [probe](Self::probe) finds the representation a value already has.
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     dictionary::Dictionary,
///     storage::Tag,
///     value::{DataValue, ValueRef},
/// };
///
/// let mut dictionary = Dictionary::new();
///
/// // An integer is stored inline, and an IRI as an entry of its store.
/// let five = dictionary.encode(DataValue::integer(5).as_ref()).expect("representable");
/// assert_eq!(five.tag, Tag::Int);
/// let iri = DataValue::iri("http://example.org/a");
/// let entry = dictionary.encode(iri.as_ref()).expect("representable");
/// assert_eq!(dictionary.decode(entry), Some(iri.as_ref()));
///
/// // Encoding a value again finds its entry, and a probe never adds one.
/// assert_eq!(dictionary.encode(iri.as_ref()), Some(entry));
/// let other = DataValue::iri("http://example.org/b");
/// assert_eq!(dictionary.probe(other.as_ref()), None);
///
/// // A null minted here is distinct from every other value.
/// let null = dictionary.fresh_null();
/// let encoded = dictionary.encode(ValueRef::Null(null)).expect("minted here");
/// assert_eq!(dictionary.decode(encoded), Some(ValueRef::Null(null)));
/// ```
pub struct Dictionary {
    /// The IRIs.
    iris: IriStore,
    /// The plain strings.
    strings: StringStore,
    /// The language-tagged strings.
    language_strings: LanguageStringStore,
    /// The literals of unsupported datatypes.
    others: OtherStore,
    /// The number of nulls minted so far; a null has no stored content.
    nulls: u64,
    /// The integers beyond the 64-bit range.
    wide_integers: WideDecimalStore,
    /// The decimals beyond the compact range.
    wide_decimals: WideDecimalStore,
    /// The dateTimes beyond the compact range.
    wide_datetimes: WideDateTimeStore,
    /// The dates beyond the compact range.
    wide_dates: WideDateStore,
    /// The times beyond the compact range.
    wide_times: WideTimeStore,
    /// The durations beyond the compact range.
    wide_durations: WideDurationStore,
    /// The tuples.
    tuples: StructureStore,
    /// The maps.
    maps: StructureStore,
}

// Reads share the dictionary across the worker threads.
const _: () = {
    const fn shared<T: Send + Sync>() {}
    shared::<Dictionary>();
};

impl Dictionary {
    /// The longest byte string a store holds:
    /// a value with a longer part, such as the text of a term, has no representation.
    pub const LENGTH_LIMIT: usize = Arena::LENGTH_LIMIT;

    /// The most elements a structured value holds:
    /// its content blob is a stored byte string.
    pub const ELEMENT_LIMIT: usize = Self::LENGTH_LIMIT / content::ELEMENT_SIZE;

    /// Creates an empty dictionary.
    pub fn new() -> Self {
        Self {
            iris: IriStore::new(),
            strings: StringStore::new(),
            language_strings: LanguageStringStore::new(),
            others: OtherStore::new(),
            nulls: 0,
            wide_integers: WideDecimalStore::new(),
            wide_decimals: WideDecimalStore::new(),
            wide_datetimes: WideDateTimeStore::new(),
            wide_dates: WideDateStore::new(),
            wide_times: WideTimeStore::new(),
            wide_durations: WideDurationStore::new(),
            tuples: StructureStore::new(),
            maps: StructureStore::new(),
        }
    }

    /// Encodes a value into its storage representation:
    /// the inline payload if one exists,
    /// and the entry of its store otherwise, interned if it was not before.
    ///
    /// Returns `None` for a value that has no representation:
    /// a null this dictionary did not mint,
    /// a value with a part longer than [LENGTH_LIMIT](Self::LENGTH_LIMIT),
    /// or a structure of more than [ELEMENT_LIMIT](Self::ELEMENT_LIMIT) elements.
    pub fn encode(&mut self, value: ValueRef<'_>) -> Option<StorageValue> {
        if let Some(inline) = StorageValue::from_value(value) {
            return Some(inline);
        }

        let stored = match value {
            ValueRef::Iri(iri) => Self::term(TermLane::Iri, self.iris.intern(iri)?),
            ValueRef::String(string) => Self::term(TermLane::String, self.strings.intern(string)?),
            ValueRef::LanguageString(value) => Self::term(
                TermLane::LanguageString,
                self.language_strings
                    .intern(value.text(), value.language())?,
            ),
            ValueRef::Other(literal) => Self::term(
                TermLane::Other,
                self.others.intern(literal.datatype(), literal.lexical())?,
            ),
            ValueRef::Null(id) => self.known_null(id)?,
            ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(wide))))
                if wide.is_integral() =>
            {
                StorageValue::new(Tag::WideInt, self.wide_integers.intern(wide)?)
            }
            ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(wide)))) => {
                StorageValue::new(Tag::WideDecimal, self.wide_decimals.intern(wide)?)
            }
            ValueRef::DateTime(value) => {
                StorageValue::new(Tag::WideDateTime, self.wide_datetimes.intern(value)?)
            }
            ValueRef::Date(value) => {
                StorageValue::new(Tag::WideDate, self.wide_dates.intern(value)?)
            }
            ValueRef::Time(value) => {
                StorageValue::new(Tag::WideTime, self.wide_times.intern(value)?)
            }
            ValueRef::Duration(value) => {
                StorageValue::new(Tag::WideDuration, self.wide_durations.intern(value)?)
            }
            ValueRef::Tuple(tuple) => {
                let Blobs { type_blob, content } =
                    Self::blobs(tuple.label(), tuple.elements(), |element| {
                        self.encode(element)
                    })?;
                StorageValue::new(Tag::Tuple, self.tuples.intern(&type_blob, &content)?)
            }
            ValueRef::Map(map) => {
                let elements = map.pairs().flat_map(|(key, value)| [key, value]);
                let Blobs { type_blob, content } =
                    Self::blobs(map.label(), elements, |element| self.encode(element))?;
                StorageValue::new(Tag::Map, self.maps.intern(&type_blob, &content)?)
            }
            ValueRef::Boolean(_)
            | ValueRef::Number(
                NumberRef::Decimal(DecimalRef(
                    decimal::ReprRef::Int(_) | decimal::ReprRef::Compact(_),
                ))
                | NumberRef::Float(_)
                | NumberRef::Double(_),
            ) => unreachable!("an inline value was encoded above"),
        };

        Some(stored)
    }

    /// Returns the storage representation a value already has,
    /// the inline payload or its existing store entry,
    /// or `None` for a value that occurs in no stored table.
    pub fn probe(&self, value: ValueRef<'_>) -> Option<StorageValue> {
        if let Some(inline) = StorageValue::from_value(value) {
            return Some(inline);
        }
        if let Some(entry) = self.entry_of(value) {
            return Some(entry);
        }

        let stored = match value {
            ValueRef::Iri(iri) => Self::term(TermLane::Iri, self.iris.find(iri)?),
            ValueRef::String(string) => Self::term(TermLane::String, self.strings.find(string)?),
            ValueRef::LanguageString(value) => Self::term(
                TermLane::LanguageString,
                self.language_strings.find(value.text(), value.language())?,
            ),
            ValueRef::Other(literal) => Self::term(
                TermLane::Other,
                self.others.find(literal.datatype(), literal.lexical())?,
            ),
            ValueRef::Null(id) => self.known_null(id)?,
            ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(wide))))
                if wide.is_integral() =>
            {
                StorageValue::new(Tag::WideInt, self.wide_integers.find(wide)?)
            }
            ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(wide)))) => {
                StorageValue::new(Tag::WideDecimal, self.wide_decimals.find(wide)?)
            }
            ValueRef::DateTime(value) => {
                StorageValue::new(Tag::WideDateTime, self.wide_datetimes.find(value)?)
            }
            ValueRef::Date(value) => StorageValue::new(Tag::WideDate, self.wide_dates.find(value)?),
            ValueRef::Time(value) => StorageValue::new(Tag::WideTime, self.wide_times.find(value)?),
            ValueRef::Duration(value) => {
                StorageValue::new(Tag::WideDuration, self.wide_durations.find(value)?)
            }
            ValueRef::Tuple(tuple) => {
                let Blobs { type_blob, content } =
                    Self::blobs(tuple.label(), tuple.elements(), |element| {
                        self.probe(element)
                    })?;
                StorageValue::new(Tag::Tuple, self.tuples.find(&type_blob, &content)?)
            }
            ValueRef::Map(map) => {
                let elements = map.pairs().flat_map(|(key, value)| [key, value]);
                let Blobs { type_blob, content } =
                    Self::blobs(map.label(), elements, |element| self.probe(element))?;
                StorageValue::new(Tag::Map, self.maps.find(&type_blob, &content)?)
            }
            ValueRef::Boolean(_)
            | ValueRef::Number(
                NumberRef::Decimal(DecimalRef(
                    decimal::ReprRef::Int(_) | decimal::ReprRef::Compact(_),
                ))
                | NumberRef::Float(_)
                | NumberRef::Double(_),
            ) => unreachable!("an inline value was encoded above"),
        };

        Some(stored)
    }

    /// Returns the storage value of a stored tuple or map read from this dictionary,
    /// which its view knows without a lookup, or `None` for any other value.
    pub(crate) fn entry_of(&self, value: ValueRef<'_>) -> Option<StorageValue> {
        match value {
            ValueRef::Tuple(tuple) => tuple
                .index_in(self)
                .map(|index| StorageValue::new(Tag::Tuple, index)),
            ValueRef::Map(map) => map
                .index_in(self)
                .map(|index| StorageValue::new(Tag::Map, index)),
            _ => None,
        }
    }

    /// Decodes a storage value into the value it represents,
    /// borrowed from the dictionary if it is a store entry,
    /// or returns `None` for an entry this dictionary never produced.
    ///
    /// A stored tuple or map decodes to a view of its entry,
    /// which decodes an element when it is read.
    pub fn decode(&self, value: StorageValue) -> Option<ValueRef<'_>> {
        match value.tag {
            Tag::Term => self.term_value(TermId::from_raw(value.payload)),
            Tag::WideInt => self.wide_integers.get(value.payload).map(|wide| {
                ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(wide))))
            }),
            Tag::WideDecimal => self.wide_decimals.get(value.payload).map(|wide| {
                ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(wide))))
            }),
            Tag::WideDateTime => self
                .wide_datetimes
                .get(value.payload)
                .map(ValueRef::DateTime),
            Tag::WideDate => self.wide_dates.get(value.payload).map(ValueRef::Date),
            Tag::WideTime => self.wide_times.get(value.payload).map(ValueRef::Time),
            Tag::WideDuration => self
                .wide_durations
                .get(value.payload)
                .map(ValueRef::Duration),
            Tag::Tuple => (value.payload < self.tuples.len())
                .then(|| ValueRef::Tuple(TupleRef::stored(self, value.payload))),
            Tag::Map => (value.payload < self.maps.len())
                .then(|| ValueRef::Map(MapRef::stored(self, value.payload))),
            Tag::Bool
            | Tag::Int
            | Tag::Decimal
            | Tag::Float
            | Tag::Double
            | Tag::DateTime
            | Tag::Date
            | Tag::Time
            | Tag::Duration => value.to_value(),
        }
    }

    /// Mints a fresh null, distinct from every existing value,
    /// and returns its id, the one [ValueRef::Null] carries.
    pub fn fresh_null(&mut self) -> u64 {
        let index = self.nulls;
        self.nulls += 1;

        TermId::compose(TermLane::Null, index).raw()
    }

    /// Returns the number of entries in the store behind a tag, or zero for an inline tag.
    pub fn stored(&self, tag: Tag) -> u64 {
        match tag {
            Tag::Term => {
                self.iris.len()
                    + self.strings.len()
                    + self.language_strings.len()
                    + self.others.len()
                    + self.nulls
            }
            Tag::WideInt => self.wide_integers.len(),
            Tag::WideDecimal => self.wide_decimals.len(),
            Tag::WideDateTime => self.wide_datetimes.len(),
            Tag::WideDate => self.wide_dates.len(),
            Tag::WideTime => self.wide_times.len(),
            Tag::WideDuration => self.wide_durations.len(),
            Tag::Tuple => self.tuples.len(),
            Tag::Map => self.maps.len(),
            Tag::Bool
            | Tag::Int
            | Tag::Decimal
            | Tag::Float
            | Tag::Double
            | Tag::DateTime
            | Tag::Date
            | Tag::Time
            | Tag::Duration => 0,
        }
    }

    /// Returns the allocated memory of the dictionary, by store.
    pub fn memory(&self) -> DictionaryMemory {
        DictionaryMemory {
            iris: self.iris.memory_bytes(),
            strings: self.strings.memory_bytes(),
            language_strings: self.language_strings.memory_bytes(),
            others: self.others.memory_bytes(),
            wide_integers: self.wide_integers.memory_bytes(),
            wide_decimals: self.wide_decimals.memory_bytes(),
            wide_datetimes: self.wide_datetimes.memory_bytes(),
            wide_dates: self.wide_dates.memory_bytes(),
            wide_times: self.wide_times.memory_bytes(),
            wide_durations: self.wide_durations.memory_bytes(),
            tuples: self.tuples.memory_bytes(),
            maps: self.maps.memory_bytes(),
        }
    }

    /// Returns the storage value of the term with the given lane and index.
    fn term(lane: TermLane, index: u64) -> StorageValue {
        StorageValue::term(TermId::compose(lane, index).raw())
    }

    /// Returns the term with the given id, borrowed from its store,
    /// or `None` for an id this dictionary never produced.
    fn term_value(&self, id: TermId) -> Option<ValueRef<'_>> {
        let (lane, index) = id.decompose()?;
        match lane {
            TermLane::Iri => self.iris.get(index).map(ValueRef::Iri),
            TermLane::String => self.strings.get(index).map(ValueRef::String),
            TermLane::LanguageString => self.language_strings.get(index).map(|(text, language)| {
                ValueRef::LanguageString(LanguageStringRef::from_parts(text, language))
            }),
            TermLane::Other => self.others.get(index).map(|(datatype, lexical)| {
                ValueRef::Other(OtherLiteralRef::from_parts(datatype, lexical))
            }),
            TermLane::Null => (index < self.nulls).then_some(ValueRef::Null(id.raw())),
        }
    }

    /// Returns the storage value of a null this dictionary minted,
    /// or `None` for any other id.
    fn known_null(&self, id: u64) -> Option<StorageValue> {
        match TermId::from_raw(id).decompose() {
            Some((TermLane::Null, index)) if index < self.nulls => Some(StorageValue::term(id)),
            _ => None,
        }
    }

    /// Returns the type and content blobs of a structured value
    /// whose elements `represent` translates into storage values,
    /// or `None` for a structure of more than [ELEMENT_LIMIT](Self::ELEMENT_LIMIT) elements
    /// or with an element that has no representation.
    fn blobs<'v>(
        label: Option<&str>,
        elements: impl IntoIterator<Item = ValueRef<'v>>,
        represent: impl FnMut(ValueRef<'v>) -> Option<StorageValue>,
    ) -> Option<Blobs> {
        let elements: Vec<StorageValue> =
            elements.into_iter().map(represent).collect::<Option<_>>()?;

        (elements.len() <= Self::ELEMENT_LIMIT).then(|| Blobs::build(label, &elements))
    }

    /// Returns the stored tuple with the given index, read in place.
    ///
    /// # Panics
    ///
    /// Panics for an index the tuple store does not hold.
    pub(crate) fn stored_tuple(&self, index: u64) -> StoredStructure<'_> {
        let blobs = self.tuples.get(index).expect("a view reads a stored tuple");
        StoredStructure::new(self, blobs)
    }

    /// Returns the stored map with the given index, read in place.
    ///
    /// # Panics
    ///
    /// Panics for an index the map store does not hold.
    pub(crate) fn stored_map(&self, index: u64) -> StoredStructure<'_> {
        let blobs = self.maps.get(index).expect("a view reads a stored map");
        StoredStructure::new(self, blobs)
    }
}

impl Default for Dictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Dictionary {
    /// Writes the number of entries of each store.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dictionary")
            .field("iris", &self.iris.len())
            .field("strings", &self.strings.len())
            .field("language_strings", &self.language_strings.len())
            .field("others", &self.others.len())
            .field("nulls", &self.nulls)
            .field("wide_integers", &self.wide_integers.len())
            .field("wide_decimals", &self.wide_decimals.len())
            .field("wide_datetimes", &self.wide_datetimes.len())
            .field("wide_dates", &self.wide_dates.len())
            .field("wide_times", &self.wide_times.len())
            .field("wide_durations", &self.wide_durations.len())
            .field("tuples", &self.tuples.len())
            .field("maps", &self.maps.len())
            .finish()
    }
}

/// The allocated memory of a dictionary in bytes, by store.
///
/// The numbers cover what a store has allocated,
/// the arenas with their unused chunk tails, the lookup tables and the entry lists,
/// not just the payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DictionaryMemory {
    /// The bytes of the IRI store.
    pub iris: usize,
    /// The bytes of the plain string store.
    pub strings: usize,
    /// The bytes of the language-tagged string store.
    pub language_strings: usize,
    /// The bytes of the other-literal store.
    pub others: usize,
    /// The bytes of the wide integer store.
    pub wide_integers: usize,
    /// The bytes of the wide decimal store.
    pub wide_decimals: usize,
    /// The bytes of the wide dateTime store.
    pub wide_datetimes: usize,
    /// The bytes of the wide date store.
    pub wide_dates: usize,
    /// The bytes of the wide time store.
    pub wide_times: usize,
    /// The bytes of the wide duration store.
    pub wide_durations: usize,
    /// The bytes of the tuple store, excluding the materialized values.
    pub tuples: usize,
    /// The bytes of the map store, excluding the materialized values.
    pub maps: usize,
}

impl DictionaryMemory {
    /// Returns the total allocated bytes across all stores.
    pub fn total(&self) -> usize {
        self.iris
            + self.strings
            + self.language_strings
            + self.others
            + self.wide_integers
            + self.wide_decimals
            + self.wide_datetimes
            + self.wide_dates
            + self.wide_times
            + self.wide_durations
            + self.tuples
            + self.maps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::hash::{BuildHasher, RandomState};

    use crate::value::{DataValue, IriRef, MapValue, TupleValue, datatype::xsd, number::decimal};

    /// Asserts that a value round-trips through encode and decode.
    fn assert_round_trip(dictionary: &mut Dictionary, value: &DataValue) -> StorageValue {
        let encoded = dictionary
            .encode(value.as_ref())
            .expect("value must be representable");
        let decoded = dictionary
            .decode(encoded)
            .expect("encoded values must decode");
        assert_eq!(&decoded.to_owned(), value);
        encoded
    }

    #[test]
    fn values_round_trip_across_stores() {
        let mut dictionary = Dictionary::new();
        let values = [
            DataValue::iri("http://example.org/a"),
            DataValue::iri("urn:uuid:1234"),
            DataValue::string("hello"),
            DataValue::string(""),
            DataValue::language_string("hallo", "de").expect("tag is nonempty"),
            DataValue::decimal_from_lexical("123456789012345678901")
                .expect("lexical form is well-formed"),
            DataValue::decimal_from_lexical("123456789012345678901.5")
                .expect("lexical form is well-formed"),
            DataValue::datetime_from_lexical("-13798000000-01-01T00:00:00Z").expect("valid"),
            DataValue::datetime_from_lexical("2024-01-01T00:00:00.1234").expect("valid"),
            DataValue::other("http://example.org/unit", "5cm").expect("datatype is unsupported"),
            DataValue::integer(-42),
            DataValue::double(f64::NAN),
            DataValue::boolean(true),
        ];

        for value in &values {
            assert_round_trip(&mut dictionary, value);
        }
    }

    #[test]
    fn encoding_is_idempotent() {
        let mut dictionary = Dictionary::new();
        let owned = DataValue::iri("http://example.org/a");
        assert_eq!(dictionary.probe(owned.as_ref()), None);

        let first = dictionary
            .encode(owned.as_ref())
            .expect("IRIs are dictionary values");
        let second = dictionary
            .encode(ValueRef::Iri(IriRef::new("http://example.org/a")))
            .expect("IRIs are dictionary values");

        assert_eq!(first, second);
        assert_eq!(first.tag, Tag::Term);
        assert_eq!(dictionary.stored(Tag::Term), 1);
        assert_eq!(dictionary.probe(owned.as_ref()), Some(first));
        assert_eq!(dictionary.decode(first), Some(owned.as_ref()));
    }

    #[test]
    fn inline_values_bypass_the_dictionary() {
        let mut dictionary = Dictionary::new();
        let encoded = dictionary
            .encode(ValueRef::Number(NumberRef::Decimal(42.into())))
            .expect("integers are inline");

        assert_eq!(encoded, StorageValue::integer(42));
        assert_eq!(dictionary.stored(Tag::Int), 0);
        assert_eq!(dictionary.stored(Tag::Term), 0);

        let compact = decimal::Compact::new(15, -1).expect("value is representable");
        assert_eq!(
            dictionary.encode(ValueRef::Number(NumberRef::Decimal(DecimalRef(
                decimal::ReprRef::Compact(compact)
            )))),
            Some(StorageValue::decimal(compact))
        );
    }

    #[test]
    fn wide_values_take_their_tags_and_keep_their_identity() {
        let mut dictionary = Dictionary::new();
        let cases = [
            (
                "15e-300",
                DataValue::decimal(15, -300).expect("bounded"),
                Tag::WideDecimal,
            ),
            (
                "seventeen digits",
                DataValue::decimal_from_lexical("1.2345678901234567").expect("well-formed"),
                Tag::WideDecimal,
            ),
            (
                "wide integer",
                DataValue::decimal_from_lexical("123456789012345678901").expect("well-formed"),
                Tag::WideInt,
            ),
            (
                "wide power of ten",
                DataValue::decimal(1, 300).expect("bounded"),
                Tag::WideInt,
            ),
            (
                "the same digits as a fraction",
                DataValue::decimal(1, -300).expect("bounded"),
                Tag::WideDecimal,
            ),
            (
                "far year",
                DataValue::datetime_from_lexical("-13798000000-01-01T00:00:00Z").expect("valid"),
                Tag::WideDateTime,
            ),
            (
                "fine fraction",
                DataValue::datetime_from_lexical("2024-01-01T00:00:00.1234Z").expect("valid"),
                Tag::WideDateTime,
            ),
            (
                "far date",
                DataValue::typed_literal(xsd::DATE, "-13798000000-01-01").expect("valid"),
                Tag::WideDate,
            ),
            (
                "fine time",
                DataValue::typed_literal(xsd::TIME, "12:00:00.0000000001Z").expect("valid"),
                Tag::WideTime,
            ),
            (
                "fine duration",
                DataValue::typed_literal(xsd::DAY_TIME_DURATION, "PT0.0001S").expect("valid"),
                Tag::WideDuration,
            ),
            (
                "long mixed duration",
                DataValue::typed_literal(xsd::DURATION, "P50000Y1D").expect("valid"),
                Tag::WideDuration,
            ),
        ];

        for (name, value, tag) in cases {
            let encoded = assert_round_trip(&mut dictionary, &value);
            assert_eq!(encoded.tag, tag, "{name}");
            // Interned twice, one entry.
            assert_eq!(dictionary.encode(value.as_ref()), Some(encoded), "{name}");
            assert_eq!(dictionary.probe(value.as_ref()), Some(encoded), "{name}");
        }

        assert_eq!(dictionary.stored(Tag::WideDecimal), 3);
        assert_eq!(dictionary.stored(Tag::WideInt), 2);
        assert_eq!(dictionary.stored(Tag::WideDateTime), 2);
        assert_eq!(dictionary.stored(Tag::WideDate), 1);
        assert_eq!(dictionary.stored(Tag::WideTime), 1);
        assert_eq!(dictionary.stored(Tag::WideDuration), 2);
    }

    #[test]
    fn identity_never_splits_between_literal_and_value() {
        // A typed literal and its value are one entry; `other` refuses the
        // datatypes that would let the lexical form become a second one.
        let mut dictionary = Dictionary::new();
        let big = DataValue::decimal_from_lexical("123456789012345678901").expect("well-formed");
        let literal = DataValue::typed_literal(
            "http://www.w3.org/2001/XMLSchema#integer",
            "123456789012345678901",
        )
        .expect("valid");

        assert_eq!(
            dictionary.encode(big.as_ref()),
            dictionary.encode(literal.as_ref())
        );
        assert!(
            DataValue::other(
                "http://www.w3.org/2001/XMLSchema#integer",
                "123456789012345678901"
            )
            .is_none()
        );

        // An unsupported datatype with the same lexical is a term of its own.
        let other = DataValue::other("http://example.org/unit", "123456789012345678901")
            .expect("datatype is unsupported");
        let encoded = assert_round_trip(&mut dictionary, &other);

        assert_eq!(encoded.tag, Tag::Term);
        assert_eq!(dictionary.stored(Tag::Term), 1);
        assert_eq!(dictionary.stored(Tag::WideInt), 1);
    }

    #[test]
    fn nulls_are_minted_fresh_and_foreign_nulls_are_rejected() {
        let mut dictionary = Dictionary::new();
        let first = dictionary.fresh_null();
        let second = dictionary.fresh_null();
        assert_ne!(first, second);

        let value = DataValue::null(first);
        assert_eq!(
            dictionary.encode(value.as_ref()),
            Some(StorageValue::term(first))
        );
        assert_round_trip(&mut dictionary, &value);
        assert_eq!(dictionary.stored(Tag::Term), 2);

        // A null this dictionary never minted, and an id that is no null.
        let foreign = TermId::compose(TermLane::Null, 99).raw();
        assert_eq!(dictionary.encode(ValueRef::Null(foreign)), None);
        assert_eq!(dictionary.probe(ValueRef::Null(foreign)), None);
        assert_eq!(dictionary.encode(ValueRef::Null(0)), None);
    }

    #[test]
    fn structured_values_round_trip_and_share_shapes() {
        let mut dictionary = Dictionary::new();
        let inner = DataValue::tuple(TupleValue::labeled(
            "point",
            [DataValue::integer(1), DataValue::integer(2)],
        ));
        let outer = DataValue::tuple(TupleValue::new([
            inner.clone(),
            DataValue::iri("http://example.org/a"),
            DataValue::double(2.5),
        ]));
        let map = DataValue::map(
            MapValue::labeled(
                "config",
                [
                    (DataValue::string("port"), DataValue::integer(80)),
                    (DataValue::string("host"), inner.clone()),
                ],
            )
            .expect("keys are unique"),
        );

        assert_eq!(assert_round_trip(&mut dictionary, &inner).tag, Tag::Tuple);
        assert_round_trip(&mut dictionary, &outer);
        assert_eq!(assert_round_trip(&mut dictionary, &map).tag, Tag::Map);

        // A second point shares the first one's type blob.
        let sibling = DataValue::tuple(TupleValue::labeled(
            "point",
            [DataValue::integer(3), DataValue::integer(4)],
        ));
        assert_round_trip(&mut dictionary, &sibling);
        assert_eq!(dictionary.stored(Tag::Tuple), 3);
        assert_eq!(dictionary.tuples.shape_count(), 2);

        // Probing finds what is there and nothing else.
        assert_eq!(
            dictionary.probe(sibling.as_ref()),
            Some(StorageValue::new(Tag::Tuple, 2))
        );
        let stranger = DataValue::tuple(TupleValue::labeled("point", [DataValue::integer(9)]));
        assert_eq!(dictionary.probe(stranger.as_ref()), None);
        let empty = DataValue::map(MapValue::new([]).expect("no keys"));
        assert_eq!(dictionary.probe(empty.as_ref()), None);
        assert_round_trip(&mut dictionary, &empty);
    }

    #[test]
    fn stored_structures_are_read_in_place() {
        let mut dictionary = Dictionary::new();
        let iri = DataValue::iri("http://example.org/a");
        let inner = DataValue::tuple(TupleValue::labeled(
            "point",
            [DataValue::integer(1), iri.clone()],
        ));
        let map = DataValue::map(
            MapValue::new([
                (DataValue::string("b"), inner.clone()),
                (DataValue::string("a"), DataValue::integer(2)),
            ])
            .expect("keys are unique"),
        );
        let inner_entry = dictionary.encode(inner.as_ref()).expect("fits");
        let map_entry = dictionary.encode(map.as_ref()).expect("fits");

        let Some(ValueRef::Map(view)) = dictionary.decode(map_entry) else {
            panic!("a map decodes to a map");
        };
        assert_eq!(view.len(), 2);
        assert_eq!(
            view.get(DataValue::string("a").as_ref()),
            Some(DataValue::integer(2).as_ref())
        );
        assert_eq!(view.get(DataValue::string("c").as_ref()), None);

        // A nested structure is a view of its own entry, which a probe finds without a lookup.
        let Some(ValueRef::Tuple(nested)) = view.get(DataValue::string("b").as_ref()) else {
            panic!("the value at `b` is a tuple");
        };
        assert_eq!(nested.label(), Some("point"));
        assert_eq!(nested.element(1), Some(iri.as_ref()));
        assert_eq!(nested.element(2), None);
        assert_eq!(
            dictionary.entry_of(ValueRef::Tuple(nested)),
            Some(inner_entry)
        );
        assert_eq!(dictionary.probe(ValueRef::Tuple(nested)), Some(inner_entry));
        assert_eq!(dictionary.entry_of(inner.as_ref()), None);

        // A view and its owned value compare, hash and print alike.
        let state = RandomState::new();
        assert_eq!(ValueRef::Tuple(nested), inner.as_ref());
        assert_eq!(
            state.hash_one(ValueRef::Map(view)),
            state.hash_one(map.as_ref())
        );
        assert_eq!(ValueRef::Map(view).to_string(), map.to_string());
        assert_eq!(ValueRef::Map(view).to_owned(), map);

        // A view of another dictionary encodes element by element.
        let mut other = Dictionary::new();
        let entry = other.encode(ValueRef::Map(view)).expect("fits");
        assert_eq!(other.decode(entry).map(ValueRef::to_owned), Some(map));
    }

    #[test]
    fn unknown_entries_do_not_decode() {
        let dictionary = Dictionary::new();

        assert_eq!(dictionary.decode(StorageValue::term(0)), None);
        assert_eq!(
            dictionary.decode(StorageValue::term(
                TermId::compose(TermLane::String, 0).raw()
            )),
            None
        );
        assert_eq!(dictionary.decode(StorageValue::new(Tag::WideInt, 0)), None);
        assert_eq!(dictionary.decode(StorageValue::new(Tag::Tuple, 0)), None);
    }

    #[test]
    fn memory_reporting_covers_all_stores() {
        // One value for each store, and each store reports the bytes it took.
        let empty = Dictionary::new().memory();
        let mut dictionary = Dictionary::new();
        let values = [
            DataValue::iri("http://example.org/a"),
            DataValue::string("hello"),
            DataValue::language_string("hallo", "de").expect("tag is nonempty"),
            DataValue::other("http://example.org/unit", "5cm").expect("datatype is unsupported"),
            DataValue::decimal_from_lexical("123456789012345678901").expect("well-formed"),
            DataValue::decimal(15, -300).expect("bounded"),
            DataValue::datetime_from_lexical("-13798000000-01-01T00:00:00Z").expect("valid"),
            DataValue::typed_literal(xsd::DATE, "-13798000000-01-01").expect("valid"),
            DataValue::typed_literal(xsd::TIME, "12:00:00.0000000001Z").expect("valid"),
            DataValue::typed_literal(xsd::DURATION, "P50000Y1D").expect("valid"),
            DataValue::tuple(TupleValue::new([DataValue::integer(1)])),
            DataValue::map(
                MapValue::new([(DataValue::integer(1), DataValue::integer(2))])
                    .expect("keys are unique"),
            ),
        ];
        for value in &values {
            dictionary.encode(value.as_ref()).expect("fits");
        }

        // Destructured whole, so that a new store cannot go unchecked.
        let memory = dictionary.memory();
        let DictionaryMemory {
            iris,
            strings,
            language_strings,
            others,
            wide_integers,
            wide_decimals,
            wide_datetimes,
            wide_dates,
            wide_times,
            wide_durations,
            tuples,
            maps,
        } = memory;
        let stores = [
            ("IRI", iris, empty.iris),
            ("string", strings, empty.strings),
            ("language string", language_strings, empty.language_strings),
            ("other literal", others, empty.others),
            ("wide integer", wide_integers, empty.wide_integers),
            ("wide decimal", wide_decimals, empty.wide_decimals),
            ("wide dateTime", wide_datetimes, empty.wide_datetimes),
            ("wide date", wide_dates, empty.wide_dates),
            ("wide time", wide_times, empty.wide_times),
            ("wide duration", wide_durations, empty.wide_durations),
            ("tuple", tuples, empty.tuples),
            ("map", maps, empty.maps),
        ];
        for (store, bytes, before) in stores {
            assert!(
                bytes > before,
                "the {store} store reports no bytes for its value"
            );
        }
        let total: usize = stores.iter().map(|&(_, bytes, _)| bytes).sum();
        assert_eq!(memory.total(), total);
    }
}
