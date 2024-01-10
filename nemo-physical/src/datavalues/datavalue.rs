//! This module provides the general trait for representing a [`DataValue`], its relevant
//! [`ValueDomain`]s, and possible errors that occur when dealing with them.

use std::{fmt::Debug, hash::Hash};

use super::{AnyDataValue, IriDataValue};

/// Encloses a string in double quotes, and escapes inner quotes `\"`, newlines `\n`, carriage returns `\r`,
/// tabs `\t`, and backslashes `\\`.
pub(crate) fn quote_string(s: String) -> String {
    "\"".to_owned()
        + &s.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        + "\""
}

/// Encloses a string in pointy brackets. No other escaping is done, since we assume that the IRI has already
/// been processed to be in a suitable form without inner `<` or `>`.
pub(crate) fn quote_iri(s: &str) -> String {
    "<".to_owned() + &s + ">"
}

/// Enum of different value domains that are distinguished in this code,
/// such as "string" or "64bit floating point numbers". Even in an untyped context,
/// it is often useful to distinguish some basic domains and to treat values accordingly.
///
/// Most domains are disjoint to other domains. Where this is not the case, we make sure
/// that the intersectoin of overlapping domains is also available as a constant.
///
/// The main case where this is needed are integer numbers, which exist in several ranges
/// that we need to distinguish to storing them efficiently.
/// Mainly, we care about signed and unsigned 32bit and 64bit numbers, and we add auxiliary
/// domains for the integers that are both in i32 and in u64 (i.e., "u31"), and for those that are both
/// in i64 and in u64 (i.e., "u63").
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ValueDomain {
    /// Domain of all strings of Unicode glyphs.
    String,
    /// Domain of all strings of Unicode glyphs, together with additional language tags
    /// (which are non-empty strings).
    LanguageTaggedString,
    /// Domain of all IRIs in canonical form (no escape characters)
    Iri,
    /// Domain of all 32bit floating point numbers, excluding ±Inf and NaN.
    /// This set of values is disjoint from all other numerical domains (including [ValueDomain::Double]).
    Float,
    /// Domain of all finite 64bit floating point numbers, excluding ±Inf and NaN.
    /// This set of values is disjoint from all other numerical domains (including [ValueDomain::Float]).
    Double,
    /// Domain of all unsigned 64bit integer numbers: 0…+18446744073709551615, or 0 … +2^64-1.
    /// This is a superset of [`ValueDomain::NonNegativeLong`] and its respective subtypes.
    UnsignedLong,
    /// Domain of all signed 64bit integer numbers that are not negative: 0…+9223372036854775807, or 0 … +2^63-1.
    /// This is a superset of [`ValueDomain::Int`].
    NonNegativeLong,
    /// Domain of all unsigned 32bit integer numbers: 0…+4294967295, or 0 … +2^32-1.
    /// This is a superset of [`ValueDomain::NonNegativeInt`].
    UnsignedInt,
    /// Domain of all signed 32bit integer numbers that are not negative: 0…+2147483647, or 0 … +2^31-1.
    /// This is the smallest integer domain we consider, contained in all others.
    NonNegativeInt,
    /// Domain of all signed 64bit integer numbers: -9223372036854775808…+9223372036854775807, or
    /// -2^63 … +2^63-1.
    /// It is a superset of [`ValueDomain::NonNegativeLong`] and [`ValueDomain::Int`], and their
    /// respective subdomains.
    Long,
    /// Domain of all signed 32bit integer numbers: -2147483648…+2147483647, or -2^31 … +2^31-1.
    /// It is a superset of [`ValueDomain::NonNegativeInt`] and a subset of [`ValueDomain::Long`].
    Int,
    /// Domain of all tuples.
    Tuple,
    /// Domain of all maps.
    Map,
    /// Domain of all boolean values (true and false)
    Boolean,
    /// Domain of all named nulls.
    Null,
    /// Domain of all data values not covered by the remaining domains
    Other,
}

impl ValueDomain {
    /// Obtain the datatype IRI that we generally use in canonical values
    /// of a certain domain. In many cases, we use existing identifiers, especially
    /// the XML Schema datatypes that are also used in RDF.
    ///
    /// The method panics on undefined cases, especially when called for [`ValueDomain::Other`],
    /// which does not have a canonical type that is determined by the domain.
    pub(crate) fn type_iri(&self) -> String {
        match self {
            ValueDomain::String => "http://www.w3.org/2001/XMLSchema#string".to_string(),
            ValueDomain::LanguageTaggedString => "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_string(),
            ValueDomain::Iri => "http://www.w3.org/2001/XMLSchema#anyURI".to_string(),
            ValueDomain::Float => "http://www.w3.org/2001/XMLSchema#float".to_string(),
            ValueDomain::Double => "http://www.w3.org/2001/XMLSchema#double".to_string(),
            // We prefer long and int for integer types where possible, since they are most widely supported:
            ValueDomain::UnsignedLong => "http://www.w3.org/2001/XMLSchema#unsignedLong".to_string(),
            ValueDomain::Long => "http://www.w3.org/2001/XMLSchema#long".to_string(),
            ValueDomain::NonNegativeLong => "http://www.w3.org/2001/XMLSchema#long".to_string(),
            ValueDomain::UnsignedInt => "http://www.w3.org/2001/XMLSchema#long".to_string(),
            ValueDomain::Int => "http://www.w3.org/2001/XMLSchema#int".to_string(),
            ValueDomain::NonNegativeInt => "http://www.w3.org/2001/XMLSchema#int".to_string(),
            // Tuples have no type in RDF
            ValueDomain::Tuple => "nemo:tuple".to_string(),
            // Maps have no type in RDF
            ValueDomain::Map => "nemo:map".to_string(),
            // Other literals cannot have a fixed canonical type by definition
            ValueDomain::Other => panic!("There is no canonical datatype for {:?}. Use the type of the value directly.", self),
            ValueDomain::Boolean => "http://www.w3.org/2001/XMLSchema#boolean".to_string(),
            ValueDomain::Null => panic!("There is no canonical datatype for {:?} defined in Nemo yet. Nulls can be serialized as blank nodes.", self),
        }
    }

    /// We order values across distinct domains by using this
    /// integer id. The details matter, e.g., to ensure that comparisons
    /// of integers from different domains agree with the natural order
    /// of these integers. This is why we do not derive this.
    ///
    /// See [ValueDomain::cmp] for further documentation on the requirements
    /// for this order.
    fn relative_domain_position(&self) -> i8 {
        // In addition to the documentation above, we also keep the values in a block
        // that we would store in a dictionary internaly.
        match self {
            // Initial elements as required by SPARQL
            ValueDomain::Null => 10,
            ValueDomain::Iri => 13,
            // Continuing with elements we keep in dictionaries
            ValueDomain::String => 20,
            ValueDomain::LanguageTaggedString => 22,
            ValueDomain::Other => 24,
            ValueDomain::Tuple => 26,
            ValueDomain::Map => 28,
            ValueDomain::Boolean => 30,
            // Followed by the floating points
            ValueDomain::Float => 50,
            ValueDomain::Double => 52,
            // And finally the integer values
            // The order used here is based on the assumption that the given domain is the most
            // specific domain for a value. For example, [ValueDomain::Long] is only used for
            // i64 numbers that are smaller than -2147483648 (hence don't fit inti i32).
            ValueDomain::Long => 100,
            ValueDomain::Int => 102,
            ValueDomain::NonNegativeInt => 104,
            ValueDomain::UnsignedInt => 106,
            ValueDomain::NonNegativeLong => 108,
            ValueDomain::UnsignedLong => 110,
        }
    }
}

impl Ord for ValueDomain {
    /// The order of domains governs the order of values from distinct domains.
    /// Integer domains are ordered in such a way that this order matches the
    /// natural order of numbers provided that the domain is the most specific
    /// (smallest) domain for that value.
    ///
    /// Moreover, we respect the constraints defined in SPARQL 1.1:
    ///
    /// > SPARQL also fixes an order between some kinds of RDF terms that would not otherwise be ordered:
    /// > (Lowest) no value assigned to the variable or expression in this solution.
    /// > Blank nodes
    /// > IRIs
    /// > RDF literals
    ///
    /// There are no "blank" (unassigned) values for us, but the other cases are
    /// treated as required above, where "blank node" corresponds to our nulls.
    ///
    /// SPARQL (and XPath) actually requires that numeric values of different types
    /// are compared by "promoting" values to a compatible type, as described in
    /// detail in the [XPath specification](https://www.w3.org/TR/xpath-31/#promotion).
    /// This is not considered here, and distinct types still have distinct positions
    /// in the ordering. A comparison that conforms to these standards needs to be
    /// realized on the level of individual values, if desired, not on the value domains.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.relative_domain_position()
            .cmp(&other.relative_domain_position())
    }
}

impl PartialOrd for ValueDomain {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

/// Trait for a data value. Implementations of this trait define a single
/// semantic value. It is fully identified by its datatype IRI and lexical
/// value, and different datatype IRI -- lexical value combinations always
/// mean different values (i.e., the representation is canonical). The possible
/// set of datatypes is unrestricted, but values of unknown types are treated
/// literally and cannot be canonized.
pub trait DataValue: Debug + Into<AnyDataValue> + PartialEq + Eq + Hash + Ord {
    /// Return the datatype of this value, specified by an IRI.
    /// For example, the RDF literal `"abc"^^<http://www.w3.org/2001/XMLSchema#string>`
    /// has datatype IRI `http://www.w3.org/2001/XMLSchema#string`, without any surrounding
    /// brackets.
    #[must_use]
    fn datatype_iri(&self) -> String;

    /// Return the canonical lexical representation of this value.
    /// For example, the RDF literal `"42"^^<http://www.w3.org/2001/XMLSchema#int>`
    /// has lexical value `42`, without any surrounding quotes.
    #[must_use]
    fn lexical_value(&self) -> String;

    /// Return the most specific [`ValueDomain`] of this value.
    #[must_use]
    fn value_domain(&self) -> ValueDomain;

    /// Return a canonical string representation of the datavalue. Its format generally conforms with the
    /// syntax of RDF terms as used in N3, SPARQL, and Turtle. In this format, plain strings are delimited
    /// by double quotes (followed by `@tag` for language-tagged strings), IRIs are delimited by pointy brackets
    /// `<` and `>`, and other literals are formatted as `"encoded literal value"^^<IRI that denotes the datatype>`.
    #[must_use]
    fn canonical_string(&self) -> String;

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::String`].
    #[must_use]
    fn to_string(&self) -> Option<String> {
        match self.value_domain() {
            ValueDomain::String => Some(self.to_string_unchecked()),
            _ => None,
        }
    }

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::String`]. Otherwise it panics.
    #[must_use]
    fn to_string_unchecked(&self) -> String {
        panic!("Value is not a string.");
    }

    /// Return the pair of string and language tag that this value represents, if it is a value in
    /// the domain [`ValueDomain::LanguageTaggedString`].
    #[must_use]
    fn to_language_tagged_string(&self) -> Option<(String, String)> {
        match self.value_domain() {
            ValueDomain::LanguageTaggedString => Some(self.to_language_tagged_string_unchecked()),
            _ => None,
        }
    }

    /// Return the pair of string and language tag that this value represents, if it is a value in
    /// the domain [`ValueDomain::LanguageTaggedString`]. Otherwise it panics.
    #[must_use]
    fn to_language_tagged_string_unchecked(&self) -> (String, String) {
        panic!("Value is not a language tagged string.");
    }

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::Iri`].
    #[must_use]
    fn to_iri(&self) -> Option<String> {
        match self.value_domain() {
            ValueDomain::Iri => Some(self.to_iri_unchecked()),
            _ => None,
        }
    }

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::Iri`]. Otherwise it panics.
    #[must_use]
    fn to_iri_unchecked(&self) -> String {
        panic!("Value is not an IRI.");
    }

    /// Return the f32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Float`].
    #[must_use]
    fn to_f32(&self) -> Option<f32> {
        match self.value_domain() {
            ValueDomain::Float => Some(self.to_f32_unchecked()),
            _ => None,
        }
    }

    /// Return the f32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Float`]. Otherwise it panics.
    #[must_use]
    fn to_f32_unchecked(&self) -> f32 {
        panic!("Value is not a float (32bit floating point number).");
    }

    /// Return the f64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Double`].
    #[must_use]
    fn to_f64(&self) -> Option<f64> {
        match self.value_domain() {
            ValueDomain::Double => Some(self.to_f64_unchecked()),
            _ => None,
        }
    }

    /// Return the f64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Double`]. Otherwise it panics.
    #[must_use]
    fn to_f64_unchecked(&self) -> f64 {
        panic!("Value is not a double (64bit floating point number).");
    }

    /// Return true if the value is an integer number that lies within the
    /// range of an i64 number, i.e., in the interval from -9223372036854775808 to +9223372036854775807.
    #[must_use]
    fn fits_into_i64(&self) -> bool {
        false
    }

    /// Return true if the value is an integer number that lies within the
    /// range of an i32 number, i.e., in the interval from -2147483648 to +2147483647.
    #[must_use]
    fn fits_into_i32(&self) -> bool {
        false
    }

    /// Return true if the value is an integer number that lies within the
    /// range of an u64 number, i.e., in the interval from 0 to +18446744073709551615.
    fn fits_into_u64(&self) -> bool {
        false
    }

    /// Return true if the value is an integer number that lies within the
    /// range of an u32 number, i.e., in the interval from 0 to +4294967295.
    #[must_use]
    fn fits_into_u32(&self) -> bool {
        false
    }

    /// Return the i64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Long`] or a subdoman thereof.
    #[must_use]
    fn to_i64(&self) -> Option<i64> {
        if self.fits_into_i64() {
            Some(self.to_i64_unchecked())
        } else {
            None
        }
    }

    /// Return the i64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Long`] or a subdoman thereof. This can also
    /// be checked by calling [`Self::fits_into_i64()`].
    /// Otherwise it panics.
    #[must_use]
    fn to_i64_unchecked(&self) -> i64 {
        panic!("Value is not a long (64bit signed integer number).");
    }

    /// Return the i32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Int`] or a subdoman thereof.
    #[must_use]
    fn to_i32(&self) -> Option<i32> {
        if self.fits_into_i32() {
            Some(self.to_i32_unchecked())
        } else {
            None
        }
    }

    /// Return the i32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Int`] or a subdoman thereof. Otherwise it panics.
    #[must_use]
    fn to_i32_unchecked(&self) -> i32 {
        panic!("Value is not an int (32bit signed integer number).");
    }

    /// Return the u64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::UnsignedLong`] or a subdoman thereof.
    #[must_use]
    fn to_u64(&self) -> Option<u64> {
        if self.fits_into_u64() {
            Some(self.to_u64_unchecked())
        } else {
            None
        }
    }

    /// Return the i64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::UnsignedLong`] or a subdoman thereof. This can also
    /// be checked by calling [`Self::fits_into_i64()`].
    /// Otherwise it panics.
    #[must_use]
    fn to_u64_unchecked(&self) -> u64 {
        panic!("Value is not a long (64bit signed integer number).");
    }

    /// Return the u32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::UnsignedInt`] or a subdoman thereof.
    #[must_use]
    fn to_u32(&self) -> Option<u32> {
        if self.fits_into_u32() {
            Some(self.to_u32_unchecked())
        } else {
            None
        }
    }

    /// Return the u32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::UnsignedInt`] or a subdoman thereof. Otherwise it panics.
    #[must_use]
    fn to_u32_unchecked(&self) -> u32 {
        panic!("Value is not an int (32bit signed integer number).");
    }

    /// If this value is a boolean, return its value.
    #[must_use]
    fn to_boolean(&self) -> Option<bool> {
        match self.value_domain() {
            ValueDomain::Boolean => Some(self.to_boolean_unchecked()),
            _ => None,
        }
    }

    /// If this value is a boolean, returns its value.
    ///
    /// # Panics
    /// Panics if this value is not a boolean.
    #[must_use]
    fn to_boolean_unchecked(&self) -> bool {
        panic!("Value is not a boolean.");
    }

    /// Return the IRI-valued label of this complex value if it has a label,
    /// and None otherwise. Only values in the domain [`ValueDomain::Tuple`] or
    /// [`ValueDomain::Map`] can have labels.
    #[must_use]
    fn label(&self) -> Option<&IriDataValue> {
        None
    }

    /// Return the length of this complex value if it is a
    /// value in the domain [`ValueDomain::Tuple`] or
    /// [`ValueDomain::Map`].
    #[must_use]
    fn len(&self) -> Option<usize> {
        match self.value_domain() {
            ValueDomain::Tuple | ValueDomain::Map => Some(self.len_unchecked()),
            _ => None,
        }
    }

    /// Return the length of this complex value if it is a
    /// value in the domain [`ValueDomain::Tuple`] or
    /// [`ValueDomain::Map`].
    ///
    /// # Panics
    /// Panics if the value is not a collection.
    #[must_use]
    fn len_unchecked(&self) -> usize {
        panic!("Value is not a collection (tuple or map)");
    }

    /// Return the value of the tuple element at the given index
    /// as an [AnyDataValue].
    #[must_use]
    fn tuple_element(&self, index: usize) -> Option<&AnyDataValue> {
        match self.value_domain() {
            ValueDomain::Tuple => {
                if index < self.len_unchecked() {
                    Some(self.tuple_element_unchecked(index))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Return the value of the tuple element at the given index
    /// as an [AnyDataValue].
    ///
    /// # Panics
    /// Panics if index is out of bounds or if value is not a tuple.
    #[must_use]
    fn tuple_element_unchecked(&self, _index: usize) -> &AnyDataValue {
        panic!("Value is not a tuple");
    }

    /// Returns an iterator over all keys in a value that is a map.
    /// None is returned for values that are no maps.
    #[must_use]
    fn map_keys(&self) -> Option<Box<dyn Iterator<Item = &AnyDataValue> + '_>> {
        None
    }

    /// Returns true if the value is a map that contains the given value
    /// as a key. Otherwise, false is returned.
    #[must_use]
    fn contains(&self, _key: &AnyDataValue) -> bool {
        false
    }

    /// Return the value of the map element for the given key
    /// as an [AnyDataValue]. None is returned if the value is not a map
    /// or the value does not exist.
    fn map_element(&self, _key: &AnyDataValue) -> Option<&AnyDataValue> {
        None
    }

    /// Return the value of the map element for the given key
    /// as an [AnyDataValue].
    ///
    /// # Panics
    /// Panics if the key does not exist, or the value is not a map.
    #[must_use]
    fn map_element_unchecked(&self, _key: &AnyDataValue) -> &AnyDataValue {
        panic!("Value is not a map");
    }
}

#[cfg(test)]
mod test {
    use super::ValueDomain;
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    #[test]
    fn test() {
        let mut hasher = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        let vdn = ValueDomain::Null;
        let vdi = ValueDomain::Iri;

        vdn.hash(&mut hasher);
        vdi.hash(&mut hasher2);
        assert_ne!(hasher.finish(), hasher2.finish());
    }
}
