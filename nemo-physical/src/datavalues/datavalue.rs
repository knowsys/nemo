


/// Enum of different value domains that are distinguished in this code,
/// such as "string" or "64bit floating point numbers". Even in an untyped context,
/// it is often useful to distinguish some basic domains and to treat values accordingly.
///
/// Note: This is meant as an example now, not as a full list of everything we might need.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ValueDomain {
    /// Domain of all strings of Unicode glyphs.
    String,
    /// Domain of all strings of Unicode glyphs, together with additional language tags
    /// (which are non-empty strings).
    LanguageTaggedString,
    /// Domain of all IRIs in canonical form (no escape characters)
    Iri,
    /// Domain of all 64bit floating point numbers incl. ±Inf, ±0, NaN.
    /// This set of values is disjoint from all other numerical domains.
    Double,
    /// Domain of all signed 64bit integer numbers: -9223372036854775808…+9223372036854775807.
    /// This is a superset of [`ValueDomain::Int`].
    Long,
    /// Domain of all signed 32bit integer numbers: -2147483648…+2147483647.
    /// This is a subset of [`ValueDomain::Long`].
    Int,
    /// Domain of all tuples.
    Tuple,
    /// Domain of all data values not covered by the remaining domains
    Other,
}

/// Trait for a data value. Implementations of this trait define a single
/// semantic value. It is fully identified by its datatype IRI and lexical
/// value, and different datatype IRI -- lexical value combinations always
/// mean different values (i.e., the representation is canonical). The possible
/// set of datatypes is unrestricted, but values of unknown types are treated
/// literally and cannot be canonized.
pub trait DataValue {
    /// Return the datatype of this value, specified by an IRI.
    /// For example, the RDF literal `"abc"^^<http://www.w3.org/2001/XMLSchema#string>`
    /// has datatype IRI `http://www.w3.org/2001/XMLSchema#string`, without any surrounding
    /// brackets.
    fn datatype_iri(&self) -> String;

    /// Return the canonical lexical representation of this value.
    /// For example, the RDF literal `"42"^^<http://www.w3.org/2001/XMLSchema#int>`
    /// has lexical value `42`, without any surrounding quotes.
    fn lexical_value(&self) -> String;

    /// Return the most specific [`ValueDomain`] of this value.
    fn value_domain(&self) -> ValueDomain;

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::String`].
    fn to_string(&self) -> Option<String> {
        match self.value_domain() {
            ValueDomain::String =>  Some(self.to_string_unchecked()),
            _ => None
        }
    }

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::String`]. Otherwise it panics.
    fn to_string_unchecked(&self) -> String {
        panic!("Value is not a string.");
    }

    /// Return the pair of string and language tag that this value represents, if it is a value in
    /// the domain [`ValueDomain::LanguageTaggedString`].
    fn to_language_tagged_string(&self) -> Option<(String, String)> {
        match self.value_domain() {
            ValueDomain::LanguageTaggedString =>  Some(self.to_language_tagged_string_unchecked()),
            _ => None
        }
    }

    /// Return the pair of string and language tag that this value represents, if it is a value in
    /// the domain [`ValueDomain::LanguageTaggedString`]. Otherwise it panics.
    fn to_language_tagged_string_unchecked(&self) -> (String, String) {
        panic!("Value is not a language tagged string.");
    }

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::Iri`].
    fn to_iri(&self) -> Option<String> {
        match self.value_domain() {
            ValueDomain::Iri =>  Some(self.to_iri_unchecked()),
            _ => None
        }
    }

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::Iri`]. Otherwise it panics.
    fn to_iri_unchecked(&self) -> String {
        panic!("Value is not an IRI.");
    }

    /// Return the f64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Double`].
    fn to_f64(&self) -> Option<f64> {
        match self.value_domain() {
            ValueDomain::Double =>  Some(self.to_f64_unchecked()),
            _ => None
        }
    }

    /// Return the f64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Double`]. Otherwise it panics.
    fn to_f64_unchecked(&self) -> f64 {
        panic!("Value is not a double (64bit floating point number).");
    }

    /// Return true if the value is an integer number that lies within the
    /// range of an i64 number, i.e., in the interval from -9223372036854775808 to +9223372036854775807.
    fn fits_into_i64(&self) -> bool {
        false
    }

    /// Return true if the value is an integer number that lies within the
    /// range of an i32 number, i.e., in the interval from -2147483648 to +2147483647.
    fn fits_into_i32(&self) -> bool {
        false
    }

    /// Return the i64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Long`] or a subdoman thereof.
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
    fn to_i64_unchecked(&self) -> i64 {
        panic!("Value is not a long (64bit signed integer number).");
    }

    /// Return the i32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Int`] or a subdoman thereof.
    fn to_i32(&self) -> Option<i32> {
        if self.fits_into_i32() {
            Some(self.to_i32_unchecked())
        } else {
            None
        }
    }

    /// Return the i32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Int`] or a subdoman thereof. Otherwise it panics.
    fn to_i32_unchecked(&self) -> i32 {
        panic!("Value is not an int (32bit signed integer number).");
    }

    /// Return the value of the tuple element at the given index.
    fn tuple_element(&self, index: usize) -> Option<&dyn DataValue> {
        match self.value_domain() {
            ValueDomain::Tuple =>  { 
                if index < self.tuple_len_unchecked() {
                    Some(self.tuple_element_unchecked(index))
                } else {
                    None
                }
            },
            _ => None
        }
    }

    /// Return the length of the tuple element if
    /// if it is a value in the domain [`ValueDomain::Tuple`].
    fn tuple_len(&self) -> Option<usize> {
        match self.value_domain() {
            ValueDomain::Tuple => Some(self.tuple_len_unchecked()),
            _ => None
        }
    }

    /// Return the length of the tuple element if
    /// if it is a value in the domain [`ValueDomain::Tuple`].
    /// Otherwise it panics.
    fn tuple_len_unchecked(&self) -> usize {
        panic!("Value is not a tuple");
    }

    /// Return the value of the tuple element at the given index.
    /// Panics if index is out of bounds or if value is not a tuple.
    fn tuple_element_unchecked(&self, _index: usize) -> &dyn DataValue {
        panic!("Value is not a tuple");
    }
}