


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
            ValueDomain::Double => "http://www.w3.org/2001/XMLSchema#double".to_string(),
            // We prefer long and int for integer types where possible, since they are most widely supported:
            ValueDomain::UnsignedLong => "http://www.w3.org/2001/XMLSchema#unsignedLong".to_string(),
            ValueDomain::Long => "http://www.w3.org/2001/XMLSchema#long".to_string(),
            ValueDomain::NonNegativeLong => "http://www.w3.org/2001/XMLSchema#long".to_string(),
            ValueDomain::UnsignedInt => "http://www.w3.org/2001/XMLSchema#long".to_string(),
            ValueDomain::Int => "http://www.w3.org/2001/XMLSchema#int".to_string(),
            ValueDomain::NonNegativeInt => "http://www.w3.org/2001/XMLSchema#int".to_string(),
            // Tuples have no type in RDF
            ValueDomain::Tuple => panic!("There is no canonical datatype for {:?} defined in Nemo yet. We'll need an IRI there.", self),
            // Other literals cannot have a fixed canonical type by definition
            ValueDomain::Other => panic!("There is no canonical datatype for {:?}. Use the type of the value directly.", self),
        }
    }
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

    /// Return true if the value is an integer number that lies within the
    /// range of an u64 number, i.e., in the interval from 0 to +18446744073709551615.
    fn fits_into_u64(&self) -> bool {
        false
    }

    /// Return true if the value is an integer number that lies within the
    /// range of an u32 number, i.e., in the interval from 0 to +4294967295.
    fn fits_into_u32(&self) -> bool {
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

    /// Return the u64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::UnsignedLong`] or a subdoman thereof.
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
    fn to_u64_unchecked(&self) -> u64 {
        panic!("Value is not a long (64bit signed integer number).");
    }

    /// Return the u32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::UnsignedInt`] or a subdoman thereof.
    fn to_u32(&self) -> Option<u32> {
        if self.fits_into_u32() {
            Some(self.to_u32_unchecked())
        } else {
            None
        }
    }

    /// Return the u32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::UnsignedInt`] or a subdoman thereof. Otherwise it panics.
    fn to_u32_unchecked(&self) -> u32 {
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