use std::fmt::write;

use crate::error::Error;

/// Enum of different value domains that are distinguished in this code,
/// such as "string" or "64bit floating point numbers". Even in an untyped context,
/// it is often useful to distinguish some basic domains and to treat values accordingly.
/// 
/// Note: This is meant as an example now, not as a full list of everything we might need.
#[derive(Debug, Copy, Clone)]
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
    /// the domain [`ValueDomain::String`]. Otherwise it panics.
    fn to_string(&self) -> String {
        panic!("Value is not a string.");
    }

    /// Return the pair of string and language tag that this value represents, if it is a value in
    /// the domain [`ValueDomain::LanguageTaggedString`]. Otherwise it panics.
    fn to_language_tagged_string(&self) -> (String,String) {
        panic!("Value is not a language tagged string.");
    }

    /// Return the string that this value represents, if it is a value in
    /// the domain [`ValueDomain::Iri`]. Otherwise it panics.
    fn to_iri(&self) -> String {
        panic!("Value is not an IRI.");
    }

    /// Return the f64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Double`]. Otherwise it panics.
    fn to_f64(&self) -> f64 {
        panic!("Value is not a double (64bit floating point number).");
    }

    /// Return the i64 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Long`] or a subdoman thereof. Otherwise it panics.
    fn to_i64(&self) -> i64 {
        panic!("Value is not a long (64bit signed integer number).");
    }

    /// Return the i32 that this value represents, if it is a value in
    /// the domain [`ValueDomain::Int`] or a subdoman thereof. Otherwise it panics.
    fn to_i32(&self) -> i32 {
        panic!("Value is not an int (32bit signed integer number).");
    }

}


/// String value obtained from the physical layer
// #[derive(Debug)]
// pub struct PhysicalString<'a> {
//     // TODO: Just guessing
//     prefix: &'a str,
//     content: &'a str,
// }

// impl<'a> DataValue for PhysicalString<'a> {
//     fn data_type(&self) -> ValueDomain {
//         ValueDomain::String
//     }

//     fn to_string(&self) -> Option<String> {
//         let mut string = String::from(self.prefix);
//         string.push_str(self.content);

//         Some(string)
//     }

//     fn to_u64(&self) -> Option<u64> {
//         None
//     }

//     fn to_u32(&self) -> Option<u32> {
//         None
//     }
// }

/// Physical representation of an integer as an i64.
#[derive(Debug, Clone, Copy)]
pub struct Long(i64);

impl DataValue for Long {

    fn datatype_iri(&self) -> String {
        match self.value_domain() {
            ValueDomain::Long => "http://www.w3.org/2001/XMLSchema#long".to_owned(),
            ValueDomain::Int => "http://www.w3.org/2001/XMLSchema#int".to_owned(),
            _ => panic!("Unexpected value domain for i64"),
        }
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()   
    }

    /// The function needs to find the tightest domain for the given value.
    fn value_domain(&self) -> ValueDomain {
        if self.0 <= std::i32::MAX && self.0 >= std::i32::MIN {
            ValueDomain::Int
        } else {
            ValueDomain::Long
        }
    }

    fn to_i64(&self) -> i64 {
        self.0
    }

    fn to_i32(&self) -> i32 {
        // TODO: Maybe give a more informative error message here.
        self.0.try_into().unwrap()
    }
}
