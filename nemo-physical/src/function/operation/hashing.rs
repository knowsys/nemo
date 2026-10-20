//! Cryptographic digests of strings, as lowercase hex:
//! SPARQL's [hash functions](https://www.w3.org/TR/sparql12-query/#func-hash)
//! over the UTF-8 bytes of a plain string.

use std::fmt::Write;

use md5::Digest;

use crate::{
    function::StackValue,
    value::{DataValue, EvaluationError, ValueRef},
};

pub(crate) use md5::Md5;
pub(crate) use sha1::Sha1;
pub(crate) use sha2::{Sha256, Sha384, Sha512};

/// Returns the digest of a plain string under an algorithm,
/// one of the types this module re-exports.
pub(crate) fn digest<Algorithm: Digest>(value: ValueRef<'_>) -> StackValue<'static> {
    let ValueRef::String(text) = value else {
        return EvaluationError::TypeMismatch.into();
    };

    DataValue::string(hex(&Algorithm::digest(text.as_bytes()))).into()
}

/// Formats bytes as lowercase hex.
fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        write!(text, "{byte:02x}").expect("writing to a string cannot fail");
    }

    text
}
