//! Cryptographic digests of strings,
//! SPARQL's [hash functions](https://www.w3.org/TR/sparql12-query/#func-hash).
//!
//! A digest is computed over the UTF-8 bytes of a plain string and written in lowercase hex.

use std::fmt::Write;

use md5::Digest;

use crate::{
    function::StackValue,
    value::{DataValue, EvaluationError, ValueRef},
};

pub(crate) use md5::Md5;
pub(crate) use sha1::Sha1;
pub(crate) use sha2::{Sha256, Sha384, Sha512};

/// Returns the digest of a plain string under `Algorithm`,
/// one of the algorithms that this module re-exports.
///
/// Any other value, a language-tagged string included,
/// is a [TypeMismatch](EvaluationError::TypeMismatch).
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
