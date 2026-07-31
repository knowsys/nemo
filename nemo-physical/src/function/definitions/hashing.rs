//! This module defines hash functions on strings.

use std::{fmt::Write, marker::PhantomData};

use digest::Digest;
use md5::Md5;
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};

use crate::{
    datatypes::StorageTypeName,
    datavalues::{AnyDataValue, DataValue},
};

use super::{FunctionTypePropagation, UnaryFunction};

/// Hash of a string, using hash algorithm `D`
///
/// Corresponds to the SPARQL functions MD5(arg), SHA1(arg), SHA256(arg), SHA384(arg), and SHA512(arg).
/// Returns the checksum (lowercase hex) of the UTF-8 representation of the argument.
///
/// Returns `None` if the argument is not a plain string.
#[derive(Debug, Default)]
pub struct StringHash<D: Digest> {
    _digest: PhantomData<D>,
}

impl<D: Digest> Clone for StringHash<D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D: Digest> Copy for StringHash<D> {}

impl<D: Digest> PartialEq for StringHash<D> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<D: Digest> Eq for StringHash<D> {}

impl<D: Digest> UnaryFunction for StringHash<D> {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let string = parameter.to_plain_string()?;
        let digest = D::digest(string.as_bytes());

        let mut hex = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(hex, "{byte:02x}").expect("writing to a string should not fail");
        }

        Some(AnyDataValue::new_plain_string(hex))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// MD5 hash of a string, corresponding to SPARQL MD5(arg)
pub type StringMd5 = StringHash<Md5>;
/// SHA1 hash of a string, corresponding to SPARQL SHA1(arg)
pub type StringSha1 = StringHash<Sha1>;
/// SHA256 hash of a string, corresponding to SPARQL SHA256(arg)
pub type StringSha256 = StringHash<Sha256>;
/// SHA384 hash of a string, corresponding to SPARQL SHA384(arg)
pub type StringSha384 = StringHash<Sha384>;
/// SHA512 hash of a string, corresponding to SPARQL SHA512(arg)
pub type StringSha512 = StringHash<Sha512>;
