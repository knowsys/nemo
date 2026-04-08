//! This module defines hash functions on strings.

use digest::Digest;
use md5::Md5;
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};

use crate::{
    datatypes::StorageTypeName,
    datavalues::{AnyDataValue, DataValue},
};

use super::{FunctionTypePropagation, UnaryFunction};

/// Compute the lowercase hex digest of `input` using hash algorithm `D`.
fn hex_digest<D: Digest>(input: &str) -> String {
    let result = D::digest(input.as_bytes());
    result.iter().map(|b| format!("{b:02x}")).collect()
}

/// MD5 hash of a string
///
/// Corresponds to SPARQL MD5(arg).
/// Returns the MD5 checksum (lowercase hex) of the UTF-8 representation of the argument.
///
/// Returns `None` if the argument is not a plain string.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StringMd5;
impl UnaryFunction for StringMd5 {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let s = parameter.to_plain_string()?;
        Some(AnyDataValue::new_plain_string(hex_digest::<Md5>(&s)))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// SHA1 hash of a string
///
/// Corresponds to SPARQL SHA1(arg).
/// Returns the SHA1 checksum (lowercase hex) of the UTF-8 representation of the argument.
///
/// Returns `None` if the argument is not a plain string.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StringSha1;
impl UnaryFunction for StringSha1 {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let s = parameter.to_plain_string()?;
        Some(AnyDataValue::new_plain_string(hex_digest::<Sha1>(&s)))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// SHA256 hash of a string
///
/// Corresponds to SPARQL SHA256(arg).
/// Returns the SHA256 checksum (lowercase hex) of the UTF-8 representation of the argument.
///
/// Returns `None` if the argument is not a plain string.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StringSha256;
impl UnaryFunction for StringSha256 {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let s = parameter.to_plain_string()?;
        Some(AnyDataValue::new_plain_string(hex_digest::<Sha256>(&s)))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// SHA384 hash of a string
///
/// Corresponds to SPARQL SHA384(arg).
/// Returns the SHA384 checksum (lowercase hex) of the UTF-8 representation of the argument.
///
/// Returns `None` if the argument is not a plain string.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StringSha384;
impl UnaryFunction for StringSha384 {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let s = parameter.to_plain_string()?;
        Some(AnyDataValue::new_plain_string(hex_digest::<Sha384>(&s)))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// SHA512 hash of a string
///
/// Corresponds to SPARQL SHA512(arg).
/// Returns the SHA512 checksum (lowercase hex) of the UTF-8 representation of the argument.
///
/// Returns `None` if the argument is not a plain string.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StringSha512;
impl UnaryFunction for StringSha512 {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let s = parameter.to_plain_string()?;
        Some(AnyDataValue::new_plain_string(hex_digest::<Sha512>(&s)))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}
