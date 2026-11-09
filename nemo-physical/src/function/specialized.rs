//! Stack programs with their specializations,
//! the [identity form](IdentityCondition) and the [integer form](IntegerProgram).
//!
//! A specialization evaluates a program on storage values directly,
//! without decoding them into values.

mod identity;
mod integer;

pub(crate) use self::{
    identity::{IdentityCondition, IdentityOperand},
    integer::{IntegerProgram, IntegerValue},
};

use crate::function::StackProgram;

/// A form of a stack program that evaluates on storage values directly.
#[derive(Debug, Clone)]
enum Specialization {
    /// The identity form, which applies to columns of any tag.
    Identity(IdentityCondition),
    /// The integer form, which applies if every input reads a column of values
    /// with the tag [Int](crate::storage::Tag::Int) only.
    Integer(IntegerProgram),
}

/// A stack program together with its specialization, if it has one.
///
/// The program itself is always kept, since the integer form applies only to columns
/// of 64-bit integers, and only to the rows on which its semantics agree with the program's.
#[derive(Debug, Clone)]
pub(crate) struct SpecializedProgram {
    /// The program.
    generic: StackProgram,
    /// Its specialization, compiled from it.
    specialization: Option<Specialization>,
}

impl SpecializedProgram {
    /// Creates the program with its specialization, if it has one.
    ///
    /// The identity form takes precedence, since it applies to columns of any tag.
    pub(crate) fn new(generic: StackProgram) -> Self {
        let specialization = generic
            .identity_specialization()
            .map(Specialization::Identity)
            .or_else(|| {
                generic
                    .integer_specialization()
                    .map(Specialization::Integer)
            });

        Self {
            generic,
            specialization,
        }
    }

    /// Returns the program.
    pub(crate) fn generic(&self) -> &StackProgram {
        &self.generic
    }

    /// Returns the identity form of the program, if it has one.
    pub(crate) fn identity(&self) -> Option<&IdentityCondition> {
        match &self.specialization {
            Some(Specialization::Identity(identity)) => Some(identity),
            _ => None,
        }
    }

    /// Returns the integer form of the program, if it has one.
    pub(crate) fn integer(&self) -> Option<&IntegerProgram> {
        match &self.specialization {
            Some(Specialization::Integer(integer)) => Some(integer),
            _ => None,
        }
    }
}
