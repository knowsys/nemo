//! This module defines [ProgramConstructionError]

use thiserror::Error;

use super::component::{
    atom::Atom,
    fact::Fact,
    term::{primitive::variable::Variable, Term},
};

/// Error returned during the construction of objects from nemo's logical rule model
#[derive(Error, Debug)]
pub enum ProgramConstructionError {
    #[error("variable \"{0}\" has an invalid name")]
    InvalidVariableName(String),
    #[error("term \"{0}\" has an invalid name")]
    InvalidIdentifier(String),
    #[error("atom \"{0}\" has an invalid name")]
    InvalidAtomName(String),
    #[error("fact {0} contains ")]
    NonGroundFact(Fact),
    #[error("parse error")] // TODO: Return parser error here
    ParseError,
}
