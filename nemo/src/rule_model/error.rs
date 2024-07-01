use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProgramConstructionError {
    #[error("invalid variable name: {0}")]
    InvalidVariableName(String),
    #[error("parse error")] // TODO: Return parser error here
    ParseError,
}
