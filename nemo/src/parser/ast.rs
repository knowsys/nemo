//! This module defines the abstract syntax tree representation of a nemo program.

pub mod expression;
pub mod program;
pub mod rule;
pub mod tag;
pub mod token;

use super::{span::ProgramSpan, ParserInput, ParserResult};

/// Trait implemented by nodes in the abstract syntax tree
pub trait ProgramAST<'a>: Sync {
    /// Return all children of this node.
    fn children(&self) -> Vec<&dyn ProgramAST>;

    /// Return the region of text this node originates from.
    fn span(&self) -> ProgramSpan;

    /// Parse the given input into this type of node
    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a;
}
