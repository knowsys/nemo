//! This module defines the abstract syntax tree representation of a nemo program.

pub mod expression;
pub mod program;
pub mod rule;
pub mod tag;
pub mod token;

use crate::rule_model::origin::Origin;

use super::{context::ParserContext, span::ProgramSpan, ParserInput, ParserResult};

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

    /// Return [ParserContext] indicating the type of node.
    fn context(&self) -> ParserContext;

    /// Locate a node from a stack of [Origin]s.
    fn locate(&'a self, origin_stack: &[Origin]) -> Option<&'a dyn ProgramAST<'a>>
    where
        Self: Sized + 'a,
    {
        let mut current_node: &dyn ProgramAST = self;

        for origin in origin_stack {
            if let &Origin::External(index) = origin {
                current_node = *current_node.children().get(index)?;
            } else {
                return None;
            }
        }

        Some(current_node)
    }
}
