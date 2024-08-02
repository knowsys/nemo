//! This module defines the abstract syntax tree representation of a nemo program.

pub mod attribute;
pub mod comment;
pub mod directive;
pub mod expression;
pub mod program;
pub mod rule;
pub mod sequence;
pub mod statement;
pub mod tag;
pub mod token;

use std::fmt::Debug;

use super::{context::ParserContext, span::Span, ParserInput, ParserResult};
use ascii_tree::Tree;

/// Trait implemented by nodes in the abstract syntax tree
pub trait ProgramAST<'a>: Debug + Sync {
    /// Return all children of this node.
    fn children(&'a self) -> Vec<&'a dyn ProgramAST>;

    /// Return the region of text this node originates from.
    fn span(&self) -> Span<'a>;

    /// Parse the given input into this type of node
    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a;

    /// Return [ParserContext] indicating the type of node.
    fn context(&self) -> ParserContext;
}

pub(crate) fn ast_to_ascii_tree<'a>(node: &'a dyn ProgramAST<'a>) -> Tree {
    let mut vec = Vec::new();
    for child in node.children() {
        vec.push(ast_to_ascii_tree(child));
    }
    let colour = if node.children().is_empty() {
        "\x1b[91m"
    } else {
        "\x1b[92m"
    };
    let fragment = *node.span().0.fragment();
    let str = if fragment.len() > 60 {
        format!("{:?}[…]", &fragment[0..60])
    } else {
        format!("{:?}", fragment)
    };
    Tree::Node(
        format!(
            "{} \x1b[34m@{}:{} {colour}{str}\x1b[0m",
            node.context().name().to_string(),
            node.span().0.location_line(),
            node.span().0.get_utf8_column()
        ),
        vec,
    )
}
