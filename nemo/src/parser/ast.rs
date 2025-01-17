//! This module defines the abstract syntax tree representation of a nemo program.

pub mod attribute;
pub mod comment;
pub mod directive;
pub mod expression;
pub mod guard;
pub mod program;
pub mod rule;
pub mod sequence;
pub mod statement;
pub mod tag;
pub mod token;

use std::fmt::Debug;

use super::{context::ParserContext, span::Span, ParserInput, ParserResult};
use ascii_tree::Tree;
use colored::Colorize;

/// Trait implemented by nodes in the abstract syntax tree
pub trait ProgramAST<'a>: Debug + Sync {
    /// Return all children of this node.
    fn children(&self) -> Vec<&dyn ProgramAST<'a>>;

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
    let span = node.span();
    let str = if span.fragment().len() > 60 {
        format!("{:?}[…]", &span.fragment()[0..60])
    } else {
        format!("{:?}", span.fragment())
    };
    Tree::Node(
        format!(
            "{} @{}:{} {}",
            node.context().name(),
            node.span().location_line().to_string().blue(),
            node.span().get_utf8_column(),
            if node.children().is_empty() {
                str.bright_red()
            } else {
                str.bright_green()
            }
        ),
        vec,
    )
}
