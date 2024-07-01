//! This module defines a logical component and ...

use std::fmt::{Debug, Display};

use crate::io::parser::ast::AstNode;

use super::{
    error::ProgramConstructionError,
    origin::{ComponentOrigin, OriginParseReference},
};

pub trait ProgramComponent: Debug + Display {
    type Node<'a>: AstNode;

    fn from_ast_node<'a>(node: Self::Node<'a>, origin: OriginParseReference) -> Self;

    fn parse(string: &str) -> Result<Self, ProgramConstructionError>
    where
        Self: Sized;

    fn origin(&self) -> &ComponentOrigin;
}

pub mod variable;
