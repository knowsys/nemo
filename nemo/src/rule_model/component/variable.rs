use std::fmt::Display;

use crate::{
    io::parser::ast::term::Term,
    rule_model::{
        error::ProgramConstructionError,
        origin::{ComponentOrigin, OriginParseReference},
    },
};

use super::ProgramComponent;

/// Name of a variable
#[derive(Debug, Clone)]
pub struct VariableName(String);

impl VariableName {
    fn new(name: String) -> Result<Self, ProgramConstructionError> {
        // TODO: Validate name
        if name.is_empty() {
            return Err(ProgramConstructionError::InvalidVariableName(name));
        }

        Ok(Self::new_unvalidated(name))
    }

    fn new_unvalidated(name: String) -> Self {
        Self(name)
    }
}

impl Display for VariableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
pub struct UniversalVariale {
    origin: ComponentOrigin,

    name: Option<VariableName>,
}

impl UniversalVariale {
    fn from_term(term: Term) -> Self {
        todo!()
    }
}

impl Display for UniversalVariale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(name) => write!(f, "?{}", name),
            None => write!(f, "_"),
        }
    }
}

impl ProgramComponent for UniversalVariale {
    type Node<'a> = Term<'a>;

    fn from_ast_node<'a>(node: Term<'a>, origin: OriginParseReference) -> Self {
        if let Term::UniversalVariable(token) = node {
            let string = token.span.to_string();
        }

        todo!()
    }

    fn parse(string: &str) -> Result<Self, ProgramConstructionError> {
        todo!()
    }

    fn origin(&self) -> &ComponentOrigin {
        &self.origin
    }
}

#[derive(Debug)]
pub struct ExistentialVariable {
    origin: ComponentOrigin,

    name: VariableName,
}

#[derive(Debug)]
pub enum Variable {
    Universal(UniversalVariale),
    Existential(ExistentialVariable),
}

mod test {
    #[test]
    fn create_variable() {}
}
