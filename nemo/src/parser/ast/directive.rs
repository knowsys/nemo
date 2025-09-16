//! This module defines [Directive]s.
#![allow(missing_docs)]

use base::Base;
use declare::Declare;
use enum_assoc::Assoc;
use export::Export;
use import::Import;
use nom::{branch::alt, combinator::map};
use output::Output;
use parameter::ParameterDeclaration;
use prefix::Prefix;
use strum_macros::EnumIter;
use unknown::UnknownDirective;

use crate::parser::{
    ParserResult,
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

use super::{ProgramAST, token::TokenKind};

pub mod base;
pub mod declare;
pub mod export;
pub mod import;
pub mod output;
pub mod parameter;
pub mod prefix;
pub mod unknown;

/// Types of directives
#[derive(Debug, Assoc, EnumIter, Clone, Copy, PartialEq, Eq)]
#[func(pub fn token(&self) -> Option<TokenKind>)]
pub enum DirectiveKind {
    /// Base
    #[assoc(token = TokenKind::BaseDirective)]
    Base,
    /// Declare
    #[assoc(token = TokenKind::DeclareDirective)]
    Declare,
    /// Export
    #[assoc(token = TokenKind::ExportDirective)]
    Export,
    /// Import
    #[assoc(token = TokenKind::ImportDirective)]
    Import,
    /// Output
    #[assoc(token = TokenKind::OutputDirective)]
    Output,
    /// Prefix
    #[assoc(token = TokenKind::PrefixDirective)]
    Prefix,
    /// Parameter
    #[assoc(token = TokenKind::ParameterDirective)]
    Parameter,
    /// Unknown
    Unknown,
}

/// Directive
#[derive(Assoc, Debug)]
#[func(pub fn kind(&self) -> DirectiveKind)]
pub enum Directive<'a> {
    /// Base
    #[assoc(kind = DirectiveKind::Base)]
    Base(Base<'a>),
    /// Declare
    #[assoc(kind = DirectiveKind::Declare)]
    Declare(Declare<'a>),
    /// Export
    #[assoc(kind = DirectiveKind::Export)]
    Export(Export<'a>),
    /// Import
    #[assoc(kind = DirectiveKind::Import)]
    Import(Import<'a>),
    /// Output
    #[assoc(kind = DirectiveKind::Output)]
    Output(Output<'a>),
    /// Prefix
    #[assoc(kind = DirectiveKind::Prefix)]
    Prefix(Prefix<'a>),
    /// Parameter
    #[assoc(kind = DirectiveKind::Parameter)]
    Parameter(ParameterDeclaration<'a>),
    /// Unknown
    #[assoc(kind = DirectiveKind::Base)]
    Unknown(UnknownDirective<'a>),
}

impl Directive<'_> {
    /// Return the context of the underlying directive.
    pub fn context_type(&self) -> ParserContext {
        match self {
            Directive::Base(directive) => directive.context(),
            Directive::Declare(directive) => directive.context(),
            Directive::Export(directive) => directive.context(),
            Directive::Import(directive) => directive.context(),
            Directive::Output(directive) => directive.context(),
            Directive::Prefix(directive) => directive.context(),
            Directive::Parameter(directive) => directive.context(),
            Directive::Unknown(directive) => directive.context(),
        }
    }
}

const CONTEXT: ParserContext = ParserContext::Directive;

impl<'a> ProgramAST<'a> for Directive<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![match self {
            Directive::Base(directive) => directive,
            Directive::Declare(directive) => directive,
            Directive::Export(directive) => directive,
            Directive::Import(directive) => directive,
            Directive::Output(directive) => directive,
            Directive::Prefix(directive) => directive,
            Directive::Parameter(directive) => directive,
            Directive::Unknown(directive) => directive,
        }]
    }

    fn span(&self) -> Span<'a> {
        match self {
            Directive::Base(directive) => directive.span(),
            Directive::Declare(directive) => directive.span(),
            Directive::Export(directive) => directive.span(),
            Directive::Import(directive) => directive.span(),
            Directive::Output(directive) => directive.span(),
            Directive::Prefix(directive) => directive.span(),
            Directive::Parameter(directive) => directive.span(),
            Directive::Unknown(directive) => directive.span(),
        }
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(
            CONTEXT,
            alt((
                map(Base::parse, Directive::Base),
                map(Declare::parse, Directive::Declare),
                map(Export::parse, Directive::Export),
                map(Import::parse, Directive::Import),
                map(Output::parse, Directive::Output),
                map(Prefix::parse, Directive::Prefix),
                map(ParameterDeclaration::parse, Directive::Parameter),
                map(UnknownDirective::parse, Directive::Unknown),
            )),
        )(input)
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, directive::Directive},
        context::ParserContext,
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_directive() {
        let test = vec![
            ("@base <test>", ParserContext::Base),
            ("@declare test(a:int)", ParserContext::Declare),
            ("@export test :- csv {}", ParserContext::Export),
            ("@import test :- csv {}", ParserContext::Import),
            ("@output test", ParserContext::Output),
            ("@prefix test: <test>", ParserContext::Prefix),
            ("@test something", ParserContext::UnknownDirective),
            ("@basetest <test>", ParserContext::UnknownDirective),
            ("@declaretest test(a:int)", ParserContext::UnknownDirective),
            (
                "@exporttest test :- csv {}",
                ParserContext::UnknownDirective,
            ),
            (
                "@importtest test :- csv {}",
                ParserContext::UnknownDirective,
            ),
            ("@outputtest test", ParserContext::UnknownDirective),
            ("@prefixtest test: <test>", ParserContext::UnknownDirective),
            ("@parameter $param = 5", ParserContext::ParameterDecl),
        ];

        for (input, expect) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Directive::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(result.1.context_type(), expect);
        }
    }
}
