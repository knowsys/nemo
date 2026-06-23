//! This module defines [N3Path].

use std::ops::Not;

use nom::{branch::alt, bytes::complete::tag, combinator::map, multi::many0, sequence::pair};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        expression::basic::{blank::Blank, iri::Iri},
        n3::expression::{literal::N3Literal, variable::N3Variable},
        tag::structure::StructureTag,
    },
    context::{Notation3Context, ParserContext, context},
    input::ParserInput,
    span::Span,
};

use super::formula::N3Formula;

/// A Notation3 expression
#[derive(Clone, Debug)]
pub enum N3PathItemKind<'a> {
    /// IRI
    Iri(StructureTag<'a>),
    /// Blank node
    Bnode(Blank<'a>),
    /// Variable
    Variable(N3Variable<'a>),
    /// Collection
    Collection, // FIXME
    /// Blank Node property list
    BnodePropertyList, // FIXME
    /// IRI property list
    IriPropertyList, // FIXME
    /// Literal
    Literal(N3Literal<'a>),
    /// Formula
    Formula(N3Formula<'a>),
}

impl N3PathItemKind<'_> {
    /// Return the [ParserContext] of the underlying path item kind.
    pub fn context_type(&self) -> ParserContext {
        match self {
            Self::Iri(iri) => iri.context(),
            Self::Bnode(blank) => blank.context(),
            Self::Variable(n3_variable) => n3_variable.context(),
            Self::Collection => todo!(),
            Self::BnodePropertyList => todo!(),
            Self::IriPropertyList => todo!(),
            Self::Literal(n3_literal) => n3_literal.context(),
            Self::Formula(n3_formula) => n3_formula.context(),
        }
    }
}

impl<'a> ProgramAST<'a> for N3PathItemKind<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match self {
            Self::Iri(iri) => iri.children(),
            Self::Bnode(blank) => blank.children(),
            Self::Variable(n3_variable) => n3_variable.children(),
            Self::Collection => todo!(),
            Self::BnodePropertyList => todo!(),
            Self::IriPropertyList => todo!(),
            Self::Literal(n3_literal) => n3_literal.children(),
            Self::Formula(n3_formula) => n3_formula.children(),
        }
    }

    fn span(&self) -> Span<'a> {
        match self {
            Self::Iri(iri) => iri.span(),
            Self::Bnode(blank) => blank.span(),
            Self::Variable(n3_variable) => n3_variable.span(),
            Self::Collection => todo!(),
            Self::BnodePropertyList => todo!(),
            Self::IriPropertyList => todo!(),
            Self::Literal(n3_literal) => n3_literal.span(),
            Self::Formula(n3_formula) => n3_formula.span(),
        }
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        alt((
            map(
                context(
                    ParserContext::notation3(Notation3Context::Iri),
                    StructureTag::parse,
                ),
                Self::Iri,
            ),
            map(Blank::parse, Self::Bnode),
            map(N3Variable::parse, Self::Variable),
            map(N3Literal::parse, Self::Literal),
            map(N3Formula::parse, Self::Formula),
        ))(input)
    }

    fn context(&self) -> ParserContext {
        self.context_type()
    }
}

/// The direction of an element in an [N3Path].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum N3PathDirection {
    /// Forwards path segment.
    Forward,
    /// Backwards path segment.
    Backward,
}

impl N3PathDirection {
    fn parse<'a>(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        alt((
            map(tag("!"), |_| Self::Forward),
            map(tag("^"), |_| Self::Backward),
        ))(input)
    }
}

impl Not for N3PathDirection {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::Forward => Self::Backward,
            Self::Backward => Self::Forward,
        }
    }
}

/// A Notation3 path item
#[derive(Clone, Debug)]
pub struct N3PathItem<'a> {
    span: Span<'a>,

    pub(crate) kind: N3PathItemKind<'a>,
}

impl<'a> N3PathItem<'a> {
    pub(crate) fn from_span_and_iri(span: Span<'a>, iri: &'a str) -> Self {
        Self {
            span,
            kind: N3PathItemKind::Iri(StructureTag::from_span_and_iri(
                span,
                Iri::from_span_and_content(span, iri),
            )),
        }
    }

    /// Return the [ParserContext] of the underlying expression type.
    pub fn context_type(&self) -> ParserContext {
        self.kind.context_type()
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::PathItem);

impl<'a> ProgramAST<'a> for N3PathItem<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        self.kind.children()
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(CONTEXT, N3PathItemKind::parse)(input).map(|(rest, kind)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    kind,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

/// A Notation3 path.
#[derive(Clone, Debug)]
pub struct N3Path<'a> {
    span: Span<'a>,

    pub(crate) items: Vec<(N3PathDirection, N3PathItem<'a>)>,
}

impl<'a> N3Path<'a> {
    pub(crate) fn from_span_and_iri(span: Span<'a>, iri: &'a str) -> Self {
        Self {
            span,
            items: vec![(
                N3PathDirection::Forward,
                N3PathItem::from_span_and_iri(span, iri),
            )],
        }
    }

    /// Return an iterator over the path components.
    pub fn iter(&self) -> impl Iterator<Item = &(N3PathDirection, N3PathItem<'a>)> {
        self.items.iter()
    }

    pub(crate) fn reverse(path: N3Path<'a>) -> Self {
        Self {
            span: path.span(),
            items: path
                .items
                .into_iter()
                .map(|(direction, item)| (!direction, item))
                .collect(),
        }
    }
}

const PATH_CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Path);

impl<'a> ProgramAST<'a> for N3Path<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for (_, child) in &self.items {
            result.push(child);
        }

        result
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            PATH_CONTEXT,
            pair(
                N3PathItem::parse,
                many0(pair(N3PathDirection::parse, N3PathItem::parse)),
            ),
        )(input)
        .map(|(rest, (first, mut further))| {
            let rest_span = rest.span;
            let mut items = vec![(N3PathDirection::Forward, first)];
            items.append(&mut further);

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    items,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        PATH_CONTEXT
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches;

    use nom::combinator::all_consuming;

    use crate::parser::{
        ParserState,
        ast::{
            ProgramAST,
            n3::expression::path::{N3Path, N3PathItem, N3PathItemKind},
        },
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_paths() {
        let iri = r#":doerthe"#;

        let parser_input = ParserInput::new(iri, ParserState::default());
        let result = all_consuming(N3PathItem::parse)(parser_input);

        assert_matches!(
            result,
            Ok((
                _,
                N3PathItem {
                    kind: N3PathItemKind::Iri(_),
                    ..
                }
            ))
        );

        let path = r#":joe!:hasMother^:hasMother"#;

        let parser_input = ParserInput::new(path, ParserState::default());
        let result = all_consuming(N3Path::parse)(parser_input);

        assert_matches!(result, Ok((_, N3Path { .. })));
    }
}
