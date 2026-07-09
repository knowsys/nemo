//! This module defines [N3Statement].

use nom::{
    branch::alt,
    combinator::{map, opt, verify},
    sequence::{preceded, tuple},
};

use crate::{
    parser::{
        ParserResult,
        ast::{
            directive::{Directive, base::Base, prefix::Prefix},
            expression::basic::iri::Iri,
        },
        context::{ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    syntax,
};

use super::{
    super::{
        ProgramAST,
        n3::comment::{N3Comment, WSoC},
        token::Token,
    },
    triples::N3Triples,
};

use super::directive::N3Directive;

#[allow(clippy::large_enum_variant)]
/// Types of [Statement]s
#[derive(Clone, Debug)]
pub enum N3StatementKind<'a> {
    /// Triple
    Triples(N3Triples<'a>),
    /// Directive
    Directive(N3Directive<'a>),
    /// This represents a statement that has an error that could not get recovered in a child node.
    Error(Token<'a>),
}

impl<'a> N3StatementKind<'a> {
    /// Return the [ParserContext] of the underlying statement.
    pub fn context(&self) -> ParserContext {
        match self {
            Self::Triples(statement) => statement.context(),
            Self::Directive(statement) => statement.context(),
            Self::Error(_statement) => ParserContext::Error,
        }
    }

    /// Parse the [StatementKind].
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        context(
            ParserContext::StatementKind,
            alt((
                map(N3Directive::parse, Self::Directive),
                map(N3Triples::parse, Self::Triples),
            )),
        )(input)
    }
}

/// Statement in a program
#[derive(Clone, Debug)]
pub struct N3Statement<'a> {
    /// [Span] associated with this node
    pub(crate) span: Span<'a>,

    /// Comment associated with this statement
    pub(crate) comment: Option<N3Comment<'a>>,
    /// The statement
    pub(crate) kind: N3StatementKind<'a>,
}

impl<'a> N3Statement<'a> {
    /// Return the comment attached to this statement,
    /// if there is any
    pub fn comment(&self) -> Option<&N3Comment<'a>> {
        self.comment.as_ref()
    }

    /// Return the [N3StatementKind].
    pub fn kind(&self) -> &N3StatementKind<'a> {
        &self.kind
    }

    fn parse_sparql_directive(input: ParserInput<'a>) -> ParserResult<'a, N3StatementKind<'a>> {
        enum SparqlDirective<'a> {
            Base(Iri<'a>),
            Prefix(Token<'a>, Iri<'a>),
        }

        let input_span = input.span;

        alt((
            map(
                context(
                    ParserContext::Base,
                    preceded(
                        tuple((
                            verify(Token::name, |tag| {
                                tag.span()
                                    .fragment()
                                    .eq_ignore_ascii_case(syntax::directive::BASE)
                            }),
                            WSoC::parse,
                        )),
                        Base::parse_body,
                    ),
                ),
                SparqlDirective::Base,
            ),
            map(
                context(
                    ParserContext::Prefix,
                    preceded(
                        tuple((
                            verify(Token::name, |tag| {
                                tag.span()
                                    .fragment()
                                    .eq_ignore_ascii_case(syntax::directive::PREFIX)
                            }),
                            WSoC::parse,
                        )),
                        Prefix::parse_body,
                    ),
                ),
                |(prefix, iri)| SparqlDirective::Prefix(prefix, iri),
            ),
        ))(input)
        .map(|(rest, directive)| {
            let rest_span = rest.span;

            (
                rest,
                N3StatementKind::Directive(
                    N3Directive::new(match directive {
                        SparqlDirective::Base(iri) => Directive::Base(Base::from_span_and_iri(
                            input_span.until_rest(&rest_span),
                            iri,
                        )),
                        SparqlDirective::Prefix(prefix, iri) => {
                            Directive::Prefix(Prefix::from_span_prefix_and_iri(
                                input_span.until_rest(&rest_span),
                                prefix,
                                iri,
                            ))
                        }
                    })
                    .expect("is a valid directive"),
                ),
            )
        })
    }
}

const CONTEXT: ParserContext = ParserContext::Statement;

impl<'a> ProgramAST<'a> for N3Statement<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match &self.kind {
            N3StatementKind::Triples(statement) => vec![statement],
            N3StatementKind::Directive(statement) => vec![statement],
            N3StatementKind::Error(_) => vec![],
        }
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
            CONTEXT,
            tuple((
                opt(N3Comment::parse),
                WSoC::parse,
                alt((Self::parse_sparql_directive, N3StatementKind::parse)),
            )),
        )(input)
        .map(|(rest, (comment, _, statement))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    comment,
                    kind: statement,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;
    use std::assert_matches;
    use test_log::test;

    use crate::parser::{
        ParserState,
        ast::{
            ProgramAST,
            n3::statement::{N3Statement, N3StatementKind},
        },
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_triple() {
        let triple = r#":doerthe a :Person"#;

        let parser_input = ParserInput::new(triple, ParserState::default());
        let result = N3StatementKind::parse(parser_input);

        assert_matches!(result, Ok(_));

        let parser_input = ParserInput::new(triple, ParserState::default());
        let result = all_consuming(N3Statement::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_prefix() {
        let prefix = r#"@prefix : <http://example.com/>"#;

        let parser_input = ParserInput::new(prefix, ParserState::default());
        let result = N3StatementKind::parse(parser_input);

        assert_matches!(result, Ok(_));

        let parser_input = ParserInput::new(prefix, ParserState::default());
        let result = all_consuming(N3Statement::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_rule() {
        // from the N3 Test suite
        let rule = r#"{ ?x a :Person } => { ?x a :Animal }"#;

        let parser_input = ParserInput::new(rule, ParserState::default());
        let result = all_consuming(N3Statement::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_sparql_directives() {
        let prefix = r#"pReFiX : <http://example.com/>"#;
        let base = r#"bAsE <http://example.com/>"#;

        let parser_input = ParserInput::new(prefix, ParserState::default());
        let result = all_consuming(N3Statement::parse)(parser_input);

        assert_matches!(result, Ok(_));

        let parser_input = ParserInput::new(base, ParserState::default());
        let result = all_consuming(N3Statement::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }
}
