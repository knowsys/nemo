//! This module defines [Atom].

use nom::sequence::{delimited, pair};

use crate::{
    parser::{
        ast::{
            comment::wsoc::WSoC,
            expression::Expression,
            sequence::simple::ExpressionSequenceSimple,
            tag::structure::StructureTag,
            token::{Token, TokenKind},
            ProgramAST,
        },
        context::{context, ParserContext},
        input::ParserInput,
        span::Span,
        ParserResult,
    },
    syntax::pretty_printing::fits_on_line,
};

/// A possibly tagged sequence of [Expression]s.
#[derive(Debug)]
pub struct Atom<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Tag of this Atom
    tag: StructureTag<'a>,
    /// List of underlying expressions
    expressions: ExpressionSequenceSimple<'a>,
}

impl<'a> Atom<'a> {
    /// Return an iterator over the underlying [Expression]s.
    pub fn expressions(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.expressions.iter()
    }

    /// Return the tag of this atom.
    pub fn tag(&self) -> &StructureTag<'a> {
        &self.tag
    }
}

const CONTEXT: ParserContext = ParserContext::Atom;

impl<'a> ProgramAST<'a> for Atom<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();
        result.push(&self.tag);

        for expression in &self.expressions {
            result.push(expression)
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
            CONTEXT,
            pair(
                StructureTag::parse,
                delimited(
                    pair(Token::atom_open, WSoC::parse),
                    ExpressionSequenceSimple::parse,
                    pair(WSoC::parse, Token::atom_close),
                ),
            ),
        )(input)
        .map(|(rest, (tag, expressions))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    tag,
                    expressions,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }

    fn pretty_print(&self, indent_level: usize) -> Option<String> {
        let mut result = format!(
            "{}{}",
            self.tag.pretty_print(indent_level)?,
            TokenKind::AtomOpen
        );
        let content_indent = indent_level + result.len();
        let terms = self
            .expressions()
            .flat_map(|expression| expression.pretty_print(content_indent))
            .collect::<Vec<_>>();
        let all_terms = terms.join(&format!("{} ", TokenKind::SequenceSeparator));

        let content = if fits_on_line(
            &all_terms,
            content_indent,
            None,
            Some(&format!("{}", TokenKind::AtomClose)),
        ) {
            all_terms
        } else {
            let indent = " ".repeat(content_indent);
            format!(
                "\n{}{}",
                &indent,
                terms.join(&format!("{}\n{}", TokenKind::SequenceSeparator, &indent))
            )
        };

        result.push_str(&content);
        if content.contains('\n') {
            result.push_str(&" ".repeat(content_indent - 1));
        }

        result.push_str(&format!("{}", TokenKind::AtomClose));

        Some(result)
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::complex::atom::Atom, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_atom() {
        let test = vec![
            ("abc(1)", ("abc".to_string(), 1)),
            ("abc(1,2)", ("abc".to_string(), 2)),
            ("abc( 1 )", ("abc".to_string(), 1)),
            ("abc( 1 , 2 )", ("abc".to_string(), 2)),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Atom::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                (result.1.tag().to_string(), result.1.expressions().count())
            );
        }
    }
}
