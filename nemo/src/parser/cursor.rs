//! The parser's state, a cursor over the token buffer,
//! and the basic operations that the grammar uses to read the buffer.

use std::borrow::Cow;

use nemo_physical::{
    diagnostics::{Diagnostic, Location},
    value::Iri,
};

use crate::{
    diagnostics::ParseError,
    model::{FileId, Span, Statement, Term},
    parser::lexer::{self, Token, TokenKind},
};

/// Returns a diagnostic of parsing `source` in which every label has the line and column
/// at which its span starts, as its location without a name.
///
/// `lines` must be the [line starts](Span::line_starts) of `source`.
fn located(diagnostic: Diagnostic, source: &str, lines: &[usize]) -> Diagnostic {
    diagnostic.with_locations(|span| {
        let (line, column) = span.line_column_in(source, lines);
        Some(Location {
            source: None,
            line,
            column,
        })
    })
}

/// The state of a parser over one token buffer.
///
/// The functions of the grammar are its methods, spread over the modules of the parser.
pub(super) struct Parser<'a> {
    /// The source text that the spans of the tokens point into.
    source: &'a str,
    /// The token buffer, ending with [TokenKind::End].
    tokens: Vec<Token>,
    /// The index of the next token in the buffer.
    position: usize,
    /// The diagnostics reported so far.
    diagnostics: Vec<Diagnostic>,
    /// The current nesting depth of terms.
    depth: usize,
    /// The name of the end of the token buffer in messages, such as `the end of the input`.
    end: &'static str,
}

/// The deepest nesting of terms that the parser accepts.
///
/// The term parser is recursive,
/// so deeper nesting is reported as an error instead of overflowing the stack.
pub(super) const MAX_NESTING: usize = 128;

impl<'a> Parser<'a> {
    /// Creates a parser over a token buffer, starting at the given nesting depth.
    ///
    /// `end` is the name of the buffer's end in messages.
    fn over(source: &'a str, tokens: Vec<Token>, depth: usize, end: &'static str) -> Self {
        Self {
            source,
            tokens,
            position: 0,
            diagnostics: Vec::new(),
            depth,
            end,
        }
    }

    /// Tokenizes a whole source text and creates a parser over its tokens,
    /// keeping the diagnostics of the lexer.
    fn new(source: &'a str, file: FileId) -> Self {
        let (tokens, diagnostics) = lexer::tokenize(source, file);
        Self {
            diagnostics,
            ..Self::over(source, tokens, 0, "the end of the input")
        }
    }

    /// Parses one item with `parse`, and requires the item to span the whole token buffer.
    ///
    /// `(tokens, diagnostics)` is the output of the lexer.
    /// If the lexer reported a diagnostic, the first one is returned as the error.
    /// `depth` is the nesting depth to start at, and `end` is the name of the buffer's end
    /// in messages.
    fn whole<T>(
        source: &'a str,
        (tokens, diagnostics): (Vec<Token>, Vec<Diagnostic>),
        depth: usize,
        end: &'static str,
        parse: impl FnOnce(&mut Self) -> Result<T, Diagnostic>,
    ) -> Result<T, Diagnostic> {
        if let Some(problem) = diagnostics.into_iter().next() {
            return Err(problem);
        }
        let mut parser = Self::over(source, tokens, depth, end);
        let parsed = parse(&mut parser)?;
        if !parser.at(&TokenKind::End) {
            return Err(parser.unexpected(end));
        }
        Ok(parsed)
    }

    /// Parses the statements of a whole source text.
    ///
    /// A statement that does not parse becomes a diagnostic,
    /// and parsing resumes after the next ` .`.
    /// A statement that fails only because it reaches a token that the lexer rejected
    /// adds no diagnostic, since the lexer has already reported one.
    pub(super) fn program(source: &'a str, file: FileId) -> (Vec<Statement>, Vec<Diagnostic>) {
        let mut parser = Self::new(source, file);
        let mut statements = Vec::new();
        while !parser.at(&TokenKind::End) {
            match parser.statement() {
                Ok(statement) => statements.push(statement),
                Err(diagnostic) => {
                    let repeats_lexer =
                        parser.at(&TokenKind::Error) && diagnostic.code() == crate::codes::EXPECTED;
                    if !repeats_lexer {
                        parser.diagnostics.push(diagnostic);
                    }
                    parser.recover();
                }
            }
        }
        if parser.diagnostics.is_empty() {
            return (statements, parser.diagnostics);
        }
        let lines = Span::line_starts(source);
        let diagnostics = parser
            .diagnostics
            .into_iter()
            .map(|diagnostic| located(diagnostic, source, &lines))
            .collect();
        (statements, diagnostics)
    }

    /// Parses one component with `parse`, and requires it to span the whole input.
    ///
    /// The spans of the component and of the error have the file id 0.
    pub(super) fn component<T>(
        source: &str,
        parse: impl FnOnce(&mut Parser<'_>) -> Result<T, Diagnostic>,
    ) -> Result<T, Diagnostic> {
        Parser::whole(
            source,
            lexer::tokenize(source, FileId::new(0)),
            0,
            "the end of the input",
            parse,
        )
        .map_err(|problem| located(problem, source, &Span::line_starts(source)))
    }

    /// Parses the term that `span` covers in the source text,
    /// such as an expression inside a format string.
    ///
    /// The nesting depth continues from the current one,
    /// so format strings nested in format strings meet the same [MAX_NESTING] limit
    /// as any other term.
    pub(super) fn embedded(&self, span: Span) -> Result<Term, Diagnostic> {
        let slice = &self.source[span.start as usize..span.end as usize];
        Parser::whole(
            self.source,
            lexer::tokenize_at(slice, span.file, span.start as usize),
            self.depth,
            "the end of the embedded expression",
            Self::term,
        )
    }

    /// Runs `parse` one level of nesting deeper,
    /// or returns an error if that level would exceed [MAX_NESTING].
    ///
    /// Every recursive step of the term parser must go through this function,
    /// so that the limit holds.
    pub(super) fn nested<T>(
        &mut self,
        parse: impl FnOnce(&mut Self) -> Result<T, Diagnostic>,
    ) -> Result<T, Diagnostic> {
        if self.depth >= MAX_NESTING {
            return Err(ParseError::TooDeep { limit: MAX_NESTING }.at(Some(self.peek_span())));
        }
        self.depth += 1;
        let parsed = parse(self);
        self.depth -= 1;
        parsed
    }

    /// Returns the token `n` places after the next one,
    /// or the final [End](TokenKind::End) token if the buffer ends before it.
    pub(super) fn lookahead(&self, n: usize) -> &Token {
        &self.tokens[(self.position + n).min(self.tokens.len() - 1)]
    }

    /// Returns the next token's kind.
    pub(super) fn peek(&self) -> &TokenKind {
        &self.lookahead(0).kind
    }

    /// Returns the kind of the token after the next one.
    pub(super) fn peek_second(&self) -> &TokenKind {
        &self.lookahead(1).kind
    }

    /// Returns the next token's span.
    pub(super) fn peek_span(&self) -> Span {
        self.lookahead(0).span
    }

    /// Returns the span of the most recently consumed token,
    /// or of the first token if none has been consumed.
    pub(super) fn previous_span(&self) -> Span {
        self.tokens[self.position.saturating_sub(1)].span
    }

    /// Returns whether the next token has the given kind.
    pub(super) fn at(&self, kind: &TokenKind) -> bool {
        self.peek() == kind
    }

    /// Returns whether the next token is the given bare name.
    ///
    /// The grammar's contextual keywords, such as the `per` of an aggregate,
    /// are bare names that it recognizes only in certain positions.
    pub(super) fn at_word(&self, word: &str) -> bool {
        self.at(&TokenKind::Ident) && self.text(self.lookahead(0)) == word
    }

    /// Consumes and returns the next token.
    ///
    /// At the final [End](TokenKind::End) token, the position stays where it is.
    pub(super) fn advance(&mut self) -> Token {
        let token = self.tokens[self.position].clone();
        if self.position + 1 < self.tokens.len() {
            self.position += 1;
        }
        token
    }

    /// Consumes and returns the next token if it has the given kind.
    pub(super) fn eat(&mut self, kind: &TokenKind) -> Option<Token> {
        self.at(kind).then(|| self.advance())
    }

    /// Consumes and returns the next token,
    /// or returns an error if it does not have the given kind.
    ///
    /// `what` describes the expected token in the error.
    pub(super) fn expect(&mut self, kind: &TokenKind, what: &str) -> Result<Token, Diagnostic> {
        self.eat(kind).ok_or_else(|| self.unexpected(what))
    }

    /// Consumes the next token, or returns an error if it is not the given bare name.
    ///
    /// `what` describes the expected token in the error if the next token is not a bare name,
    /// and `error` builds the error if it is a different name.
    pub(super) fn expect_word(
        &mut self,
        word: &str,
        what: &str,
        error: impl FnOnce() -> ParseError,
    ) -> Result<(), Diagnostic> {
        let token = self.expect(&TokenKind::Ident, what)?;
        if self.text(&token) != word {
            return Err(error().at(Some(token.span)));
        }
        Ok(())
    }

    /// Returns the source text of a token without its first `front` and its last `back` bytes.
    fn slice(&self, token: &Token, front: usize, back: usize) -> &'a str {
        &self.source[token.span.start as usize + front..token.span.end as usize - back]
    }

    /// Returns the source text of a token.
    pub(super) fn text(&self, token: &Token) -> &'a str {
        self.slice(token, 0, 0)
    }

    /// Returns the source text a span covers.
    pub(super) fn span_text(&self, span: Span) -> &'a str {
        &self.source[span.start as usize..span.end as usize]
    }

    /// Returns the source text of a token without its first character,
    /// such as the `?` of a variable or the `@` of a directive.
    pub(super) fn marked_text(&self, token: &Token) -> &'a str {
        self.slice(token, 1, 0)
    }

    /// Returns the IRI of an IRI token,
    /// which is its text without the angle brackets and with its escapes decoded.
    pub(super) fn iri_text(&self, token: &Token) -> Cow<'a, str> {
        Iri::unescape(self.slice(token, 1, 1))
    }

    /// Returns the label of a blank node token, without its `_:`.
    pub(super) fn blank_label(&self, token: &Token) -> &'a str {
        self.slice(token, 2, 0)
    }

    /// Returns whether two spans touch without any characters between them.
    pub(super) fn adjacent(left: Span, right: Span) -> bool {
        left.end == right.start
    }

    /// Returns the span from the start of `start` to the end of the last consumed token.
    pub(super) fn span_from(&self, start: Span) -> Span {
        let end = self.tokens[self.position.saturating_sub(1)].span.end;
        Span {
            file: start.file,
            start: start.start,
            end,
        }
    }

    /// Returns the error that the next token is not the `expected` one.
    ///
    /// The error describes a string or a format string by its kind,
    /// and quotes any other token only as far as [excerpt] does.
    /// A token can span many lines or megabytes, and a message is one line.
    pub(super) fn unexpected(&self, expected: &str) -> Diagnostic {
        let token = &self.tokens[self.position];
        let found = match &token.kind {
            TokenKind::End => String::from(self.end),
            TokenKind::String(_) => String::from("a string literal"),
            TokenKind::FString(_) => String::from("a format string"),
            _ => format!("`{}`", excerpt(self.text(token))),
        };
        ParseError::Expected {
            expected: expected.to_owned(),
            found,
        }
        .at(Some(token.span))
    }

    /// Skips past the next ` .` token, which ends a statement, or to the end of the input.
    fn recover(&mut self) {
        loop {
            match self.peek() {
                TokenKind::End => return,
                TokenKind::Dot => {
                    self.advance();
                    return;
                }
                _ => {
                    self.advance();
                }
            }
        }
    }
}

/// The maximum number of characters of a token that a message quotes.
const EXCERPT: usize = 40;

/// Returns the part of a token's text that a message quotes.
///
/// This is the first line of the text, cut at [EXCERPT] characters,
/// with an ellipsis if anything is left out.
pub(super) fn excerpt(text: &str) -> String {
    let line = text.lines().next().unwrap_or("");
    match line.char_indices().nth(EXCERPT) {
        Some((cut, _)) => format!("{}…", &line[..cut]),
        None if line.len() < text.len() => format!("{line}…"),
        None => line.to_owned(),
    }
}
