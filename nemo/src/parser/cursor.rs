//! The parser state: a cursor over the token buffer, and the primitives the grammar reads it by.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{FileId, Span, Statement, Term},
    parser::lexer::{self, Token, TokenKind},
};

/// The parser state over one token buffer.
pub(super) struct Parser<'a> {
    /// The source text, for token content.
    source: &'a str,
    /// The token buffer, ending with [TokenKind::End].
    tokens: Vec<Token>,
    /// The position of the next token.
    position: usize,
    /// The problems found so far.
    diagnostics: Vec<Diagnostic>,
    /// How many terms the term parser is currently nested in.
    depth: usize,
    /// What messages call the end of the token buffer.
    end: &'static str,
}

/// How deeply terms may nest.
///
/// The term parser descends recursively,
/// so deeper nesting is reported as an error instead of overflowing the stack.
pub(super) const MAX_NESTING: usize = 128;

impl<'a> Parser<'a> {
    /// Creates a parser over a token buffer, starting at the given nesting depth.
    ///
    /// `end` names the buffer's end for messages.
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

    /// Creates a parser over a source file, keeping its lexical problems.
    fn new(source: &'a str, file: FileId) -> Self {
        let (tokens, diagnostics) = lexer::tokenize(source, file);
        Self {
            diagnostics,
            ..Self::over(source, tokens, 0, "the end of the input")
        }
    }

    /// Parses one piece that must span a whole token buffer.
    ///
    /// Returns the first lexical problem as the error, if there is one.
    /// `end` names the buffer's end for messages.
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

    /// Parses the statements of a whole source file.
    ///
    /// A statement that fails becomes a diagnostic, and parsing resumes after it.
    /// A statement that fails only because it reaches a token the lexer rejected
    /// adds nothing to the lexer's diagnostic.
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
        (statements, parser.diagnostics)
    }

    /// Parses one component that must span the whole input (file id 0).
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
    }

    /// Parses an embedded expression at its absolute source region,
    /// which is how the expressions of format strings re-enter the term grammar.
    ///
    /// The expression continues the current nesting,
    /// so format strings nested in format strings meet the same limit as any other term.
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

    /// Parses a term at one more level of nesting,
    /// returning an error past [MAX_NESTING] levels.
    ///
    /// Every recursive step of the term parser goes through here.
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
    /// or the final [End](TokenKind::End) where the buffer is shorter.
    pub(super) fn lookahead(&self, n: usize) -> &Token {
        &self.tokens[(self.position + n).min(self.tokens.len() - 1)]
    }

    /// Returns the next token's kind.
    pub(super) fn peek(&self) -> &TokenKind {
        &self.lookahead(0).kind
    }

    /// Returns the kind after the next token.
    pub(super) fn peek_second(&self) -> &TokenKind {
        &self.lookahead(1).kind
    }

    /// Returns the next token's span.
    pub(super) fn peek_span(&self) -> Span {
        self.lookahead(0).span
    }

    /// Returns the most recently consumed token's span,
    /// which adjacency checks compare the next token against.
    pub(super) fn previous_span(&self) -> Span {
        self.tokens[self.position.saturating_sub(1)].span
    }

    /// Returns whether the next token has the given kind.
    pub(super) fn at(&self, kind: &TokenKind) -> bool {
        self.peek() == kind
    }

    /// Returns whether the next token is the given bare word,
    /// a contextual keyword the grammar reads only in its position.
    pub(super) fn at_word(&self, word: &str) -> bool {
        self.at(&TokenKind::Ident) && self.text(self.lookahead(0)) == word
    }

    /// Consumes and returns the next token.
    pub(super) fn advance(&mut self) -> Token {
        let token = self.tokens[self.position].clone();
        if self.position + 1 < self.tokens.len() {
            self.position += 1;
        }
        token
    }

    /// Consumes the next token if it has the given kind.
    pub(super) fn eat(&mut self, kind: &TokenKind) -> Option<Token> {
        self.at(kind).then(|| self.advance())
    }

    /// Consumes the next token, requiring the given kind.
    pub(super) fn expect(&mut self, kind: &TokenKind, what: &str) -> Result<Token, Diagnostic> {
        self.eat(kind).ok_or_else(|| self.unexpected(what))
    }

    /// Consumes the given bare word, requiring it.
    ///
    /// `what` describes the word in the error if the next token is not a word,
    /// and `error` builds the error if it is a different word.
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

    /// Returns the source text of a token, less `front` bytes of markup before it and `back` after.
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

    /// Returns the source text of a token, without its first marker character
    /// (variable, directive, and aggregate names).
    pub(super) fn marked_text(&self, token: &Token) -> &'a str {
        self.slice(token, 1, 0)
    }

    /// Returns the content of an IRI token, without the angle brackets.
    pub(super) fn iri_text(&self, token: &Token) -> &'a str {
        self.slice(token, 1, 1)
    }

    /// Returns the label of a blank node token, without its `_:`.
    pub(super) fn blank_label(&self, token: &Token) -> &'a str {
        self.slice(token, 2, 0)
    }

    /// Returns whether two spans touch without any characters between them.
    pub(super) fn adjacent(left: Span, right: Span) -> bool {
        left.end == right.start
    }

    /// Returns the span from a start position to the end of the last consumed
    /// token.
    pub(super) fn span_from(&self, start: Span) -> Span {
        let end = self.tokens[self.position.saturating_sub(1)].span.end;
        Span {
            file: start.file,
            start: start.start,
            end,
        }
    }

    /// Returns a diagnostic for an unexpected token.
    ///
    /// A string is described by its kind rather than quoted, and any
    /// other token only as far as [EXCERPT] shows it: a token can run
    /// over lines or megabytes, and a message is one line.
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

    /// Skips to the next statement boundary: past the next ` .`, or to
    /// the end of the input.
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

/// How many characters of a token a message quotes.
const EXCERPT: usize = 40;

/// Returns the start of a token's text, as far as a message quotes it: its
/// first line, cut at [EXCERPT] characters, with an ellipsis for what
/// is left out.
pub(super) fn excerpt(text: &str) -> String {
    let line = text.lines().next().unwrap_or("");
    match line.char_indices().nth(EXCERPT) {
        Some((cut, _)) => format!("{}…", &line[..cut]),
        None if line.len() < text.len() => format!("{line}…"),
        None => line.to_owned(),
    }
}
