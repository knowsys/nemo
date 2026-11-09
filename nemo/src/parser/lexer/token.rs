//! Tokens, which the lexer produces and the parser reads.

use crate::model::{Span, VariableKind};

/// The kinds of tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::parser) enum TokenKind {
    /// A bare name like `parent`, or the local name after the colon of a prefixed name.
    Ident,
    /// A variable, including the marker that gives its kind, like `?x`.
    Variable(VariableKind),
    /// A directive's name with its `@`, like `@prefix`.
    At,
    /// An aggregate's name with its `#`, like `#sum`.
    Hash,
    /// An IRI in angle brackets.
    ///
    /// The IRI is the token's text without the brackets, with escapes decoded.
    Iri,
    /// A blank node `_:label`, whose label is the token's text after the `_:`.
    BlankNode,
    /// A number literal, such as `42`, `1.5`, `2e10`, `0xFF` or `1.5f`.
    Number,
    /// A string literal, with its value, in which escapes are decoded.
    String(String),
    /// A format string `f"…"`, with its parts.
    FString(Vec<FStringPart>),
    /// `(`
    ParenOpen,
    /// `)`
    ParenClose,
    /// `{`
    BraceOpen,
    /// `}`
    BraceClose,
    /// `#[`
    AttributeOpen,
    /// `[`
    BracketOpen,
    /// `]`
    BracketClose,
    /// `,`
    Comma,
    /// `.`
    Dot,
    /// `:`
    Colon,
    /// `::`
    DoubleColon,
    /// `:-`
    ArrowLeft,
    /// `->`
    ArrowRight,
    /// `~`
    Tilde,
    /// `|`
    Bar,
    /// `=`
    Equal,
    /// `!=`
    Unequal,
    /// `==`
    ValueEqual,
    /// `!==`
    ValueUnequal,
    /// `<`
    Less,
    /// `<=`
    LessEq,
    /// `>`
    Greater,
    /// `>=`
    GreaterEq,
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `*`
    Star,
    /// `/`
    Slash,
    /// `^^`
    Carets,
    /// A language tag like `@en`, directly after a string literal.
    LanguageTag,
    /// A text that the lexer rejected and reported a diagnostic for.
    Error,
    /// The end of the input.
    End,
}

impl TokenKind {
    /// Returns whether a token of this kind ends the term before it, rather than starting one.
    ///
    /// These are the closing brackets, `,`, `.`, the arrows of rules, the end of the input,
    /// and the operators that only stand between two terms, such as `=`, `*` or `^^`.
    pub(in crate::parser) fn ends_term(&self) -> bool {
        matches!(
            self,
            TokenKind::ParenClose
                | TokenKind::BracketClose
                | TokenKind::BraceClose
                | TokenKind::Comma
                | TokenKind::Dot
                | TokenKind::ArrowLeft
                | TokenKind::ArrowRight
                | TokenKind::Bar
                | TokenKind::Equal
                | TokenKind::Unequal
                | TokenKind::ValueEqual
                | TokenKind::ValueUnequal
                | TokenKind::Less
                | TokenKind::LessEq
                | TokenKind::Greater
                | TokenKind::GreaterEq
                | TokenKind::Star
                | TokenKind::Slash
                | TokenKind::Carets
                | TokenKind::End
        )
    }
}

/// One part of a format string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::parser) enum FStringPart {
    /// Literal text, with escapes decoded.
    Text(String),
    /// The span of an embedded expression, without its braces.
    Expression(Span),
}

/// One token of the source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::parser) struct Token {
    /// The token's kind.
    pub(in crate::parser) kind: TokenKind,
    /// The token's location in the source text.
    pub(in crate::parser) span: Span,
}
