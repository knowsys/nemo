//! Tokens: the lexer's output and the parser's input.

use crate::model::{Span, VariableKind};

/// The kinds of tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::parser) enum TokenKind {
    /// A bare name like `parent`.
    Ident,
    /// A variable, with its kind marker consumed.
    Variable(VariableKind),
    /// A directive keyword with its `@`, like `@prefix`.
    At,
    /// An aggregate name with its `#`, like `#sum`.
    Hash,
    /// An IRI in angle brackets, whose content is the token text without the brackets.
    Iri,
    /// A blank node `_:label`, whose label is the token text after the `_:`.
    BlankNode,
    /// A number literal (integer, decimal, or exponent form).
    Number,
    /// A string literal, carrying its value with the escapes processed.
    String(String),
    /// A format string `f"…"`: literal text pieces (escapes processed)
    /// interleaved with embedded expressions, kept as source regions.
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
    /// `@lang` directly after a string literal.
    LanguageTag,
    /// A character sequence no token matches.
    Error,
    /// The end of the input.
    End,
}

/// One piece of a format string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::parser) enum FStringPart {
    /// Literal text, escapes processed.
    Text(String),
    /// An embedded expression's source region (inside the braces).
    Expression(Span),
}

/// One token: its kind and source region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::parser) struct Token {
    /// The token's kind.
    pub(in crate::parser) kind: TokenKind,
    /// The token's source region.
    pub(in crate::parser) span: Span,
}
