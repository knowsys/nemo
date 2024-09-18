use strum_macros::EnumIter;
use nemo::parser::context::ParserContext;
use tower_lsp::lsp_types::SemanticTokenType;

/// All syntax highlighting types that are used in Nemo programs
#[derive(Copy, Clone, EnumIter)]
#[repr(u32)]
pub(super) enum TokenType {
    Type,
    Variable,
    String,
    Function,
    Number,
    Bool,
    Property,
    Operator,
    Comment,
}

impl TokenType {
    /// ParserContext (i.e. AST node types) are mapped to syntax highlighting types or None if they
    /// shall not be highlighted
    pub(super) fn from_parser_context(ctx: ParserContext) -> Option<TokenType> {
        match ctx {
            ParserContext::DataType => Some(TokenType::Type),
            ParserContext::Variable => Some(TokenType::Variable),
            ParserContext::Iri | ParserContext::Constant | ParserContext::RdfLiteral | ParserContext::Blank | ParserContext::String => Some(TokenType::String),
            ParserContext::StructureTag => Some(TokenType::Function),
            ParserContext::Number => Some(TokenType::Number),
            ParserContext::Boolean => Some(TokenType::Bool),
            ParserContext::Negation
            | ParserContext::AggregationTag
            | ParserContext::OperationTag
            | ParserContext::Infix => Some(TokenType::Operator),
            ParserContext::Comment | ParserContext::DocComment | ParserContext::TopLevelComment => {
                Some(TokenType::Comment)
            }
            // TODO: imports, base, etc. (everything starting with @ should be handled via
            // ParserContext::Token {} but this requires changes to the children method of the AST
            // nodes in the nemo crate)
            _ => None,
        }
    }

    /// The TokenType is translated to a syntax highlighting type that is understood by the LSP
    pub(super) fn to_semantic_token_type(self) -> SemanticTokenType {
        match self {
            Self::Type => SemanticTokenType::TYPE,
            Self::Variable => SemanticTokenType::VARIABLE,
            Self::String => SemanticTokenType::STRING,
            Self::Function => SemanticTokenType::FUNCTION,
            Self::Number => SemanticTokenType::NUMBER,
            Self::Bool => SemanticTokenType::new("bool"),
            Self::Property => SemanticTokenType::PROPERTY,
            Self::Operator => SemanticTokenType::OPERATOR,
            Self::Comment => SemanticTokenType::COMMENT,
        }
    }
}
