use strum_macros::EnumIter;
use nemo::parser::context::ParserContext;
use tower_lsp::lsp_types::SemanticTokenType;

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
    pub(super) fn from_parser_context(ctx: ParserContext) -> Option<TokenType> {
        match ctx {
            ParserContext::DataType => Some(TokenType::Type),
            ParserContext::Variable => Some(TokenType::Variable),
            ParserContext::Iri | ParserContext::Constant | ParserContext::RdfLiteral | ParserContext::Blank | ParserContext::String => Some(TokenType::String),
            ParserContext::StructureTag => Some(TokenType::Function),
            ParserContext::Number => Some(TokenType::Number),
            ParserContext::Boolean => Some(TokenType::Bool),
            //ParserContext::Base
            //| ParserContext::Declare
            //| ParserContext::Export
            //| ParserContext::Import
            //| ParserContext::Output
            //| ParserContext::Prefix
            //| ParserContext::UnknownDirective => Some(TokenType::Property),
            ParserContext::Negation
            | ParserContext::AggregationTag
            | ParserContext::OperationTag
            | ParserContext::Infix => Some(TokenType::Operator),
            ParserContext::Comment | ParserContext::DocComment | ParserContext::TopLevelComment => {
                Some(TokenType::Comment)
            }
            _ => None,
        }
    }

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
