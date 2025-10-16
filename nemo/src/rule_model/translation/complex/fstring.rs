use crate::{
    newtype_wrapper,
    parser::ast::{self, expression::complex::fstring::FormatStringElement},
    rule_model::{
        components::term::{
            Term,
            operation::{Operation, operation_kind::OperationKind},
        },
        origin::Origin,
        translation::TranslationComponent,
    },
    syntax::expression::format_string::{EXPRESSION_END, EXPRESSION_START},
};

pub(crate) struct FormatStringLiteral(Operation);
newtype_wrapper!(FormatStringLiteral: Operation);

impl TranslationComponent for FormatStringLiteral {
    type Ast<'a> = ast::expression::complex::fstring::FormatString<'a>;

    fn build_component<'a>(
        translation: &mut crate::rule_model::translation::ASTProgramTranslation,
        format_string: &Self::Ast<'a>,
    ) -> Option<Self> {
        let mut subterms = Vec::new();

        for element in format_string.elements() {
            let term = match element {
                FormatStringElement::String(token) => Term::from(token.to_string()),
                FormatStringElement::Expression(expression) => {
                    let inner_term = Term::build_component(translation, expression)?;
                    let string_conversion =
                        Operation::new(OperationKind::LexicalValue, vec![inner_term]);
                    Term::from(string_conversion)
                }
                FormatStringElement::EscapedStart => Term::from(EXPRESSION_START),
                FormatStringElement::EscapedEnd => Term::from(EXPRESSION_END),
            };

            subterms.push(term);
        }

        Some(FormatStringLiteral(Origin::ast(
            Operation::new(OperationKind::StringConcatenation, subterms),
            format_string,
        )))
    }
}
