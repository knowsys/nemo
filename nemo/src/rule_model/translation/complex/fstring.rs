use crate::{
    newtype_wrapper,
    parser::ast::{self},
    rule_model::{
        components::term::{
            operation::{operation_kind::OperationKind, Operation},
            Term,
        },
        origin::Origin,
        translation::TranslationComponent,
    },
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
                ast::expression::complex::fstring::FormatStringElement::String(token) => {
                    Term::from(token.to_string())
                }
                ast::expression::complex::fstring::FormatStringElement::Expression(expression) => {
                    let inner_term = Term::build_component(translation, expression)?;
                    let string_conversion =
                        Operation::new(OperationKind::LexicalValue, vec![inner_term]);
                    Term::from(string_conversion)
                }
            };

            subterms.push(term);
        }

        Some(FormatStringLiteral(Origin::ast(
            Operation::new(OperationKind::StringConcatenation, subterms),
            format_string,
        )))
    }
}
