//! This module contains a function to create an operation term
//! from the corresponding ast node.

use crate::{
    newtype_wrapper,
    parser::ast::{self},
    rule_model::{
        components::term::{Term, operation::Operation},
        origin::Origin,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

pub(crate) struct FunctionLikeOperation(Operation);
newtype_wrapper!(FunctionLikeOperation: Operation);

impl TranslationComponent for FunctionLikeOperation {
    type Ast<'a> = ast::expression::complex::operation::Operation<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        operation: &Self::Ast<'a>,
    ) -> Option<Self> {
        let kind = operation.kind();
        let mut subterms = Vec::new();
        for expression in operation.expressions() {
            subterms.push(Term::build_component(translation, expression)?);
        }

        Some(FunctionLikeOperation(Origin::ast(
            Operation::new(kind, subterms),
            operation,
        )))
    }
}
