//! This module contains a function to create a variable
//! from the corresponding ast node.

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::term::primitive::variable::Variable,
        error::{hint::Hint, translation_error::TranslationError},
        origin::Origin,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for Variable {
    type Ast<'a> = ast::expression::basic::variable::Variable<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        variable: &Self::Ast<'a>,
    ) -> Option<Self> {
        let result = match variable.kind() {
            ast::expression::basic::variable::VariableType::Universal => {
                if let Some(variable_name) = variable.name() {
                    Variable::universal(&variable_name)
                } else {
                    translation
                        .report
                        .add(variable, TranslationError::UnnamedVariable)
                        .add_hint(Hint::AnonymousVariables);

                    return None;
                }
            }
            ast::expression::basic::variable::VariableType::Existential => {
                if let Some(variable_name) = variable.name() {
                    Variable::existential(&variable_name)
                } else {
                    translation
                        .report
                        .add(variable, TranslationError::UnnamedVariable);

                    return None;
                }
            }
            ast::expression::basic::variable::VariableType::Anonymous => {
                if variable.name().is_none() {
                    Variable::anonymous()
                } else {
                    translation.report.add(
                        variable,
                        TranslationError::NamedAnonymous {
                            name: variable.span().fragment().to_string(),
                        },
                    );

                    return None;
                }
            }
            ast::expression::basic::variable::VariableType::Global => {
                let Some(variable_name) = variable.name() else {
                    translation
                        .report
                        .add(variable, TranslationError::UnnamedVariable);
                    return None;
                };

                Variable::global(&variable_name)
            }
        };

        Some(Origin::ast(result, variable))
    }
}
