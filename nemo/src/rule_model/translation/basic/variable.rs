use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::{term::primitive::variable::Variable, ProgramComponent},
        error::{hint::Hint, translation_error::TranslationErrorKind, TranslationError},
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for Variable {
    type Ast<'a> = ast::expression::basic::variable::Variable<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        variable: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let result = match variable.kind() {
            ast::expression::basic::variable::VariableType::Universal => {
                if let Some(variable_name) = variable.name() {
                    Variable::universal(&variable_name)
                        .set_origin(translation.register_node(variable))
                } else {
                    return Err(TranslationError::new(
                        variable.span(),
                        TranslationErrorKind::UnnamedVariable,
                    )
                    .add_hint(Hint::AnonymousVariables));
                }
            }
            ast::expression::basic::variable::VariableType::Existential => {
                if let Some(variable_name) = variable.name() {
                    Variable::existential(&variable_name)
                        .set_origin(translation.register_node(variable))
                } else {
                    return Err(TranslationError::new(
                        variable.span(),
                        TranslationErrorKind::UnnamedVariable,
                    ));
                }
            }
            ast::expression::basic::variable::VariableType::Anonymous => {
                if variable.name().is_none() {
                    Variable::anonymous().set_origin(translation.register_node(variable))
                } else {
                    return Err(TranslationError::new(
                        variable.span(),
                        TranslationErrorKind::NamedAnonymous(
                            variable.span().fragment().to_string(),
                        ),
                    ));
                }
            }
        };

        Ok(translation.register_component(result, variable))
    }
}
