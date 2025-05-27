use crate::{
    parser::ast::{self},
    rule_model::{
        components::{
            parameter::ParameterDeclaration,
            term::{primitive::variable::Variable, Term},
        },
        error::translation_error::TranslationError,
        origin::Origin,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for ParameterDeclaration {
    type Ast<'a> = ast::directive::parameter::ParameterDeclaration<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        ast: &Self::Ast<'a>,
    ) -> Option<Self> {
        let Variable::Global(variable) = Variable::build_component(translation, ast.variable())?
        else {
            translation
                .report
                .add(ast.variable(), TranslationError::ParamDeclarationNotGlobal);
            return None;
        };

        let mut result = Origin::ast(Self::new(variable), ast);

        if let Some(expression) = ast.expression() {
            let term = Term::build_component(translation, expression)?;
            result.set_expression(term);
        }

        Some(result)
    }
}
