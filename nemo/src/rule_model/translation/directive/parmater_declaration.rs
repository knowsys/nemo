use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::{
            parameter::ParameterDeclaration,
            term::{
                primitive::{variable::Variable, Primitive},
                Term,
            },
            IterablePrimitives, ProgramComponent,
        },
        error::{translation_error::TranslationErrorKind, TranslationError},
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for ParameterDeclaration {
    type Ast<'a> = ast::directive::parameter::ParameterDeclaration<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        ast: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let Variable::Global(variable) = Variable::build_component(translation, ast.variable())?
        else {
            return Err(TranslationError::new(
                ast.variable().span(),
                TranslationErrorKind::ParamDeclarationNotGlobal,
            ));
        };

        let mut result = translation.register_component(Self::new(variable), ast);

        if let Some(expression) = ast.expression() {
            let term = Term::build_component(translation, expression)?;

            if let Some(primitive) = term.primitive_terms().find(|primitive| {
                matches!(
                    primitive,
                    Primitive::Variable(Variable::Universal(_))
                        | Primitive::Variable(Variable::Existential(_))
                )
            }) {
                todo!()

                // return Err(TranslationError::new(
                //     translation
                //         .origin_map
                //         .get(primitive.origin())
                //         .expect("already registered")
                //         .span(),
                //     TranslationErrorKind::ParameterDeclarationNotGroundish,
                // ));
            }

            result.set_expression(term);
        }

        Ok(result)
    }
}
