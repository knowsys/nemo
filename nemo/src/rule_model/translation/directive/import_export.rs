//! This module contains a function for handling import/export statements.

use crate::{
    parser::{
        ast::{self, tag::structure::StructureTag, ProgramAST},
        context::ParserContext,
    },
    rule_model::{
        components::{
            import_export::{
                attribute::ImportExportAttribute, specification::ImportExportSpec, ExportDirective,
                ImportDirective,
            },
            tag::Tag,
            term::{operation::Operation, Term},
        },
        error::{translation_error::TranslationErrorKind, TranslationError},
        translation::{
            complex::infix::InfixOperation, ASTProgramTranslation, TranslationComponent,
        },
    },
};

impl TranslationComponent for ImportExportSpec {
    type Ast<'a> = ast::expression::complex::map::Map<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        map: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let mut subterms = Vec::new();
        for (key, value) in map.key_value() {
            let key = ImportExportAttribute::build_component(translation, key)?;
            let value = Term::build_component(translation, value)?;

            subterms.push((key, value));
        }

        let format = map.tag().map(StructureTag::to_string).unwrap_or_default();

        let result = ImportExportSpec::new(&format, subterms);
        Ok(translation.register_component(result, map))
    }
}

impl TranslationComponent for ImportExportAttribute {
    type Ast<'a> = ast::expression::Expression<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        expression: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        if let ast::expression::Expression::Constant(constant) = expression {
            let result = ImportExportAttribute::new(constant.tag().to_string());
            Ok(translation.register_component(result, expression))
        } else {
            Err(TranslationError::new(
                expression.span(),
                TranslationErrorKind::KeyWrongType {
                    found: expression.context_type().name().to_owned(),
                    expected: ParserContext::Constant.name().to_owned(),
                },
            ))
        }
    }
}

/// Translate additional bindings
fn import_export_bindings<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    guards: Option<&'b ast::sequence::Sequence<'a, ast::guard::Guard<'a>>>,
) -> Result<Vec<Operation>, TranslationError> {
    let mut bindings = Vec::new();

    if let Some(guards) = guards {
        for guard in guards {
            let term = match guard {
                ast::guard::Guard::Expression(expression) => {
                    if let Term::Operation(operation) =
                        Term::build_component(translation, expression)?
                    {
                        operation
                    } else {
                        return Err(TranslationError::new(
                            expression.span(),
                            TranslationErrorKind::DirectiveNonOperation {
                                found: expression.context().name().to_owned(),
                            },
                        ));
                    }
                }
                ast::guard::Guard::Infix(infix_expression) => {
                    InfixOperation::build_component(translation, infix_expression)?.into_inner()
                }
            };
            bindings.push(term);
        }
    }

    Ok(bindings)
}

impl TranslationComponent for ImportDirective {
    type Ast<'a> = ast::directive::import::Import<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        import: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let predicate = Tag::new(translation.resolve_tag(import.predicate())?)
            .set_origin(translation.register_node(import.predicate()));

        let spec = ImportExportSpec::build_component(translation, import.instructions())?;

        let bindings = import_export_bindings(translation, import.guards())?;

        Ok(translation.register_component(ImportDirective::new(predicate, spec, bindings), import))
    }
}

impl TranslationComponent for ExportDirective {
    type Ast<'a> = ast::directive::export::Export<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        export: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let predicate = Tag::new(translation.resolve_tag(export.predicate())?)
            .set_origin(translation.register_node(export.predicate()));

        let spec = ImportExportSpec::build_component(translation, export.instructions())?;

        let bindings = import_export_bindings(translation, export.guards())?;

        Ok(translation.register_component(ExportDirective::new(predicate, spec, bindings), export))
    }
}
