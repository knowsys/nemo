//! This module contains a function for handling import/export statements.

use crate::{
    parser::{
        ast::{self, ProgramAST, tag::structure::StructureTag},
        context::ParserContext,
    },
    rule_model::{
        components::{
            import_export::{
                ExportDirective, ImportDirective, attribute::ImportExportAttribute,
                specification::ImportExportSpec,
            },
            tag::Tag,
            term::{Term, operation::Operation},
        },
        error::translation_error::TranslationError,
        origin::Origin,
        translation::{
            ASTProgramTranslation, TranslationComponent, complex::infix::InfixOperation,
        },
    },
};

impl TranslationComponent for ImportExportSpec {
    type Ast<'a> = ast::expression::complex::map::Map<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        map: &Self::Ast<'a>,
    ) -> Option<Self> {
        let mut subterms = Vec::new();
        for (key, value) in map.key_value() {
            let key = ImportExportAttribute::build_component(translation, key)?;
            let value = Term::build_component(translation, value)?;

            subterms.push((key, value));
        }

        let format = map.tag().map(StructureTag::to_string).unwrap_or_default();

        let result = ImportExportSpec::new(&format, subterms);
        Some(Origin::ast(result, map))
    }
}

impl TranslationComponent for ImportExportAttribute {
    type Ast<'a> = ast::expression::Expression<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        expression: &Self::Ast<'a>,
    ) -> Option<Self> {
        if let ast::expression::Expression::Constant(constant) = expression {
            let result = ImportExportAttribute::new(constant.tag().to_string());
            Some(Origin::ast(result, expression))
        } else {
            translation.report.add(
                expression,
                TranslationError::KeyWrongType {
                    found: expression.context_type().name().to_owned(),
                    expected: ParserContext::Constant.name().to_owned(),
                },
            );
            None
        }
    }
}

/// Translate additional bindings
fn import_export_bindings<'a>(
    translation: &mut ASTProgramTranslation,
    guards: Option<&ast::sequence::Sequence<'a, ast::guard::Guard<'a>>>,
) -> Option<Vec<Operation>> {
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
                        translation.report.add(
                            expression,
                            TranslationError::DirectiveNonOperation {
                                found: expression.context().name().to_owned(),
                            },
                        );
                        return None;
                    }
                }
                ast::guard::Guard::Infix(infix_expression) => {
                    InfixOperation::build_component(translation, infix_expression)?.into_inner()
                }
            };
            bindings.push(term);
        }
    }

    Some(bindings)
}

impl TranslationComponent for ImportDirective {
    type Ast<'a> = ast::directive::import::Import<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        import: &Self::Ast<'a>,
    ) -> Option<Self> {
        let predicate = Origin::ast(
            Tag::new(translation.resolve_tag(import.predicate())?),
            import.predicate(),
        );

        let spec = ImportExportSpec::build_component(translation, import.instructions())?;

        let bindings = import_export_bindings(translation, import.guards())?;

        Some(Origin::ast(
            ImportDirective::new(predicate, spec, bindings),
            import,
        ))
    }
}

impl TranslationComponent for ExportDirective {
    type Ast<'a> = ast::directive::export::Export<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        export: &Self::Ast<'a>,
    ) -> Option<Self> {
        let predicate = Origin::ast(
            Tag::new(translation.resolve_tag(export.predicate())?),
            export.predicate(),
        );

        let spec = ImportExportSpec::build_component(translation, export.instructions())?;

        let bindings = import_export_bindings(translation, export.guards())?;

        Some(Origin::ast(
            ExportDirective::new(predicate, spec, bindings),
            export,
        ))
    }
}
