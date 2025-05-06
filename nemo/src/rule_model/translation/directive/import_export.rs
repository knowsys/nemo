//! This module contains a function for handling import/export statements.

use nemo_physical::datavalues::DataValue;

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::{
            import_export::{ExportDirective, ImportDirective, ImportExportSpec},
            tag::Tag,
            term::{
                map::Map,
                primitive::{variable::Variable, Primitive},
                value_type::ValueType,
                Term,
            },
            IterablePrimitives, ProgramComponent,
        },
        error::{translation_error::TranslationErrorKind, ComplexErrorLabelKind, TranslationError},
        substitution::Substitution,
        translation::{
            complex::infix::InfixOperation, ASTProgramTranslation, TranslationComponent,
        },
    },
};

fn import_export_bindings<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    guards: impl Iterator<Item = &'b ast::guard::Guard<'a>>,
) -> Result<Substitution, TranslationError> {
    let mut result = Vec::new();

    for guard in guards {
        let ast::guard::Guard::Infix(infix) = guard else {
            return Err(TranslationError::new(
                guard.span(),
                TranslationErrorKind::NonAssignment {
                    found: "expression".to_string(),
                },
            ));
        };

        let operation = InfixOperation::build_component(translation, infix)?.into_inner();

        let Some((left, right)) = operation.variable_assignment() else {
            return Err(TranslationError::new(
                guard.span(),
                TranslationErrorKind::NonAssignment {
                    found: operation.operation_kind().to_string(),
                },
            ));
        };

        result.push((left.clone(), right.clone()));
    }

    Ok(Substitution::new(result))
}

fn import_export_spec<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    instructions: &'b ast::expression::complex::map::Map<'a>,
    guards: Option<&'b ast::sequence::Sequence<'a, ast::guard::Guard<'a>>>,
) -> Result<ImportExportSpec, TranslationError> {
    let mut spec = Map::build_component(translation, instructions)?;

    let substitution = guards
        .map(|guards| import_export_bindings(translation, guards.iter()))
        .transpose()?
        .unwrap_or_default();

    substitution.apply(&mut spec);
    spec = spec.reduce_with_substitution(&Substitution::default());

    let Some(format_tag) = spec.tag() else {
        let span = instructions.span().beginning();
        return Err(TranslationError::new(
            span,
            TranslationErrorKind::FileFormatMissing,
        ));
    };

    let mut result = ImportExportSpec::new(*spec.origin(), format_tag);

    for (key, value) in spec.key_value() {
        let Term::Primitive(Primitive::Ground(key_term)) = key else {
            let span = translation
                .origin_map
                .get(key.origin())
                .map(|ast| ast.span())
                .unwrap_or(instructions.span());

            return Err(TranslationError::new(
                span,
                TranslationErrorKind::NonGroundTerm {
                    found: key.to_string(),
                },
            ));
        };

        let Some(key) = key_term.value().to_iri() else {
            let span = translation
                .origin_map
                .get(key_term.origin())
                .map(|ast| ast.span())
                .unwrap_or(instructions.span());

            return Err(TranslationError::new(
                span,
                TranslationErrorKind::KeyWrongType {
                    key: key_term.to_string(),
                    expected: ValueType::Constant.name().to_string(),
                    found: key_term.value_type().name().to_string(),
                },
            ));
        };

        if value
            .primitive_terms()
            .any(|p| !p.is_ground() && !matches!(&p, Primitive::Variable(Variable::Global(_))))
        {
            let span = translation
                .origin_map
                .get(value.origin())
                .map(|ast| ast.span())
                .unwrap_or(instructions.span());

            return Err(TranslationError::new(
                span,
                TranslationErrorKind::NonGroundTerm {
                    found: value.to_string(),
                },
            ));
        };

        if let Some((prev_origin, _)) =
            result.push_attribute((key.clone(), *key_term.origin()), value.clone())
        {
            let key_span = translation
                .origin_map
                .get(key_term.origin())
                .map(|ast| ast.span())
                .unwrap_or(instructions.span());

            let mut error = TranslationError::new(
                key_span,
                TranslationErrorKind::MapParameterRedefined { key },
            );

            if let Some(prev_key) = translation.origin_map.get(&prev_origin) {
                error = error.add_label(
                    ComplexErrorLabelKind::Information,
                    prev_key.span().range(),
                    "first definition occurred here",
                );
            }

            return Err(error);
        }
    }

    log::trace!("Import/Export spec {result}");
    Ok(result)
}

impl TranslationComponent for ImportDirective {
    type Ast<'a> = ast::directive::import::Import<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        import: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let predicate = Tag::new(translation.resolve_tag(import.predicate())?)
            .set_origin(translation.register_node(import.predicate()));

        let spec = import_export_spec(translation, import.instructions(), import.guards())?;

        Ok(translation.register_component(ImportDirective::new(predicate, spec), import))
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

        let spec = import_export_spec(translation, export.instructions(), export.guards())?;

        Ok(translation.register_component(ExportDirective::new(predicate, spec), export))
    }
}
