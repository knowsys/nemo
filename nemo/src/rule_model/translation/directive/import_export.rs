//! This module contains a function for handling import/export statements.

use std::path::Path;

use strum::IntoEnumIterator;

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::{
            import_export::{
                compression::CompressionFormat,
                file_formats::{FileFormat, FILE_FORMATS_RDF},
                ExportDirective, ImportDirective,
            },
            tag::Tag,
            term::{map::Map, primitive::Primitive},
            IterablePrimitives, ProgramComponent,
        },
        error::{translation_error::TranslationErrorKind, TranslationError},
        substitution::Substitution,
        translation::{
            complex::infix::InfixOperation, ASTProgramTranslation, TranslationComponent,
        },
    },
    syntax::import_export::file_format::RDF_UNSPECIFIED,
};

fn import_export_extension<'a, 'b>(
    map: &'b ast::expression::complex::map::Map<'a>,
) -> Option<(String, &'b ast::expression::Expression<'a>)> {
    for (key, value) in map.key_value() {
        let ast::expression::Expression::Constant(constant) = key else {
            continue;
        };

        if &constant.tag().to_string() != "resource" {
            continue;
        }

        let ast::expression::Expression::String(literal) = value else {
            continue;
        };

        let (_, path) = CompressionFormat::from_resource(&literal.content());

        return Some((
            Path::new(&path)
                .extension()?
                .to_owned()
                .into_string()
                .ok()?,
            value,
        ));
    }

    None
}

/// Find the [FileFormat] associated in the given import/export map.
fn import_export_format<'a, 'b>(
    map: &'b ast::expression::complex::map::Map<'a>,
) -> Result<FileFormat, TranslationError> {
    let Some(structure_tag) = map.tag() else {
        return Err(TranslationError::new(
            map.span().beginning(),
            TranslationErrorKind::FileFormatMissing,
        ));
    };

    let format_tag = structure_tag.to_string();

    if format_tag.to_ascii_lowercase() == RDF_UNSPECIFIED {
        let extension = import_export_extension(map);

        let Some((extension, origin)) = extension else {
            return Err(TranslationError::new(
                map.span().beginning(),
                TranslationErrorKind::RdfUnspecifiedMissingExtension,
            ));
        };

        for &rdf_format in FILE_FORMATS_RDF {
            if extension.eq_ignore_ascii_case(rdf_format.extension()) {
                return Ok(rdf_format);
            }
        }

        Err(TranslationError::new(
            origin.span(),
            TranslationErrorKind::RdfUnspecifiedUnknownExtension(extension),
        ))
    } else {
        for format in FileFormat::iter() {
            if format_tag.eq_ignore_ascii_case(format.name()) {
                return Ok(format);
            }
        }

        Err(TranslationError::new(
            structure_tag.span(),
            TranslationErrorKind::FileFormatUnknown(structure_tag.to_string()),
        ))
    }
}

/// Handle a import ast node.
pub fn handle_import<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    import: &'b ast::directive::import::Import<'a>,
) -> Result<(), TranslationError> {
    let import_directive = ImportDirective::build_component(translation, import)?;
    let _ = import_directive.validate(&mut translation.validation_error_builder);

    translation.program_builder.add_import(import_directive);

    Ok(())
}

/// Handle a export ast node.
pub fn handle_export<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    export: &'b ast::directive::export::Export<'a>,
) -> Result<(), TranslationError> {
    let export_directive = ExportDirective::build_component(translation, export)?;
    let _ = export_directive.validate(&mut translation.validation_error_builder);

    translation.program_builder.add_export(export_directive);

    Ok(())
}

fn process_import_export_substitution<'a, 'b>(
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

        let operation = InfixOperation::build_component(translation, &infix)?.into_inner();

        let Some((left, right)) = operation.variable_assignment() else {
            return Err(TranslationError::new(
                guard.span(),
                TranslationErrorKind::NonAssignment {
                    found: operation.operation_kind().to_string(),
                },
            ));
        };

        let right = right.reduce();
        let terms = right.primitive_terms().collect::<Vec<_>>();

        if !right.is_ground() || terms.len() != 1 {
            return Err(TranslationError::new(
                infix.pair().1.span(),
                TranslationErrorKind::NonGroundTerm {
                    found: right.kind().name().to_string(),
                },
            ));
        }

        result.push((
            left.clone(),
            (*terms.first().expect("is not empty")).clone(),
        ));
    }

    Ok(Substitution::new(result))
}

impl TranslationComponent for ImportDirective {
    type Ast<'a> = ast::directive::import::Import<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        import: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let predicate = Tag::new(translation.resolve_tag(import.predicate())?)
            .set_origin(translation.register_node(import.predicate()));
        let attributes = Map::build_component(translation, import.instructions())?;
        let file_format = import_export_format(import.instructions())?;

        let substitution = if let Some(guards) = import.guards() {
            process_import_export_substitution(translation, guards.iter())?
        } else {
            let iter: [(Primitive, Primitive); 0] = [];
            Substitution::new(iter)
        };

        Ok(translation.register_component(
            ImportDirective::new(predicate, file_format, attributes, substitution),
            import,
        ))
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
        let attributes = Map::build_component(translation, export.instructions())?;
        let file_format = import_export_format(export.instructions())?;

        let substitution = if let Some(guards) = export.guards() {
            process_import_export_substitution(translation, guards.iter())?
        } else {
            let iter: [(Primitive, Primitive); 0] = [];
            Substitution::new(iter)
        };

        Ok(translation.register_component(
            ExportDirective::new(predicate, file_format, attributes, substitution),
            export,
        ))
    }
}
