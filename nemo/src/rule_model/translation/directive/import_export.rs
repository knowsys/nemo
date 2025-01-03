//! This module contains a function for handling import/export statements.

use std::path::Path;

use strum::IntoEnumIterator;

use crate::{
    parser::{
        ast::{self, ProgramAST},
        span::Span,
    },
    rule_model::{
        components::{
            import_export::{
                compression::CompressionFormat,
                file_formats::{FileFormat, FILE_FORMATS_RDF},
                ExportDirective, ImportDirective,
            },
            tag::Tag,
            IterablePrimitives, ProgramComponent,
        },
        error::{translation_error::TranslationErrorKind, TranslationError},
        substitution::Substitution,
        translation::ASTProgramTranslation,
    },
    syntax::import_export::file_format::RDF_UNSPECIFIED,
};

impl<'a> ASTProgramTranslation<'a> {
    /// Find the extension given for this import/export statement.
    fn import_export_extension(
        map: &'a ast::expression::complex::map::Map,
    ) -> Option<(String, &'a ast::expression::Expression<'a>)> {
        for (key, value) in map.key_value() {
            if let ast::expression::Expression::Constant(constant) = key {
                if &constant.tag().to_string() == "resource" {
                    if let ast::expression::Expression::String(string) = value {
                        let (_, path) = CompressionFormat::from_resource(&string.content());

                        return Some((
                            Path::new(&path)
                                .extension()?
                                .to_owned()
                                .into_string()
                                .ok()?,
                            value,
                        ));
                    }
                }
            }
        }

        None
    }

    /// Find the [FileFormat] associated in the given import/export map.
    fn import_export_format(
        &self,
        map: &'a ast::expression::complex::map::Map,
    ) -> Result<FileFormat, TranslationError> {
        if let Some(structure_tag) = map.tag() {
            let format_tag = structure_tag.to_string();

            if format_tag.to_ascii_lowercase() == RDF_UNSPECIFIED {
                let extension = Self::import_export_extension(map);

                if let Some((extension, origin)) = extension {
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
                    Err(TranslationError::new(
                        map.span().beginning(),
                        TranslationErrorKind::RdfUnspecifiedMissingExtension,
                    ))
                }
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
        } else {
            Err(TranslationError::new(
                map.span().beginning(),
                TranslationErrorKind::FileFormatMissing,
            ))
        }
    }

    fn import_export_bindings(
        &mut self,
        guards: &'a Option<ast::sequence::Sequence<'a, ast::guard::Guard<'a>>>,
        span: Span,
    ) -> Result<Substitution, TranslationError> {
        let mut result = Vec::new();

        if let Some(guards) = guards {
            for guard in guards {
                if let ast::guard::Guard::Infix(infix) = guard {
                    let operation = self.build_infix(infix)?;

                    if let Some((left, right)) = operation.variable_assignment() {
                        let right = right.reduce();
                        let terms = right.primitive_terms().collect::<Vec<_>>();

                        if !right.is_ground() || terms.len() != 1 {
                            return Err(TranslationError::new(
                                span,
                                TranslationErrorKind::NonGroundTerm {
                                    found: right.kind().name().to_string(),
                                },
                            ));
                        }

                        result.push((
                            left.clone(),
                            (*terms.first().expect("is not empty")).clone(),
                        ));
                    } else {
                        return Err(TranslationError::new(
                            span,
                            TranslationErrorKind::NonAssignment {
                                found: operation.operation_kind().to_string(),
                            },
                        ));
                    }
                } else {
                    return Err(TranslationError::new(
                        span,
                        TranslationErrorKind::NonAssignment {
                            found: "expression".to_string(),
                        },
                    ));
                }
            }
        }

        Ok(Substitution::new(result.into_iter()))
    }

    /// Given a [ast::directive::import::Import], build an [ImportDirective].
    pub fn build_import(
        &mut self,
        import: &'a ast::directive::import::Import,
    ) -> Result<ImportDirective, TranslationError> {
        let predicate = Tag::new(self.resolve_tag(import.predicate())?)
            .set_origin(self.register_node(import.predicate()));
        let attributes = self.build_map(import.instructions())?;
        let file_format = self.import_export_format(import.instructions())?;
        let bindings = self.import_export_bindings(import.guards(), import.span())?;

        Ok(self.register_component(
            ImportDirective::new(predicate, file_format, attributes, bindings),
            import,
        ))
    }

    /// Handle a import ast node.
    pub fn handle_import(
        &mut self,
        import: &'a ast::directive::import::Import,
    ) -> Result<(), TranslationError> {
        let import_directive = self.build_import(import)?;
        let _ = import_directive.validate(&mut self.validation_error_builder);

        self.program_builder.add_import(import_directive);

        Ok(())
    }

    /// Given a [ast::directive::export::Export], builds a [ExportDirective].
    pub fn build_export(
        &mut self,
        export: &'a ast::directive::export::Export,
    ) -> Result<ExportDirective, TranslationError> {
        let predicate = Tag::new(self.resolve_tag(export.predicate())?)
            .set_origin(self.register_node(export.predicate()));
        let attributes = self.build_map(export.instructions())?;
        let file_format = self.import_export_format(export.instructions())?;
        let bindings = self.import_export_bindings(export.guards(), export.span())?;

        Ok(self.register_component(
            ExportDirective::new(predicate, file_format, attributes, bindings),
            export,
        ))
    }

    /// Handle a export ast node.
    pub fn handle_export(
        &mut self,
        export: &'a ast::directive::export::Export,
    ) -> Result<(), TranslationError> {
        let export_directive = self.build_export(export)?;
        let _ = export_directive.validate(&mut self.validation_error_builder);

        self.program_builder.add_export(export_directive);

        Ok(())
    }
}
