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
            ProgramComponent,
        },
        error::{translation_error::TranslationErrorKind, TranslationError},
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
                        if extension.to_ascii_lowercase()
                            == rdf_format.extension().to_ascii_lowercase()
                        {
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
                    if format_tag.to_ascii_lowercase() == format.name().to_ascii_lowercase() {
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

    /// Given a [ast::directive::import::Import], build an [ImportDirective].
    pub fn build_import(
        &mut self,
        import: &'a ast::directive::import::Import,
    ) -> Result<ImportDirective, TranslationError> {
        let predicate = Tag::new(self.resolve_tag(import.predicate())?)
            .set_origin(self.register_node(import.predicate()));
        let attributes = self.build_map(import.instructions())?;
        let file_format = self.import_export_format(import.instructions())?;

        Ok(self.register_component(
            ImportDirective::new(predicate, file_format, attributes),
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

        Ok(self.register_component(
            ExportDirective::new(predicate, file_format, attributes),
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
