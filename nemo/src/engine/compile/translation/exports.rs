//! Export directives: resolving them into specifications, and the
//! checks that need every predicate's arity.

#[cfg(feature = "parquet")]
use nemo_io::compression::Compression;
use nemo_io::file_format::{FileFormat, RdfVariant};
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::TranslationError,
    engine::{
        ExportFormat, ExportSpec,
        compile::translation::{Translator, unsupported},
        registered::Registered,
    },
    model::{ExportDirective, Identifier},
};

/// Returns the error for an RDF export whose predicate's arity does not fit the serialization,
/// which takes three columns, or four with the graph first.
pub(crate) fn rdf_export_arity(
    variant: RdfVariant,
    predicate: &str,
    arity: usize,
) -> TranslationError {
    TranslationError::RdfExportArity {
        columns: variant.arity().into(),
        layout: String::from(if variant.quads() {
            " (graph, subject, predicate, object)"
        } else {
            " (subject, predicate, object)"
        }),
        predicate: predicate.to_owned(),
        arity: arity.into(),
    }
}

impl<'a> Translator<'a> {
    /// Resolves one export directive into its specification.
    ///
    /// The arity checks wait for [validate_exports](Self::validate_exports).
    pub(super) fn export_directive(&mut self, export: &ExportDirective) -> Result<(), Diagnostic> {
        let predicate = self.resolve(&export.predicate, export.span)?;
        let Identifier::Bare(name) = &export.format else {
            return Err(TranslationError::UnknownFormat {
                name: export.format.to_string(),
            }
            .at(export.span));
        };
        let Some(format) = FileFormat::from_name(name) else {
            return Err(TranslationError::UnknownFormat { name: name.clone() }.at(export.span));
        };
        let Some(spec) = self.export_spec(export, predicate, format)? else {
            return Ok(());
        };
        self.export_spans.push(export.span);
        self.exports.push(spec);
        Ok(())
    }

    /// Returns the specification of an export directive in a format,
    /// or `None` for a Parquet export a check accepts in a build without the feature.
    fn export_spec(
        &mut self,
        export: &ExportDirective,
        predicate: String,
        format: FileFormat,
    ) -> Result<Option<ExportSpec>, Diagnostic> {
        let bindings = self.guard_bindings(&export.guards)?;
        let spec = match format {
            FileFormat::Csv | FileFormat::Tsv | FileFormat::Dsv => {
                let shape =
                    self.dsv_directive(format, &export.options, &bindings, export.span, false)?;
                ExportSpec {
                    predicate,
                    resource: shape.resource,
                    format: ExportFormat::Dsv {
                        delimiter: shape.delimiter,
                        extension: shape.extension,
                        formats: shape.formats,
                    },
                    compression: shape.compression,
                    limit: shape.limit,
                }
            }
            FileFormat::Rdf(_) => {
                let shape = self.plain_directive(format, &export.options, &bindings, false)?;
                let variant = self.rdf_variant(format, shape.resource.as_deref(), export.span)?;
                // The program's own prefixes and base, so that the
                // output abbreviates IRIs as the program does.
                let mut prefixes: Vec<(String, String)> = self
                    .prefixes
                    .iter()
                    .map(|(name, iri)| (name.clone(), iri.clone()))
                    .collect();
                prefixes.sort();
                ExportSpec {
                    predicate,
                    resource: shape.resource,
                    format: ExportFormat::Rdf {
                        variant,
                        base: shape.base.or_else(|| self.base_iri.clone()),
                        prefixes,
                    },
                    compression: shape.compression,
                    limit: shape.limit,
                }
            }
            FileFormat::Json => {
                let shape = self.plain_directive(format, &export.options, &bindings, false)?;
                ExportSpec {
                    predicate,
                    resource: shape.resource,
                    format: ExportFormat::Json,
                    compression: shape.compression,
                    limit: shape.limit,
                }
            }
            FileFormat::Parquet => {
                // A check accepts what this build cannot write;
                // the program may be meant for one that can.
                #[cfg(not(feature = "parquet"))]
                return if self.purpose == super::Purpose::Check {
                    Ok(None)
                } else {
                    Err(TranslationError::FeatureUnavailable {
                        what: String::from("a Parquet export"),
                        feature: String::from("parquet"),
                    }
                    .at(export.span))
                };
                #[cfg(feature = "parquet")]
                {
                    let shape = self.plain_directive(format, &export.options, &bindings, false)?;
                    ExportSpec {
                        predicate,
                        resource: shape.resource,
                        format: ExportFormat::Parquet,
                        compression: Compression::Plain,
                        limit: shape.limit,
                    }
                }
            }
            FileFormat::Sparql => {
                return Err(unsupported("exporting to a SPARQL endpoint", export.span));
            }
        };
        Ok(Some(spec))
    }

    /// Runs the export checks that need every predicate's arity:
    /// an RDF export takes three columns, or four with the graph first,
    /// and the `format` option of a DSV export lists one column format per column.
    pub(super) fn validate_exports(&mut self) -> Result<(), Diagnostic> {
        let mut problems = Vec::new();
        for (spec, &span) in self.exports.iter().zip(&self.export_spans) {
            let Some(&Registered { arity, .. }) = self.predicates.get(&spec.predicate) else {
                continue;
            };
            match &spec.format {
                ExportFormat::Rdf { variant, .. } if arity != variant.arity() => {
                    problems.push(rdf_export_arity(*variant, &spec.predicate, arity).at(span));
                }
                ExportFormat::Dsv {
                    formats: Some(formats),
                    ..
                } if formats.len() != arity => {
                    problems.push(
                        TranslationError::ExportFormatArity {
                            predicate: spec.predicate.clone(),
                            declared: formats.len().into(),
                            arity: arity.into(),
                        }
                        .at(span),
                    );
                }
                _ => {}
            }
        }
        for problem in problems {
            self.recover(problem)?;
        }
        Ok(())
    }
}
