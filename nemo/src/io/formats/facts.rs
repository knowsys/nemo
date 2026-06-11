//! Handler for resources of type Facts (Nemo syntax).

use crate::{
    io::format_builder::{
        AnyImportExportBuilder, FormatBuilder, StandardParameter, SupportedFormatTag, format_tag,
    },
    syntax::import_export::file_format,
};

use super::FileFormatMeta;

#[derive(Debug, Clone, Copy)]
pub struct FactsHandler {}

impl FactsHandler {
    /// Return the [SupportedFormatTag] for this handler.
    pub fn format_tag(&self) -> SupportedFormatTag {
        SupportedFormatTag::Facts(FactsTag::Facts)
    }
}

impl FileFormatMeta for FactsHandler {
    fn media_type(&self) -> String {
        file_format::MEDIA_TYPE_RLS.to_string()
    }

    fn default_extension(&self) -> String {
        file_format::EXTENSION_RLS.to_string()
    }
}

format_tag! {
    pub enum FactsTag(SupportedFormatTag::Facts) {
        Facts => file_format::FACTS,
    }
}

impl From<FactsHandler> for AnyImportExportBuilder {
    fn from(value: FactsHandler) -> Self {
        Self::Facts(value)
    }
}

impl FormatBuilder for FactsHandler {
    type Tag = FactsTag;

    type Parameter = StandardParameter;

    fn new(
        tag: Self::Tag,
        parameters: &crate::io::format_builder::Parameters<Self>,
        direction: crate::rule_model::components::import_export::Direction,
    ) -> Result<Self, crate::rule_model::error::validation_error::ValidationError> {
        todo!()
    }

    fn expected_arity(&self) -> Option<usize> {
        None
    }

    fn build_import(
        &self,
        arity: usize,
        patterns: Vec<nemo_physical::tabular::filters::FilterTransformPattern>,
    ) -> std::sync::Arc<dyn super::ImportHandler + Send + Sync + 'static> {
        unimplemented!("Facts import is currently not supported")
    }

    fn build_export(
        &self,
        arity: usize,
        patterns: Vec<nemo_physical::tabular::filters::FilterTransformPattern>,
    ) -> std::sync::Arc<dyn super::ExportHandler + Send + Sync + 'static> {
        todo!()
    }
}
