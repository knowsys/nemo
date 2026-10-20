//! JSON imports: a document's node/edge triples into a ternary
//! predicate.

use std::sync::Arc;

use nemo_io::{compression::Compression, json::JsonReader, load::Reader};
use nemo_physical::value::Type;

use crate::{
    diagnostics::AboutResource,
    engine::compile::translation::{
        Translator,
        imports::{ImportFailure, PendingImport, required_resource},
    },
};

impl<'a> Translator<'a> {
    /// Executes one JSON import (see [execute_imports](Self::execute_imports)).
    pub(super) fn json_import(
        &mut self,
        import: &PendingImport,
        fetched: Option<Arc<[u8]>>,
    ) -> Result<(), ImportFailure> {
        let bindings = self.guard_bindings(&import.guards)?;
        let directive = self.plain_directive(import.format, &import.options, &bindings, true)?;
        let resource = required_resource(directive.resource, import)?;
        let layout = self.plan.layout_of(&import.predicate, 3);
        let selection = self.import_selection(import, &layout)?;
        let table = self.register_import(import, vec![Type::ANY; layout.kept()], &layout)?;
        if self.options_only(import) {
            return Ok(());
        }
        let reader = JsonReader::with_selection(selection);
        let loaded = match self.source_stream(&resource, Compression::Auto, fetched) {
            Some(source) => reader
                .read(source, self.dictionary)
                .map_err(|diagnostic| diagnostic.with_resource(resource.clone())),
            None => reader.read_path(self.resource_path(import.span, &resource), self.dictionary),
        }
        .map_err(ImportFailure::Reading)?;
        self.absorb(table, layout.delivered(), loaded, &resource);
        Ok(())
    }
}
