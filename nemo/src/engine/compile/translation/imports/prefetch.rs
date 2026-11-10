//! The remote resources of a translation's imports.
//!
//! [Preparing](Translator::prepare) a translation lists the requests for the remote resources
//! that its imports need, before any import reads its data.
//! The translation then [commits](Translator::commit) with the responses,
//! each found by the position of its import's directive.

use std::sync::Arc;

use nemo_io::{file_format::FileFormat, remote::Request};
use nemo_physical::{diagnostics::Diagnostic, value::ValueRef};

use crate::{
    diagnostics::{AboutResource, TranslationError},
    engine::{
        compile::translation::{Translator, imports::PendingImport, options::on_error},
        remote::{Failure, FetchedResource, ResourceRequest},
    },
    model::{Directive, Identifier, OnError, Statement, TermKind},
    pipeline::StatementId,
};

impl<'a> Translator<'a> {
    /// Returns the requests for the remote resources that the program's imports read,
    /// in statement order, each tagged with the position of its directive.
    ///
    /// An import needs no request if its data is already stored,
    /// if its resource is registered through [add_resource](crate::engine::Engine::add_resource),
    /// or if it fetches during reasoning, as an import with atoms in its `where` clause does.
    /// A translation that reads no data needs no request.
    pub(in crate::engine::compile::translation) fn remote_requests(
        &mut self,
        statements: &[Statement],
        ids: &[StatementId],
    ) -> Result<Vec<ResourceRequest>, Diagnostic> {
        let mut requests = Vec::new();
        if !self.purpose.reads() {
            return Ok(requests);
        }
        for (index, statement) in statements.iter().enumerate() {
            let Statement::Directive(Directive::Import(directive)) = statement else {
                continue;
            };
            // The `on_error` option, which says what a failed fetch does,
            // is validated before anything is fetched.
            let request = self.pending_import(directive, index, 0).and_then(|import| {
                if import.preloaded {
                    return Ok(None);
                }
                let request = self.import_request(&import)?;
                import_on_error(&import)?;
                Ok(request)
            });
            match request {
                Ok(Some(request)) => requests.push(request),
                Ok(None) => {}
                Err(problem) => {
                    let problem = self.pipeline.label_instances(problem, ids[index]);
                    self.recover(problem)?;
                }
            }
        }
        Ok(requests)
    }

    /// Returns the bytes of the remote resources of this translation's imports,
    /// taken from what was fetched for them by the position of their directives.
    ///
    /// The result has one entry per import, which is `None` if nothing was fetched for it.
    /// Standard input is read here too, once.
    /// A failed fetch fails its import,
    /// or, with `on_error = warn`, leaves the import reading nothing, with a warning.
    /// A translation that reads no data reads nothing.
    ///
    /// # Panics
    ///
    /// Panics for a remote resource that the translation was not prepared with.
    pub(super) fn prefetch(
        &mut self,
        imports: &mut [PendingImport],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Diagnostic> {
        let mut fetched: Vec<Option<Arc<[u8]>>> = Vec::new();
        fetched.resize_with(imports.len(), || None);
        if !self.purpose.reads() {
            return Ok(fetched);
        }

        let mut piped = Vec::new();
        let mut missing: Vec<ResourceRequest> = Vec::new();
        for (index, import) in imports.iter_mut().enumerate() {
            if import.preloaded {
                continue;
            }
            match self.resource_name(import) {
                Ok(Some(resource)) if resource.is_empty() => piped.push(index),
                Ok(_) => {}
                Err(problem) => self.recover(problem)?,
            }
            if let Some(FetchedResource { request, response }) =
                self.fetched.remove(&import.statement)
            {
                fetched[index] = match response {
                    Ok(bytes) => Some(bytes),
                    Err(failure) => self.failed_fetch(import, &request, &failure)?,
                };
                continue;
            }
            match self.import_request(import) {
                Ok(Some(request)) => missing.push(request),
                Ok(None) => {}
                Err(problem) => self.recover(problem)?,
            }
        }
        assert!(
            missing.is_empty(),
            "a translation commits with the remote resources it was prepared with"
        );

        // The empty resource is standard input, read once and shared by
        // every import naming it, since it can be read only once.
        if !piped.is_empty() {
            let bytes = standard_input()?;
            for index in piped {
                fetched[index] = Some(Arc::clone(&bytes));
            }
        }
        Ok(fetched)
    }

    /// Handles the failed fetch of an import's remote resource.
    ///
    /// It fails the import, or, with `on_error = warn`, reports a warning and returns empty
    /// bytes, so that the import reads nothing.
    fn failed_fetch(
        &mut self,
        import: &mut PendingImport,
        request: &ResourceRequest,
        failure: &Failure,
    ) -> Result<Option<Arc<[u8]>>, Diagnostic> {
        let mut problem = failure.diagnostic(&request.request.url);
        if let Some(span) = import.span {
            problem = problem.with_label(span, "requested by this import");
        }
        if import_on_error(import)? == OnError::Fail {
            return Err(problem);
        }
        self.pipeline.report(
            TranslationError::ImportFailed {
                resource: request.resource.clone(),
                reason: failure.to_string(),
            }
            .at(import.span)
            .with_resource(request.resource.clone()),
        );
        // Marked as preloaded, the import registers its table and reads nothing.
        import.preloaded = true;
        Ok(Some(Arc::from([])))
    }

    /// Returns the request that one import needs, or `None` for a local or registered source.
    ///
    /// Only the options that the request needs are evaluated here,
    /// and the import's own option parsing validates the rest.
    fn import_request(
        &self,
        import: &PendingImport,
    ) -> Result<Option<ResourceRequest>, Diagnostic> {
        if import.format == FileFormat::Sparql {
            // An import with atoms in its `where` clause sends its requests during
            // reasoning, never at load.
            if !import.bindings.is_empty() {
                return Ok(None);
            }
            let spec = self.sparql_import_spec(import)?;
            let resource = spec.resource();
            if self.resources.contains_key(&resource) {
                return Ok(None);
            }
            return Ok(Some(ResourceRequest {
                statement: import.statement,
                resource,
                request: spec.request(),
            }));
        }
        let Some(resource) = self.remote_resource(import)? else {
            return Ok(None);
        };
        let mut request = Request::get(&resource);
        request.accept = import.format.media_type();
        let bindings = self.guard_bindings(&import.guards)?;
        for (key, value) in &import.options {
            if matches!(&key.kind, TermKind::Constant(Identifier::Bare(name)) if name == "http_headers")
            {
                request.headers = self.header_option(value, &bindings)?;
            }
        }
        Ok(Some(ResourceRequest {
            statement: import.statement,
            resource,
            request,
        }))
    }

    /// Returns the remote, unregistered resource of an import, if it has one.
    ///
    /// Bytes registered under the resource name take precedence over the network,
    /// so data added through [add_resource](crate::engine::Engine::add_resource) is not fetched
    /// again.
    fn remote_resource(&self, import: &PendingImport) -> Result<Option<String>, Diagnostic> {
        if matches!(import.format, FileFormat::Sparql | FileFormat::Parquet) {
            // SPARQL builds its own request, and Parquet does not accept
            // remote resources.
            return Ok(None);
        }
        Ok(self.resource_name(import)?.filter(|resource| {
            Request::is_remote(resource) && !self.resources.contains_key(resource)
        }))
    }

    /// Returns the evaluated resource that an import names,
    /// or `None` if it names none, or names it by a value that is neither a string nor an IRI.
    fn resource_name(&self, import: &PendingImport) -> Result<Option<String>, Diagnostic> {
        let bindings = self.guard_bindings(&import.guards)?;
        for (key, value) in &import.options {
            if !matches!(&key.kind, TermKind::Constant(Identifier::Bare(name)) if name == "resource")
            {
                continue;
            }
            let evaluated = self.option_value(value, &bindings)?;
            return Ok(match evaluated.as_ref() {
                ValueRef::String(path) => Some(path.to_owned()),
                ValueRef::Iri(iri) => Some(iri.to_string()),
                _ => None,
            });
        }
        Ok(None)
    }
}

/// Returns what a failed fetch of an import does, as its `on_error` option sets it,
/// which is [OnError::Fail] by default.
fn import_on_error(import: &PendingImport) -> Result<OnError, Diagnostic> {
    import
        .options
        .iter()
        .find(|(key, _)| matches!(&key.kind, TermKind::Constant(Identifier::Bare(name)) if name == "on_error"))
        .map_or(Ok(OnError::Fail), |(_, value)| on_error(value))
}

/// Returns the bytes of standard input, read to its end.
fn standard_input() -> Result<Arc<[u8]>, Diagnostic> {
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut std::io::stdin().lock(), &mut bytes).map_err(|error| {
        Diagnostic::error(
            nemo_io::codes::UNREADABLE,
            format!("could not read `<stdin>`: {error}"),
        )
        .with_resource(String::from("<stdin>"))
    })?;
    Ok(Arc::from(bytes))
}
