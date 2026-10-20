//! Prefetching for the imports of one translation.
//!
//! Before any import reads, the HTTP requests of the remote resources are collected
//! and matched with the responses passed to `respond`.
//! If a request has no response yet, the translation suspends.

use std::sync::Arc;

use nemo_io::{file_format::FileFormat, remote::Request};
use nemo_physical::{diagnostics::Diagnostic, value::ValueRef};

use crate::{
    diagnostics::{AboutResource, EngineError, TranslationError},
    engine::{
        compile::translation::{Translator, imports::PendingImport, options::on_error},
        remote::ResourceRequest,
    },
    model::{Identifier, OnError, TermKind},
};

impl<'a> Translator<'a> {
    /// Returns the bytes of the remote resources of this translation's imports,
    /// from the responses passed to `respond` after an earlier translation suspended.
    ///
    /// The result has one entry per import,
    /// `None` if nothing was (or needed to be) fetched.
    /// Standard input is read here too, once.
    ///
    /// - A request without a response suspends the translation before any import reads:
    ///   the request is recorded (see [take_requests](Self::take_requests))
    ///   and N0491 is returned.
    /// - A failed request fails its import,
    ///   or, under `on_error = warn`, leaves the import reading nothing, with a warning.
    /// - A check sends no requests.
    pub(super) fn prefetch(
        &mut self,
        imports: &mut [PendingImport],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Diagnostic> {
        let mut fetched: Vec<Option<Arc<[u8]>>> = Vec::new();
        fetched.resize_with(imports.len(), || None);
        if !self.purpose.reads() {
            return Ok(fetched);
        }

        let ImportRequests { remote, piped } = self.collect_requests(imports)?;
        let waiting = self.take_responses(imports, remote, &mut fetched)?;
        // The translation suspends before anything reads, standard input
        // included, which the resumed translation could not read again.
        if !waiting.is_empty() {
            let first = waiting[0].request.url.clone();
            let count = waiting.len();
            self.requests = waiting;
            return Err(EngineError::AwaitingRemote {
                count: count.into(),
                first,
            }
            .diagnostic());
        }

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

    /// Returns the requests the imports of this translation need, by import with its failure
    /// behavior, and the imports reading standard input.
    fn collect_requests(
        &mut self,
        imports: &[PendingImport],
    ) -> Result<ImportRequests, Diagnostic> {
        let mut remote: Vec<(usize, ResourceRequest, OnError)> = Vec::new();
        let mut piped: Vec<usize> = Vec::new();
        for (index, import) in imports.iter().enumerate() {
            if import.preloaded {
                continue;
            }
            match self.resource_name(import) {
                Ok(Some(resource)) if resource.is_empty() => piped.push(index),
                Ok(_) => {}
                Err(problem) => self.recover(problem)?,
            }
            // A remote import's options are evaluated before the
            // translation can suspend for its request.
            match self
                .import_request(import)
                .and_then(|request| Ok((request, import_on_error(import)?)))
            {
                Ok((Some(request), on_error)) => remote.push((index, request, on_error)),
                Ok((None, _)) => {}
                Err(problem) => self.recover(problem)?,
            }
        }
        Ok(ImportRequests { remote, piped })
    }

    /// Takes the responses passed to `respond` for the imports' requests into `fetched`,
    /// returning the requests still without a response.
    ///
    /// A failed request fails its import,
    /// or, under `on_error = warn`, leaves the import reading nothing, with a warning.
    fn take_responses(
        &mut self,
        imports: &mut [PendingImport],
        remote: Vec<(usize, ResourceRequest, OnError)>,
        fetched: &mut [Option<Arc<[u8]>>],
    ) -> Result<Vec<ResourceRequest>, Diagnostic> {
        let mut waiting = Vec::new();
        for (index, request, on_error) in remote {
            match self
                .responses
                .and_then(|responses| responses.get(&request.resource))
            {
                None => waiting.push(request),
                Some(Ok(bytes)) => fetched[index] = Some(Arc::clone(bytes)),
                Some(Err(failure)) => {
                    let import = &mut imports[index];
                    let mut problem = failure.diagnostic(&request.request.url);
                    if let Some(span) = import.span {
                        problem = problem.with_label(span, "requested by this import");
                    }
                    if on_error == OnError::Fail {
                        return Err(problem);
                    }
                    self.pipeline.report(
                        TranslationError::ImportFailed {
                            resource: request.resource.clone(),
                            reason: failure.to_string(),
                        }
                        .at(import.span)
                        .with_resource(request.resource),
                    );
                    // Marked preloaded: it registers its table and reads
                    // nothing.
                    import.preloaded = true;
                    fetched[index] = Some(Arc::from([]));
                }
            }
        }
        Ok(waiting)
    }

    /// Returns the request one import needs, or `None` for a local (or registered) source.
    ///
    /// Only the options the request needs are evaluated here.
    /// The import's own option parsing validates the rest.
    fn import_request(
        &self,
        import: &PendingImport,
    ) -> Result<Option<ResourceRequest>, Diagnostic> {
        if import.format == FileFormat::Sparql {
            // An import with a `where` clause sends its requests during
            // reasoning, never at load.
            if !import.bindings.is_empty() {
                return Ok(None);
            }
            // Without the `sparql` feature, `sparql_import` reports the error.
            #[cfg(feature = "sparql")]
            {
                let spec = self.sparql_import_spec(import)?;
                let resource = spec.resource();
                if self.resources.contains_key(&resource) {
                    return Ok(None);
                }
                return Ok(Some(ResourceRequest {
                    resource,
                    request: spec.request(),
                }));
            }
            #[cfg(not(feature = "sparql"))]
            return Ok(None);
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
        Ok(Some(ResourceRequest { resource, request }))
    }

    /// Returns the remote, unregistered resource of an import, if it has one.
    ///
    /// Bytes registered under the resource name take precedence over the network,
    /// so data added through `add_resource` is not fetched again.
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

    /// Returns the resource an import names, evaluated,
    /// or `None` if it names none, or names it by something other than a string or IRI.
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

/// Returns what an import's failed fetch does, as its `on_error` option sets it.
fn import_on_error(import: &PendingImport) -> Result<OnError, Diagnostic> {
    import
        .options
        .iter()
        .find(|(key, _)| matches!(&key.kind, TermKind::Constant(Identifier::Bare(name)) if name == "on_error"))
        .map_or(Ok(OnError::Fail), |(_, value)| on_error(value))
}

/// The requests the imports of one translation need, and the imports reading standard input.
struct ImportRequests {
    /// Per import with a remote resource, its index, its request, and what a failed fetch does.
    remote: Vec<(usize, ResourceRequest, OnError)>,
    /// The indices of the imports reading standard input.
    piped: Vec<usize>,
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
