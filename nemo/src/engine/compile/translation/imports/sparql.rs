//! SPARQL imports: a query's solutions,
//! fetched into a predicate of the projection's width at load time,
//! or during reasoning for a bound import (one with a `where` clause).
//!
//! A bound import compiles into a fetch rule (`fetch`).
//! A condition the filter pass pushed onto the import is added to the query
//! if it has a SPARQL form (`condition`).

#[cfg(feature = "sparql")]
mod condition;
#[cfg(feature = "sparql")]
mod fetch;

use std::sync::Arc;

#[cfg(feature = "sparql")]
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::TranslationError,
    engine::compile::translation::{
        Translator,
        imports::{ImportFailure, PendingImport},
    },
};

/// The resolved options of a SPARQL import.
#[cfg(feature = "sparql")]
pub(super) struct SparqlImportSpec {
    /// The endpoint URL.
    pub(super) endpoint: String,
    /// The validated query to send, with any pushed condition added.
    pub(super) query: nemo_io::sparql::SparqlQuery,
    /// The query text as the directive writes it, without any pushed condition.
    ///
    /// The import's resource is named after it,
    /// so that solutions added through `add_resource` and the load reading them agree on the
    /// name.
    pub(super) written_query: String,
    /// Request headers, applied to the fetch.
    pub(super) headers: Vec<(String, String)>,
    /// The projection's columns as the directive keeps and the plan
    /// prunes them.
    pub(super) layout: crate::engine::compile::layout::ColumnLayout,
    /// At most this many binding rows per atom per request of a bound
    /// import.
    pub(super) page_size: usize,
    /// What a failed request does.
    pub(super) on_error: crate::model::OnError,
}

/// The evaluated options of a SPARQL directive.
#[cfg(feature = "sparql")]
struct SparqlDirectiveOptions {
    /// The endpoint URL, if given.
    endpoint: Option<String>,
    /// The endpoint's source position, if given.
    endpoint_span: Option<crate::model::Span>,
    /// The query text, if given.
    query: Option<String>,
    /// The query's source position, or the directive's.
    query_span: Option<crate::model::Span>,
    /// Request headers.
    headers: Vec<(String, String)>,
    /// The base IRI the query resolves against, if given.
    base: Option<String>,
    /// The base IRI's source position, if given.
    base_span: Option<crate::model::Span>,
    /// The declared column formats, if given.
    formats: Option<Vec<nemo_io::format::ValueFormat>>,
    /// The column formats' source position, if given.
    formats_span: Option<crate::model::Span>,
    /// The binding rows per atom per request of a bound import.
    page_size: usize,
    /// What a failed request does.
    on_error: crate::model::OnError,
}

/// The binding rows per atom one request of a bound import carries unless the `page` option
/// sets another number.
///
/// The number stays within what public endpoints accept in one `VALUES` block.
#[cfg(feature = "sparql")]
const DEFAULT_PAGE_SIZE: usize = 10_000;

#[cfg(feature = "sparql")]
impl SparqlImportSpec {
    /// Returns the request fetching this import's solutions.
    pub(super) fn request(&self) -> nemo_io::remote::Request {
        let mut request = self.query.request(&self.endpoint);
        request.headers.extend(self.headers.iter().cloned());
        request
    }

    /// Returns the resource name this import's solutions are registered under:
    /// the endpoint with the query, so that two queries to one endpoint get different names.
    pub(super) fn resource(&self) -> String {
        format!("{}?query={}", self.endpoint, self.written_query)
    }
}

#[cfg(feature = "sparql")]
impl<'a> Translator<'a> {
    /// Executes one SPARQL import,
    /// whose query's solutions, fetched at load time, are the predicate's rows
    /// (see [execute_imports](Self::execute_imports)).
    pub(super) fn sparql_import(
        &mut self,
        import: &PendingImport,
        fetched: Option<Arc<[u8]>>,
    ) -> Result<(), ImportFailure> {
        use nemo_physical::value::Type;

        let spec = self.sparql_import_spec(import)?;
        let source_width = spec.layout.source_width();
        // The selection checks that the columns named after the predicate
        // align with the query's projection, bound or not.
        let selection = self.import_selection(import, &spec.layout)?;
        let table =
            self.register_import(import, vec![Type::ANY; spec.layout.kept()], &spec.layout)?;
        // A bound import fetches during reasoning, like a rule: nothing
        // is read at load, in any mode.
        if !import.bindings.is_empty() {
            let (rule, names) = self.bound_sparql_import(import, &spec, table, selection)?;
            self.fetch_rules.push((import.rule_index, rule, names));
            return Ok(());
        }
        if self.options_only(import) {
            return Ok(());
        }
        // The solutions came from the fetch, or were added through
        // `add_resource` under the import's resource name. Otherwise the
        // prefetch returned an error.
        let bytes = fetched
            .or_else(|| self.resources.get(&spec.resource()).cloned())
            .expect("the prefetch fetched or diagnosed every SPARQL import");
        let loaded = nemo_io::sparql::SparqlReader::new(source_width, selection)
            .read(&spec.endpoint, &bytes, self.dictionary, self.context)
            .map_err(ImportFailure::Reading)?;
        self.absorb(table, spec.layout.delivered(), loaded, &spec.endpoint);
        Ok(())
    }

    /// Returns the resolved options of a SPARQL import: its endpoint, its
    /// validated query with the pushed condition, its column layout,
    /// and the request headers.
    pub(super) fn sparql_import_spec(
        &self,
        import: &PendingImport,
    ) -> Result<SparqlImportSpec, Diagnostic> {
        use crate::engine::compile::translation::options::term_columns;

        let options = self.sparql_options(import)?;
        let Some(endpoint) = options.endpoint else {
            return Err(TranslationError::OptionMissing {
                format: String::from("sparql"),
                option: String::from("endpoint"),
            }
            .at(import.span));
        };
        if !nemo_io::remote::Request::is_remote(&endpoint) {
            return Err(TranslationError::EndpointNotHttp { endpoint }
                .at(options.endpoint_span.or(import.span)));
        }
        let text = options
            .query
            .unwrap_or_else(|| String::from(nemo_io::sparql::DEFAULT_QUERY));
        let query = nemo_io::sparql::SparqlQuery::parse(
            &text,
            options.base.as_deref().or(self.base_iri.as_deref()),
            self.prefixes
                .iter()
                .map(|(name, iri)| (name.as_str(), iri.as_str())),
        )
        .map_err(|error| match error {
            // A base the directive gives is its option's fault.
            nemo_io::sparql::QueryError::Base { reason, .. } if options.base.is_some() => {
                TranslationError::option_type("base", "an absolute IRI")
                    .at(options.base_span.or(import.span))
                    .with_note(reason)
            }
            error => TranslationError::NotASparqlQuery {
                reason: error.to_string(),
            }
            .at(options.query_span),
        })?;
        // Checked before anything is fetched: the solutions of such a
        // query have no columns to import.
        if query.arity() == 0 {
            return Err(TranslationError::SparqlNoProjection.at(options.query_span));
        }
        let kept = term_columns(
            import.format,
            options.formats.as_deref(),
            query.arity(),
            options.formats_span.or(import.span),
        )?;
        let layout = self.plan.layout(&import.predicate, kept);
        let written_query = query.text().to_owned();
        let condition = self
            .import_condition(import, &layout)?
            .map(|(condition, _)| condition);
        let query = self.pushed_query(condition.as_ref(), query, &layout);
        Ok(SparqlImportSpec {
            endpoint,
            query,
            written_query,
            headers: options.headers,
            layout,
            page_size: options.page_size,
            on_error: options.on_error,
        })
    }

    /// Evaluates a SPARQL directive's options, the guard bindings in
    /// scope.
    fn sparql_options(&self, import: &PendingImport) -> Result<SparqlDirectiveOptions, Diagnostic> {
        use nemo_physical::value::ValueRef;

        use crate::{
            diagnostics::TranslationError,
            engine::compile::translation::options::{iri_or_string, on_error, option_name},
        };

        let bindings = self.guard_bindings(&import.guards)?;
        let mut options = SparqlDirectiveOptions {
            endpoint: None,
            endpoint_span: None,
            query: None,
            query_span: import.span,
            headers: Vec::new(),
            base: None,
            base_span: None,
            formats: None,
            formats_span: None,
            page_size: DEFAULT_PAGE_SIZE,
            on_error: Default::default(),
        };
        for (key, value) in &import.options {
            let name = option_name(key, "sparql")?;
            if name == "format" {
                options.formats = Some(self.column_formats(value)?);
                options.formats_span = value.span;
                continue;
            }
            if name == "on_error" {
                options.on_error = on_error(value)?;
                continue;
            }
            if name == "http_headers" {
                options.headers = self.header_option(value, &bindings)?;
                continue;
            }
            let evaluated = self.option_value(value, &bindings)?;
            match (name, evaluated.as_ref()) {
                ("page", ValueRef::Number(count))
                    if count.is_integral() && !count.is_negative() && !count.is_zero() =>
                {
                    options.page_size = count
                        .to_i64()
                        .and_then(|count| usize::try_from(count).ok())
                        .unwrap_or(usize::MAX);
                }
                ("page", other) => {
                    return Err(TranslationError::PageNotACount {
                        value: other.to_string(),
                    }
                    .at(value.span));
                }
                ("base", ValueRef::Iri(_) | ValueRef::String(_)) => {
                    options.base = iri_or_string(evaluated.as_ref());
                    options.base_span = value.span;
                }
                ("base", _) => {
                    return Err(
                        TranslationError::option_type("base", "an IRI or a string").at(value.span)
                    );
                }
                ("endpoint", ValueRef::Iri(_) | ValueRef::String(_)) => {
                    options.endpoint = iri_or_string(evaluated.as_ref());
                    options.endpoint_span = value.span;
                }
                ("endpoint", _) => {
                    return Err(
                        TranslationError::option_type("endpoint", "a string or an IRI")
                            .at(value.span),
                    );
                }
                ("query", ValueRef::String(text)) => {
                    options.query = Some(text.to_owned());
                    options.query_span = value.span.or(import.span);
                }
                ("query", _) => {
                    return Err(TranslationError::option_type("query", "a string").at(value.span));
                }
                _ => {
                    return Err(TranslationError::unknown_option(name, "sparql").at(key.span));
                }
            }
        }
        Ok(options)
    }

    /// Returns the query with the condition an import's rows must pass, if there is one:
    /// the conditions of its `where` clause and the one the filter pass pushed.
    ///
    /// The endpoint applies each conjunct of the condition that has a SPARQL form,
    /// and the local filter applies all of it regardless.
    /// The condition refers to delivered columns,
    /// which the layout maps back to the projection's positions.
    fn pushed_query(
        &self,
        condition: Option<&crate::model::Operation>,
        query: nemo_io::sparql::SparqlQuery,
        layout: &crate::engine::compile::layout::ColumnLayout,
    ) -> nemo_io::sparql::SparqlQuery {
        let Some(condition) = condition else {
            return query;
        };
        if matches!(query.shape(), nemo_io::sparql::QueryShape::Opaque(_)) {
            return query;
        }
        let delivered = layout.delivered_columns();
        let mut conjuncts = Vec::new();
        condition.conjuncts(&mut conjuncts);
        let expression = conjuncts
            .into_iter()
            .filter_map(|conjunct| self.sparql_condition(conjunct, query.variables(), &delivered))
            .reduce(|left, right| {
                nemo_io::sparql::spargebra::algebra::Expression::And(
                    Box::new(left),
                    Box::new(right),
                )
            });
        match expression {
            Some(expression) => query.with_filter(expression),
            None => query,
        }
    }
}

#[cfg(not(feature = "sparql"))]
impl<'a> Translator<'a> {
    /// Returns an error for a SPARQL import in a run of a build without the `sparql` feature,
    /// rather than misreading it.
    ///
    /// A check accepts what this build cannot validate,
    /// since the program may be meant for a build that can.
    pub(super) fn sparql_import(
        &mut self,
        import: &PendingImport,
        _fetched: Option<Arc<[u8]>>,
    ) -> Result<(), ImportFailure> {
        use crate::engine::compile::translation::Purpose;

        if self.purpose == Purpose::Check {
            self.open.insert(import.predicate.clone());
            return Ok(());
        }
        Err(TranslationError::FeatureUnavailable {
            what: String::from("a SPARQL import"),
            feature: String::from("sparql"),
        }
        .at(import.span)
        .into())
    }
}
