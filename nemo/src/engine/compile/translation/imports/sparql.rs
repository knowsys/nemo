//! SPARQL imports: loading a query's solutions into a predicate with one column per projected
//! variable.
//!
//! An import without atoms in its `where` clause is fetched at load time.
//! A *bound import*, one with atoms in its `where` clause,
//! is fetched during reasoning by its fetch rule (`fetch`).
//! The import's condition is added to the query as far as it has a SPARQL form (`condition`).

mod condition;
mod fetch;

use std::sync::Arc;

use nemo_physical::{diagnostics::Diagnostic, value::ValueRef};

use crate::{
    diagnostics::{AboutPredicate, AboutResource, TranslationError},
    engine::compile::translation::{
        Translator,
        imports::{ImportFailure, PendingImport},
        options::{iri_or_string, on_error, option_name, term_columns},
    },
};

/// The resolved options of a SPARQL import.
pub(super) struct SparqlImportSpec {
    /// The endpoint URL.
    pub(super) endpoint: String,
    /// The validated query to send,
    /// with the import's condition added as far as it has a SPARQL form.
    pub(super) query: nemo_io::sparql::SparqlQuery,
    /// The query text as the directive writes it, without the import's condition,
    /// preceded by a `BASE` clause if a base IRI applies to it.
    ///
    /// The import's resource is named after it,
    /// so that the solutions added through
    /// [add_resource](crate::engine::Engine::add_resource) and the load that reads them agree
    /// on the name.
    /// The parsed query cannot give the name, since parsing names some variables and blank
    /// nodes at random.
    pub(super) written_query: String,
    /// The HTTP headers sent with each request.
    pub(super) headers: Vec<(String, String)>,
    /// The layout of the projection's columns, which the directive keeps and the plan prunes.
    pub(super) layout: crate::engine::compile::layout::ColumnLayout,
    /// The largest number of rows of one atom that a request of a bound import sends.
    pub(super) page_size: usize,
    /// What happens when a request fails.
    pub(super) on_error: crate::model::OnError,
}

/// The evaluated options of a SPARQL directive.
struct SparqlDirectiveOptions {
    /// The endpoint URL, if given.
    endpoint: Option<String>,
    /// The endpoint's source position, if given.
    endpoint_span: Option<crate::model::Span>,
    /// The query text, if given.
    query: Option<String>,
    /// The query's source position, or the directive's.
    query_span: Option<crate::model::Span>,
    /// The HTTP headers.
    headers: Vec<(String, String)>,
    /// The base IRI against which the query is resolved, if given.
    base: Option<String>,
    /// The base IRI's source position, if given.
    base_span: Option<crate::model::Span>,
    /// The column formats of the `format` option, if given.
    formats: Option<Vec<nemo_io::format::ValueFormat>>,
    /// The column formats' source position, if given.
    formats_span: Option<crate::model::Span>,
    /// The largest number of rows of one atom that a request of a bound import sends.
    page_size: usize,
    /// What happens when a request fails.
    on_error: crate::model::OnError,
}

/// The largest number of rows of one atom that a request of a bound import sends,
/// unless the `page` option sets another number.
///
/// The number is assumed to stay within what common endpoints accept in one `VALUES` block.
const DEFAULT_PAGE_SIZE: usize = 10_000;

impl SparqlImportSpec {
    /// Returns the request that fetches this import's solutions.
    pub(super) fn request(&self) -> nemo_io::remote::Request {
        let mut request = self.query.request(&self.endpoint);
        request.headers.extend(self.headers.iter().cloned());
        request
    }

    /// Returns the resource name under which this import's solutions are registered.
    ///
    /// It is the endpoint with the query, so that two queries to one endpoint get different
    /// names.
    pub(super) fn resource(&self) -> String {
        format!("{}?query={}", self.endpoint, self.written_query)
    }
}

impl<'a> Translator<'a> {
    /// Executes one SPARQL import (see [execute_imports](Self::execute_imports)).
    ///
    /// The query's solutions, fetched at load time, become the predicate's rows.
    /// A bound import is compiled into its fetch rule instead, and reads nothing at load.
    pub(super) fn sparql_import(
        &mut self,
        import: &PendingImport,
        fetched: Option<Arc<[u8]>>,
    ) -> Result<(), ImportFailure> {
        use nemo_physical::value::Type;

        let spec = self.sparql_import_spec(import)?;
        if !spec.query.keeps_names() {
            for variable in spec.query.never_bound_variables() {
                self.warn(
                    TranslationError::NeverBoundVariable {
                        variable: variable.to_owned(),
                    }
                    .at(import.span)
                    .with_resource(spec.endpoint.clone())
                    .with_predicate(import.predicate.clone()),
                );
            }
        }
        let source_width = spec.layout.source_width();
        // Building the selection checks that the columns named after the predicate
        // match the query's projection, for a bound import too.
        let selection = self.import_selection(import, &spec.layout)?;
        let table =
            self.register_import(import, vec![Type::ANY; spec.layout.kept()], &spec.layout)?;
        // A bound import fetches during reasoning, like a rule,
        // so nothing is read at load, whatever the translation's purpose.
        if !import.bindings.is_empty() {
            let (rule, names) = self.bound_sparql_import(import, &spec, table, selection)?;
            self.fetch_rules.push((import.rule_index, rule, names));
            return Ok(());
        }
        if self.options_only(import) {
            return Ok(());
        }
        // The solutions came from the fetch, or were registered under the
        // import's resource name. Otherwise the prefetch returned an error.
        let bytes = fetched
            .or_else(|| self.resources.get(&spec.resource()).cloned())
            .expect("the prefetch fetched or diagnosed every SPARQL import");
        let loaded = nemo_io::sparql::SparqlReader::new(source_width, selection)
            .with_endpoint_blank_nodes()
            .read(&spec.endpoint, &bytes, self.dictionary, self.context)
            .map_err(ImportFailure::Reading)?;
        self.absorb(table, spec.layout.delivered(), loaded, &spec.endpoint);
        Ok(())
    }

    /// Returns the resolved options of a SPARQL import: its endpoint,
    /// its validated query with the import's condition, its column layout,
    /// and the HTTP headers.
    pub(super) fn sparql_import_spec(
        &self,
        import: &PendingImport,
    ) -> Result<SparqlImportSpec, Diagnostic> {
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
            // An error in a base that the directive gives is the fault of the `base` option.
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
        // This is checked before anything is fetched,
        // since the solutions of a query without a projection have no columns to import.
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
        let written_query = match options.base.as_deref().or(self.base_iri.as_deref()) {
            Some(base) => format!("BASE <{base}>\n{text}"),
            None => text,
        };
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

    /// Evaluates a SPARQL directive's options, with the guard bindings in scope.
    fn sparql_options(&self, import: &PendingImport) -> Result<SparqlDirectiveOptions, Diagnostic> {
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

    /// Returns the query with the parts of an import's condition that have a SPARQL form
    /// added.
    ///
    /// `condition` is the [import_condition](Self::import_condition), if there is one.
    /// A conjunct that requires a column to be identical to a constant is added as a `VALUES`
    /// block, and any other conjunct with a SPARQL form as a filter.
    /// The load still applies the whole condition to the rows it receives.
    /// The condition refers to delivered columns,
    /// which `layout` maps back to positions in the projection.
    /// An opaque query is returned unchanged.
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
        let mut blocks = Vec::new();
        let mut expressions = Vec::new();
        for conjunct in conjuncts {
            if let Some(block) = self.sparql_values(conjunct, &delivered) {
                blocks.push(block);
            } else if let Some(expression) =
                self.sparql_condition(conjunct, query.variables(), &delivered)
            {
                expressions.push(expression);
            }
        }
        let query = if blocks.is_empty() {
            query
        } else {
            query.with_values(&blocks)
        };
        let expression = expressions.into_iter().reduce(|left, right| {
            nemo_io::sparql::spargebra::algebra::Expression::And(Box::new(left), Box::new(right))
        });
        match expression {
            Some(expression) => query.with_filter(expression),
            None => query,
        }
    }
}
