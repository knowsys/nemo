//! The options of import and export directives, evaluated.
//!
//! This module evaluates:
//! - the resource, compression, request headers, failure behavior, and row limit every format
//!   takes,
//! - the column formats,
//! - the RDF variant a format name and a resource name resolve to.
//!
//! The option list of a directive is parsed in one of two ways:
//! one for the delimiter-separated formats, one for the rest.

use std::collections::HashMap;

use nemo_io::{
    compression::Compression,
    file_format::{FileFormat, FormatOptions, RdfVariant},
    format::ValueFormat,
    remote,
};
use nemo_physical::{
    diagnostics::Diagnostic,
    value::{DataValue, ValueRef},
};

use crate::{
    diagnostics::TranslationError,
    engine::compile::translation::Translator,
    model::{Identifier, OnError, Span, Term, TermKind},
};

/// Returns a `limit` option value as a row count.
fn row_limit(value: ValueRef<'_>, span: Option<Span>) -> Result<u64, Diagnostic> {
    match value {
        ValueRef::Number(count) if count.is_integral() && !count.is_negative() => Ok(count
            .to_i64()
            .and_then(|count| u64::try_from(count).ok())
            .unwrap_or(u64::MAX)),
        other => Err(TranslationError::LimitNotACount {
            value: other.to_string(),
        }
        .at(span)),
    }
}

/// Returns the name of a directive option of the named format, which is a bare keyword.
pub(super) fn option_name<'k>(key: &'k Term, format: &str) -> Result<&'k str, Diagnostic> {
    match &key.kind {
        TermKind::Constant(Identifier::Bare(name)) => Ok(name.as_str()),
        _ => Err(TranslationError::unknown_option(&key.to_string(), format)
            .at(key.span)
            .with_hint("write the option name bare, as `resource = …`")),
    }
}

/// Returns an IRI or string option value as text (for resources, bases, and endpoints),
/// or `None` for a value of another kind.
pub(super) fn iri_or_string(value: ValueRef<'_>) -> Option<String> {
    match value {
        ValueRef::Iri(iri) => Some(iri.to_string()),
        ValueRef::String(text) => Some(text.to_owned()),
        _ => None,
    }
}

/// Returns the value of an `on_error` option, a bare keyword matched without evaluation.
pub(super) fn on_error(value: &Term) -> Result<OnError, Diagnostic> {
    OnError::from_term(value).ok_or_else(|| {
        TranslationError::UnknownOptionValue {
            option: String::from("`on_error` value"),
            value: value.to_string(),
            accepted: OnError::NAMES.to_owned(),
        }
        .at(value.span)
    })
}

/// Returns whether a text is a valid HTTP header name: a token of letters, digits, and the
/// symbols HTTP allows in one.
fn is_header_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

/// Returns whether a text is a header value an HTTP request can carry:
/// printable ASCII characters and tabs.
fn is_header_value(value: &str) -> bool {
    value
        .chars()
        .all(|character| character == '\t' || (' '..='~').contains(&character))
}

/// Returns one request header, checked to be one HTTP can carry,
/// with the spans of its name and its value as written.
fn header(
    name: String,
    value: ValueRef<'_>,
    name_span: Option<Span>,
    value_span: Option<Span>,
) -> Result<(String, String), Diagnostic> {
    if !is_header_name(&name) {
        return Err(TranslationError::HeaderName { name }.at(name_span));
    }
    match value {
        ValueRef::String(text) if is_header_value(text) => Ok((name, text.to_owned())),
        _ => Err(TranslationError::HeaderValue { name }.at(value_span)),
    }
}

/// Returns, per column of a source of terms (RDF statements, SPARQL solutions),
/// whether a `format` option keeps it.
///
/// `term` keeps and `skip` drops.
/// No other format applies, since the source writes the terms itself.
/// `span` is the position of the `format` option's value.
pub(super) fn term_columns(
    format: FileFormat,
    formats: Option<&[ValueFormat]>,
    columns: usize,
    span: Option<Span>,
) -> Result<Vec<bool>, Diagnostic> {
    let Some(formats) = formats else {
        return Ok(vec![true; columns]);
    };
    if formats.len() != columns {
        return Err(TranslationError::ColumnFormatCount {
            format: format.to_string(),
            columns: columns.into(),
            found: formats.len().into(),
        }
        .at(span));
    }
    formats
        .iter()
        .map(|column| match column {
            ValueFormat::Term => Ok(true),
            ValueFormat::Skip => Ok(false),
            other => Err(TranslationError::TermColumnFormat {
                format: other.name().to_owned(),
            }
            .at(span)),
        })
        .collect()
}

/// The options every format's directive takes, as far as the format accepts them:
/// the data's resource, how the stream is compressed,
/// the request headers and what a failed fetch does, and the row limit.
struct ResourceDirectiveOptions {
    /// The declared resource, if any.
    resource: Option<String>,
    /// The stream compression.
    compression: Compression,
    /// Request headers for a remote resource.
    headers: Vec<(String, String)>,
    /// The position of the `http_headers` option, if given.
    headers_span: Option<Span>,
    /// What a failed fetch of a remote resource does, if declared.
    on_error: Option<OnError>,
    /// The position of the `on_error` option, if given.
    on_error_span: Option<Span>,
    /// At most this many rows, if declared.
    limit: Option<u64>,
}

impl ResourceDirectiveOptions {
    /// Creates options with nothing declared: automatic compression, every row.
    fn new() -> Self {
        Self {
            resource: None,
            compression: Compression::Auto,
            headers: Vec::new(),
            headers_span: None,
            on_error: None,
            on_error_span: None,
            limit: None,
        }
    }

    /// Takes an option whose value is a keyword, matched before any evaluation,
    /// if it is one of these and the format accepts it.
    ///
    /// Returns `Ok(false)` for an option of another kind.
    fn take_keyword(
        &mut self,
        key: &Term,
        name: &str,
        value: &Term,
        accepts: FormatOptions,
    ) -> Result<bool, Diagnostic> {
        if name != "on_error" || !accepts.headers {
            return Ok(false);
        }
        self.on_error = Some(on_error(value)?);
        self.on_error_span = key.span;
        Ok(true)
    }

    /// Takes an evaluated option if it is one of these and the format accepts it.
    ///
    /// Returns `Ok(false)` for an option of another kind.
    fn take(
        &mut self,
        name: &str,
        value: ValueRef<'_>,
        span: Option<Span>,
        accepts: FormatOptions,
    ) -> Result<bool, Diagnostic> {
        match (name, value) {
            ("limit", value) if accepts.limit => self.limit = Some(row_limit(value, span)?),
            ("resource", value) => {
                self.resource = Some(iri_or_string(value).ok_or_else(|| {
                    TranslationError::option_type("resource", "a string or an IRI").at(span)
                })?);
            }
            ("compression", ValueRef::String(kind)) if accepts.compression => {
                self.compression = Compression::from_name(kind).ok_or_else(|| {
                    TranslationError::UnknownOptionValue {
                        option: String::from("`compression` value"),
                        value: kind.to_owned(),
                        accepted: String::from("gzip, none"),
                    }
                    .at(span)
                })?;
            }
            ("compression", _) if accepts.compression => {
                return Err(TranslationError::option_type("compression", "a string").at(span));
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Checks that request headers and failure behavior are given for a remote resource only.
    fn check_remote(&self) -> Result<(), Diagnostic> {
        let remote = self
            .resource
            .as_deref()
            .is_some_and(remote::Request::is_remote);
        if !self.headers.is_empty() && !remote {
            return Err(TranslationError::HeadersNotRemote.at(self.headers_span));
        }
        if self.on_error.is_some() && !remote {
            return Err(TranslationError::OnErrorNotRemote.at(self.on_error_span));
        }
        Ok(())
    }
}

/// The evaluated options of a DSV directive, for imports and exports alike.
pub(super) struct DsvDirectiveOptions {
    /// The column delimiter.
    pub(super) delimiter: u8,
    /// The format name (also the default file extension).
    pub(super) extension: String,
    /// The declared resource, if any.
    pub(super) resource: Option<String>,
    /// The declared column formats, if any.
    pub(super) formats: Option<Vec<ValueFormat>>,
    /// Whether `"` quotes fields.
    pub(super) quoting: bool,
    /// Whether the first record is skipped.
    pub(super) ignore_headers: bool,
    /// The stream compression.
    pub(super) compression: Compression,
    /// At most this many rows, if declared.
    pub(super) limit: Option<u64>,
}

/// The evaluated options of a JSON, RDF, or Parquet directive.
pub(super) struct PlainDirectiveOptions {
    /// The declared resource, if any.
    pub(super) resource: Option<String>,
    /// The declared base IRI, if the format takes one.
    pub(super) base: Option<String>,
    /// The stream compression.
    pub(super) compression: Compression,
    /// At most this many rows, if the format takes a limit.
    pub(super) limit: Option<u64>,
    /// The declared column formats, if the format takes them.
    pub(super) formats: Option<Vec<ValueFormat>>,
    /// The position of the `format` option's value, if given.
    pub(super) formats_span: Option<Span>,
}

impl<'a> Translator<'a> {
    /// Parses a DSV directive's format name and options,
    /// the option values evaluating with the guard bindings in scope.
    pub(super) fn dsv_directive(
        &self,
        format: FileFormat,
        options: &[(Term, Term)],
        bindings: &HashMap<String, DataValue>,
        span: Option<Span>,
        reading: bool,
    ) -> Result<DsvDirectiveOptions, Diagnostic> {
        assert!(
            format.is_dsv(),
            "`{format}` is not a delimiter-separated format"
        );
        let accepts = format.options(reading);
        let mut common = ResourceDirectiveOptions::new();
        let mut delimiter_option = None;
        let mut formats = None;
        let mut quoting = true;
        let mut ignore_headers = false;
        for (key, value) in options {
            let name = option_name(key, format.name())?;
            if !reading
                && matches!(
                    name,
                    "ignore_headers" | "quoting" | "http_headers" | "on_error"
                )
            {
                return Err(TranslationError::ReadOptionOnExport {
                    option: name.to_owned(),
                }
                .at(key.span));
            }
            // The column formats are keywords, matched without
            // evaluation. Every other option value evaluates to one value.
            if name == "format" {
                formats = Some(self.column_formats(value)?);
                continue;
            }
            if name == "http_headers" && accepts.headers {
                common.headers = self.header_option(value, bindings)?;
                common.headers_span = key.span;
                continue;
            }
            if common.take_keyword(key, name, value, accepts)? {
                continue;
            }
            let evaluated = self.option_value(value, bindings)?;
            if common.take(name, evaluated.as_ref(), value.span, accepts)? {
                continue;
            }
            match (name, evaluated.as_ref()) {
                // `csv` and `tsv` name their delimiters.
                ("delimiter", _) if format != FileFormat::Dsv => {
                    return Err(TranslationError::unknown_option(name, format.name())
                        .at(key.span)
                        .with_hint("read or write another delimiter with `dsv`"));
                }
                ("delimiter", ValueRef::String(text)) if text.len() == 1 => {
                    delimiter_option = Some(text.as_bytes()[0]);
                }
                ("delimiter", _) => {
                    return Err(TranslationError::option_type(
                        "delimiter",
                        "a string of one ASCII character",
                    )
                    .at(value.span));
                }
                ("ignore_headers", ValueRef::Boolean(flag)) => ignore_headers = flag,
                ("quoting", ValueRef::Boolean(flag)) => quoting = flag,
                ("ignore_headers" | "quoting", _) => {
                    return Err(TranslationError::option_type(name, "a boolean").at(value.span));
                }
                _ => {
                    return Err(TranslationError::unknown_option(name, format.name()).at(key.span));
                }
            }
        }
        common.check_remote()?;
        let delimiter = match format {
            FileFormat::Csv => b',',
            FileFormat::Tsv => b'\t',
            _ => delimiter_option.ok_or_else(|| {
                TranslationError::OptionMissing {
                    format: String::from("dsv"),
                    option: String::from("delimiter"),
                }
                .at(span)
            })?,
        };
        Ok(DsvDirectiveOptions {
            delimiter,
            extension: format.name().to_owned(),
            resource: common.resource,
            formats,
            quoting,
            ignore_headers,
            compression: common.compression,
            limit: common.limit,
        })
    }

    /// Parses the options of a JSON, RDF, or Parquet directive, of an import if `reading` is
    /// set and of an export otherwise.
    ///
    /// Option values evaluate with the guard bindings in scope,
    /// and an option outside the accepted set is an error.
    pub(super) fn plain_directive(
        &self,
        format: FileFormat,
        options: &[(Term, Term)],
        bindings: &HashMap<String, DataValue>,
        reading: bool,
    ) -> Result<PlainDirectiveOptions, Diagnostic> {
        let accepts = format.options(reading);
        let mut common = ResourceDirectiveOptions::new();
        let mut base = None;
        let mut formats = None;
        let mut formats_span = None;
        for (key, value) in options {
            let name = option_name(key, format.name())?;
            // An option about fetching, which this format's imports take.
            if !reading
                && format.options(true).headers
                && matches!(name, "http_headers" | "on_error")
            {
                return Err(TranslationError::ReadOptionOnExport {
                    option: name.to_owned(),
                }
                .at(key.span));
            }
            // The column formats are keywords, matched without evaluation.
            if name == "format" && accepts.formats {
                formats = Some(self.column_formats(value)?);
                formats_span = value.span;
                continue;
            }
            if name == "http_headers" && accepts.headers {
                common.headers = self.header_option(value, bindings)?;
                common.headers_span = key.span;
                continue;
            }
            if common.take_keyword(key, name, value, accepts)? {
                continue;
            }
            let evaluated = self.option_value(value, bindings)?;
            if common.take(name, evaluated.as_ref(), value.span, accepts)? {
                continue;
            }
            match (name, evaluated.as_ref()) {
                ("base", ValueRef::Iri(_) | ValueRef::String(_)) if accepts.base => {
                    base = iri_or_string(evaluated.as_ref());
                }
                ("base", _) if accepts.base => {
                    return Err(
                        TranslationError::option_type("base", "an IRI or a string").at(value.span)
                    );
                }
                _ => {
                    return Err(TranslationError::unknown_option(name, format.name()).at(key.span));
                }
            }
        }
        common.check_remote()?;
        Ok(PlainDirectiveOptions {
            resource: common.resource,
            base,
            compression: common.compression,
            limit: common.limit,
            formats,
            formats_span,
        })
    }

    /// Returns the name–value pairs of an `http_headers` option.
    ///
    /// A key written as a bare name is the header name as written,
    /// since the program's base and prefixes apply to the names of values, not to headers.
    /// A value given otherwise, as a parameter holding a map, evaluates first.
    pub(super) fn header_option(
        &self,
        value: &Term,
        bindings: &HashMap<String, DataValue>,
    ) -> Result<Vec<(String, String)>, Diagnostic> {
        if let TermKind::Map { label: None, pairs } = &value.kind {
            return pairs
                .iter()
                .map(|(key, entry)| {
                    let name = match &key.kind {
                        TermKind::Constant(Identifier::Bare(name)) => name.clone(),
                        TermKind::String(text) => text.clone(),
                        _ => {
                            let evaluated = self.option_value(key, bindings)?;
                            match evaluated.as_ref() {
                                ValueRef::String(text) => text.to_owned(),
                                _ => {
                                    return Err(TranslationError::HeaderName {
                                        name: key.to_string(),
                                    }
                                    .at(key.span.or(value.span)));
                                }
                            }
                        }
                    };
                    let evaluated = self.option_value(entry, bindings)?;
                    header(
                        name,
                        evaluated.as_ref(),
                        key.span.or(value.span),
                        entry.span.or(value.span),
                    )
                })
                .collect();
        }
        let evaluated = self.option_value(value, bindings)?;
        let ValueRef::Map(map) = evaluated.as_ref() else {
            return Err(
                TranslationError::option_type("http_headers", "a map of names to strings")
                    .at(value.span),
            );
        };
        map.pairs()
            .map(|(name, entry)| {
                let Some(name) = iri_or_string(name) else {
                    return Err(TranslationError::HeaderName {
                        name: name.to_string(),
                    }
                    .at(value.span));
                };
                header(name, entry, value.span, value.span)
            })
            .collect()
    }

    /// Returns the variant of an RDF format,
    /// resolving the generic `rdf` format by the resource's extension.
    ///
    /// Returns an error if the generic format has no resource to resolve by,
    /// or if the extension names no variant.
    pub(super) fn rdf_variant(
        &self,
        format: FileFormat,
        resource: Option<&str>,
        span: Option<Span>,
    ) -> Result<RdfVariant, Diagnostic> {
        if resource.is_none() && format == FileFormat::Rdf(None) {
            return Err(TranslationError::OptionMissing {
                format: format.name().to_owned(),
                option: String::from("resource"),
            }
            .at(span));
        }
        format
            .rdf_variant(resource)
            .ok_or_else(|| TranslationError::RdfExtensionUnknown.at(span))
    }

    /// Parses a `format` option value: a column format name, or a tuple
    /// of them.
    pub(super) fn column_formats(&self, value: &Term) -> Result<Vec<ValueFormat>, Diagnostic> {
        let single = |term: &Term| -> Result<ValueFormat, Diagnostic> {
            let TermKind::Constant(Identifier::Bare(name)) = &term.kind else {
                return Err(TranslationError::ColumnFormatNotName.at(term.span));
            };
            ValueFormat::from_name(name).ok_or_else(|| {
                TranslationError::UnknownOptionValue {
                    option: String::from("column format"),
                    value: name.clone(),
                    accepted: ValueFormat::NAMES.to_owned(),
                }
                .at(term.span)
            })
        };
        match &value.kind {
            TermKind::Tuple(terms) => terms.iter().map(single).collect(),
            _ => Ok(vec![single(value)?]),
        }
    }
}
