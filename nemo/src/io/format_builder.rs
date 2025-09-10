//! Infrastructure for validating and creating objects for data import/export

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    str::FromStr,
    sync::Arc,
};

use nemo_physical::{
    datavalues::{AnyDataValue, DataValue},
    resource::{Resource, ResourceBuilder, ResourceValidationErrorKind},
};
use strum::IntoEnumIterator;

use crate::{
    chase_model::components::rule::ChaseRule,
    rule_model::{
        components::{
            ComponentSource,
            import_export::{Direction, specification::ImportExportSpec},
            rule::Rule,
            tag::Tag,
            term::{operation::Operation, primitive::ground::GroundTerm, value_type::ValueType},
        },
        error::{ValidationReport, hint::Hint, info::Info, validation_error::ValidationError},
        substitution::Substitution,
    },
    syntax::import_export::attribute,
};

use super::{
    compression_format::CompressionFormat,
    formats::{
        Export, ExportHandler, Import, ImportHandler,
        dsv::{DsvBuilder, DsvTag},
        json::{JsonHandler, JsonTag},
        rdf::{RdfHandler, RdfTag},
        sparql::{SparqlBuilder, SparqlTag},
    },
    http_parameters,
};

pub(crate) trait FormatParameter<Tag>:
    FromStr<Err = ()> + Debug + ToString + IntoEnumIterator + Copy + Eq + Hash
{
    fn required_for(&self, tag: Tag) -> bool;
    fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationError>;
}

pub(super) fn value_type_matches(
    param: impl ToString,
    value: &AnyDataValue,
    supported_types: &[ValueType],
) -> Result<(), ValidationError> {
    let value_type = ValueType::from(value.value_domain());
    supported_types.contains(&value_type).then_some(()).ok_or(
        ValidationError::ImportExportAttributeValueType {
            parameter: param.to_string(),
            given: value_type.name().into(),
            expected: format!(
                "one of {}",
                supported_types
                    .iter()
                    .map(|typ| typ.name())
                    .intersperse(", ")
                    .collect::<String>()
            ),
        },
    )
}

pub(crate) trait FormatTag:
    FromStr<Err = ()> + Debug + ToString + Copy + Eq + 'static + Into<SupportedFormatTag>
{
    // NOTE: the only implementations of this trait happen in macros and are for some reason not recognized
    #[allow(dead_code)]
    const VARIANTS: &'static [(Self, &'static str)];
}

/// Define an enum for format tags (the "predicate-name" in import/export declarations)
/// that are handled by one particular [`FormatBuilder`]
macro_rules! format_tag {
    { $vis:vis enum $type_name:ident(SupportedFormatTag::$sup_tag:ident) { $($tag_name:ident => $tag_value:expr,)* } } => {
        #[allow(unused_imports)]
        use enum_assoc::Assoc;

        #[derive(Assoc, Debug, Copy, Clone, PartialEq, Eq, Hash)]
        #[func(pub fn from_str(input: &str) -> Option<Self>)]
        #[func(pub fn name(&self) -> &'static str)]
        $vis enum $type_name {
            $(
                #[assoc(from_str = $tag_value)]
                #[assoc(name = $tag_value)]
                $tag_name
            ),*
        }

        impl From<$type_name> for crate::io::format_builder::SupportedFormatTag {
            fn from(value: $type_name) -> crate::io::format_builder::SupportedFormatTag {
                crate::io::format_builder::SupportedFormatTag::$sup_tag(value)
            }
        }

        impl std::str::FromStr for $type_name {
            type Err = ();

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::from_str(s).ok_or(())
            }
        }

        impl std::fmt::Display for $type_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.name())
            }
        }

        impl crate::io::format_builder::FormatTag for $type_name {
            const VARIANTS: &'static [(Self, &'static str)] = &[$((Self::$tag_name, $tag_value)),*];
        }
    };
}

pub(crate) use format_tag;

/// Define an enum for the parameters supplied to a [`FormatBuilder`]
macro_rules! format_parameter {
    { $vis:vis enum $type_name:ident($base_name:ty) {
        $($param_variant:ident(name = $param_name:path, supported_types = $supported_types:expr),)*
    } } => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
        #[allow(private_interfaces)]
        $vis enum $type_name {
            BaseParamType($base_name),
            $($param_variant),*
        }

        impl $type_name {
            pub(crate) fn supported_types(&self) -> &'static [ValueType] {
                match self {
                    Self::BaseParamType(base) => base.supported_types(),
                    $(Self::$param_variant => $supported_types,)*
                }
            }
        }

        impl From<$base_name> for $type_name {
            fn from(value: $base_name) -> Self {
                Self::BaseParamType(value)
            }
        }

        impl strum::IntoEnumIterator for $type_name {
            type Iterator = std::vec::IntoIter<Self>;

            fn iter() -> Self::Iterator {
                #[allow(unused_mut)]
                let mut vec = Vec::from_iter(<$base_name as strum::IntoEnumIterator>::iter().map(Self::BaseParamType));
                $(vec.push(Self::$param_variant);)*
                vec.into_iter()
            }
        }

        impl std::str::FromStr for $type_name {
            type Err = ();

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                match input {
                    $($param_name => Ok(Self::$param_variant),)*
                    other => <$base_name as std::str::FromStr>::from_str(other).map(Self::BaseParamType)
                }
            }
        }

        impl std::fmt::Display for $type_name {
            fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::BaseParamType(base) => <$base_name as std::fmt::Display>::fmt(base, fmt),
                    $(Self::$param_variant => fmt.write_str($param_name),)*
                }
            }
        }
    };
}

pub(crate) use format_parameter;

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum_macros::EnumIter, Hash)]
enum NoParameters {}

impl NoParameters {
    // NOTE: this method is actually needed for the format_parameter macro to work
    #[allow(dead_code)]
    fn name(&self) -> &'static str {
        match *self {}
    }

    fn supported_types(&self) -> &'static [ValueType] {
        match *self {}
    }
}

impl std::fmt::Display for NoParameters {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl FromStr for NoParameters {
    type Err = ();

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Err(())
    }
}

format_parameter! {
    pub(crate) enum StandardParameter(NoParameters) {
        Resource(name = attribute::RESOURCE, supported_types = &[ValueType::String, ValueType::Constant]),
        Compression(name = attribute::COMPRESSION, supported_types = &[ValueType::String]),
        HttpHeaders(name = attribute::HTTP_HEADERS, supported_types= &[ValueType::Map]),
        HttpGetParameters(name = attribute::HTTP_GET_PARAMETERS, supported_types= &[ValueType::Map]),
        IriFragment(name = attribute::IRI_FRAGMENT, supported_types= &[ValueType::String]),
        HttpPostParameters(name = attribute::HTTP_POST_PARAMETERS, supported_types= &[ValueType::Map]),
    }
}

impl<Tag> FormatParameter<Tag> for StandardParameter {
    fn required_for(&self, _tag: Tag) -> bool {
        false
    }

    fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationError> {
        value_type_matches(self, &value, self.supported_types())?;

        match self {
            StandardParameter::BaseParamType(no_parameters) => match *no_parameters {},
            StandardParameter::Resource => ResourceBuilder::try_from(value)
                .and(Ok(()))
                .map_err(ValidationError::from),
            StandardParameter::Compression => {
                CompressionFormat::from_name(&value.to_plain_string_unchecked())
                    .and(Some(()))
                    .ok_or(ValidationError::ImportExportUnknownCompression {
                        format: value.to_string(),
                    })
            }
            StandardParameter::HttpHeaders => http_parameters::validate_headers(value),
            StandardParameter::HttpGetParameters => {
                http_parameters::validate_http_parameters(value)
            }
            StandardParameter::IriFragment => Ok(()),
            StandardParameter::HttpPostParameters => {
                http_parameters::validate_http_parameters(value)
            }
        }
    }
}

pub(crate) trait FormatBuilder: Debug + Sized + Into<AnyImportExportBuilder> {
    type Tag: FormatTag + 'static;
    type Parameter: FormatParameter<Self::Tag> + From<StandardParameter> + 'static;

    fn new(
        tag: Self::Tag,
        parameters: &Parameters<Self>,
        direction: Direction,
    ) -> Result<Self, ValidationError>;

    fn expected_arity(&self) -> Option<usize>;

    fn supports_tag(tag: &str) -> bool {
        Self::Tag::from_str(tag).is_ok()
    }

    fn customize_resource_builder(
        &self,
        _direction: Direction,
        builder: Option<ResourceBuilder>,
    ) -> Result<Option<ResourceBuilder>, ResourceValidationErrorKind> {
        Ok(builder)
    }

    fn build_import(
        &self,
        arity: usize,
        filter_rules: Vec<ChaseRule>,
    ) -> Arc<dyn ImportHandler + Send + Sync + 'static>;
    fn build_export(
        &self,
        arity: usize,
        filter_rules: Vec<ChaseRule>,
    ) -> Arc<dyn ExportHandler + Send + Sync + 'static>;
}

#[derive(Debug)]
pub(crate) struct Parameters<B: FormatBuilder>(HashMap<B::Parameter, AnyDataValue>);

impl<B: FormatBuilder> Parameters<B> {
    pub(crate) fn get_required(&self, param: B::Parameter) -> AnyDataValue {
        self.get_optional(param)
            .expect("presence of required parameters is checked upon validation")
    }

    pub(crate) fn get_optional(&self, param: B::Parameter) -> Option<AnyDataValue> {
        self.0.get(&param).cloned()
    }

    /// Take as input a list of bindings (statements of the form ?variable = ground term),
    /// validate that they are of the correct form
    /// and consturct a [Substitution].
    fn build_substitution(
        bindings: &[Operation],
        report: &mut ValidationReport,
    ) -> Option<Substitution> {
        let mut result = HashMap::new();

        for binding in bindings {
            let Some((left, right)) = binding.variable_assignment() else {
                report.add(binding, ValidationError::DirectiveNonAssignment);
                continue;
            };

            if !right.is_resolvable() {
                report.add(binding, ValidationError::DirectiveAssignmentNotGround);
            };

            if let Some((_, previous_binding)) =
                result.insert(left.clone(), (right.clone(), binding))
            {
                report
                    .add(
                        binding,
                        ValidationError::DirectiveConflictingAssignments {
                            variable: Box::new(left.clone()),
                        },
                    )
                    .add_context(previous_binding, Info::FirstDefinition);
            }
        }

        Some(Substitution::new(
            result
                .into_iter()
                .map(|(key, (value, _origin))| (key, value)),
        ))
    }

    pub(crate) fn validate_filter_rule(
        predicate: &Tag,
        rule: &Rule,
        report: &mut ValidationReport,
    ) -> Option<()> {
        let mut valid = true;

        for literal in rule.body() {
            match literal.predicate() {
                None => (),
                Some(filter_predicate) => {
                    if *predicate != filter_predicate {
                        valid = false;
                        report.add(literal, todo!("add new error code"));
                    }
                }
            }
        }

        valid.then_some(())
    }

    pub(crate) fn validate(
        predicate: Tag,
        spec: &ImportExportSpec,
        bindings: &[Operation],
        filter_rules: &[Rule],
        direction: Direction,
        report: &mut ValidationReport,
    ) -> Option<Self> {
        let Ok(format_tag) = B::Tag::from_str(spec.format().name()) else {
            report.add(
                spec,
                ValidationError::ImportExportFileFormatUnknown {
                    format: spec.format().name().to_owned(),
                },
            );
            return None;
        };

        for rule in filter_rules {
            Self::validate_filter_rule(&predicate, rule, report)?;
        }

        let mut has_errors = false;
        let mut spec = spec.clone();
        let substitution = Self::build_substitution(bindings, report)?;
        substitution.apply(&mut spec);

        let mut required_parameters: HashSet<_> = B::Parameter::iter()
            .filter(|param| param.required_for(format_tag))
            .collect();

        let mut result = HashMap::new();

        for (key, value_term) in spec.key_value() {
            let Ok(parameter) = B::Parameter::from_str(key.value()) else {
                report
                    .add(
                        key,
                        ValidationError::ImportExportUnrecognizedAttribute {
                            format: format_tag.to_string(),
                            attribute: key.to_string(),
                        },
                    )
                    .add_hint_option(Hint::similar(
                        "parameter",
                        key.value(),
                        B::Parameter::iter().map(|attribute| attribute.to_string()),
                    ));

                has_errors = true;
                continue;
            };

            required_parameters.remove(&parameter);

            if !value_term.is_resolvable() {
                report.add(
                    value_term,
                    ValidationError::ImportExportParameterNotGround {
                        term: Box::new(value_term.clone()),
                    },
                );

                has_errors = true;
                continue;
            }

            let value = match GroundTerm::try_from(value_term.clone()) {
                Ok(ground_term) => ground_term.value(),
                Err(_) => {
                    has_errors = true;
                    continue;
                }
            };

            if let Err(error) = parameter.is_value_valid(value.clone()) {
                report.add(value_term, error);
                has_errors = true;
                continue;
            }

            result.insert(parameter, value);
        }

        for param in required_parameters {
            report.add(
                &spec,
                ValidationError::ImportExportMissingRequiredAttribute {
                    attribute: param.to_string(),
                    direction: direction.to_string(),
                },
            );
            has_errors = true;
        }

        if has_errors { None } else { Some(Self(result)) }
    }
}

/// Validates parameters for an import/export operation and finally builds the
/// corresponding handler (i.e. [`ImportHandler`] or [`ExportHandler`])
#[derive(Clone, Debug)]
pub struct ImportExportBuilder {
    inner: AnyImportExportBuilder,
    resource: Option<Resource>,
    compression: CompressionFormat,
}

/// Supported import formats
#[derive(Debug, Clone, Copy)]
pub enum SupportedFormatTag {
    /// Delimiter-separated values
    Dsv(DsvTag),
    /// Resource Description Framework
    Rdf(RdfTag),
    /// JSON
    Json(JsonTag),
    /// SPARQL
    Sparql(SparqlTag),
}

impl FromStr for SupportedFormatTag {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if DsvBuilder::supports_tag(s) {
            Ok(Self::Dsv(s.parse().unwrap()))
        } else if RdfHandler::supports_tag(s) {
            Ok(Self::Rdf(s.parse().unwrap()))
        } else if JsonHandler::supports_tag(s) {
            Ok(Self::Json(s.parse().unwrap()))
        } else if SparqlBuilder::supports_tag(s) {
            Ok(Self::Sparql(s.parse().unwrap()))
        } else {
            Err(())
        }
    }
}

impl ImportExportBuilder {
    /// Returns the inherent arity of this file format, if applicable
    pub fn expected_arity(&self) -> Option<usize> {
        match &self.inner {
            AnyImportExportBuilder::Dsv(inner) => inner.expected_arity(),
            AnyImportExportBuilder::Rdf(inner) => inner.expected_arity(),
            AnyImportExportBuilder::Json(inner) => inner.expected_arity(),
            AnyImportExportBuilder::Sparql(inner) => inner.expected_arity(),
        }
    }

    /// Return the [SupportedFormatTag] of this file format.
    pub fn format(&self) -> SupportedFormatTag {
        self.inner.format_tag()
    }

    /// The resource as specified in the parameters
    pub fn resource(&self) -> Option<Resource> {
        self.resource.clone()
    }

    fn new_with_tag<B: FormatBuilder>(
        predicate: Tag,
        tag: B::Tag,
        spec: &ImportExportSpec,
        bindings: &[Operation],
        filter_rules: &[Rule],
        direction: Direction,
        report: &mut ValidationReport,
    ) -> Option<ImportExportBuilder> {
        let origin = spec.origin();
        let parameters =
            Parameters::<B>::validate(predicate, spec, bindings, filter_rules, direction, report)?;

        let resource_builder =
            if let Some(value) = parameters.get_optional(StandardParameter::Resource.into()) {
                match ResourceBuilder::try_from(value) {
                    Ok(builder) => Some(builder),
                    Err(error) => {
                        report.add_source(origin.clone(), error.into());
                        return None;
                    }
                }
            } else {
                None
            };

        let compression = parameters
            .get_optional(StandardParameter::Compression.into())
            .map(|compression| {
                CompressionFormat::from_name(&compression.to_plain_string_unchecked()).unwrap()
            })
            .unwrap_or_else(|| CompressionFormat::from_resource_builder(&resource_builder));

        let inner = match B::new(tag, &parameters, direction) {
            Ok(b) => b,
            Err(error) => {
                report.add_source(origin.clone(), error);
                return None;
            }
        };

        let resource_builder = match inner.customize_resource_builder(direction, resource_builder) {
            Ok(builder) => builder,
            Err(error) => {
                report.add_source(origin.clone(), error.into());
                return None;
            }
        };

        let Some(mut resource_builder) = resource_builder else {
            return Some(ImportExportBuilder {
                inner: inner.into(),
                resource: None,
                compression,
            });
        };

        if let Some(headers) = parameters.get_optional(StandardParameter::HttpHeaders.into())
            && let Err(error) = http_parameters::unpack_headers(headers).and_then(|mut headers| {
                headers.try_for_each(|(key, value)| {
                    resource_builder
                        .add_header(key, value)
                        .and(Ok(()))
                        .map_err(ValidationError::from)
                })
            })
        {
            report.add_source(origin.clone(), error);
        }

        if let Some(parameters) =
            parameters.get_optional(StandardParameter::HttpGetParameters.into())
            && let Err(error) =
                http_parameters::unpack_http_parameters(parameters).and_then(|mut parameters| {
                    parameters.try_for_each(|(key, value)| {
                        resource_builder
                            .add_get_parameter(key, value)
                            .and(Ok(()))
                            .map_err(ValidationError::from)
                    })
                })
        {
            report.add_source(origin.clone(), error);
        }

        if let Some(parameters) =
            parameters.get_optional(StandardParameter::HttpPostParameters.into())
            && let Err(error) =
                http_parameters::unpack_http_parameters(parameters).and_then(|mut parameters| {
                    parameters.try_for_each(|(key, value)| {
                        resource_builder
                            .add_post_parameter(key, value)
                            .and(Ok(()))
                            .map_err(ValidationError::from)
                    })
                })
        {
            report.add_source(origin.clone(), error);
        }

        if let Some(fragment) = parameters.get_optional(StandardParameter::IriFragment.into())
            && let Err(error) = resource_builder
                .set_fragment(fragment.to_plain_string_unchecked())
                .and(Ok(()))
                .map_err(ValidationError::from)
        {
            report.add_source(origin.clone(), error);
        }

        Some(ImportExportBuilder {
            inner: inner.into(),
            resource: Some(resource_builder.finalize()),
            compression,
        })
    }

    /// Create a new [ImportExportBuilder].
    pub(crate) fn new(
        predicate: Tag,
        spec: &ImportExportSpec,
        bindings: &[Operation],
        filter_rules: &[Rule],
        direction: Direction,
        report: &mut ValidationReport,
    ) -> Option<Self> {
        let format_tag = spec.format().name().to_owned();
        let Ok(tag) = format_tag.parse::<SupportedFormatTag>() else {
            report.add(
                spec,
                ValidationError::ImportExportFileFormatUnknown {
                    format: format_tag.to_string(),
                },
            );

            return None;
        };

        match tag {
            SupportedFormatTag::Dsv(tag) => Self::new_with_tag::<DsvBuilder>(
                predicate,
                tag,
                spec,
                bindings,
                filter_rules,
                direction,
                report,
            ),
            SupportedFormatTag::Rdf(tag) => Self::new_with_tag::<RdfHandler>(
                predicate,
                tag,
                spec,
                bindings,
                filter_rules,
                direction,
                report,
            ),
            SupportedFormatTag::Json(tag) => Self::new_with_tag::<JsonHandler>(
                predicate,
                tag,
                spec,
                bindings,
                filter_rules,
                direction,
                report,
            ),
            SupportedFormatTag::Sparql(tag) => Self::new_with_tag::<SparqlBuilder>(
                predicate,
                tag,
                spec,
                bindings,
                filter_rules,
                direction,
                report,
            ),
        }
    }

    /// Finalize and create an [`Import`] with the specified parameters
    pub fn build_import(
        &self,
        predicate_name: &str,
        arity: usize,
        filter_rules: Vec<ChaseRule>,
    ) -> Import {
        let handler = match &self.inner {
            AnyImportExportBuilder::Dsv(dsv_builder) => {
                dsv_builder.build_import(arity, filter_rules)
            }
            AnyImportExportBuilder::Rdf(rdf_handler) => {
                rdf_handler.build_import(arity, filter_rules)
            }
            AnyImportExportBuilder::Json(json_handler) => {
                json_handler.build_import(arity, filter_rules)
            }
            AnyImportExportBuilder::Sparql(sparql_builder) => {
                sparql_builder.build_import(arity, filter_rules)
            }
        };

        let resource = self.resource.clone().unwrap_or(
            ResourceBuilder::default_resource_builder(
                predicate_name,
                handler.as_ref().default_extension(),
            )
            .finalize(),
        );

        Import {
            resource,
            compression: self.compression,
            predicate_arity: arity,
            handler,
        }
    }

    /// Finalize and create an [`Export`] with the specified parameters
    pub fn build_export(
        &self,
        predicate_name: &str,
        arity: usize,
        filter_rules: Vec<ChaseRule>,
    ) -> Export {
        let handler = match &self.inner {
            AnyImportExportBuilder::Dsv(dsv_builder) => {
                dsv_builder.build_export(arity, filter_rules)
            }
            AnyImportExportBuilder::Rdf(rdf_handler) => {
                rdf_handler.build_export(arity, filter_rules)
            }
            AnyImportExportBuilder::Json(json_handler) => {
                json_handler.build_export(arity, filter_rules)
            }
            AnyImportExportBuilder::Sparql(sparql_builder) => {
                sparql_builder.build_export(arity, filter_rules)
            }
        };

        let resource = self.resource.clone().unwrap_or(
            ResourceBuilder::default_resource_builder(
                predicate_name,
                handler.as_ref().default_extension(),
            )
            .finalize(),
        );

        Export {
            resource,
            compression: self.compression,
            predicate_arity: arity,
            handler,
        }
    }
}

#[derive(Clone)]
pub(crate) enum AnyImportExportBuilder {
    Dsv(DsvBuilder),
    Rdf(RdfHandler),
    Json(JsonHandler),
    Sparql(Box<SparqlBuilder>),
}

impl AnyImportExportBuilder {
    /// Return the format of this builder.
    pub fn format_tag(&self) -> SupportedFormatTag {
        match self {
            AnyImportExportBuilder::Dsv(dsv) => dsv.format_tag(),
            AnyImportExportBuilder::Rdf(rdf) => rdf.format_tag(),
            AnyImportExportBuilder::Json(json) => json.format_tag(),
            AnyImportExportBuilder::Sparql(sparql) => sparql.format_tag(),
        }
    }
}

impl Debug for AnyImportExportBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dsv(_) => f.debug_tuple("Dsv").field(&"...").finish(),
            Self::Rdf(_) => f.debug_tuple("Rdf").field(&"...").finish(),
            Self::Json(_) => f.debug_tuple("Json").field(&"...").finish(),
            Self::Sparql(_) => f.debug_tuple("Sparql").field(&"...").finish(),
        }
    }
}
