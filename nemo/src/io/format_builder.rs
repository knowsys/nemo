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
    resource::{Resource, ResourceBuilder},
};
use strum::IntoEnumIterator;

use crate::{
    rule_model::{
        components::{
            import_export::{Direction, ImportExportSpec},
            term::value_type::ValueType,
            ProgramComponent,
        },
        error::{hint::Hint, validation_error::ValidationErrorKind, ValidationErrorBuilder},
    },
    syntax::import_export::attribute,
};

use super::{
    compression_format::CompressionFormat,
    formats::{
        dsv::{DsvBuilder, DsvTag},
        json::{JsonHandler, JsonTag},
        rdf::{RdfHandler, RdfTag},
        sparql::{SparqlBuilder, SparqlTag},
        Export, ExportHandler, Import, ImportHandler,
    },
    http_parameters,
};

pub(crate) trait FormatParameter<Tag>:
    FromStr<Err = ()> + ToString + IntoEnumIterator + Copy + Eq + Hash
{
    fn required_for(&self, tag: Tag) -> bool;
    fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationErrorKind>;
}

pub(super) fn value_type_matches(
    param: impl ToString,
    value: &AnyDataValue,
    supported_types: &[ValueType],
) -> Result<(), ValidationErrorKind> {
    let value_type = ValueType::from(value.value_domain());
    supported_types.contains(&value_type).then_some(()).ok_or(
        ValidationErrorKind::ImportExportAttributeValueType {
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
    FromStr<Err = ()> + ToString + Copy + Eq + 'static + Into<SupportedFormatTag>
{
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

    fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationErrorKind> {
        value_type_matches(self, &value, self.supported_types())?;

        match self {
            StandardParameter::BaseParamType(no_parameters) => match *no_parameters {},
            StandardParameter::Resource => ResourceBuilder::try_from(value)
                .and(Ok(()))
                .map_err(ValidationErrorKind::from),
            StandardParameter::Compression => {
                CompressionFormat::from_name(&value.to_plain_string_unchecked())
                    .and(Some(()))
                    .ok_or(ValidationErrorKind::ImportExportUnknownCompression {
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

pub(crate) trait FormatBuilder: Sized + Into<AnyImportExportBuilder> {
    type Tag: FormatTag + 'static;
    type Parameter: FormatParameter<Self::Tag> + From<StandardParameter> + 'static;

    fn new(
        tag: Self::Tag,
        parameters: &Parameters<Self>,
        direction: Direction,
    ) -> Result<Self, ValidationErrorKind>;

    fn expected_arity(&self) -> Option<usize>;

    fn supports_tag(tag: &str) -> bool {
        Self::Tag::from_str(tag).is_ok()
    }

    fn customize_resource_builder(
        &self,
        _direction: Direction,
        builder: Option<ResourceBuilder>,
    ) -> Option<ResourceBuilder> {
        builder
    }

    fn build_import(&self, arity: usize) -> Arc<dyn ImportHandler + Send + Sync + 'static>;
    fn build_export(&self, arity: usize) -> Arc<dyn ExportHandler + Send + Sync + 'static>;
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

    pub(crate) fn validate(
        spec: ImportExportSpec,
        direction: Direction,
        builder: &mut ValidationErrorBuilder,
    ) -> Option<Self> {
        let Ok(format_tag) = B::Tag::from_str(spec.format_tag().name()) else {
            builder.report_error(
                *spec.format_tag().origin(),
                ValidationErrorKind::ImportExportFileFormatUnknown(spec.format_tag().name().into()),
            );
            return None;
        };

        let mut required_parameters: HashSet<_> = B::Parameter::iter()
            .filter(|param| param.required_for(format_tag))
            .collect();

        let mut result = HashMap::new();

        for (key, value_term) in spec.key_value() {
            let Ok(parameter) = B::Parameter::from_str(key.name()) else {
                builder
                    .report_error(
                        *key.origin(),
                        ValidationErrorKind::ImportExportUnrecognizedAttribute {
                            format: format_tag.to_string(),
                            attribute: key.to_string(),
                        },
                    )
                    .add_hint_option(Hint::similar(
                        "parameter",
                        key.name(),
                        B::Parameter::iter().map(|attribute| attribute.to_string()),
                    ));

                return None;
            };

            required_parameters.remove(&parameter);

            if let Err(kind) = parameter.is_value_valid(value_term.value()) {
                builder.report_error(*value_term.origin(), kind);
                return None;
            }

            result.insert(parameter, value_term.value());
        }

        if required_parameters.is_empty() {
            Some(Self(result))
        } else {
            for param in required_parameters {
                builder.report_error(
                    *spec.origin(),
                    ValidationErrorKind::ImportExportMissingRequiredAttribute {
                        attribute: param.to_string(),
                        direction: direction.to_string(),
                    },
                );
            }

            None
        }
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

#[derive(Clone, Debug)]
pub(crate) enum SupportedFormatTag {
    Dsv(DsvTag),
    Rdf(RdfTag),
    Json(JsonTag),
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

    /// The resource as specified in the parameters
    pub fn resource(&self) -> Option<Resource> {
        self.resource.clone()
    }

    fn new_with_tag<B: FormatBuilder>(
        tag: B::Tag,
        spec: ImportExportSpec,
        direction: Direction,
        builder: &mut ValidationErrorBuilder,
    ) -> Option<ImportExportBuilder> {
        let origin = *spec.origin();
        let parameters = Parameters::<B>::validate(spec, direction, builder)?;

        let resource_builder = parameters
            .get_optional(StandardParameter::Resource.into())
            .and_then(|value| {
                let resource = ResourceBuilder::try_from(value)
                    .map_err(|err| builder.report_error(origin, err.into()))
                    .ok()?;
                Some(resource)
            });

        let compression = parameters
            .get_optional(StandardParameter::Compression.into())
            .map(|compression| {
                CompressionFormat::from_name(&compression.to_plain_string_unchecked()).unwrap()
            })
            .unwrap_or_else(|| CompressionFormat::from_resource_builder(&resource_builder));

        let inner = match B::new(tag, &parameters, direction) {
            Ok(res) => Some(res),
            Err(kind) => {
                builder.report_error(origin, kind);
                None
            }
        }?;

        let resource_builder = inner.customize_resource_builder(direction, resource_builder);

        let resource = resource_builder
            .map(|mut rb| {
                parameters
                    .get_optional(StandardParameter::HttpHeaders.into())
                    .map(|headers| {
                        http_parameters::unpack_headers(headers).and_then(|mut headers| {
                            headers.try_for_each(|(key, value)| {
                                rb.add_header(key, value)
                                    .and(Ok(()))
                                    .map_err(ValidationErrorKind::from)
                            })
                        })
                    })
                    .transpose()?;

                parameters
                    .get_optional(StandardParameter::HttpGetParameters.into())
                    .map(|parameters| {
                        http_parameters::unpack_http_parameters(parameters).and_then(
                            |mut parameters| {
                                parameters.try_for_each(|(key, value)| {
                                    rb.add_get_parameter(key, value)
                                        .and(Ok(()))
                                        .map_err(ValidationErrorKind::from)
                                })
                            },
                        )
                    })
                    .transpose()?;

                parameters
                    .get_optional(StandardParameter::HttpPostParameters.into())
                    .map(|parameters| {
                        http_parameters::unpack_http_parameters(parameters).and_then(
                            |mut parameters| {
                                parameters.try_for_each(|(key, value)| {
                                    rb.add_post_parameter(key, value)
                                        .and(Ok(()))
                                        .map_err(ValidationErrorKind::from)
                                })
                            },
                        )
                    })
                    .transpose()?;

                parameters
                    .get_optional(StandardParameter::IriFragment.into())
                    .map(|fragment| {
                        rb.set_fragment(fragment.to_plain_string_unchecked())
                            .and(Ok(()))
                            .map_err(ValidationErrorKind::from)
                    })
                    .transpose()?;

                Ok(rb.finalize())
            })
            .transpose()
            .map_err(|err| builder.report_error(origin, err))
            .ok()?;

        Some(ImportExportBuilder {
            inner: inner.into(),
            resource,
            compression,
        })
    }

    pub(crate) fn new(
        spec: ImportExportSpec,
        direction: Direction,
        error_builder: &mut ValidationErrorBuilder,
    ) -> Option<Self> {
        let format_tag = spec.format_tag().name();
        let Ok(tag) = format_tag.parse::<SupportedFormatTag>() else {
            error_builder.report_error(
                *spec.format_tag().origin(),
                ValidationErrorKind::ImportExportFileFormatUnknown(format_tag.to_string()),
            );
            return None;
        };

        match tag {
            SupportedFormatTag::Dsv(tag) => {
                Self::new_with_tag::<DsvBuilder>(tag, spec, direction, error_builder)
            }
            SupportedFormatTag::Rdf(tag) => {
                Self::new_with_tag::<RdfHandler>(tag, spec, direction, error_builder)
            }
            SupportedFormatTag::Json(tag) => {
                Self::new_with_tag::<JsonHandler>(tag, spec, direction, error_builder)
            }
            SupportedFormatTag::Sparql(tag) => {
                Self::new_with_tag::<SparqlBuilder>(tag, spec, direction, error_builder)
            }
        }
    }

    /// Finalize and create an [`Import`] with the specified parameters
    pub fn build_import(&self, predicate_name: &str, arity: usize) -> Import {
        let handler = match &self.inner {
            AnyImportExportBuilder::Dsv(dsv_builder) => dsv_builder.build_import(arity),
            AnyImportExportBuilder::Rdf(rdf_handler) => rdf_handler.build_import(arity),
            AnyImportExportBuilder::Json(json_handler) => json_handler.build_import(arity),
            AnyImportExportBuilder::Sparql(sparql_builder) => sparql_builder.build_import(arity),
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
    pub fn build_export(&self, predicate_name: &str, arity: usize) -> Export {
        let handler = match &self.inner {
            AnyImportExportBuilder::Dsv(dsv_builder) => dsv_builder.build_export(arity),
            AnyImportExportBuilder::Rdf(rdf_handler) => rdf_handler.build_export(arity),
            AnyImportExportBuilder::Json(json_handler) => json_handler.build_export(arity),
            AnyImportExportBuilder::Sparql(sparql_builder) => sparql_builder.build_export(arity),
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
