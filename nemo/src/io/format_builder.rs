//! Infrastructure for validating and creating objects for data import/export

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    str::FromStr,
    sync::Arc,
};

use nemo_physical::datavalues::{AnyDataValue, DataValue};
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
        dsv::DsvBuilder, rdf::RdfHandler, Export, ExportHandler, Import, ImportHandler,
        ResourceSpec,
    },
};

pub(crate) trait FormatParameter:
    FromStr<Err = ()> + ToString + IntoEnumIterator + Copy + Eq + Hash
{
    type Tag;

    fn required_for(&self, tag: Self::Tag) -> bool;
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

pub(crate) trait FormatTag: FromStr<Err = ()> + ToString + Copy + Eq + 'static {
    const VARIANTS: &'static [(Self, &'static str)];
}

/// Define an enum for format tags (the "predicate-name" in import/export declarations)
/// that are handled by one particular [`FormatBuilder`]
macro_rules! format_tag {
    { $vis:vis enum $type_name:ident { $($tag_name:ident => $tag_value:expr,)* } } => {
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
    pub enum StandardParameter(NoParameters) {
        Resource(name = attribute::RESOURCE, supported_types = &[ValueType::String, ValueType::Constant]),
        Compression(name = attribute::COMPRESSION, supported_types = &[ValueType::String]),
    }
}

impl StandardParameter {
    pub(crate) fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationErrorKind> {
        value_type_matches(self, &value, &self.supported_types())?;

        match self {
            StandardParameter::BaseParamType(no_parameters) => match *no_parameters {},
            StandardParameter::Resource => Ok(()),
            StandardParameter::Compression => {
                CompressionFormat::from_name(&value.to_plain_string_unchecked())
                    .and(Some(()))
                    .ok_or(ValidationErrorKind::ImportExportUnknownCompression {
                        format: value.to_string(),
                    })
            }
        }
    }
}

pub(crate) trait FormatBuilder: Sized + Into<AnyImportExportBuilder> {
    type Tag: FormatTag + 'static;
    type Parameter: FormatParameter<Tag = Self::Tag> + From<StandardParameter> + 'static;

    fn new(
        tag: Self::Tag,
        parameters: &Parameters<Self::Parameter>,
    ) -> Result<Self, ValidationErrorKind>;

    fn expected_arity(&self) -> Option<usize>;

    fn supports_tag(tag: &str) -> bool {
        Self::Tag::from_str(tag).is_ok()
    }

    fn build_import(&self, arity: usize) -> Arc<dyn ImportHandler + Send + Sync + 'static>;
    fn build_export(&self, arity: usize) -> Arc<dyn ExportHandler + Send + Sync + 'static>;
}

pub(crate) struct Parameters<P>(HashMap<P, AnyDataValue>);

impl<P: FormatParameter> Parameters<P> {
    pub(crate) fn get_required(&self, param: P) -> AnyDataValue {
        self.get_optional(param)
            .expect("presence of required parameters is checked upon validation")
    }

    pub(crate) fn get_optional(&self, param: P) -> Option<AnyDataValue> {
        self.0.get(&param).cloned()
    }
}

impl<P: FormatParameter + Hash> Parameters<P>
where
    P::Tag: FormatTag,
{
    pub(crate) fn validate(
        spec: ImportExportSpec,
        direction: Direction,
        builder: &mut ValidationErrorBuilder,
    ) -> Option<Self> {
        let Ok(format_tag) = P::Tag::from_str(spec.format_tag().name()) else {
            builder.report_error(
                *spec.format_tag().origin(),
                ValidationErrorKind::ImportExportFileFormatUnknown(spec.format_tag().name().into()),
            );
            return None;
        };

        let mut required_parameters: HashSet<_> = P::iter()
            .filter(|param| param.required_for(format_tag))
            .collect();

        let mut result = HashMap::new();

        for (key, value_term) in spec.key_value() {
            let Ok(parameter) = P::from_str(key.name()) else {
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
                        P::iter().map(|attribute| attribute.to_string()),
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

            return None;
        }
    }
}

#[derive(Clone, Debug)]
pub struct ImportExportBuilder {
    inner: AnyImportExportBuilder,
    resource: Option<ResourceSpec>,
    compression: CompressionFormat,
}

enum SupportedFormatTag {
    Dsv(<DsvBuilder as FormatBuilder>::Tag),
    Rdf(<RdfHandler as FormatBuilder>::Tag),
}

impl FromStr for SupportedFormatTag {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if DsvBuilder::supports_tag(s) {
            Ok(Self::Dsv(s.parse().unwrap()))
        } else if RdfHandler::supports_tag(s) {
            Ok(Self::Rdf(s.parse().unwrap()))
        } else {
            Err(())
        }
    }
}

impl ImportExportBuilder {
    pub fn expected_arity(&self) -> Option<usize> {
        match &self.inner {
            AnyImportExportBuilder::Dsv(inner) => inner.expected_arity(),
            AnyImportExportBuilder::Rdf(inner) => inner.expected_arity(),
        }
    }

    pub fn resource(&self) -> Option<ResourceSpec> {
        self.resource.clone()
    }

    fn new_with_tag<B: FormatBuilder>(
        tag: B::Tag,
        spec: ImportExportSpec,
        direction: Direction,
        builder: &mut ValidationErrorBuilder,
    ) -> Option<ImportExportBuilder> {
        let origin = *spec.origin();
        let parameters = Parameters::<B::Parameter>::validate(spec, direction, builder)?;

        let resource = parameters
            .get_optional(StandardParameter::Resource.into())
            .map(|resource| ResourceSpec::from_string(resource.to_plain_string_unchecked()));

        let compression = parameters
            .get_optional(StandardParameter::Compression.into())
            .map(|compression| {
                CompressionFormat::from_name(&compression.to_plain_string_unchecked()).unwrap()
            })
            .unwrap_or_default();

        let inner = match B::new(tag, &parameters) {
            Ok(res) => Some(res),
            Err(kind) => {
                builder.report_error(origin, kind);
                None
            }
        }?;

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
        }
    }

    pub fn build_import(&self, predicate_name: &str, arity: usize) -> Import {
        let handler = match &self.inner {
            AnyImportExportBuilder::Dsv(dsv_builder) => dsv_builder.build_import(arity),
            AnyImportExportBuilder::Rdf(rdf_handler) => rdf_handler.build_import(arity),
        };

        let resource_spec = self.resource.clone().unwrap_or({
            let default_file_name = format!("{}.{}", predicate_name, handler.default_extension());
            ResourceSpec::from_string(default_file_name)
        });

        Import {
            resource_spec,
            compression: self.compression.clone(),
            predicate_arity: arity,
            handler,
        }
    }

    pub fn build_export(&self, predicate_name: &str, arity: usize) -> Export {
        let handler = match &self.inner {
            AnyImportExportBuilder::Dsv(dsv_builder) => dsv_builder.build_export(arity),
            AnyImportExportBuilder::Rdf(rdf_handler) => rdf_handler.build_export(arity),
        };

        let resource_spec = self.resource.clone().unwrap_or({
            let default_file_name = format!("{}.{}", predicate_name, handler.default_extension());
            ResourceSpec::from_string(default_file_name)
        });

        Export {
            resource_spec,
            compression: self.compression.clone(),
            predicate_arity: arity,
            handler,
        }
    }
}

#[derive(Clone)]
pub(super) enum AnyImportExportBuilder {
    Dsv(DsvBuilder),
    Rdf(RdfHandler),
}

impl Debug for AnyImportExportBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dsv(_) => f.debug_tuple("Dsv").field(&"...").finish(),
            Self::Rdf(_) => f.debug_tuple("Rdf").field(&"...").finish(),
        }
    }
}
