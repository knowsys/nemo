//! This module contains functions to validate and unpack HTTP parameter

use crate::rule_model::error::validation_error::ValidationErrorKind;
use nemo_physical::{
    datavalues::{AnyDataValue, DataValue, ValueDomain},
    resource::{ResourceBuilder, ResourceValidationErrorKind},
};

/// An example IRI to build a ResourceBuilder
const EXAMPLE_IRI: &str = "http://example.org";

/// Validate HTTP headers with a dummy [ResourceBuilder]
pub fn validate_headers(headers: AnyDataValue) -> Result<(), ValidationErrorKind> {
    let mut builder = TryInto::<ResourceBuilder>::try_into(String::from(EXAMPLE_IRI))
        .map_err(ValidationErrorKind::from)?;
    let vec = unpack_headers(headers)?;
    for (key, value) in vec {
        builder
            .add_header(key, value)
            .map_err(ValidationErrorKind::from)?;
    }
    Ok(())
}

/// Validate HTTP parameters with a dummy [ResourceBuilder]
pub fn validate_http_parameters(parameters: AnyDataValue) -> Result<(), ValidationErrorKind> {
    let mut builder = TryInto::<ResourceBuilder>::try_into(String::from(EXAMPLE_IRI))?;
    let vec = unpack_http_parameters(parameters)?;
    for (key, value) in vec {
        // Since GET and POST parameters have identical requirements add_get_parameter() will work for both parameter types
        builder
            .add_get_parameter(key, value)
            .map_err(|err: ResourceValidationErrorKind| ValidationErrorKind::from(err))?;
    }
    Ok(())
}

/// Convert headers into a key-value iterator
pub fn unpack_headers(
    map: AnyDataValue,
) -> Result<impl Iterator<Item = (String, String)>, ValidationErrorKind> {
    if map.value_domain() != ValueDomain::Map {
        return Err(ValidationErrorKind::HttpParameterNotInValueDomain {
            expected: (vec![ValueDomain::Map]),
            given: (map.value_domain()),
        });
    }
    let keys = map.map_keys().expect("Verified to be a map");
    let result = keys
        .into_iter()
        .map(|key| {
            validate_http_parameter_value_domain(key)?;
            let element = map.map_element_unchecked(key);
            validate_http_parameter_value_domain(element)?;
            Ok::<(String, String), ValidationErrorKind>((
                key.lexical_value(),
                element.lexical_value(),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(result.into_iter())
}

/// Flatten HTTP parameters into a key-value vector
pub fn unpack_http_parameters(
    parameters: AnyDataValue,
) -> Result<impl Iterator<Item = (String, String)>, ValidationErrorKind> {
    if parameters.value_domain() != ValueDomain::Map {
        return Err(ValidationErrorKind::HttpParameterNotInValueDomain {
            expected: (vec![ValueDomain::Map]),
            given: (parameters.value_domain()),
        });
    }
    parameters
        .map_keys()
        .expect("Verified to be a map")
        .try_for_each(|key| {
            validate_http_parameter_value_domain(key)?;
            let tuple = parameters.map_element_unchecked(key);
            if tuple.value_domain() != ValueDomain::Tuple {
                return Err(ValidationErrorKind::HttpParameterNotInValueDomain {
                    expected: (vec![ValueDomain::Tuple]),
                    given: (tuple.value_domain()),
                });
            }
            Ok(())
        })?;

    let result = parameters
        .map_keys()
        .expect("Verified to be a map")
        .flat_map(|key| {
            let tuple = parameters.map_element_unchecked(key);

            (0..tuple.len_unchecked()).map(|idx| {
                let element = tuple.tuple_element_unchecked(idx);
                validate_http_parameter_value_domain(element)?;
                Ok::<(String, String), ValidationErrorKind>((
                    key.lexical_value(),
                    element.lexical_value(),
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(result.into_iter())
}

/// Validate the [ValueDomain] of an HTTP parameter
pub fn validate_http_parameter_value_domain(
    value: &AnyDataValue,
) -> Result<(), ValidationErrorKind> {
    match value.value_domain() {
        ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri => Ok(()),
        _ => Err(ValidationErrorKind::HttpParameterNotInValueDomain {
            expected: vec![ValueDomain::PlainString, ValueDomain::Int, ValueDomain::Iri],
            given: value.value_domain(),
        }),
    }
}
