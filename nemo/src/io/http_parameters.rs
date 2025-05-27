//! This module contains functions to validate and unpack HTTP parameter

use nemo_physical::{
    datavalues::{AnyDataValue, DataValue, ValueDomain},
    resource::ResourceBuilder,
};

use crate::rule_model::error::validation_error::ValidationError;

/// An example IRI to build a ResourceBuilder
const EXAMPLE_IRI: &str = "http://example.org";

/// Validate HTTP headers with a dummy [ResourceBuilder]
pub fn validate_headers(headers: AnyDataValue) -> Result<(), ValidationError> {
    let mut builder =
        ResourceBuilder::try_from(String::from(EXAMPLE_IRI)).map_err(ValidationError::from)?;
    let vec = unpack_headers(headers)?;
    for (key, value) in vec {
        builder
            .add_header(key, value)
            .map_err(ValidationError::from)?;
    }
    Ok(())
}

/// Validate HTTP parameters with a dummy [ResourceBuilder]
pub fn validate_http_parameters(parameters: AnyDataValue) -> Result<(), ValidationError> {
    let mut builder = ResourceBuilder::try_from(String::from(EXAMPLE_IRI))?;
    let vec = unpack_http_parameters(parameters)?;
    for (key, value) in vec {
        // Since GET and POST parameters have identical requirements add_get_parameter() will work for both parameter types
        builder
            .add_get_parameter(key, value)
            .map_err(ValidationError::from)?;
    }
    Ok(())
}

/// Convert headers into a key-value iterator
pub fn unpack_headers(
    map: AnyDataValue,
) -> Result<impl Iterator<Item = (String, String)>, ValidationError> {
    if map.value_domain() != ValueDomain::Map {
        return Err(ValidationError::HttpParameterNotInValueDomain {
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
            Ok::<(String, String), ValidationError>((key.lexical_value(), element.lexical_value()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(result.into_iter())
}

/// Flatten HTTP parameters into a key-value vector
pub fn unpack_http_parameters(
    parameters: AnyDataValue,
) -> Result<impl Iterator<Item = (String, String)>, ValidationError> {
    if parameters.value_domain() != ValueDomain::Map {
        return Err(ValidationError::HttpParameterNotInValueDomain {
            expected: (vec![ValueDomain::Map]),
            given: (parameters.value_domain()),
        });
    }
    parameters
        .map_keys()
        .expect("Verified to be a map")
        .try_for_each(|key| {
            validate_http_parameter_value_domain(key)?;
            let value = parameters.map_element_unchecked(key);
            if !matches!(
                value.value_domain(),
                ValueDomain::Tuple | ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
            ) {
                return Err(ValidationError::HttpParameterNotInValueDomain {
                    expected: (vec![
                        ValueDomain::Tuple,
                        ValueDomain::PlainString,
                        ValueDomain::Int,
                        ValueDomain::Iri,
                    ]),
                    given: (value.value_domain()),
                });
            }
            Ok(())
        })?;

    let result = parameters
        .map_keys()
        .expect("Verified to be a map")
        .flat_map(|key| {
            let value = parameters.map_element_unchecked(key);

            // value was verified to be either a constant or tuple
            if let Some(len) = value.length() {
                (0..len)
                    .map(|idx| {
                        let element = value.tuple_element_unchecked(idx);
                        validate_http_parameter_value_domain(element)?;
                        Ok::<(String, String), ValidationError>((
                            key.lexical_value(),
                            element.lexical_value(),
                        ))
                    })
                    .collect::<Vec<Result<_, _>>>()
            } else {
                vec![
                    (Ok::<(String, String), ValidationError>((
                        key.lexical_value(),
                        value.lexical_value(),
                    ))),
                ]
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(result.into_iter())
}

/// Validate the [ValueDomain] of an HTTP parameter
pub fn validate_http_parameter_value_domain(value: &AnyDataValue) -> Result<(), ValidationError> {
    match value.value_domain() {
        ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri => Ok(()),
        _ => Err(ValidationError::HttpParameterNotInValueDomain {
            expected: vec![ValueDomain::PlainString, ValueDomain::Int, ValueDomain::Iri],
            given: value.value_domain(),
        }),
    }
}
