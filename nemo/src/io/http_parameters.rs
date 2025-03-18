mod http_parameters {
    /// An example IRI to build a ResourceBuilder
    const EXAMPLE_IRI: &str = "http://example.org";
    
    /// Validate HTTP headers with a dummy [ResourceBuilder]
    pub fn validate_headers(headers: AnyDataValue) -> Result<(), ResourceValidationErrorKind> {
        let mut builder = TryInto::<Self>::try_into(String::from(EXAMPLE_IRI))?;
        let vec = builder.unpack_headers(headers)?;
        for (key, value) in vec {
            builder.add_header(key, value)?;
        }
        Ok(())
    }

    /// Validate HTTP parameters with a dummy [ResourceBuilder]
    pub fn validate_http_parameters(
        parameters: AnyDataValue,
    ) -> Result<(), ResourceValidationErrorKind> {
        let mut builder = TryInto::<Self>::try_into(String::from(EXAMPLE_IRI))?;
        let vec = builder.unpack_http_parameters(parameters)?;
        for (key, value) in vec {
            // Since GET and POST parameters have identical requirements add_get_parameter() will work for both types
            builder.add_get_parameter(key, value)?;
        }
        Ok(())
    }

    /// Convert headers into a key-value iterator
    pub fn unpack_headers(
        &self,
        map: AnyDataValue,
    ) -> Result<impl Iterator<Item = (String, String)>, ResourceValidationErrorKind> {
        if map.value_domain() != ValueDomain::Map { 
            return Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain { expected: (ValueDomain::Map), given: (vec![map.value_domain()]) })
        }
        let keys = map.map_keys();
        let result = keys
            .into_iter()
            .flat_map(|keys| {
                keys.map(|key| {
                    Ok((
                        self.validate_http_parameter_value_domain(key)?,
                        self.validate_http_parameter_value_domain(map.map_element_unchecked(key))?,
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result.into_iter())
    }

    /// Flatten HTTP parameters into a key-value vector
    pub fn unpack_http_parameters(
        &self,
        parameters: AnyDataValue,
    ) -> Result<impl Iterator<Item = (String, String)>, ResourceValidationErrorKind> {
        if parameters.value_domain() != ValueDomain::Map { 
            return Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain { expected: (vec![parameters.value_domain()]), given: (ValueDomain::Map)})
        }
        let keys = parameters.map_keys();
        let result = keys
            .into_iter()
            .flat_map(|keys| {
                keys.flat_map(|key| {
                    let tuple = parameters.map_element_unchecked(key);
                    (0..tuple.len_unchecked()).map(|idx| {
                        if tuple.value_domain() != ValueDomain::Tuple { 
                            return Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain { expected: (vec![tuple.value_domain()]), given: (ValueDomain::Tuple)})
                        }
                        let element = tuple.tuple_element_unchecked(idx);
                        Ok((validate_http_parameter_value_domain(key)?.lexical_value(), validate_http_parameter_value_domain(element)?.lexical_value()))
                    })
                    // Map: Result<(String, String), Error>
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result.into_iter())
    }

    /// Validate the [ValueDomain] of an HTTP parameter
    fn validate_http_parameter_value_domain(value: &AnyDataValue) -> Result<&AnyDataValue, ResourceValidationErrorKind> {
        match value.value_domain() {
            ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri => {
                Ok(value)
            }
            _ => Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain {
                expected: vec![ValueDomain::PlainString, ValueDomain::Int, ValueDomain::Iri],
                given: value.value_domain(),
            }),
        }
    }
}