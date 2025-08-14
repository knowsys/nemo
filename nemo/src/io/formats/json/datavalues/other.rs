pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, OtherDataValue};
    use nom::InputLength;
    use serde::{
        de::{Error, Visitor},
        Deserializer, Serializer,
    };

    use crate::parser::{
        ast::{
            expression::basic::rdf_literal::RdfLiteral, tag::structure::StructureTagKind,
            ProgramAST,
        },
        input::ParserInput,
    };

    struct OtherVisitor {}

    impl<'de> Visitor<'de> for OtherVisitor {
        type Value = OtherDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "an RDF literal with datatype")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_string(v.to_string())
        }

        fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
        where
            E: Error,
        {
            let (rest, value) = RdfLiteral::parse(ParserInput::stateless(&v))
                .map_err(|err| Error::custom(err.to_string()))?;

            if rest.input_len() > 0 {
                Err(Error::custom(format!(
                    "unexpected `{rest}` after language-tagged string"
                )))
            } else if !matches!(value.tag().kind(), StructureTagKind::Iri(_)) {
                Err(Error::custom("expected literal datatype to be an IRI"))
            } else {
                Ok(OtherDataValue::new(
                    value.content(),
                    value.tag().to_string(),
                ))
            }
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(OtherDataValue::new(
                "null".to_string(),
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON".to_string(),
            ))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<OtherDataValue, D::Error>
    where
        D: Deserializer<'de> + Copy,
    {
        deserializer
            .deserialize_string(OtherVisitor {})
            .or_else(|_| deserializer.deserialize_unit(OtherVisitor {}))
    }

    pub(crate) fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: DataValue,
        S: Serializer,
    {
        if value.datatype_iri() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"
            && value.lexical_value() == "null"
        {
            serializer.serialize_none()
        } else {
            serializer.serialize_str(&value.canonical_string())
        }
    }

    #[cfg(test)]
    mod test {
        use std::assert_matches::assert_matches;

        use crate::io::formats::json::variants::default::JsonAnyDataValue;
        use nemo_physical::datavalues::{AnyDataValue, DataValue};
        use serde_json::{from_value, Value};
        use test_log::test;

        #[test]
        fn null() {
            let value: Result<JsonAnyDataValue, _> = from_value(Value::Null);
            assert_matches!(value, Ok(_));
            let value = value.unwrap();

            assert_matches!(value, JsonAnyDataValue::Other(_));
            let value = AnyDataValue::from(value);
            assert_eq!("null", value.lexical_value());
            assert_eq!(
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON",
                value.datatype_iri()
            );
        }
    }
}
