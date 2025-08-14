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

        fn visit_none<E>(self) -> Result<Self::Value, E>
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
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(OtherVisitor {})
    }

    pub(crate) fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: DataValue,
        S: Serializer,
    {
        serializer.serialize_str(&value.canonical_string())
    }
}
