pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, IriDataValue};
    use nom::InputLength;
    use serde::{
        de::{Error, Visitor},
        Deserializer, Serializer,
    };

    use crate::parser::{
        ast::{expression::basic::iri::Iri, ProgramAST},
        input::ParserInput,
    };

    struct IriVisitor {}

    impl<'de> Visitor<'de> for IriVisitor {
        type Value = IriDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "an IRI")
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
            let (rest, value) = Iri::parse(ParserInput::stateless(&v))
                .map_err(|err| Error::custom(err.to_string()))?;

            if rest.input_len() > 0 {
                Err(Error::custom(format!("unexpected `{rest}` after IRI")))
            } else {
                Ok(IriDataValue::new(value.content()))
            }
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<IriDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(IriVisitor {})
    }

    pub(crate) fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: DataValue,
        S: Serializer,
    {
        serializer.serialize_str(&value.canonical_string())
    }
}
