pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, LangStringDataValue};
    use nom::InputLength;
    use serde::{
        de::{Error, Visitor},
        Deserializer, Serializer,
    };

    use crate::parser::{
        ast::{expression::basic::string::StringLiteral, ProgramAST},
        input::ParserInput,
    };

    struct LangStringVisitor {}

    impl<'de> Visitor<'de> for LangStringVisitor {
        type Value = LangStringDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a language-tagged string")
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
            let (rest, value) = StringLiteral::parse(ParserInput::stateless(&v))
                .map_err(|err| Error::custom(err.to_string()))?;

            if rest.input_len() > 0 {
                Err(Error::custom(format!(
                    "unexpected `{rest}` after language-tagged string"
                )))
            } else {
                Ok(LangStringDataValue::new(
                    value.content(),
                    value
                        .language_tag()
                        .ok_or(Error::custom("string literal does not have a language tag"))?,
                ))
            }
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<LangStringDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(LangStringVisitor {})
    }

    pub(crate) fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: DataValue,
        S: Serializer,
    {
        serializer.serialize_str(&value.canonical_string())
    }
}
