pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, NullDataValue, StringDataValue};
    use nom::InputLength;
    use serde::{
        de::{Error, Visitor},
        Deserializer, Serializer,
    };

    use crate::parser::{
        ast::{expression::basic::blank::Blank, ProgramAST},
        input::ParserInput,
    };

    struct NullVisitor {}

    impl<'de> Visitor<'de> for NullVisitor {
        type Value = NullDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a named null `_:<identifier>`")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            let (rest, blank) = Blank::parse(ParserInput::stateless(v))
                .map_err(|err| Error::custom(err.to_string()))?;

            if rest.input_len() > 0 {
                Err(Error::custom(format!(
                    "unexpected `{rest}` after named null"
                )))
            } else {
                Ok(NullDataValue::new(blank.name()))
            }
        }
    }
}
