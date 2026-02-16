pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, DoubleDataValue};
    use serde::{de::Visitor, ser::Error, Deserializer, Serializer};

    struct DoubleVisitor {}

    impl<'de> Visitor<'de> for DoubleVisitor {
        type Value = DoubleDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a finite double (f64) number")
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            DoubleDataValue::from_f64(v).map_err(|err| E::custom(err.to_string()))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<DoubleDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_f64(DoubleVisitor {})
    }

    pub(crate) fn serialize<S>(value: &DoubleDataValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(
            value
                .to_f64()
                .ok_or_else(|| S::Error::custom("value is not a Double"))?,
        )
    }
}
