pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, FloatDataValue};
    use serde::{de::Visitor, ser::Error, Deserializer, Serializer};

    struct FloatVisitor {}

    impl<'de> Visitor<'de> for FloatVisitor {
        type Value = FloatDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a finite float (f32) number")
        }

        fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            FloatDataValue::from_f32(v).map_err(|err| E::custom(err.to_string()))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<FloatDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_f32(FloatVisitor {})
    }

    pub(crate) fn serialize<S>(value: &FloatDataValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f32(
            value
                .to_f32()
                .ok_or_else(|| S::Error::custom("value is not a Float"))?,
        )
    }
}
