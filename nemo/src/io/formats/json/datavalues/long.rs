pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, LongDataValue};
    use serde::{de::Visitor, Deserializer, Serializer};

    struct LongVisitor {}

    impl<'de> Visitor<'de> for LongVisitor {
        type Value = LongDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a signed integer fitting into i64")
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(LongDataValue::new(v))
        }

        fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(LongDataValue::new(i64::from(v)))
        }

        fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(LongDataValue::new(i64::from(v)))
        }

        fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(LongDataValue::new(i64::from(v)))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<LongDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_i64(LongVisitor {})
    }

    pub(crate) fn serialize<S>(value: &LongDataValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(value.to_i64_unchecked())
    }
}
