pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, UnsignedLongDataValue};
    use serde::{de::Visitor, Deserializer, Serializer};

    struct UnsignedVisitor {}

    impl<'de> Visitor<'de> for UnsignedVisitor {
        type Value = UnsignedLongDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "an unsigned integer fitting into u64")
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(UnsignedLongDataValue::new(v))
        }

        fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(UnsignedLongDataValue::new(u64::from(v)))
        }

        fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(UnsignedLongDataValue::new(u64::from(v)))
        }

        fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(UnsignedLongDataValue::new(u64::from(v)))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<UnsignedLongDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(UnsignedVisitor {})
    }

    pub(crate) fn serialize<S>(
        value: &UnsignedLongDataValue,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(value.to_u64_unchecked())
    }
}
