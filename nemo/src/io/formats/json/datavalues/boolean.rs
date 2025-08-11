pub(crate) mod default {
    use nemo_physical::datavalues::{BooleanDataValue, DataValue};
    use serde::{de::Visitor, Deserializer, Serializer};

    struct BooleanVisitor {}

    impl<'de> Visitor<'de> for BooleanVisitor {
        type Value = BooleanDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a boolean")
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(BooleanDataValue::new(v))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<BooleanDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(BooleanVisitor {})
    }

    pub(crate) fn serialize<S>(value: &BooleanDataValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bool(value.to_boolean_unchecked())
    }
}
