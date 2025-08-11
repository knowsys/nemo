pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, TupleDataValue};
    use serde::{de::Visitor, Deserializer, Serializer};

    struct TupleVisitor {}

    impl<'de> Visitor<'de> for TupleVisitor {
        type Value = TupleDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a boolean")
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(TupleDataValue::new(v))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<TupleDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(TupleVisitor {})
    }

    pub(crate) fn serialize<S>(value: &TupleDataValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bool(value.to_boolean_unchecked())
    }
}
