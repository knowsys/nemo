pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, StringDataValue};
    use serde::{de::Visitor, Deserializer, Serializer};

    struct PlainStringVisitor {}

    impl<'de> Visitor<'de> for PlainStringVisitor {
        type Value = StringDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_string(v.to_string())
        }

        fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(StringDataValue::new(v))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<StringDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(PlainStringVisitor {})
    }

    pub(crate) fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: DataValue,
        S: Serializer,
    {
        serializer.serialize_str(&value.canonical_string())
    }
}
