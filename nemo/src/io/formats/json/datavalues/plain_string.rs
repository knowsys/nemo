pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, StringDataValue};
    use serde::{de::Visitor, Deserializer, Serializer};

    struct PlainStringVisitor {}

    impl<'de> Visitor<'de> for PlainStringVisitor {
        type Value = StringDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a string")
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<StringDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!()
    }

    pub(crate) fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: DataValue,
        S: Serializer,
    {
        serializer.serialize_str(&value.canonical_string())
    }
}
