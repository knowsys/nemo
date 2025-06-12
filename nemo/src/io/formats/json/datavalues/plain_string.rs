pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, StringDataValue};
    use serde::{Deserializer, Serializer};

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
