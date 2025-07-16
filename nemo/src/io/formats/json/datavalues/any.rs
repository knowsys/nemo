pub(crate) mod default {
    use nemo_physical::datavalues::{AnyDataValue, DataValue};
    use serde::{Deserializer, Serializer};

    pub(crate) fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: DataValue,
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
