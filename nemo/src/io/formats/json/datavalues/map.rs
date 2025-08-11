pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, MapDataValue};
    use serde::{de::Visitor, Deserializer, Serializer};

    struct MapVisitor {}

    impl<'de> Visitor<'de> for MapVisitor {
        type Value = MapDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a boolean")
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(MapDataValue::new(v))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<MapDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(MapVisitor {})
    }

    pub(crate) fn serialize<S>(value: &MapDataValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bool(value.to_boolean_unchecked())
    }
}
