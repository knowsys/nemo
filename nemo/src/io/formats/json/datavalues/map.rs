pub(crate) mod default {

    use nemo_physical::datavalues::{DataValue, MapDataValue};
    use serde::{de::Visitor, ser::SerializeMap, Deserializer, Serializer};

    type AnyWrapper = crate::io::formats::json::variants::default::JsonAnyDataValue;
    struct MapVisitor {}

    impl<'de> Visitor<'de> for MapVisitor {
        type Value = MapDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a map")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut entries =
                Vec::<(AnyWrapper, AnyWrapper)>::with_capacity(map.size_hint().unwrap_or_default());

            while let Some(entry) = map.next_entry()? {
                entries.push(entry)
            }

            Ok(MapDataValue::new(
                None,
                entries
                    .into_iter()
                    .map(|(key, value)| (key.into(), value.into())),
            ))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<MapDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(MapVisitor {})
    }

    pub(crate) fn serialize<S>(value: &MapDataValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(value.length())?;

        assert!(value.label().is_none(), "cannot serialize maps with labels");

        if let Some(entries) = value.map_items() {
            for (key, value) in entries {
                map.serialize_entry(
                    &AnyWrapper::from(key.clone()),
                    &AnyWrapper::from(value.clone()),
                )?;
            }
        }

        map.end()
    }
}
