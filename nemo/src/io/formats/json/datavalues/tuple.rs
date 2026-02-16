pub(crate) mod default {
    use nemo_physical::datavalues::{DataValue, TupleDataValue};
    use serde::{de::Visitor, ser::SerializeSeq, Deserializer, Serializer};

    type AnyWrapper = crate::io::formats::json::variants::default::JsonAnyDataValue;
    struct TupleVisitor {}

    impl<'de> Visitor<'de> for TupleVisitor {
        type Value = TupleDataValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a list")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut entries = Vec::<AnyWrapper>::with_capacity(seq.size_hint().unwrap_or_default());

            while let Some(entry) = seq.next_element()? {
                entries.push(entry);
            }

            Ok(TupleDataValue::new(
                None,
                entries.into_iter().map(|entry| entry.into()),
            ))
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<TupleDataValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(TupleVisitor {})
    }

    pub(crate) fn serialize<S>(value: &TupleDataValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        assert!(
            value.label().is_none(),
            "cannot serialize tuples with labels"
        );
        let length = value.length().expect("tuples always have a fixed length");
        let mut seq = serializer.serialize_seq(Some(length))?;

        for idx in 0..length {
            seq.serialize_element(&AnyWrapper::from(
                value.tuple_element_unchecked(idx).clone(),
            ))?;
        }

        seq.end()
    }
}
