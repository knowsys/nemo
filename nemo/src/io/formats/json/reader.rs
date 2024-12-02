//! Implements a reader for json files.
//! Deserialisation is handled by [serde_json].

use std::{fmt::Debug, io::BufRead, mem::size_of};

use nemo_physical::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::AnyDataValue,
    management::bytesized::ByteSized,
};
use serde_json::Value;

pub(crate) struct JsonReader {
    read: Box<dyn BufRead>,
}

impl JsonReader {
    pub(super) fn new(read: Box<dyn BufRead>) -> Self {
        JsonReader { read }
    }
}

impl Debug for JsonReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonReader").finish()
    }
}

impl ByteSized for JsonReader {
    fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64
    }
}

impl TableProvider for JsonReader {
    fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let value: Value = serde_json::from_reader(self.read)?;
        let mut max_object_id = 0u64;

        let mut stack = vec![(max_object_id, value)];

        let type_iri = AnyDataValue::new_iri("type".into());
        let value_iri = AnyDataValue::new_iri("value".into());

        while let Some((object_id, current)) = stack.pop() {
            let id = AnyDataValue::new_integer_from_u64(object_id);

            match current {
                Value::Null => {
                    tuple_writer.add_tuple_value(id);
                    tuple_writer.add_tuple_value(type_iri.clone());

                    let type_field = AnyDataValue::new_plain_string("null".into());
                    tuple_writer.add_tuple_value(type_field);
                }
                Value::Bool(value) => {
                    tuple_writer.add_tuple_value(id.clone());
                    tuple_writer.add_tuple_value(type_iri.clone());

                    let type_field = AnyDataValue::new_plain_string("bool".into());
                    tuple_writer.add_tuple_value(type_field);

                    tuple_writer.add_tuple_value(id);
                    tuple_writer.add_tuple_value(value_iri.clone());

                    let value_field = AnyDataValue::new_boolean(value);
                    tuple_writer.add_tuple_value(value_field);
                }
                Value::Number(value) => {
                    tuple_writer.add_tuple_value(id.clone());
                    tuple_writer.add_tuple_value(type_iri.clone());

                    let type_field = AnyDataValue::new_plain_string("number".into());
                    tuple_writer.add_tuple_value(type_field);

                    tuple_writer.add_tuple_value(id);
                    tuple_writer.add_tuple_value(value_iri.clone());

                    let value_field = if let Some(value) = value.as_i64() {
                        AnyDataValue::new_integer_from_i64(value)
                    } else if let Some(value) = value.as_u64() {
                        AnyDataValue::new_integer_from_u64(value)
                    } else {
                        AnyDataValue::new_double_from_f64(
                            value
                                .as_f64()
                                .expect("numeric value should always fit in i64, u64 or f64"),
                        )?
                    };
                    tuple_writer.add_tuple_value(value_field);
                }
                Value::String(value) => {
                    tuple_writer.add_tuple_value(id.clone());
                    tuple_writer.add_tuple_value(type_iri.clone());

                    let type_field = AnyDataValue::new_plain_string("string".into());
                    tuple_writer.add_tuple_value(type_field);

                    tuple_writer.add_tuple_value(id);
                    tuple_writer.add_tuple_value(value_iri.clone());

                    let value_field = AnyDataValue::new_plain_string(value);
                    tuple_writer.add_tuple_value(value_field);
                }
                Value::Array(value) => {
                    tuple_writer.add_tuple_value(id.clone());
                    tuple_writer.add_tuple_value(type_iri.clone());

                    let type_field = AnyDataValue::new_plain_string("array".into());
                    tuple_writer.add_tuple_value(type_field);

                    for (i, element) in value.into_iter().enumerate() {
                        max_object_id += 1;
                        stack.push((max_object_id, element));

                        tuple_writer.add_tuple_value(id.clone());
                        tuple_writer.add_tuple_value(AnyDataValue::new_integer_from_u64(i as u64));
                        tuple_writer
                            .add_tuple_value(AnyDataValue::new_integer_from_u64(max_object_id))
                    }
                }
                Value::Object(value) => {
                    tuple_writer.add_tuple_value(id.clone());
                    tuple_writer.add_tuple_value(type_iri.clone());

                    let type_field = AnyDataValue::new_plain_string("object".into());
                    tuple_writer.add_tuple_value(type_field);

                    for (key, element) in value {
                        max_object_id += 1;
                        stack.push((max_object_id, element));

                        tuple_writer.add_tuple_value(id.clone());
                        tuple_writer.add_tuple_value(AnyDataValue::new_plain_string(key));
                        tuple_writer
                            .add_tuple_value(AnyDataValue::new_integer_from_u64(max_object_id))
                    }
                }
            }
        }

        Ok(())
    }

    fn arity(&self) -> usize {
        3
    }
}
