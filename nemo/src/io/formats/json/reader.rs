//! Implements a reader for json files.
//! Deserialisation is handled by [serde_json].

use core::fmt;
use std::{fmt::Debug, io::Read, mem::size_of};

use nemo_physical::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::AnyDataValue,
    error::{ExternalReadingError, ReadingError, ReadingErrorKind},
    management::bytesized::ByteSized,
    tabular::filters::FilterTransformPattern,
};

use super::variants;

pub(crate) struct JsonReader<T> {
    pub(super) read: T,
    pub(super) patterns: Vec<FilterTransformPattern>,
}

impl<T> Debug for JsonReader<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("JsonReader").finish()
    }
}

impl<T> ByteSized for JsonReader<T> {
    fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64
    }
}

#[derive(Debug)]
struct JsonReadingError(serde_json::Error);

impl fmt::Display for JsonReadingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <serde_json::Error as fmt::Display>::fmt(&self.0, f)
    }
}

impl ExternalReadingError for JsonReadingError {}

impl From<JsonReadingError> for ReadingError {
    fn from(value: JsonReadingError) -> Self {
        ReadingError::new(ReadingErrorKind::ExternalReadingError(Box::new(value)))
    }
}

#[async_trait::async_trait(?Send)]
impl<T: Read> TableProvider for JsonReader<T> {
    async fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), ReadingError> {
        let value: variants::default::JsonAnyDataValue =
            serde_json::from_reader(self.read).map_err(JsonReadingError)?;
        tuple_writer.add_tuple_value(AnyDataValue::from(value));

        Ok(())
    }

    fn arity(&self) -> usize {
        3
    }
}
