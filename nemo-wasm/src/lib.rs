#![feature(alloc_error_hook)]

use std::alloc::Layout;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::io::Cursor;

use js_sys::Array;
use js_sys::Reflect;
use js_sys::Set;
use js_sys::Uint8Array;
use nemo::execution::ExecutionEngine;

use nemo::io::parser::parse_fact;
use nemo::io::parser::parse_program;
use nemo::io::resource_providers::{ResourceProvider, ResourceProviders};
use nemo::model::types::primitive_logical_value::PrimitiveLogicalValueT;
use nemo::model::Constant;
use nemo::model::DataSource;
use nemo::model::DataSourceDeclaration;
use nemo::model::NumericLiteral;
use nemo_physical::datatypes::Double;
use nemo_physical::error::ExternalReadingError;
use nemo_physical::error::ReadingError;
use nemo_physical::table_reader::Resource;
use thiserror::Error;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use web_sys::Blob;
use web_sys::FileReaderSync;

#[wasm_bindgen]
#[derive(Clone)]
pub struct NemoProgram(nemo::model::Program);

#[derive(Error, Debug)]
enum WasmOrInternalNemoError {
    /// Nemo-internal error
    #[error(transparent)]
    NemoError(#[from] nemo::error::Error),
    #[error("Internal reflection error: {0:#?}")]
    ReflectionError(JsValue),
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct NemoError(WasmOrInternalNemoError);

#[wasm_bindgen]
impl NemoError {
    #[allow(clippy::inherent_to_string)]
    #[wasm_bindgen(js_name = "toString")]
    pub fn to_string(&self) -> String {
        format!("{:#?}", self)
    }
}

#[wasm_bindgen]
impl NemoProgram {
    #[wasm_bindgen(constructor)]
    pub fn new(input: &str) -> Result<NemoProgram, NemoError> {
        parse_program(input)
            .map(NemoProgram)
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)
    }

    #[wasm_bindgen(js_name = "getSourceResources")]
    pub fn source_resources(&self) -> Set {
        let set = Set::new(&JsValue::undefined());

        for resource in self.0.sources().flat_map(DataSourceDeclaration::resources) {
            set.add(&JsValue::from(resource));
        }

        set
    }

    #[wasm_bindgen(js_name = "getOutputPredicates")]
    pub fn output_predicates(&self) -> Array {
        self.0
            .output_predicates()
            .map(|id| JsValue::from(id.name()))
            .collect()
    }

    #[wasm_bindgen(js_name = "getEDBPredicates")]
    pub fn edb_predicates(&self) -> Set {
        let set = Set::new(&JsValue::undefined());

        for identifier in self.0.edb_predicates().into_iter() {
            set.add(&JsValue::from(identifier.name()));
        }

        set
    }
}

#[derive(Debug)]
pub struct BlobResourceProvider {
    blobs: HashMap<String, Blob>,
    file_reader_sync: FileReaderSync,
}

impl BlobResourceProvider {
    pub fn new(blobs: HashMap<String, Blob>) -> Result<Self, JsValue> {
        Ok(Self {
            blobs,
            file_reader_sync: FileReaderSync::new()?,
        })
    }
}

#[derive(Debug)]
struct BlobReadingError(JsValue);

impl std::fmt::Display for BlobReadingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Error while reading blob: {:#?}", self)
    }
}

impl ExternalReadingError for BlobReadingError {}

impl ResourceProvider for BlobResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
    ) -> Result<Option<Box<dyn std::io::Read>>, nemo_physical::error::ReadingError> {
        if let Some(blob) = self.blobs.get(resource) {
            let array_buffer: js_sys::ArrayBuffer = self
                .file_reader_sync
                .read_as_array_buffer(blob)
                .map_err(|js_value| {
                    ReadingError::ExternalReadingError(Box::new(BlobReadingError(js_value)))
                })?;

            let data = Uint8Array::new(&array_buffer).to_vec();

            let cursor = Cursor::new(data);

            Ok(Some(Box::new(cursor)))
        } else {
            Ok(None)
        }
    }
}

#[wasm_bindgen]
pub struct NemoEngine(nemo::execution::DefaultExecutionEngine);

#[cfg(web_sys_unstable_apis)]
fn std_io_error_from_js_value(js_value: JsValue, prefix: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("{prefix}: {js_value:#?}"),
    )
}

#[cfg(web_sys_unstable_apis)]
struct SyncAccessHandleWriter(web_sys::FileSystemSyncAccessHandle);

#[cfg(web_sys_unstable_apis)]
impl std::io::Write for SyncAccessHandleWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        let buf: Vec<_> = buf.into();
        let bytes_written = self.0.write_with_u8_array(&buf).map_err(|js_value| {
            std_io_error_from_js_value(
                js_value,
                "Error while writing to FileSystemSyncAccessHandle",
            )
        })?;

        // Convert to usize safely
        let converted_bytes_written = bytes_written as usize;
        if converted_bytes_written as f64 != bytes_written {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Error while converting number of bytes written to usize: {bytes_written:#?}"
                ),
            ));
        }

        Ok(converted_bytes_written)
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.0.flush().map_err(|js_value| {
            std_io_error_from_js_value(js_value, "Error while flushing FileSystemSyncAccessHandle")
        })?;

        Ok(())
    }
}

#[wasm_bindgen]
impl NemoEngine {
    #[wasm_bindgen(constructor)]
    pub fn new(
        program: &NemoProgram,
        resource_blobs_js_value: JsValue,
    ) -> Result<NemoEngine, NemoError> {
        // Parse JavaScript object into `HashMap`
        let mut resource_blobs = HashMap::new();
        for key in Reflect::own_keys(&resource_blobs_js_value)
            .map_err(WasmOrInternalNemoError::ReflectionError)
            .map_err(NemoError)?
        {
            if let Some(resource) = key.as_string() {
                let value = Reflect::get(&resource_blobs_js_value, &key)
                    .map_err(WasmOrInternalNemoError::ReflectionError)
                    .map_err(NemoError)?;
                let blob: Blob = JsCast::dyn_into(value).unwrap();

                resource_blobs.insert(resource, blob);
            }
        }

        let resource_providers = if resource_blobs.is_empty() {
            ResourceProviders::empty()
        } else {
            ResourceProviders::from(vec![Box::new(
                BlobResourceProvider::new(resource_blobs)
                    .map_err(WasmOrInternalNemoError::ReflectionError)
                    .map_err(NemoError)?,
            )])
        };

        ExecutionEngine::initialize(program.clone().0, resource_providers)
            .map(NemoEngine)
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)
    }

    #[wasm_bindgen]
    pub fn reason(&mut self) -> Result<(), NemoError> {
        self.0
            .execute()
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)
    }

    #[wasm_bindgen(js_name = "countFactsOfDerivedPredicates")]
    pub fn count_facts_of_derived_predicates(&mut self) -> usize {
        self.0.count_facts_of_derived_predicates()
    }

    #[wasm_bindgen(js_name = "countFactsOfPredicate")]
    pub fn count_facts_of_predicate(&mut self, predicate: String) -> Option<usize> {
        self.0.count_facts_of_predicate(&predicate.into())
    }

    #[wasm_bindgen(js_name = "getResult")]
    pub fn result(&mut self, predicate: String) -> Result<NemoResults, NemoError> {
        let iter = self
            .0
            .table_scan(predicate.into())
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)?;

        let results = NemoResults(Box::new(
            iter.into_iter().flatten().collect::<Vec<_>>().into_iter(),
        ));

        Ok(results)
    }

    #[cfg(web_sys_unstable_apis)]
    #[wasm_bindgen(js_name = "savePredicate")]
    pub fn write_result_to_sync_access_handle(
        &mut self,
        predicate: String,
        sync_access_handle: web_sys::FileSystemSyncAccessHandle,
    ) -> Result<(), NemoError> {
        use nemo::io::{output_file_manager::FileFormat, RecordWriter};

        let Some(record_iter) = self
            .0
            .output_serialization(predicate.into())
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)?
        else {
            return Ok(());
        };

        let writer = SyncAccessHandleWriter(sync_access_handle);

        let file_format = FileFormat::DSV(b',');

        let mut writer = file_format.create_writer(writer);

        for record in record_iter {
            writer.write_record(record).unwrap();
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = "parseAndTraceFact")]
    pub fn parse_and_trace_fact(&mut self, fact: &str) -> Result<Option<String>, NemoError> {
        let parsed_fact = parse_fact(fact.to_owned())
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)?;

        let trace = self
            .0
            .trace(parsed_fact)
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)?;

        Ok(trace.map(|t| format!("{t}")))
    }
}

#[wasm_bindgen]
pub struct NemoResults(Box<dyn Iterator<Item = Vec<PrimitiveLogicalValueT>> + Send>);

#[wasm_bindgen]
pub struct NemoResultsIteratorNext {
    pub done: bool,
    #[wasm_bindgen(getter_with_clone)]
    pub value: JsValue,
}

#[wasm_bindgen]
impl NemoResults {
    #[allow(clippy::should_implement_trait)]
    #[wasm_bindgen]
    pub fn next(&mut self) -> NemoResultsIteratorNext {
        let next = self.0.next();

        if let Some(next) = next {
            let array: Array = next
                .into_iter()
                .map(|v| match v {
                    PrimitiveLogicalValueT::Any(rdf) => match rdf {
                        Constant::Abstract(c) => JsValue::from(c.to_string()),
                        Constant::NumericLiteral(NumericLiteral::Integer(i)) => JsValue::from(i),
                        Constant::NumericLiteral(NumericLiteral::Double(d)) => {
                            JsValue::from(f64::from(d))
                        }
                        // currently we pack decimals into strings, maybe this should change
                        Constant::NumericLiteral(_) => JsValue::from(rdf.to_string()),
                        Constant::StringLiteral(s) => JsValue::from(s),
                        Constant::RdfLiteral(lit) => JsValue::from(lit.to_string()),
                    },
                    PrimitiveLogicalValueT::String(s) => JsValue::from(String::from(s)),
                    PrimitiveLogicalValueT::Integer(i) => JsValue::from(i64::from(i)),
                    PrimitiveLogicalValueT::Float64(d) => JsValue::from(f64::from(Double::from(d))),
                })
                .collect();

            NemoResultsIteratorNext {
                done: false,
                value: JsValue::from(array),
            }
        } else {
            NemoResultsIteratorNext {
                done: true,
                value: JsValue::undefined(),
            }
        }
    }
}

#[wasm_bindgen(js_name = "setPanicHook")]
pub fn set_panic_hook() {
    console_error_panic_hook::set_once();
}

fn custom_alloc_error_hook(layout: Layout) {
    panic!(
        "memory allocation of {} bytes (align {}) failed",
        layout.size(),
        layout.align()
    );
}

#[wasm_bindgen(js_name = "setAllocErrorHook")]
pub fn set_alloc_error_hook() {
    std::alloc::set_alloc_error_hook(custom_alloc_error_hook);
}

#[wasm_bindgen(js_name = "getNemoVersion")]
pub fn nemo_version() -> Option<String> {
    option_env!("CARGO_PKG_VERSION").map(|version| version.into())
}
