#![feature(alloc_error_hook)]

use std::alloc::Layout;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::io::Cursor;

use js_sys::Array;
use js_sys::Reflect;
use js_sys::Set;
use js_sys::Uint8Array;
use nemo::execution::tracing::trace::ExecutionTraceTree;
use nemo::execution::ExecutionEngine;

use nemo::io::compression_format::CompressionFormat;
use nemo::io::parser::parse_fact;
use nemo::io::parser::parse_program;
use nemo::io::resource_providers::{ResourceProvider, ResourceProviders};
use nemo::io::ImportManager;
use nemo::model::Identifier;
use nemo_physical::datavalues::AnyDataValue;
use nemo_physical::datavalues::DataValue;
use nemo_physical::error::ExternalReadingError;
use nemo_physical::error::ReadingError;
use nemo_physical::resource::Resource;
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
#[allow(dead_code)]
pub struct NemoError(WasmOrInternalNemoError);

#[wasm_bindgen]
impl NemoError {
    #[allow(clippy::inherent_to_string)]
    #[wasm_bindgen(js_name = "toString")]
    pub fn to_string(&self) -> String {
        format!("NemoError: {:#?}", self.0)
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

    /// Get all resources that are referenced in import directives of the program.
    /// Returns an error if there are problems in some import directive.
    ///
    /// TODO: Maybe rethink the validation mechanism of NemoProgram. We could also
    /// just make sure that things validate upon creation, and make sure that problems
    /// are detected early.
    #[wasm_bindgen(js_name = "getResourcesUsedInImports")]
    pub fn resources_used_in_imports(&self) -> Result<Set, NemoError> {
        let js_set = Set::new(&JsValue::undefined());

        for directive in self.0.imports() {
            let resource = ImportManager::resource(directive)
                .map_err(WasmOrInternalNemoError::NemoError)
                .map_err(NemoError)?;
            js_set.add(&JsValue::from(resource));
        }

        Ok(js_set)
    }

    // If there are no outputs, marks all predicates as outputs.
    #[wasm_bindgen(js_name = "markDefaultOutputs")]
    pub fn mark_default_output_predicates(&mut self) {
        if self.0.output_predicates().next().is_none() {
            let mut additional_outputs = Vec::new();
            for predicate in self.0.predicates() {
                additional_outputs.push(predicate);
            }
            self.0.add_output_predicates(additional_outputs);
        }
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
        let js_set = Set::new(&JsValue::undefined());

        for identifier in self.0.edb_predicates().into_iter() {
            js_set.add(&JsValue::from(identifier.name()));
        }

        js_set
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
#[allow(dead_code)]
struct BlobReadingError(JsValue);

impl std::fmt::Display for BlobReadingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Error while reading blob: {:#?}", self.0)
    }
}

impl ExternalReadingError for BlobReadingError {}

impl ResourceProvider for BlobResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
        compression: CompressionFormat,
    ) -> Result<Option<Box<dyn std::io::BufRead>>, nemo_physical::error::ReadingError> {
        if let Some(blob) = self.blobs.get(resource) {
            let array_buffer: js_sys::ArrayBuffer = self
                .file_reader_sync
                .read_as_array_buffer(blob)
                .map_err(|js_value| {
                    ReadingError::ExternalReadingError(Box::new(BlobReadingError(js_value)))
                })?;

            let data = Uint8Array::new(&array_buffer).to_vec();

            let cursor = Cursor::new(data);

            // Decompress blob
            // We currently do this in Rust after the Blob has been transferred to the WebAssembly, to reuse Nemo's compression logic.
            // We could also to this on the JavaScript side, see https://developer.mozilla.org/en-US/docs/Web/API/Compression_Streams_API .
            if let Some(reader) = compression.try_decompression(cursor) {
                Ok(Some(reader))
            } else {
                Err(ReadingError::Decompression {
                    resource: resource.to_owned(),
                    decompression_format: compression.to_string(),
                })
            }
        } else {
            Ok(None)
        }
    }
}

#[wasm_bindgen]
pub struct NemoEngine {
    program: NemoProgram,
    engine: nemo::execution::DefaultExecutionEngine,
}

#[cfg(feature = "web_sys_unstable_apis")]
fn std_io_error_from_js_value(js_value: JsValue, prefix: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("{prefix}: {js_value:#?}"),
    )
}

#[cfg(feature = "web_sys_unstable_apis")]
struct SyncAccessHandleWriter(web_sys::FileSystemSyncAccessHandle);

#[cfg(feature = "web_sys_unstable_apis")]
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
        let import_manager = ImportManager::new(resource_providers);

        let engine = ExecutionEngine::initialize(&program.0, import_manager)
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)?;

        Ok(NemoEngine {
            program: program.clone(),
            engine,
        })
    }

    #[wasm_bindgen]
    pub fn reason(&mut self) -> Result<(), NemoError> {
        self.engine
            .execute()
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)
    }

    #[wasm_bindgen(js_name = "countFactsOfDerivedPredicates")]
    pub fn count_facts_of_derived_predicates(&mut self) -> usize {
        self.engine.count_facts_of_derived_predicates()
    }

    #[wasm_bindgen(js_name = "countFactsOfPredicate")]
    pub fn count_facts_of_predicate(&mut self, predicate: String) -> Option<usize> {
        self.engine.count_facts_of_predicate(&predicate.into())
    }

    #[wasm_bindgen(js_name = "getResult")]
    pub fn result(&mut self, predicate: String) -> Result<NemoResults, NemoError> {
        let iter = self
            .engine
            .predicate_rows(&Identifier::from(predicate))
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)?;

        let results = NemoResults(Box::new(
            iter.into_iter().flatten().collect::<Vec<_>>().into_iter(),
        ));

        Ok(results)
    }

    #[cfg(feature = "web_sys_unstable_apis")]
    #[wasm_bindgen(js_name = "savePredicate")]
    pub fn write_result_to_sync_access_handle(
        &mut self,
        predicate: String,
        sync_access_handle: web_sys::FileSystemSyncAccessHandle,
    ) -> Result<(), NemoError> {
        use nemo::{
            io::ExportManager,
            model::{ExportDirective, Identifier},
        };

        let identifier = Identifier::from(predicate.clone());

        let Some(arity) = self.engine.predicate_arity(&identifier) else {
            return Ok(());
        };

        let Some(record_iter) = self
            .engine
            .predicate_rows(&identifier)
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)?
        else {
            return Ok(());
        };

        let writer = SyncAccessHandleWriter(sync_access_handle);

        let export_spec = ExportDirective::default(identifier);
        let export_manager: ExportManager = Default::default();

        export_manager
            .export_table_with_writer(&export_spec, Box::new(writer), Some(record_iter), arity)
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)
    }

    fn parse_and_trace_fact(&mut self, fact: &str) -> Option<ExecutionTraceTree> {
        let parsed_fact = parse_fact(fact.to_owned())
            .map_err(WasmOrInternalNemoError::NemoError)
            .map_err(NemoError)
            .ok()?;

        let (trace, handles) = self.engine.trace(self.program.0.clone(), vec![parsed_fact]);

        trace.tree(handles[0])
    }

    #[wasm_bindgen(js_name = "parseAndTraceFactAscii")]
    pub fn parse_and_trace_fact_ascii(&mut self, fact: &str) -> Option<String> {
        self.parse_and_trace_fact(fact)
            .as_ref()
            .map(ExecutionTraceTree::to_ascii_art)
    }

    #[wasm_bindgen(js_name = "parseAndTraceFactGraphML")]
    pub fn parse_and_trace_fact_graphml(&mut self, fact: &str) -> Option<String> {
        self.parse_and_trace_fact(fact)
            .as_ref()
            .map(ExecutionTraceTree::to_graphml)
    }
}

#[wasm_bindgen]
pub struct NemoResults(Box<dyn Iterator<Item = Vec<AnyDataValue>> + Send>);

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
                .map(|v| match v.value_domain() {
                    nemo_physical::datavalues::ValueDomain::PlainString
                    | nemo_physical::datavalues::ValueDomain::Null
                    | nemo_physical::datavalues::ValueDomain::LanguageTaggedString
                    | nemo_physical::datavalues::ValueDomain::Other => {
                        JsValue::from(v.canonical_string())
                    }
                    nemo_physical::datavalues::ValueDomain::Iri => {
                        JsValue::from(v.to_iri_unchecked())
                    }
                    nemo_physical::datavalues::ValueDomain::Double => {
                        JsValue::from(v.to_f64_unchecked())
                    }
                    nemo_physical::datavalues::ValueDomain::Float => {
                        JsValue::from(v.to_f32_unchecked())
                    }
                    nemo_physical::datavalues::ValueDomain::UnsignedLong => {
                        JsValue::from(v.to_u64_unchecked())
                    }
                    nemo_physical::datavalues::ValueDomain::NonNegativeLong
                    | nemo_physical::datavalues::ValueDomain::UnsignedInt
                    | nemo_physical::datavalues::ValueDomain::NonNegativeInt
                    | nemo_physical::datavalues::ValueDomain::Long
                    | nemo_physical::datavalues::ValueDomain::Int => {
                        JsValue::from(v.to_i64_unchecked())
                    }
                    nemo_physical::datavalues::ValueDomain::Boolean => {
                        JsValue::from(v.to_boolean_unchecked())
                    }
                    nemo_physical::datavalues::ValueDomain::Tuple
                    | nemo_physical::datavalues::ValueDomain::Map => {
                        // Currently we only create a string representation of the tuple or map
                        // We convert the tuples/maps to arrays/objects in the future if we require this in JavaScript
                        JsValue::from(v.to_string())
                    }
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
