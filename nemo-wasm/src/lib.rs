#![feature(alloc_error_hook)]

use std::{alloc::Layout, collections::HashMap, fmt::Formatter, io::Cursor};

use js_sys::{Array, Reflect, Set, Uint8Array};
use thiserror::Error;
use wasm_bindgen::{prelude::wasm_bindgen, JsCast, JsValue};
use web_sys::{Blob, FileReaderSync};

use nemo::{
    datavalues::{AnyDataValue, DataValue},
    error::ReadingError,
    execution::{
        tracing::trace::{ExecutionTrace, ExecutionTraceTree, TraceFactHandle},
        ExecutionEngine,
    },
    io::{
        resource_providers::{ResourceProvider, ResourceProviders},
        ImportManager,
    },
    rule_model::{
        components::{
            fact::Fact,
            import_export::{attributes::ImportExportAttribute, compression::CompressionFormat},
            tag::Tag,
            term::{primitive::Primitive, Term},
        },
        error::ComponentParseError,
    },
};

use nemo_physical::{error::ExternalReadingError, resource::Resource};

mod language_server;

const PROGRAM_LABEL: &str = "nemo-web";

#[wasm_bindgen]
#[derive(Clone)]
pub struct NemoProgram(nemo::rule_model::program::Program);

#[derive(Error, Debug)]
enum WasmOrInternalNemoError {
    /// Nemo-internal error
    #[error(transparent)]
    Nemo(#[from] nemo::error::Error),
    #[error("Unable to parse component:\n {0}")]
    ComponentParse(ComponentParseError),
    #[error("Unable to parse program:\n {0}")]
    Parser(String),
    #[error("Invalid program:\n {0}")]
    Program(String),
    #[error("Internal reflection error: {0:#?}")]
    Reflection(JsValue),
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
        format!("{}", self.0)
    }
}

#[wasm_bindgen]
pub struct NemoResource {
    accept: String,
    url: String,
}

#[wasm_bindgen]
impl NemoResource {
    pub fn accept(&self) -> String {
        self.accept.clone()
    }

    pub fn url(&self) -> String {
        self.url.clone()
    }
}

#[wasm_bindgen]
impl NemoProgram {
    #[wasm_bindgen(constructor)]
    pub fn new(input: &str) -> Result<NemoProgram, NemoError> {
        nemo::parser::Parser::initialize(input, PROGRAM_LABEL.to_string())
            .parse()
            .map_err(|(_, report)| WasmOrInternalNemoError::Parser(format!("{}", report)))
            .map_err(NemoError)
            .and_then(|ast| {
                nemo::rule_model::translation::ASTProgramTranslation::initialize(
                    input,
                    PROGRAM_LABEL.to_string(),
                )
                .translate(&ast)
                .map_err(|report| WasmOrInternalNemoError::Program(format!("{}", report)))
                .map_err(NemoError)
                .map(NemoProgram)
            })
    }

    /// Get all resources that are referenced in import directives of the program.
    /// Returns an error if there are problems in some import directive.
    ///
    /// TODO: Maybe rethink the validation mechanism of NemoProgram. We could also
    /// just make sure that things validate upon creation, and make sure that problems
    /// are detected early.
    #[wasm_bindgen(js_name = "getResourcesUsedInImports")]
    pub fn resources_used_in_imports(&self) -> Vec<NemoResource> {
        let mut result: Vec<NemoResource> = vec![];

        for directive in self.0.imports() {
            let format = directive.file_format().media_type();
            if let Some(resource) = directive.attributes().get(&ImportExportAttribute::Resource) {
                result.push(NemoResource {
                    accept: format.to_string(),
                    url: resource.to_string(),
                });
            }
        }

        result
    }

    // If there are no outputs, marks all predicates as outputs.
    #[wasm_bindgen(js_name = "markDefaultOutputs")]
    pub fn mark_default_output_predicates(&mut self) {
        if self.0.outputs().next().is_none() {
            for predicate in self.0.all_predicates() {
                self.0.add_output(predicate)
            }
        }
    }

    #[wasm_bindgen(js_name = "getOutputPredicates")]
    pub fn output_predicates(&self) -> Array {
        self.0
            .outputs()
            .map(|o| JsValue::from(o.predicate().to_string()))
            .collect()
    }

    #[wasm_bindgen(js_name = "getEDBPredicates")]
    pub fn edb_predicates(&self) -> Set {
        let js_set = Set::new(&JsValue::undefined());

        for tag in self.0.import_predicates().into_iter() {
            js_set.add(&JsValue::from(tag.to_string()));
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
        _media_type: &str,
    ) -> Result<Option<Box<dyn std::io::BufRead>>, nemo_physical::error::ReadingError> {
        let res = match resource {
            Resource::Path(path) => path,
            Resource::Iri { iri, .. } => &iri.to_string()
        };
        if let Some(blob) = self.blobs.get(res) {
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
            .map_err(WasmOrInternalNemoError::Reflection)
            .map_err(NemoError)?
        {
            if let Some(resource) = key.as_string() {
                let value = Reflect::get(&resource_blobs_js_value, &key)
                    .map_err(WasmOrInternalNemoError::Reflection)
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
                    .map_err(WasmOrInternalNemoError::Reflection)
                    .map_err(NemoError)?,
            )])
        };
        let import_manager = ImportManager::new(resource_providers);

        let engine = ExecutionEngine::initialize(program.0.clone(), import_manager)
            .map_err(WasmOrInternalNemoError::Nemo)
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
            .map_err(WasmOrInternalNemoError::Nemo)
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
            .predicate_rows(&Tag::from(predicate))
            .map_err(WasmOrInternalNemoError::Nemo)
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
        use nemo::io::{
            formats::{
                dsv::{value_format::DsvValueFormats, DsvHandler},
                Direction, ImportExportHandler, ImportExportResource,
            },
            ExportManager,
        };

        let identifier = Tag::from(predicate.clone());

        let Some(arity) = self.engine.predicate_arity(&identifier) else {
            return Ok(());
        };

        let Some(record_iter) = self
            .engine
            .predicate_rows(&identifier)
            .map_err(WasmOrInternalNemoError::Nemo)
            .map_err(NemoError)?
        else {
            return Ok(());
        };

        let writer = SyncAccessHandleWriter(sync_access_handle);

        let export_handler: Box<dyn ImportExportHandler> = Box::new(DsvHandler::new(
            b',',
            ImportExportResource::Stdout,
            DsvValueFormats::default(arity),
            None,
            CompressionFormat::None,
            false,
            Direction::Export,
        ));
        let export_manager: ExportManager = Default::default();

        export_manager
            .export_table_with_writer(Box::new(writer), &*export_handler, Some(record_iter))
            .map_err(WasmOrInternalNemoError::Nemo)
            .map_err(NemoError)
    }

    fn trace_fact_at_index(
        &mut self,
        predicate: String,
        row_index: usize,
    ) -> Result<Option<(ExecutionTrace, Vec<TraceFactHandle>)>, NemoError> {
        let iter = self
            .engine
            .predicate_rows(&Tag::from(predicate.clone()))
            .map_err(WasmOrInternalNemoError::Nemo)
            .map_err(NemoError)?;

        let terms_to_trace_opt: Option<Vec<AnyDataValue>> =
            iter.into_iter().flatten().nth(row_index);

        if let Some(terms_to_trace) = terms_to_trace_opt {
            let fact_to_trace = Fact::new(
                &predicate,
                terms_to_trace
                    .into_iter()
                    .map(|term| Term::Primitive(Primitive::from(term))),
            );

            let (trace, handles) = self
                .engine
                .trace(self.program.0.clone(), vec![fact_to_trace])
                .map_err(WasmOrInternalNemoError::Nemo)
                .map_err(NemoError)?;

            Ok(Some((trace, handles)))
        } else {
            Ok(None)
        }
    }

    #[wasm_bindgen(js_name = "traceFactAtIndexAscii")]
    pub fn trace_fact_at_index_ascii(
        &mut self,
        predicate: String,
        row_index: usize,
    ) -> Result<Option<String>, NemoError> {
        self.trace_fact_at_index(predicate, row_index).map(|opt| {
            opt.and_then(|(trace, handles)| {
                trace
                    .tree(handles[0])
                    .as_ref()
                    .map(ExecutionTraceTree::to_ascii_art)
            })
        })
    }

    #[wasm_bindgen(js_name = "traceFactAtIndexGraphMlTree")]
    pub fn trace_fact_at_index_graphml_tree(
        &mut self,
        predicate: String,
        row_index: usize,
    ) -> Result<Option<String>, NemoError> {
        self.trace_fact_at_index(predicate, row_index).map(|opt| {
            opt.and_then(|(trace, handles)| {
                trace
                    .tree(handles[0])
                    .as_ref()
                    .map(ExecutionTraceTree::to_graphml)
            })
        })
    }

    #[wasm_bindgen(js_name = "traceFactAtIndexGraphMlDag")]
    pub fn trace_fact_at_index_graphml_dag(
        &mut self,
        predicate: String,
        row_index: usize,
    ) -> Result<Option<String>, NemoError> {
        self.trace_fact_at_index(predicate, row_index)
            .map(|opt| opt.map(|(trace, handles)| trace.graphml(&handles)))
    }

    fn parse_and_trace_fact(
        &mut self,
        fact: &str,
    ) -> Result<Option<(ExecutionTrace, Vec<TraceFactHandle>)>, NemoError> {
        let parsed_fact = Fact::parse(fact)
            .map_err(WasmOrInternalNemoError::ComponentParse)
            .map_err(NemoError)?;

        let (trace, handles) = self
            .engine
            .trace(self.program.0.clone(), vec![parsed_fact])
            .map_err(WasmOrInternalNemoError::Nemo)
            .map_err(NemoError)?;

        Ok(Some((trace, handles)))
    }

    #[wasm_bindgen(js_name = "parseAndTraceFactAscii")]
    pub fn parse_and_trace_fact_ascii(&mut self, fact: &str) -> Result<Option<String>, NemoError> {
        self.parse_and_trace_fact(fact).map(|opt| {
            opt.and_then(|(trace, handles)| {
                trace
                    .tree(handles[0])
                    .as_ref()
                    .map(ExecutionTraceTree::to_ascii_art)
            })
        })
    }

    #[wasm_bindgen(js_name = "parseAndTraceFactGraphMlTree")]
    pub fn parse_and_trace_fact_graphml_tree(
        &mut self,
        fact: &str,
    ) -> Result<Option<String>, NemoError> {
        self.parse_and_trace_fact(fact).map(|opt| {
            opt.and_then(|(trace, handles)| {
                trace
                    .tree(handles[0])
                    .as_ref()
                    .map(ExecutionTraceTree::to_graphml)
            })
        })
    }

    #[wasm_bindgen(js_name = "parseAndTraceFactGraphMlDag")]
    pub fn parse_and_trace_fact_graphml_dag(
        &mut self,
        fact: &str,
    ) -> Result<Option<String>, NemoError> {
        self.parse_and_trace_fact(fact)
            .map(|opt| opt.map(|(trace, handles)| trace.graphml(&handles)))
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
