#![feature(alloc_error_hook)]

use std::{alloc::Layout, collections::HashMap, fmt::Formatter, io::Cursor};

use gloo_utils::format::JsValueSerdeExt;
use js_sys::{Array, Reflect, Set, Uint8Array};
use models::TableResponseBaseTableEntries;
use thiserror::Error;
use wasm_bindgen::{JsCast, JsValue, prelude::wasm_bindgen};
use web_sys::{Blob, FileReaderSync};

use nemo::{
    datavalues::{AnyDataValue, DataValue},
    error::ReadingError,
    execution::{
        ExecutionEngine,
        tracing::trace::{ExecutionTrace, ExecutionTraceTree, TraceFactHandle},
    },
    io::{
        ImportManager,
        formats::FileFormatMeta,
        resource_providers::{ResourceProvider, ResourceProviders},
    },
    rule_model::{
        components::{
            fact::Fact,
            output::Output,
            tag::Tag,
            term::{Term, primitive::Primitive},
        },
        programs::{ProgramRead, ProgramWrite, program::Program},
        translation::ProgramParseReport,
    },
};

use nemo_physical::{error::ExternalReadingError, resource::Resource};

mod language_server;
mod models;

#[wasm_bindgen]
#[derive(Clone)]
pub struct NemoProgram(Program);

#[derive(Error, Debug)]
enum WasmOrInternalNemoError {
    /// Nemo-internal error
    #[error(transparent)]
    Nemo(#[from] nemo::error::Error),
    #[error("Unable to parse component:\n {0}")]
    ComponentParse(ProgramParseReport),
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
        let ast = match nemo::parser::Parser::initialize(input).parse() {
            Ok(ast) => ast,
            Err((_, report)) => {
                return Err(NemoError(WasmOrInternalNemoError::Parser(format!(
                    "{report}"
                ))));
            }
        };

        let mut program = Program::default();
        let report = nemo::rule_model::translation::ASTProgramTranslation::default()
            .translate(&ast, &mut program);

        if report.contains_errors() {
            return Err(NemoError(WasmOrInternalNemoError::Program(format!(
                "{report}"
            ))));
        }

        Ok(NemoProgram(program))
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

        for import in self.0.imports() {
            let Some(builder) = import.builder() else {
                continue;
            };

            let format: String = builder.build_import("", 0).media_type();
            if let Some(resource) = builder.resource() {
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
                self.0.add_output(Output::new(predicate))
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

#[derive(Debug, Error)]
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
        _media_type: &str,
    ) -> Result<Option<Box<dyn std::io::Read>>, nemo_physical::error::ReadingError> {
        if let Some(blob) = self.blobs.get(&resource.to_string()) {
            let array_buffer: js_sys::ArrayBuffer = self
                .file_reader_sync
                .read_as_array_buffer(blob)
                .map_err(|js_value| {
                    ReadingError::new_external(Box::new(BlobReadingError(js_value)))
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
pub struct NemoEngine {
    engine: nemo::execution::DefaultExecutionEngine,
}

#[cfg(feature = "web_sys_unstable_apis")]
fn std_io_error_from_js_value(js_value: JsValue, prefix: &str) -> std::io::Error {
    std::io::Error::other(format!("{prefix}: {js_value:#?}"))
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
            return Err(std::io::Error::other(format!(
                "Error while converting number of bytes written to usize: {bytes_written:#?}"
            )));
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

        Ok(NemoEngine { engine })
    }

    #[wasm_bindgen]
    pub fn reason(&mut self) -> Result<(), NemoError> {
        self.engine
            .execute()
            .map_err(WasmOrInternalNemoError::Nemo)
            .map_err(NemoError)
    }

    #[wasm_bindgen(js_name = "countFactsInMemoryForDerivedPredicates")]
    pub fn count_facts_in_memory_for_derived_predicates(&mut self) -> usize {
        self.engine.count_facts_in_memory_for_derived_predicates()
    }

    #[wasm_bindgen(js_name = "countFactsInMemoryForPredicate")]
    pub fn count_facts_in_memory_for_predicate(&mut self, predicate: String) -> Option<usize> {
        self.engine
            .count_facts_in_memory_for_predicate(&predicate.into())
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
        use nemo::io::{ExportManager, formats::dsv::DsvHandler};

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

        let export_handler: DsvHandler = DsvHandler::new(b',', arity);
        let export_manager: ExportManager = Default::default();

        export_manager
            .export_table_with_writer(Box::new(writer), &export_handler, Some(record_iter))
            .map_err(WasmOrInternalNemoError::Nemo)
            .map_err(NemoError)
    }

    fn trace_fact_at_index(
        &mut self,
        predicate: String,
        row_index: usize,
    ) -> Result<Option<(ExecutionTrace, Vec<TraceFactHandle>)>, NemoError> {
        let predicate_tag = Tag::from(predicate.clone());

        let iter = self
            .engine
            .predicate_rows(&predicate_tag)
            .map_err(WasmOrInternalNemoError::Nemo)
            .map_err(NemoError)?;

        let terms_to_trace_opt: Option<Vec<AnyDataValue>> =
            iter.into_iter().flatten().nth(row_index);

        if let Some(terms_to_trace) = terms_to_trace_opt {
            let fact_to_trace = Fact::new(
                predicate_tag,
                terms_to_trace
                    .into_iter()
                    .map(|term| Term::Primitive(Primitive::from(term))),
            );

            let (trace, handles) = self
                .engine
                .trace(vec![fact_to_trace])
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
            .trace(vec![parsed_fact])
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

// This is the new tracing stuff. It got its own impl block just to keep it visually separate.
// The mock is the example from https://github.com/knowsys/EvonNemo-API-Spec/tree/main/examples/common-descendants
#[wasm_bindgen]
impl NemoEngine {
    #[wasm_bindgen(js_name = "experimentalNewTracingTreeForTable")]
    pub fn experimental_new_tracing_tree_for_table(
        &mut self,
        tree_for_table_query: JsValue,
    ) -> Result<JsValue, NemoError> {
        let tree_for_table_query: models::TreeForTableQuery = tree_for_table_query
            .into_serde()
            .map_err(|err| WasmOrInternalNemoError::Reflection(err.to_string().into()))
            .map_err(NemoError)?;

        let query =
            nemo::execution::tracing::tree_query::TreeForTableQuery::from(tree_for_table_query);

        let response = self
            .engine
            .trace_tree(query)
            .map_err(WasmOrInternalNemoError::Nemo)
            .map_err(NemoError)?;

        let tree_for_table_response = models::TreeForTableResponse::from(response);

        JsValue::from_serde(&tree_for_table_response)
            .map_err(|err| WasmOrInternalNemoError::Reflection(err.to_string().into()))
            .map_err(NemoError)
    }

    #[wasm_bindgen(js_name = "experimentalNewTracingTableEntriesForTreeNodes")]
    pub fn experimental_new_tracing_table_entries_for_tree_nodes(
        &mut self,
        table_entries_for_tree_nodes_query: JsValue,
    ) -> Result<JsValue, JsValue> {
        let table_entries_for_tree_nodes_query: models::TableEntriesForTreeNodesQuery =
            table_entries_for_tree_nodes_query
                .into_serde()
                .map_err(|err| WasmOrInternalNemoError::Reflection(err.to_string().into()))
                .map_err(NemoError)?;

        let query = nemo::execution::tracing::node_query::TableEntriesForTreeNodesQuery::from(
            table_entries_for_tree_nodes_query,
        );

        let response = self.engine.trace_node(&query);

        let table_entries_for_tree_nodes_response = response
            .elements
            .into_iter()
            .map(models::TableEntriesForTreeNodesResponseInner::from)
            .collect::<Vec<_>>();

        Ok(JsValue::from_serde(&table_entries_for_tree_nodes_response)
            .map_err(|err| WasmOrInternalNemoError::Reflection(err.to_string().into()))
            .map_err(NemoError)?)
    }

    #[wasm_bindgen(js_name = "experimentalNewTracingTreeForTableMock")]
    pub fn experimental_new_tracing_tree_for_table_mock(
        tree_for_table_query: JsValue,
    ) -> Result<JsValue, JsValue> {
        // in this mock, we ignore the query and always return the same tree
        let _tree_for_table_query: models::TreeForTableQuery = tree_for_table_query
            .into_serde()
            .map_err(|err| err.to_string())?;

        let response: models::TreeForTableResponse = models::TreeForTableResponse {
            predicate: "ancestor".to_string(),
            table_entries: Box::new(TableResponseBaseTableEntries {
                entries: vec![models::TableEntryResponse {
                    entry_id: 42,
                    term_tuple: vec!["alice".to_string(), "edward".to_string()],
                }],
                pagination: Box::new(models::TableResponseBaseTableEntriesPagination {
                    start: 0,
                    more_entries_exist: false,
                }),
            }),
            possible_rules_above: vec![], // empty for now...
            possible_rules_below: vec![], // empty for now...
            child_information: Some(Box::new(models::TreeForTableResponseChildInformation {
                rule: models::Rule {
                    id: 1,
                    relevant_head_predicate: Box::new(models::PredicateWithParameters {
                        name: "ancestor".to_string(),
                        parameters: vec!["?X".to_string(), "?Z".to_string()],
                    }),
                    relevant_head_predicate_index: 0,
                    body_predicates: vec![
                        models::PredicateWithParameters {
                            name: "ancestor".to_string(),
                            parameters: vec!["?X".to_string(), "?Y".to_string()],
                        },
                        models::PredicateWithParameters {
                            name: "parent".to_string(),
                            parameters: vec!["?Y".to_string(), "?Z".to_string()],
                        },
                    ],
                    string_representation: "ancestor(?X, ?Z) :- ancestor(?X, ?Y), parent(?Y, ?Z)"
                        .to_string(),
                },
                children: vec![
                    models::TreeForTableResponse {
                        predicate: "ancestor".to_string(),
                        table_entries: Box::new(TableResponseBaseTableEntries {
                            entries: vec![],
                            pagination: Box::new(models::TableResponseBaseTableEntriesPagination {
                                start: 0,
                                more_entries_exist: true,
                            }),
                        }),
                        possible_rules_above: vec![], // empty for now...
                        possible_rules_below: vec![], // empty for now...
                        child_information: Some(Box::new(
                            models::TreeForTableResponseChildInformation {
                                rule: models::Rule {
                                    id: 0,
                                    relevant_head_predicate: Box::new(
                                        models::PredicateWithParameters {
                                            name: "ancestor".to_string(),
                                            parameters: vec!["?X".to_string(), "?Y".to_string()],
                                        },
                                    ),
                                    relevant_head_predicate_index: 0,
                                    body_predicates: vec![models::PredicateWithParameters {
                                        name: "parent".to_string(),
                                        parameters: vec!["?X".to_string(), "?Y".to_string()],
                                    }],
                                    string_representation: "ancestor(?X, ?Y) :- parent(?X, ?Y)"
                                        .to_string(),
                                },
                                children: vec![models::TreeForTableResponse {
                                    predicate: "parent".to_string(),
                                    table_entries: Box::new(TableResponseBaseTableEntries {
                                        entries: vec![models::TableEntryResponse {
                                            entry_id: 3,
                                            term_tuple: vec![
                                                "alice".to_string(),
                                                "charlotte".to_string(),
                                            ],
                                        }],
                                        pagination: Box::new(
                                            models::TableResponseBaseTableEntriesPagination {
                                                start: 0,
                                                more_entries_exist: false,
                                            },
                                        ),
                                    }),
                                    possible_rules_above: vec![], // empty for now
                                    possible_rules_below: vec![], // empty for now
                                    child_information: None,
                                }],
                            },
                        )),
                    },
                    models::TreeForTableResponse {
                        predicate: "parent".to_string(),
                        table_entries: Box::new(TableResponseBaseTableEntries {
                            entries: vec![],
                            pagination: Box::new(models::TableResponseBaseTableEntriesPagination {
                                start: 0,
                                more_entries_exist: true,
                            }),
                        }),
                        possible_rules_above: vec![], // empty for now
                        possible_rules_below: vec![], // empty for now
                        child_information: None,
                    },
                ],
            })),
        };

        Ok(JsValue::from_serde(&response).map_err(|err| err.to_string())?)
    }

    #[wasm_bindgen(js_name = "experimentalNewTracingTableEntriesForTreeNodesMock")]
    pub fn experimental_new_tracing_table_entries_for_tree_nodes_mock(
        table_entries_for_tree_nodes_query: JsValue,
    ) -> Result<JsValue, JsValue> {
        // in this mock, we ignore the query and always return the same entries for the same nodes
        let _table_entries_for_tree_nodes_query: models::TableEntriesForTreeNodesQuery =
            table_entries_for_tree_nodes_query
                .into_serde()
                .map_err(|err| err.to_string())?;

        let response: Vec<models::TableEntriesForTreeNodesResponseInner> = vec![
            models::TableEntriesForTreeNodesResponseInner {
                address_in_tree: Some(vec![]), // this should not be an Option, I'm unsure why this
                // is currently generated like that...;
                // anyway, works for now
                predicate: "ancestor".to_string(),
                table_entries: Box::new(TableResponseBaseTableEntries {
                    entries: vec![models::TableEntryResponse {
                        entry_id: 42,
                        term_tuple: vec!["alice".to_string(), "edward".to_string()],
                    }],
                    pagination: Box::new(models::TableResponseBaseTableEntriesPagination {
                        start: 0,
                        more_entries_exist: false,
                    }),
                }),
                possible_rules_above: vec![], // empty for now...
                possible_rules_below: vec![], // empty for now...
            },
            models::TableEntriesForTreeNodesResponseInner {
                address_in_tree: Some(vec![0]),
                predicate: "ancestor".to_string(),
                table_entries: Box::new(TableResponseBaseTableEntries {
                    entries: vec![models::TableEntryResponse {
                        entry_id: 73,
                        term_tuple: vec!["alice".to_string(), "charlotte".to_string()],
                    }],
                    pagination: Box::new(models::TableResponseBaseTableEntriesPagination {
                        start: 0,
                        more_entries_exist: false,
                    }),
                }),
                possible_rules_above: vec![], // empty for now...
                possible_rules_below: vec![], // empty for now...
            },
            models::TableEntriesForTreeNodesResponseInner {
                address_in_tree: Some(vec![1]),
                predicate: "parent".to_string(),
                table_entries: Box::new(TableResponseBaseTableEntries {
                    entries: vec![models::TableEntryResponse {
                        entry_id: 21,
                        term_tuple: vec!["charlotte".to_string(), "edward".to_string()],
                    }],
                    pagination: Box::new(models::TableResponseBaseTableEntriesPagination {
                        start: 0,
                        more_entries_exist: false,
                    }),
                }),
                possible_rules_above: vec![], // empty for now...
                possible_rules_below: vec![], // empty for now...
            },
        ];

        Ok(JsValue::from_serde(&response).map_err(|err| err.to_string())?)
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
