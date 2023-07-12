use std::collections::HashMap;
use std::io::Cursor;

use js_sys::Array;
use js_sys::Reflect;
use js_sys::Set;
use js_sys::Uint8Array;
use nemo::execution::ExecutionEngine;

use nemo::io::parser::parse_program;
use nemo::io::resource_providers::{ResourceProvider, ResourceProviders};
use nemo::model::types::primitive_logical_value::PrimitiveLogicalValueT;
use nemo::model::DataSource;
use nemo::model::DataSourceDeclaration;
use nemo::model::NumericLiteral;
use nemo::model::Term;
use nemo_physical::datatypes::Double;
use nemo_physical::table_reader::Resource;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use web_sys::Blob;
use web_sys::FileReaderSync;

#[wasm_bindgen]
#[derive(Clone)]
pub struct NemoProgram(nemo::model::Program);

#[wasm_bindgen]
pub struct NemoError(nemo::error::Error);

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
        parse_program(input).map(NemoProgram).map_err(NemoError)
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

impl ResourceProvider for BlobResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
    ) -> Result<Option<Box<dyn std::io::Read>>, nemo_physical::error::ReadingError> {
        if let Some(blob) = self.blobs.get(resource) {
            // TODO: Improve error handling
            let array_buffer: js_sys::ArrayBuffer =
                self.file_reader_sync.read_as_array_buffer(blob).unwrap();

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

#[wasm_bindgen]
impl NemoEngine {
    #[wasm_bindgen(constructor)]
    pub fn new(
        program: &NemoProgram,
        resource_blobs_js_value: JsValue,
    ) -> Result<NemoEngine, NemoError> {
        // Parse JavaScript object into `HashMap`
        let mut resource_blobs = HashMap::new();
        // TODO: Improve error handling
        for key in Reflect::own_keys(&resource_blobs_js_value).unwrap() {
            if let Some(resource) = key.as_string() {
                // TODO: Improve error handling
                let value = Reflect::get(&resource_blobs_js_value, &key).unwrap();
                let blob: Blob = JsCast::dyn_into(value).unwrap();

                resource_blobs.insert(resource, blob);
            }
        }

        // TODO: Improve error handling
        let provider = BlobResourceProvider::new(resource_blobs).unwrap();

        ExecutionEngine::initialize(
            program.clone().0,
            ResourceProviders::from(vec![Box::new(provider)]),
        )
        .map(NemoEngine)
        .map_err(NemoError)
    }

    #[wasm_bindgen]
    pub fn reason(&mut self) -> Result<(), NemoError> {
        self.0.execute().map_err(NemoError)
    }

    #[wasm_bindgen(js_name = "getResult")]
    pub fn result(&mut self, predicate: String) -> Result<NemoResults, NemoError> {
        let iter = self.0.table_scan(predicate.into()).map_err(NemoError)?;

        let results = NemoResults(Box::new(
            iter.into_iter().flatten().collect::<Vec<_>>().into_iter(),
        ));

        Ok(results)
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
                        Term::Variable(_) => panic!("Variables should not occur as results!"),
                        Term::Constant(c) => JsValue::from(c.to_string()),
                        Term::NumericLiteral(NumericLiteral::Integer(i)) => JsValue::from(i),
                        Term::NumericLiteral(NumericLiteral::Double(d)) => {
                            JsValue::from(f64::from(d))
                        }
                        // currently we pack decimals into strings, maybe this should change
                        Term::NumericLiteral(_) => JsValue::from(rdf.to_string()),
                        Term::StringLiteral(s) => JsValue::from(s),
                        Term::RdfLiteral(lit) => JsValue::from(lit.to_string()),
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

#[wasm_bindgen]
pub fn set_panic_hook() {
    console_error_panic_hook::set_once();
}
