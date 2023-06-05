use js_sys::Array;
use js_sys::Set;
use nemo::execution::ExecutionEngine;
use nemo::io::parser::parse_program;
use nemo_physical::datatypes::DataValueT;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsValue;

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

#[wasm_bindgen]
pub struct NemoEngine(nemo::execution::DefaultExecutionEngine);

#[wasm_bindgen]
impl NemoEngine {
    #[wasm_bindgen(constructor)]
    pub fn new(program: &NemoProgram) -> Result<NemoEngine, NemoError> {
        ExecutionEngine::initialize(program.clone().0)
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
pub struct NemoResults(Box<dyn Iterator<Item = Vec<DataValueT>> + Send>);

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
                    DataValueT::String(s) => JsValue::from(s),
                    DataValueT::U32(n) => JsValue::from(n),
                    DataValueT::U64(n) => JsValue::from(n),
                    DataValueT::I64(n) => JsValue::from(n),
                    DataValueT::Float(n) => {
                        JsValue::from(<nemo::datatypes::Float as Into<f32>>::into(n))
                    }
                    DataValueT::Double(n) => {
                        JsValue::from(<nemo::datatypes::Double as Into<f64>>::into(n))
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
