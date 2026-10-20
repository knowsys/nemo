//! The engine: load, reason, read back.

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use js_sys::{Array, Object, Promise, Reflect, Uint8Array};
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use nemo::{
    diagnostics::render::SourceTexts,
    engine::{PredicateCursor, remote::Failure},
    values::{DataValue, TypeSyntax},
};

use crate::{
    diagnostics, fetch,
    program::NemoProgram,
    values::{to_js, to_value},
};

/// The loading methods, which take a program.
#[derive(Debug, Clone, Copy)]
enum LoadKind {
    /// `load`.
    Load,
    /// `extend`.
    Extend,
    /// `reload`.
    Reload,
}

/// What a [NemoEngine] handle shares with the calls it has started.
#[derive(Debug, Default)]
pub(crate) struct Inner {
    /// The wrapped engine.
    pub(crate) engine: nemo::engine::Engine,
    /// The source texts of this run, for rendering diagnostics.
    sources: SourceTexts,
    /// Whether an async method is waiting for responses from the network.
    pub(crate) driving: bool,
}

impl Inner {
    /// Returns a thrown error carrying one diagnostic.
    pub(crate) fn throw(&self, diagnostic: nemo::diagnostics::Diagnostic) -> JsValue {
        diagnostics::throw(vec![diagnostic], &self.sources)
    }

    /// Carries a program through one of the loading methods, and throws
    /// the errors it reports.
    fn ingest(&mut self, name: &str, text: String, kind: LoadKind) -> Result<(), JsValue> {
        let outcome = self.engine.ingest(name, text, |engine, model| match kind {
            LoadKind::Load => engine.load(model),
            LoadKind::Extend => engine.extend(model),
            LoadKind::Reload => engine.reload(model),
        });
        // Linking reads the program's modules, so their sources are
        // taken in before its outcome is reported against them.
        self.sources.absorb(self.engine.sources());
        outcome.map_err(|diagnostics| diagnostics::throw(diagnostics, &self.sources))
    }
}

/// The reasoning engine over one program:
/// load it, reason to the fixpoint, read the results back.
///
/// The methods are synchronous, so run them in a worker.
/// A method that needs data from the network suspends,
/// and its async form fetches the data and resumes it.
/// A method that may change the engine throws while an async method waits for responses.
/// Errors are thrown as an `Error` whose `diagnostics` carry the structured entries.
#[wasm_bindgen]
#[derive(Debug, Default)]
pub struct NemoEngine {
    /// The engine and its sources, shared with the async methods in
    /// progress.
    inner: Rc<RefCell<Inner>>,
}

impl NemoEngine {
    /// Returns the engine and its sources, for a call that changes nothing an
    /// async method relies on.
    fn state(&self) -> std::cell::Ref<'_, Inner> {
        self.inner.borrow()
    }

    /// Returns the engine and its sources, for a call that may change them.
    ///
    /// The call fails while an async method waits for responses, since it would abandon that
    /// method.
    fn state_mut(&self) -> Result<std::cell::RefMut<'_, Inner>, JsValue> {
        let inner = self.inner.borrow_mut();
        if inner.driving {
            return Err(js_sys::Error::new(
                "the engine is busy: an async method is waiting for responses from the network",
            )
            .into());
        }
        Ok(inner)
    }

    /// Returns the facts `trace` and `traceAscii` take, each written with or
    /// without its terminator.
    fn trace_facts(inner: &Inner, texts: &[String]) -> Result<Vec<nemo::model::Fact>, JsValue> {
        texts
            .iter()
            .map(|text| {
                nemo::model::Fact::parse_loose(text).map_err(|diagnostic| inner.throw(diagnostic))
            })
            .collect()
    }

    /// Starts an async method: the method, then the requests it suspends for
    /// fetched and responded to, settling when it completes.
    fn drive(
        &self,
        call: impl FnOnce(&mut Inner) -> Result<(), JsValue> + 'static,
    ) -> Result<Promise, JsValue> {
        self.state_mut()?.driving = true;
        let inner = Rc::clone(&self.inner);
        Ok(future_to_promise(async move {
            fetch::drive(inner, call).await.map(|()| JsValue::UNDEFINED)
        }))
    }
}

/// Returns the bytes a `Uint8Array` or a string holds,
/// or what a method named `method` throws for anything else.
fn bytes_of(content: &JsValue, method: &str) -> Result<Vec<u8>, JsValue> {
    if let Some(text) = content.as_string() {
        Ok(text.into_bytes())
    } else if let Ok(bytes) = content.clone().dyn_into::<Uint8Array>() {
        Ok(bytes.to_vec())
    } else {
        Err(js_sys::Error::new(&format!("`{method}` takes a `Uint8Array` or a string")).into())
    }
}

/// Returns a protocol value as plain JavaScript data.
fn protocol_value(value: &impl Serialize) -> Result<JsValue, JsValue> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|error| js_sys::Error::new(&error.to_string()).into())
}

/// Returns a protocol query from plain JavaScript data, through its JSON form.
fn protocol_query<T: serde::de::DeserializeOwned>(query: &JsValue) -> Result<T, JsValue> {
    let json = js_sys::JSON::stringify(query)
        .map(String::from)
        .map_err(|_| js_sys::Error::new("the query cannot be converted to JSON"))?;
    serde_json::from_str(&json)
        .map_err(|error| js_sys::Error::new(&format!("invalid query: {error}")).into())
}

#[wasm_bindgen]
impl NemoEngine {
    /// Creates an empty engine.
    #[wasm_bindgen(constructor)]
    pub fn new() -> NemoEngine {
        NemoEngine::default()
    }

    /// Adds in-memory rows to a predicate.
    ///
    /// Before `load`, they join the program as facts.
    /// After it, they land as deltas and the next `reason()` continues to the new fixpoint,
    /// and the predicate must be one the program uses (`extend` introduces new ones).
    /// `rows` is an array of arrays of values, per the module's conversion table.
    /// Throws for a row the predicate does not take.
    #[wasm_bindgen(js_name = "importRows")]
    pub fn import_rows(&mut self, predicate: &str, rows: &JsValue) -> Result<(), JsValue> {
        let rows = rows
            .clone()
            .dyn_into::<Array>()
            .map_err(|_| js_sys::Error::new("`importRows` takes an array of arrays"))?;
        let mut converted: Vec<Vec<DataValue>> = Vec::new();
        for row in rows.iter() {
            let row = row
                .dyn_into::<Array>()
                .map_err(|_| js_sys::Error::new("`importRows` takes an array of arrays"))?;
            converted.push(
                row.iter()
                    .map(|value| to_value(&value))
                    .collect::<Result<_, _>>()?,
            );
        }
        let mut inner = self.state_mut()?;
        inner
            .engine
            .import_rows(predicate, converted)
            .map_err(|diagnostic| inner.throw(diagnostic))
    }

    /// Registers an in-memory resource:
    /// an `@import` whose resource name matches reads these bytes,
    /// instead of a filesystem the browser does not have.
    ///
    /// Takes a `Uint8Array` or a string, and throws for anything else.
    #[wasm_bindgen(js_name = "addResource")]
    pub fn add_resource(&mut self, name: &str, content: &JsValue) -> Result<(), JsValue> {
        let bytes = bytes_of(content, "addResource")?;
        self.state_mut()?.engine.add_resource(name, bytes);
        Ok(())
    }

    /// Registers a module text under the name a `@use` in the program writes for it,
    /// such as `lib::tc`: a `@use` of the module reads this text,
    /// instead of a file the browser does not have.
    ///
    /// Throws if the name is not a module name.
    #[wasm_bindgen(js_name = "addModule")]
    pub fn add_module(&mut self, name: &str, text: String) -> Result<(), JsValue> {
        let mut inner = self.state_mut()?;
        inner
            .engine
            .add_module(name, text)
            .map_err(|diagnostic| inner.throw(diagnostic))
    }

    /// Keeps every imported predicate whole.
    ///
    /// Without it, loading stores only the columns and rows of an import its rules read,
    /// and a trace or a query over the rest throws.
    /// Call it before `load`.
    #[wasm_bindgen(js_name = "retainImports")]
    pub fn retain_imports(&mut self) -> Result<(), JsValue> {
        self.state_mut()?.engine.retain_imports();
        Ok(())
    }

    /// Returns the requests of the suspended method that have no response yet,
    /// empty if no method is suspended.
    ///
    /// The engine never fetches.
    /// A method that needs data from the network
    /// (`load`, `extend` or `reload` with remote imports, `reason` with a bound import)
    /// suspends: it throws code N0491 and lists the requests here.
    /// Fetch each, respond to it with `respond` or `respondFailure`, and call `resume`,
    /// which may suspend again.
    /// The async methods, such as `loadAsync`, do all of this themselves.
    /// Entries are plain objects:
    /// `{id, url, method, headers: [[name, value]…], accept, parameters: [[name, value]…],
    /// form: [[name, value]…] | null}`, the id a `BigInt`.
    #[wasm_bindgen(js_name = "pendingRequests")]
    pub fn pending_requests(&self) -> Result<Array, JsValue> {
        let pairs = |pairs: &[(String, String)]| -> Array {
            pairs
                .iter()
                .map(|(name, value)| {
                    JsValue::from(Array::of2(
                        &JsValue::from_str(name),
                        &JsValue::from_str(value),
                    ))
                })
                .collect()
        };
        self.state()
            .engine
            .pending()
            .iter()
            .map(|pending| {
                let object = Object::new();
                let request = &pending.request;
                let fields: [(&str, JsValue); 7] = [
                    ("id", JsValue::from(pending.id)),
                    ("url", JsValue::from_str(&request.url)),
                    ("method", JsValue::from_str(request.method())),
                    ("headers", pairs(&request.headers).into()),
                    (
                        "accept",
                        request.accept.map_or(JsValue::NULL, JsValue::from_str),
                    ),
                    ("parameters", pairs(&request.parameters).into()),
                    (
                        "form",
                        request
                            .form
                            .as_deref()
                            .map_or(JsValue::NULL, |form| pairs(form).into()),
                    ),
                ];
                for (name, value) in fields {
                    Reflect::set(&object, &JsValue::from_str(name), &value)?;
                }
                Ok(JsValue::from(object))
            })
            .collect()
    }

    /// Responds to a pending request with the response body, a `Uint8Array` or a string,
    /// returning whether a request waited under the id.
    pub fn respond(&mut self, id: u64, body: &JsValue) -> Result<bool, JsValue> {
        let bytes = bytes_of(body, "respond")?;
        Ok(self.state_mut()?.engine.respond(id, Ok(bytes.into())))
    }

    /// Responds to a pending request with why it got no body:
    /// the response status, if one came, and a message.
    /// Returns whether a request waited under the id.
    ///
    /// A failure is final, so retry before responding.
    /// A request too large for the server (413, 414) is sent again in smaller pieces
    /// if the engine can split it, as is one whose response `respond` passed in cut off.
    #[wasm_bindgen(js_name = "respondFailure")]
    pub fn respond_failure(
        &mut self,
        id: u64,
        status: Option<u16>,
        message: String,
    ) -> Result<bool, JsValue> {
        let failure = match status {
            Some(status) => Failure::response(status, message),
            None => Failure::transport(message),
        };
        Ok(self.state_mut()?.engine.respond(id, Err(failure)))
    }

    /// Resumes the suspended method with the responses given,
    /// and does nothing if no method is suspended.
    ///
    /// Throws as the method it resumes does,
    /// with code N0491 again if requests remain or new ones arise.
    pub fn resume(&mut self) -> Result<(), JsValue> {
        let mut inner = self.state_mut()?;
        inner
            .engine
            .resume()
            .map_err(|diagnostic| inner.throw(diagnostic))
    }

    /// Loads a program: facts become stored rows, imports read their resources, rules compile.
    ///
    /// Throws for a program that does not load,
    /// with code N0491 if it suspends for data from the network (see `pendingRequests`).
    pub fn load(&mut self, program: &NemoProgram) -> Result<(), JsValue> {
        let text = program.text.clone();
        self.state_mut()?
            .ingest(&program.name, text, LoadKind::Load)
    }

    /// Loads a program as `load` does, its remote resources fetched with the global `fetch`.
    ///
    /// The promise resolves once the program is loaded, and rejects with what `load` throws
    /// otherwise.
    /// A failed request is sent again if the server asks to wait (429, 503)
    /// or is overloaded (502, 504), up to five times.
    /// The engine is free for reads while requests are out.
    #[wasm_bindgen(js_name = "loadAsync")]
    pub fn load_async(&self, program: &NemoProgram) -> Result<Promise, JsValue> {
        let (name, text) = (program.name.clone(), program.text.clone());
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Load))
    }

    /// Extends the loaded program:
    /// the new statements join it, new facts and imports store, the program recompiles,
    /// and the next `reason()` continues to the new fixpoint.
    ///
    /// An extension cannot change what loaded statements mean:
    /// `@base`, prefix rebinding, and parameter redefinition throw,
    /// since they need a `reload`.
    pub fn extend(&mut self, program: &NemoProgram) -> Result<(), JsValue> {
        let text = program.text.clone();
        self.state_mut()?
            .ingest(&program.name, text, LoadKind::Extend)
    }

    /// Extends the loaded program as `extend` does,
    /// its remote resources fetched as `loadAsync` fetches them.
    #[wasm_bindgen(js_name = "extendAsync")]
    pub fn extend_async(&self, program: &NemoProgram) -> Result<Promise, JsValue> {
        let (name, text) = (program.name.clone(), program.text.clone());
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Extend))
    }

    /// Replaces the program:
    /// every derived row drops, every rule starts over,
    /// and the next `reason()` computes again from the loaded data.
    ///
    /// Unchanged predicates keep their data, and unchanged imports are not read again.
    /// Rows imported into a predicate whose definition changed are dropped, with a report.
    pub fn reload(&mut self, program: &NemoProgram) -> Result<(), JsValue> {
        let text = program.text.clone();
        self.state_mut()?
            .ingest(&program.name, text, LoadKind::Reload)
    }

    /// Replaces the program as `reload` does,
    /// its remote resources fetched as `loadAsync` fetches them.
    #[wasm_bindgen(js_name = "reloadAsync")]
    pub fn reload_async(&self, program: &NemoProgram) -> Result<Promise, JsValue> {
        let (name, text) = (program.name.clone(), program.text.clone());
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Reload))
    }

    /// Checks a program without loading it:
    /// no data is read, nothing is stored,
    /// and every problem is returned (as plain diagnostic objects) rather than thrown.
    pub fn check(&mut self, program: &NemoProgram) -> Result<JsValue, JsValue> {
        let mut inner = self.state_mut()?;
        let Inner {
            engine, sources, ..
        } = &mut *inner;
        let collected = engine
            .check_source(program.name.as_str(), program.text.clone())
            .into_diagnostics();
        sources.absorb(engine.sources());
        let resolved: Vec<diagnostics::Diagnostic> = collected
            .iter()
            .map(|diagnostic| diagnostics::resolve(diagnostic, sources))
            .collect();
        Ok(diagnostics::to_js(&resolved))
    }

    /// Runs semi-naive evaluation to the fixpoint.
    ///
    /// The call is synchronous and returns when the fixpoint is reached, so run it in a worker.
    /// If a bound import needs data from the network, it suspends with code N0491
    /// (see `pendingRequests`).
    pub fn reason(&mut self) -> Result<(), JsValue> {
        let mut inner = self.state_mut()?;
        inner
            .engine
            .reason()
            .map_err(|diagnostic| inner.throw(diagnostic))
    }

    /// Runs semi-naive evaluation as `reason` does,
    /// the requests of its bound imports fetched as `loadAsync` fetches them.
    ///
    /// The promise resolves at the fixpoint.
    #[wasm_bindgen(js_name = "reasonAsync")]
    pub fn reason_async(&self) -> Result<Promise, JsValue> {
        self.drive(|inner| {
            inner
                .engine
                .reason()
                .map_err(|diagnostic| inner.throw(diagnostic))
        })
    }

    /// Traces facts, each written in fact syntax as `reach(a, b)`,
    /// returning the protocol response:
    /// per fact, whether it holds and, if it does, its proof tree.
    ///
    /// Throws for an engine holding updates `reason()` has not processed.
    pub fn trace(&mut self, facts: Vec<String>) -> Result<JsValue, JsValue> {
        let mut inner = self.inner.borrow_mut();
        let facts = Self::trace_facts(&inner, &facts)?;
        let response = inner
            .engine
            .trace(&facts)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        protocol_value(&response)
    }

    /// Returns the [trace](Self::trace) rendered as ASCII proof trees,
    /// the terminal-style convenience.
    #[wasm_bindgen(js_name = "traceAscii")]
    pub fn trace_ascii(&mut self, facts: Vec<String>) -> Result<String, JsValue> {
        let mut inner = self.inner.borrow_mut();
        let facts = Self::trace_facts(&inner, &facts)?;
        let response = inner
            .engine
            .trace(&facts)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        Ok(response.ascii())
    }

    /// Answers the explanation view's `treeForTable` query:
    /// the facts it selects, their derivation expanded.
    ///
    /// Takes and returns the protocol's plain camelCase objects.
    #[wasm_bindgen(js_name = "treeForTable")]
    pub fn tree_for_table(&mut self, query: JsValue) -> Result<JsValue, JsValue> {
        let query = protocol_query(&query)?;
        let mut inner = self.inner.borrow_mut();
        let response = inner
            .engine
            .tree_for_table(query)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        protocol_value(&response)
    }

    /// Answers the explanation view's `tableEntriesForTreeNodes` query:
    /// the facts of every node of the tree it shapes, parents before children.
    ///
    /// Takes and returns the protocol's plain camelCase objects.
    #[wasm_bindgen(js_name = "tableEntriesForTreeNodes")]
    pub fn table_entries_for_tree_nodes(&mut self, query: JsValue) -> Result<JsValue, JsValue> {
        let query = protocol_query(&query)?;
        let mut inner = self.inner.borrow_mut();
        let response = inner
            .engine
            .table_entries_for_tree_nodes(query)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        protocol_value(&response)
    }

    /// Returns the rule a statement id names, as traces and queries refer to it,
    /// or `undefined` for an id that names none.
    ///
    /// The rule is `{rule, text, name?, source?: {file, start, end, line, column}, instances?}`.
    pub fn rule(&self, id: usize) -> Result<JsValue, JsValue> {
        match self.state().engine.rule(id) {
            Some(rule) => protocol_value(&rule),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Returns the stored rows of a predicate, as an array of arrays of converted values.
    ///
    /// For large results, page with `cursor`.
    pub fn rows(&mut self, predicate: &str) -> Result<Array, JsValue> {
        let mut inner = self.inner.borrow_mut();
        let rows = inner
            .engine
            .rows(predicate)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        Ok(rows
            .into_iter()
            .map(|row| {
                JsValue::from(
                    row.iter()
                        .map(|value| to_js(value.as_ref()))
                        .collect::<Array>(),
                )
            })
            .collect())
    }

    /// Opens a cursor over a predicate's rows (see [NemoCursor]).
    pub fn cursor(&self, predicate: &str) -> Result<NemoCursor, JsValue> {
        let inner = self.state();
        let cursor = inner
            .engine
            .cursor(predicate)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        Ok(NemoCursor {
            cursor,
            buffered: VecDeque::new(),
        })
    }

    /// Returns the next rows of a cursor, at most `budget` of them
    /// (a bounded default if omitted), or an empty array once exhausted.
    ///
    /// A row is a plain object `{entryId, values, terms}`:
    /// its entry id, its values per the conversion table, and the same values in term syntax.
    #[wasm_bindgen(js_name = "readChunk")]
    pub fn read_chunk(
        &mut self,
        cursor: &mut NemoCursor,
        budget: Option<usize>,
    ) -> Result<Array, JsValue> {
        let budget = budget.unwrap_or(PredicateCursor::BUDGET);
        if cursor.buffered.is_empty() && !cursor.cursor.is_done() {
            let mut inner = self.inner.borrow_mut();
            let chunk = inner
                .engine
                .read_chunk(&mut cursor.cursor, budget)
                .map_err(|diagnostic| inner.throw(diagnostic))?;
            cursor.buffered.extend(chunk.rows());
        }
        let rows = Array::new();
        let budget = u32::try_from(budget).unwrap_or(u32::MAX);
        while rows.length() < budget {
            let entry = cursor.position();
            let Some(row) = cursor.buffered.pop_front() else {
                break;
            };
            rows.push(&row_object(entry, &row)?);
        }
        Ok(rows)
    }

    /// Returns the number of stored rows of a predicate.
    pub fn count(&self, predicate: &str) -> Result<usize, JsValue> {
        let inner = self.state();
        inner
            .engine
            .count(predicate)
            .map_err(|diagnostic| inner.throw(diagnostic))
    }

    /// Returns the program's outputs:
    /// what it declares `@output` or exports, in declaration order,
    /// or every predicate rules derive if it declares neither.
    #[wasm_bindgen(js_name = "outputPredicates")]
    pub fn output_predicates(&self) -> Array {
        self.state()
            .engine
            .outputs()
            .iter()
            .map(|name| JsValue::from_str(name))
            .collect()
    }

    /// Returns every predicate of the program, sorted by name,
    /// as `[name, columns, role]` triples,
    /// the role `"extensional"` (given from outside), `"derived"` (by rules), or `"both"`.
    pub fn predicates(&self) -> Array {
        self.state()
            .engine
            .predicates()
            .into_iter()
            .map(|info| {
                JsValue::from(Array::of3(
                    &JsValue::from_str(&info.name),
                    &JsValue::from_f64(info.arity as f64),
                    &JsValue::from_str(info.role.name()),
                ))
            })
            .collect()
    }

    /// Returns the inferred column types of a predicate, in declaration syntax.
    #[wasm_bindgen(js_name = "columnTypes")]
    pub fn column_types(&self, predicate: &str) -> Option<Vec<String>> {
        self.state().engine.column_types(predicate).map(|types| {
            types
                .iter()
                .map(|datatype| TypeSyntax::of(datatype).to_string())
                .collect()
        })
    }

    /// Serializes the program's declared exports:
    /// an object mapping each export's file name to its content as a `Uint8Array`
    /// (compressed if the export says so).
    #[wasm_bindgen(js_name = "exportTables")]
    pub fn export_tables(&mut self) -> Result<Object, JsValue> {
        let mut inner = self.inner.borrow_mut();
        let specs = inner.engine.exports().to_vec();
        let tables = Object::new();
        for spec in specs {
            let bytes = inner
                .engine
                .export_bytes(&spec)
                .map_err(|diagnostic| inner.throw(diagnostic))?;
            let content = Uint8Array::from(bytes.as_slice());
            Reflect::set(&tables, &JsValue::from_str(&spec.file_name()), &content)?;
        }
        Ok(tables)
    }

    /// Serializes one predicate, any the engine holds, exported or not,
    /// as comma-separated values in the `term` column format,
    /// as bytes the page can save to a file (a download, a picked file handle).
    ///
    /// Throws for a predicate the program never used.
    #[wasm_bindgen(js_name = "exportPredicate")]
    pub fn export_predicate(&mut self, predicate: &str) -> Result<Uint8Array, JsValue> {
        let mut inner = self.inner.borrow_mut();
        // An unknown predicate throws as it does for every read.
        inner
            .engine
            .count(predicate)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        let spec = nemo::engine::ExportSpec::csv(predicate);
        let bytes = inner
            .engine
            .export_bytes(&spec)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        Ok(Uint8Array::from(bytes.as_slice()))
    }

    /// Returns the warnings (and other reports) the run collected so far, as
    /// plain diagnostic objects.
    pub fn reports(&self) -> JsValue {
        let inner = self.state();
        let resolved: Vec<diagnostics::Diagnostic> = inner
            .engine
            .reports()
            .iter()
            .map(|diagnostic| diagnostics::resolve(diagnostic, &inner.sources))
            .collect();
        diagnostics::to_js(&resolved)
    }

    /// Facts the program's predicates hold from outside: its facts and
    /// imports, and the rows added through `importRows`.
    #[wasm_bindgen(getter, js_name = "loadedFacts")]
    pub fn loaded_facts(&self) -> usize {
        self.state().engine.loaded_facts()
    }

    /// Facts the program's predicates hold from reasoning.
    #[wasm_bindgen(getter, js_name = "derivedFacts")]
    pub fn derived_facts(&self) -> usize {
        self.state().engine.derived_facts()
    }

    /// Returns the number of rule applications performed so far.
    #[wasm_bindgen(getter)]
    pub fn applications(&self) -> u64 {
        self.state().engine.applications()
    }

    /// Bytes currently allocated across tables and the dictionary.
    #[wasm_bindgen(getter, js_name = "memoryBytes")]
    pub fn memory_bytes(&self) -> usize {
        self.state().engine.memory_bytes()
    }
}

/// Returns a cursor row as a plain object: its entry id, its values per the
/// conversion table, and its values in term syntax.
fn row_object(entry: usize, row: &[DataValue]) -> Result<JsValue, JsValue> {
    let object = Object::new();
    #[allow(clippy::cast_precision_loss)]
    let entry = JsValue::from_f64(entry as f64);
    let values: Array = row.iter().map(|value| to_js(value.as_ref())).collect();
    let terms: Array = row
        .iter()
        .map(|value| JsValue::from_str(&value.to_string()))
        .collect();
    Reflect::set(&object, &JsValue::from_str("entryId"), &entry)?;
    Reflect::set(&object, &JsValue::from_str("values"), &values)?;
    Reflect::set(&object, &JsValue::from_str("terms"), &terms)?;
    Ok(object.into())
}

/// A cursor over one predicate's rows, advanced through [read_chunk](NemoEngine::read_chunk).
///
/// It is plain data, holding no reference to the engine.
#[wasm_bindgen]
#[derive(Debug)]
pub struct NemoCursor {
    /// The engine-side cursor.
    cursor: PredicateCursor,
    /// Rows fetched but not yet delivered.
    buffered: VecDeque<Vec<DataValue>>,
}

#[wasm_bindgen]
impl NemoCursor {
    /// Returns the rows not yet delivered.
    #[wasm_bindgen(getter)]
    pub fn remaining(&self) -> usize {
        self.cursor.remaining() + self.buffered.len()
    }

    /// Returns the position of the next row to deliver,
    /// the entry id a query selects that row by.
    #[wasm_bindgen(getter)]
    pub fn position(&self) -> usize {
        self.cursor.position() - self.buffered.len()
    }

    /// Returns whether every row has been delivered.
    #[wasm_bindgen(getter)]
    pub fn done(&self) -> bool {
        self.cursor.is_done() && self.buffered.is_empty()
    }
}
