//! The engine: load, reason, read back.

use std::{
    cell::RefCell,
    collections::{HashSet, VecDeque},
    rc::Rc,
};

use js_sys::{Array, Object, Promise, Reflect, Uint8Array};
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use nemo::{
    diagnostics::render::SourceTexts,
    engine::{
        PredicateCursor,
        remote::{Failure, PendingRequest, Progress, RequestId, Response},
    },
    trace::TraceResponse,
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
    ///
    /// Stepwise, the method sends no request and returns the requests it is suspended for.
    /// Otherwise it completes, and throws N0491 if it needs data from the network,
    /// since the browser engine has no client of its own.
    fn ingest(
        &mut self,
        name: &str,
        text: String,
        kind: LoadKind,
        stepwise: bool,
    ) -> Result<Progress, JsValue> {
        let mut progress = Progress::Done;
        let outcome = self.engine.ingest(name, text, |engine, model| {
            if stepwise {
                progress = match kind {
                    LoadKind::Load => engine.start_load(model),
                    LoadKind::Extend => engine.start_extend(model),
                    LoadKind::Reload => engine.start_reload(model),
                }?;
                return Ok(());
            }
            match kind {
                LoadKind::Load => engine.load(model),
                LoadKind::Extend => engine.extend(model),
                LoadKind::Reload => engine.reload(model),
            }
            .map_err(with_async_hint)
        });
        // Linking reads the program's modules, so their sources are
        // taken in before its outcome is reported against them.
        self.sources.absorb(self.engine.sources());
        outcome
            .map(|()| progress)
            .map_err(|diagnostics| diagnostics::throw(diagnostics, &self.sources))
    }
}

/// Adds to the error of a synchronous method that needed data from the network
/// the hint to call its async form, which fetches it.
fn with_async_hint(problem: nemo::diagnostics::Diagnostic) -> nemo::diagnostics::Diagnostic {
    if problem.code() == nemo::codes::AWAITING_REMOTE {
        problem.with_hint("call the method's async form, such as `loadAsync`, which fetches it")
    } else {
        problem
    }
}

/// The reasoning engine over one program:
/// load it, reason to the fixpoint, read the results back.
///
/// The methods are synchronous, so run them in a worker.
/// A method that needs data from the network has an async form, which fetches the data,
/// and a `start*` form, which returns the requests, whose responses `resume` takes.
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
    /// The call fails while an async method waits for responses, which only it may pass on.
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

    /// Starts an async method: a step of the method, then the requests it is suspended for
    /// fetched and their responses passed on, settling when it completes.
    fn drive(
        &self,
        call: impl FnOnce(&mut Inner) -> Result<Progress, JsValue> + 'static,
    ) -> Result<Promise, JsValue> {
        self.state_mut()?.driving = true;
        let inner = Rc::clone(&self.inner);
        Ok(future_to_promise(async move {
            fetch::drive(inner, call).await.map(|()| JsValue::UNDEFINED)
        }))
    }
}

/// Returns what a step of a method returns to JavaScript:
/// `null` if the method is done, else the requests it is suspended for, as `pendingRequests`
/// lists them.
fn progress_js(progress: Progress) -> Result<JsValue, JsValue> {
    match progress {
        Progress::Done => Ok(JsValue::NULL),
        Progress::Suspended(pending) => requests_js(&pending).map(JsValue::from),
    }
}

/// Returns requests as plain objects:
/// `{id, url, method, headers: [[name, value]…], accept, parameters: [[name, value]…],
/// form: [[name, value]…] | null}`, the id a `BigInt`.
fn requests_js(pending: &[PendingRequest]) -> Result<Array, JsValue> {
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
    pending
        .iter()
        .map(|pending| {
            let object = Object::new();
            let request = &pending.request;
            let fields: [(&str, JsValue); 7] = [
                ("id", JsValue::from(pending.id.value())),
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

/// Returns the responses `resume` takes, from plain objects:
/// `{id, body}` for a response, its body a `Uint8Array` or a string,
/// and `{id, status, message}` for a failure, `status` left out if no response came.
/// A failure with `partial: true` is a response holding only part of the result
/// ([partial_response](nemo::engine::remote::partial_response) tells one),
/// which a bound import splits into smaller requests.
fn responses_of(responses: &JsValue) -> Result<Vec<(RequestId, Response)>, JsValue> {
    let invalid = |what: &str| -> JsValue {
        js_sys::Error::new(&format!(
            "`resume` takes an array of `{{id, body}}` or `{{id, status, message}}`: {what}"
        ))
        .into()
    };
    let responses: Array = responses
        .clone()
        .dyn_into()
        .map_err(|_| invalid("not an array"))?;
    responses
        .iter()
        .map(|entry| {
            let id = Reflect::get(&entry, &"id".into())?;
            let id = if id.is_bigint() {
                u64::try_from(id).map_err(|_| invalid("an id out of range"))?
            } else {
                match id.as_f64() {
                    // The ids the engine hands out are far below 2^53, exact as numbers.
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    Some(number) if number >= 0.0 && number.fract() == 0.0 => number as u64,
                    _ => return Err(invalid("a response without an id")),
                }
            };
            let body = Reflect::get(&entry, &"body".into())?;
            let response = if body.is_undefined() {
                let message = Reflect::get(&entry, &"message".into())?
                    .as_string()
                    .ok_or_else(|| invalid("a response without a body or a message"))?;
                let status = Reflect::get(&entry, &"status".into())?;
                Err(if status.is_undefined() || status.is_null() {
                    Failure::transport(message)
                } else {
                    match status.as_f64() {
                        // Checked to be an HTTP status, so it fits.
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        Some(number)
                            if (100.0..=599.0).contains(&number) && number.fract() == 0.0 =>
                        {
                            let partial = Reflect::get(&entry, &"partial".into())?.is_truthy();
                            if partial {
                                Failure::partial(number as u16, message)
                            } else {
                                Failure::response(number as u16, message)
                            }
                        }
                        _ => return Err(invalid("a status that is not an HTTP status number")),
                    }
                })
            } else {
                Ok(bytes_of(&body, "resume")?.into())
            };
            Ok((RequestId::from(id), response))
        })
        .collect()
}

/// Checks that each response is under an id the pending requests list, and under one no
/// other response has, so that `resume` throws where the engine would panic.
fn check_ids(
    responses: &[(RequestId, Response)],
    pending: &[PendingRequest],
) -> Result<(), JsValue> {
    let mut given = HashSet::new();
    for (id, _) in responses {
        let problem = if !given.insert(*id) {
            format!("two responses under the id {id}")
        } else if !pending.iter().any(|request| request.id == *id) {
            format!("a response under the id {id}, which `pendingRequests` does not list")
        } else {
            continue;
        };
        return Err(js_sys::Error::new(&format!("`resume` was given {problem}")).into());
    }
    Ok(())
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

/// Replaces the ids in a trace response's JavaScript value by what they name:
/// a node's rule and premises, and a traced fact's node.
fn linked(response: &TraceResponse, value: &JsValue) -> Result<(), JsValue> {
    let nodes: Array = Reflect::get(value, &"nodes".into())?.into();
    let rules: Array = Reflect::get(value, &"rules".into())?.into();
    let at = |list: &Array, id: usize| {
        list.get(u32::try_from(id).expect("a response holds fewer than 2^32 nodes and rules"))
    };
    for (node, linked) in response.nodes.iter().zip(nodes.iter()) {
        let Some(derivation) = &node.derivation else {
            continue;
        };
        Reflect::set(&linked, &"rule".into(), &at(&rules, derivation.rule))?;
        let premises: Array = derivation
            .premises
            .iter()
            .map(|&premise| at(&nodes, premise))
            .collect();
        Reflect::set(&linked, &"premises".into(), &premises)?;
    }
    let facts: Array = Reflect::get(value, &"facts".into())?.into();
    for (traced, linked) in response.facts.iter().zip(facts.iter()) {
        if let Some(node) = traced.node {
            Reflect::set(&linked, &"node".into(), &at(&nodes, node))?;
        }
    }
    Ok(())
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
    /// The async methods, such as `loadAsync`, fetch the data a method needs from the
    /// network themselves. To fetch it another way, through a proxy or with credentials,
    /// call the method's `start*` form, such as `startLoad`, which returns the requests it
    /// is suspended for (or `null` once done), fetch each, and pass the responses to `resume`,
    /// which returns the requests the method is suspended for next.
    /// Entries are plain objects:
    /// `{id, url, method, headers: [[name, value]…], accept, parameters: [[name, value]…],
    /// form: [[name, value]…] | null}`, the id a `BigInt`.
    #[wasm_bindgen(js_name = "pendingRequests")]
    pub fn pending_requests(&self) -> Result<Array, JsValue> {
        requests_js(&self.state().engine.pending())
    }

    /// Continues the suspended method with responses to its requests:
    /// an array of `{id, body}`, the body a `Uint8Array` or a string,
    /// or `{id, status, message}` for a request that got no body,
    /// `status` left out if no response came.
    ///
    /// Returns `null` once the method is done, or the requests it is suspended for, as
    /// `startLoad` does: those without a response yet, or new ones.
    /// A failure is final, so retry before passing it in.
    /// A request too large for the server (413, 414) is sent again in smaller pieces
    /// if the engine can split it, as is one whose response the server cut off.
    /// Throws for a response under an id that `pendingRequests` does not list,
    /// or for two responses under one id, taking no response,
    /// and as the method it continues does otherwise.
    pub fn resume(&mut self, responses: &JsValue) -> Result<JsValue, JsValue> {
        let responses = responses_of(responses)?;
        let mut inner = self.state_mut()?;
        check_ids(&responses, &inner.engine.pending())?;
        let progress = inner
            .engine
            .resume(responses)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        progress_js(progress)
    }

    /// Discards the suspended method, returning whether there was one.
    ///
    /// A suspended load, extension or reload has changed nothing,
    /// and a suspended `reason` stops as after a cancellation.
    /// Throws while an async method waits for responses.
    #[wasm_bindgen(js_name = "discardPending")]
    pub fn discard_pending(&mut self) -> Result<bool, JsValue> {
        Ok(self.state_mut()?.engine.discard_pending())
    }

    /// Loads a program: facts become stored rows, imports read their resources, rules compile.
    ///
    /// Throws for a program that does not load,
    /// with code N0491 if it needs data from the network: `loadAsync` fetches it,
    /// and `startLoad` returns the requests that fetch it.
    pub fn load(&mut self, program: &NemoProgram) -> Result<(), JsValue> {
        let text = program.text.clone();
        self.state_mut()?
            .ingest(&program.name, text, LoadKind::Load, false)
            .map(drop)
    }

    /// Loads a program as `load` does, but sends no request:
    /// returns `null` once loaded, or the requests the load is suspended for, as
    /// `pendingRequests` lists them, having stored nothing. `resume` takes the responses.
    /// Throws code N0493 while another method is suspended.
    #[wasm_bindgen(js_name = "startLoad")]
    pub fn start_load(&mut self, program: &NemoProgram) -> Result<JsValue, JsValue> {
        let text = program.text.clone();
        let progress = self
            .state_mut()?
            .ingest(&program.name, text, LoadKind::Load, true)?;
        progress_js(progress)
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
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Load, true))
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
            .ingest(&program.name, text, LoadKind::Extend, false)
            .map(drop)
    }

    /// Extends the loaded program as `extend` does, but sends no request,
    /// returning the requests it is suspended for as `startLoad` does, the loaded program
    /// unchanged until it completes.
    /// Throws code N0493 while another method is suspended.
    #[wasm_bindgen(js_name = "startExtend")]
    pub fn start_extend(&mut self, program: &NemoProgram) -> Result<JsValue, JsValue> {
        let text = program.text.clone();
        let progress = self
            .state_mut()?
            .ingest(&program.name, text, LoadKind::Extend, true)?;
        progress_js(progress)
    }

    /// Extends the loaded program as `extend` does,
    /// its remote resources fetched as `loadAsync` fetches them.
    #[wasm_bindgen(js_name = "extendAsync")]
    pub fn extend_async(&self, program: &NemoProgram) -> Result<Promise, JsValue> {
        let (name, text) = (program.name.clone(), program.text.clone());
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Extend, true))
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
            .ingest(&program.name, text, LoadKind::Reload, false)
            .map(drop)
    }

    /// Replaces the program as `reload` does, but sends no request,
    /// returning the requests it is suspended for as `startLoad` does, the loaded program
    /// unchanged until it completes.
    /// Throws code N0493 while another method is suspended.
    #[wasm_bindgen(js_name = "startReload")]
    pub fn start_reload(&mut self, program: &NemoProgram) -> Result<JsValue, JsValue> {
        let text = program.text.clone();
        let progress = self
            .state_mut()?
            .ingest(&program.name, text, LoadKind::Reload, true)?;
        progress_js(progress)
    }

    /// Replaces the program as `reload` does,
    /// its remote resources fetched as `loadAsync` fetches them.
    #[wasm_bindgen(js_name = "reloadAsync")]
    pub fn reload_async(&self, program: &NemoProgram) -> Result<Promise, JsValue> {
        let (name, text) = (program.name.clone(), program.text.clone());
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Reload, true))
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
    /// If a bound import needs data from the network, it throws code N0491:
    /// `reasonAsync` fetches the data, and `startReason` returns the requests that fetch it.
    pub fn reason(&mut self) -> Result<(), JsValue> {
        let mut inner = self.state_mut()?;
        inner
            .engine
            .reason()
            .map_err(|diagnostic| inner.throw(with_async_hint(diagnostic)))
    }

    /// Runs semi-naive evaluation as `reason` does, but sends no request:
    /// returns `null` at the fixpoint, or the requests of the bound import whose fetch the run
    /// is suspended in, as `startLoad` does. `resume` takes the responses and continues the run
    /// there.
    /// Throws code N0493 while a load, extension or reload is suspended.
    #[wasm_bindgen(js_name = "startReason")]
    pub fn start_reason(&mut self) -> Result<JsValue, JsValue> {
        let mut inner = self.state_mut()?;
        let progress = inner
            .engine
            .start_reason()
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        progress_js(progress)
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
                .start_reason()
                .map_err(|diagnostic| inner.throw(diagnostic))
        })
    }

    /// Traces facts, each written in fact syntax as `reach(a, b)`,
    /// returning the protocol response:
    /// per fact, its node if it holds, and the nodes of the derivations.
    /// A node's `rule` is the rule's object and its `premises` are the premises' nodes,
    /// so a derivation walks like a tree, and a fact several derivations use is one object.
    ///
    /// Throws for an engine holding updates `reason()` has not processed.
    pub fn trace(&mut self, facts: Vec<String>) -> Result<JsValue, JsValue> {
        let mut inner = self.inner.borrow_mut();
        let facts = Self::trace_facts(&inner, &facts)?;
        let response = inner
            .engine
            .trace(&facts)
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        let value = protocol_value(&response)?;
        linked(&response, &value)?;
        Ok(value)
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
