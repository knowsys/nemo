//! The `NemoEngine` class, which loads programs, reasons, and reads the results.

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

/// A loading method of the engine.
#[derive(Debug, Clone, Copy)]
enum LoadKind {
    /// [Engine::load](nemo::engine::Engine::load).
    Load,
    /// [Engine::extend](nemo::engine::Engine::extend).
    Extend,
    /// [Engine::reload](nemo::engine::Engine::reload).
    Reload,
}

/// The state that a [NemoEngine] shares with its async methods in progress.
#[derive(Debug, Default)]
pub(crate) struct Inner {
    /// The wrapped engine.
    pub(crate) engine: nemo::engine::Engine,
    /// The texts of the engine's sources, for rendering diagnostics.
    sources: SourceTexts,
    /// Whether an async method is waiting for responses from the network.
    pub(crate) driving: bool,
}

impl Inner {
    /// Returns the `Error` to throw for one diagnostic.
    pub(crate) fn throw(&self, diagnostic: nemo::diagnostics::Diagnostic) -> JsValue {
        diagnostics::throw(vec![diagnostic], &self.sources)
    }

    /// Passes a program to the loading method `kind`,
    /// and returns the error to throw for the diagnostics of a failure.
    ///
    /// With `start_form`, the method's `start_*` form runs,
    /// which sends nothing and returns the requests that it is suspended for.
    /// Otherwise the plain form runs, which fails with [NO_CLIENT](nemo::codes::NO_CLIENT)
    /// if the program needs data from the network, since the engine in the browser has no
    /// client.
    fn ingest(
        &mut self,
        name: &str,
        text: String,
        kind: LoadKind,
        start_form: bool,
    ) -> Result<Progress, JsValue> {
        let mut progress = Progress::Done;
        let outcome = self.engine.ingest(name, text, |engine, model| {
            if start_form {
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
        });
        // Linking reads the program's modules, so their sources are added before the
        // outcome is rendered against them.
        self.sources.absorb(self.engine.sources());
        outcome
            .map(|()| progress)
            .map_err(|diagnostics| diagnostics::throw(diagnostics, &self.sources))
    }
}

/// The reasoning engine for one program,
/// which loads the program, reasons to the fixpoint, and reads the results.
///
/// The methods are synchronous, so the engine should run in a worker.
/// A method that needs data from the network has an async form, such as `loadAsync`,
/// which fetches the data, and a `start` form, such as `startLoad`,
/// which returns the requests instead, so that `resume` can receive their responses.
/// A method that may change the engine throws while an async method waits for responses.
/// An error is thrown as an `Error` whose `diagnostics` property lists the diagnostics as
/// plain objects.
#[wasm_bindgen]
#[derive(Debug, Default)]
pub struct NemoEngine {
    /// The engine and its sources, shared with the async methods in progress.
    inner: Rc<RefCell<Inner>>,
}

impl NemoEngine {
    /// Borrows the engine's state, for a method that changes nothing an async method relies
    /// on.
    fn state(&self) -> std::cell::Ref<'_, Inner> {
        self.inner.borrow()
    }

    /// Borrows the engine's state mutably, for a method that may change it.
    ///
    /// Fails while an async method waits for responses, since only that method may pass
    /// them on.
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

    /// Parses the facts given to `trace` or `traceAscii`,
    /// each written with or without its terminating `.`.
    fn trace_facts(inner: &Inner, texts: &[String]) -> Result<Vec<nemo::model::Fact>, JsValue> {
        texts
            .iter()
            .map(|text| {
                nemo::model::Fact::parse_loose(text).map_err(|diagnostic| inner.throw(diagnostic))
            })
            .collect()
    }

    /// Starts the async method that `call` starts, and returns its promise,
    /// which [fetch::drive] settles.
    ///
    /// Fails while another async method waits for responses.
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

/// Returns the progress of a method as JavaScript data:
/// `null` if the method is done, and otherwise the requests it is suspended for,
/// in the form of [requests_js].
fn progress_js(progress: Progress) -> Result<JsValue, JsValue> {
    match progress {
        Progress::Done => Ok(JsValue::NULL),
        Progress::Suspended(pending) => requests_js(&pending).map(JsValue::from),
    }
}

/// Returns requests as plain objects.
///
/// Each is `{id, url, method, headers: [[name, value]…], accept, parameters: [[name, value]…],
/// form: [[name, value]…] | null}`, where `id` is a `BigInt` and `accept` may be `null`.
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

/// Returns the responses given to `resume`, read from plain objects.
///
/// - `{id, body}` is a response, with a body that is a `Uint8Array` or a string.
/// - `{id, status, message}` is a [Failure], without `status` if no response came.
/// - A failure with `partial: true` is a response that contains only part of the result,
///   as [Failure::of_partial_response](nemo::engine::remote::Failure::of_partial_response)
///   detects.
///
/// An id is a `BigInt` or an integral, non-negative number.
/// Returns the error to throw for anything else.
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
                    // The engine's ids are far below 2^53, so a number holds them exactly.
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
                        // The guard checks that it is an HTTP status, so it fits in a `u16`.
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

/// Checks that each response has an id that `pending` lists, and that no two responses share
/// an id.
///
/// So `resume` throws where [Engine::resume](nemo::engine::Engine::resume) would panic.
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

/// Returns the bytes of a `Uint8Array`, or of a string in UTF-8.
///
/// For anything else, returns the error to throw, which names `method`.
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

/// Replaces the ids in the JavaScript value of a trace response with the objects they refer
/// to.
///
/// A node's `rule` becomes the rule's object, and its `premises` become the premises' nodes.
/// A traced fact's `node` becomes the node's object.
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

/// Returns a protocol query made from plain JavaScript data, converted through their JSON
/// form.
///
/// Fails if the data cannot be converted to JSON, or if the JSON does not have the query's
/// shape.
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

    /// Adds rows from memory to a predicate.
    ///
    /// `rows` is an array of rows, each an array of values,
    /// which are converted by the bindings' conversion table.
    /// Before `load`, the rows become facts of the program.
    /// After it, they are new facts, and the next `reason()` continues to the new fixpoint.
    /// The predicate must then be one that the program uses, since only `extend` adds new ones.
    /// Throws for a value that does not convert, for a row that the predicate does not accept,
    /// and with code N0493 while a method is suspended.
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

    /// Registers a resource in memory under `name`.
    ///
    /// An `@import` of a resource with that name reads `content`,
    /// since the browser has no file system.
    /// `content` is a `Uint8Array` or a string, and anything else throws.
    #[wasm_bindgen(js_name = "addResource")]
    pub fn add_resource(&mut self, name: &str, content: &JsValue) -> Result<(), JsValue> {
        let bytes = bytes_of(content, "addResource")?;
        self.state_mut()?.engine.add_resource(name, bytes);
        Ok(())
    }

    /// Registers the text of a module under `name`, the name with which a `@use` refers to it,
    /// such as `lib::tc`.
    ///
    /// A `@use` of the module then reads this text, since the browser has no files.
    /// Throws if `name` is not a module name.
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
    /// Without it, loading stores only the columns and rows of an import that the rules read,
    /// and tracing or reading the rest throws.
    /// Call it before `load`.
    #[wasm_bindgen(js_name = "retainImports")]
    pub fn retain_imports(&mut self) -> Result<(), JsValue> {
        self.state_mut()?.engine.retain_imports();
        Ok(())
    }

    /// Returns the requests of the suspended method that have no response yet,
    /// or none if no method is suspended.
    ///
    /// The async methods, such as `loadAsync`, fetch the data that a method needs from the
    /// network themselves.
    /// To fetch it another way, such as through a proxy or with credentials,
    /// call the method's `start` form, such as `startLoad`.
    /// It returns the requests it is suspended for, or `null` once it is done.
    /// Fetch each request, and pass the responses to `resume`,
    /// which returns the requests that the method is suspended for next.
    /// Each request is a plain object
    /// `{id, url, method, headers: [[name, value]…], accept, parameters: [[name, value]…],
    /// form: [[name, value]…] | null}`, where `id` is a `BigInt` and `accept` may be `null`.
    #[wasm_bindgen(js_name = "pendingRequests")]
    pub fn pending_requests(&self) -> Result<Array, JsValue> {
        requests_js(&self.state().engine.pending())
    }

    /// Continues the suspended method with the responses to its requests.
    ///
    /// `responses` is an array with an entry per response:
    /// - `{id, body}` for a response, with a body that is a `Uint8Array` or a string;
    /// - `{id, status, message}` for a request that got no body,
    ///   without `status` if no response came;
    /// - `{id, status, message, partial: true}` for a successful response that contains only
    ///   part of the result, as a server marks a result that it cut off.
    ///
    /// Returns `null` once the method is done, or the requests that it is suspended for,
    /// as `startLoad` does: those without a response yet, or new ones.
    /// A failure is final, so retry a request before passing its failure in.
    /// A request of a run that was too large for the server (status 413 or 414),
    /// or whose response was partial, is listed again in smaller pieces if the engine can
    /// split it.
    /// Throws, and passes on no response, for a response with an id that `pendingRequests`
    /// does not list, or for two responses with the same id.
    /// Otherwise it throws what the continued method throws.
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

    /// Discards the suspended method, and returns whether there was one.
    ///
    /// A suspended `load`, `extend` or `reload` has changed nothing,
    /// and a suspended `reason` stops as after a cancellation.
    /// Throws while an async method waits for responses.
    #[wasm_bindgen(js_name = "discardPending")]
    pub fn discard_pending(&mut self) -> Result<bool, JsValue> {
        Ok(self.state_mut()?.engine.discard_pending())
    }

    /// Loads a program: it stores the facts, reads the imports and compiles the rules.
    ///
    /// Throws for a program that does not parse or load,
    /// and with code N0491 if it needs data from the network.
    /// `loadAsync` fetches that data, and `startLoad` returns the requests for it.
    pub fn load(&mut self, program: &NemoProgram) -> Result<(), JsValue> {
        let text = program.text.clone();
        self.state_mut()?
            .ingest(&program.name, text, LoadKind::Load, false)
            .map(drop)
    }

    /// Loads a program as `load` does, but returns the requests for the data it needs from
    /// the network instead of throwing.
    ///
    /// Returns `null` once the program is loaded, or the requests that the load is suspended
    /// for, in the form that `pendingRequests` returns, with nothing stored yet.
    /// Pass their responses to `resume`.
    /// Throws with code N0493 while another method is suspended.
    #[wasm_bindgen(js_name = "startLoad")]
    pub fn start_load(&mut self, program: &NemoProgram) -> Result<JsValue, JsValue> {
        let text = program.text.clone();
        let progress = self
            .state_mut()?
            .ingest(&program.name, text, LoadKind::Load, true)?;
        progress_js(progress)
    }

    /// Loads a program as `load` does, and fetches its remote resources with the global
    /// `fetch`.
    ///
    /// The promise resolves once the program is loaded, and otherwise rejects with what
    /// `load` throws.
    /// A failed request is sent again, up to five times,
    /// if the server asks to wait (status 429 or 503) or is overloaded (status 502 or 504).
    /// The engine can still be read while requests are out.
    #[wasm_bindgen(js_name = "loadAsync")]
    pub fn load_async(&self, program: &NemoProgram) -> Result<Promise, JsValue> {
        let (name, text) = (program.name.clone(), program.text.clone());
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Load, true))
    }

    /// Extends the loaded program with the statements of `program`.
    ///
    /// The new facts and imports are stored, the whole program is compiled again,
    /// and the next `reason()` continues to the new fixpoint.
    /// An extension cannot change what the loaded statements mean,
    /// so a new `@base`, a prefix bound to another IRI, or a parameter declared again throws,
    /// since these need a `reload`.
    /// It also throws as `load` does.
    pub fn extend(&mut self, program: &NemoProgram) -> Result<(), JsValue> {
        let text = program.text.clone();
        self.state_mut()?
            .ingest(&program.name, text, LoadKind::Extend, false)
            .map(drop)
    }

    /// Extends the loaded program as `extend` does,
    /// but returns the requests that it is suspended for, as `startLoad` does.
    ///
    /// The loaded program is unchanged until the extension completes.
    /// Throws with code N0493 while another method is suspended.
    #[wasm_bindgen(js_name = "startExtend")]
    pub fn start_extend(&mut self, program: &NemoProgram) -> Result<JsValue, JsValue> {
        let text = program.text.clone();
        let progress = self
            .state_mut()?
            .ingest(&program.name, text, LoadKind::Extend, true)?;
        progress_js(progress)
    }

    /// Extends the loaded program as `extend` does,
    /// and fetches its remote resources as `loadAsync` does.
    #[wasm_bindgen(js_name = "extendAsync")]
    pub fn extend_async(&self, program: &NemoProgram) -> Result<Promise, JsValue> {
        let (name, text) = (program.name.clone(), program.text.clone());
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Extend, true))
    }

    /// Replaces the loaded program with `program`.
    ///
    /// Every derived row is dropped, every rule starts over,
    /// and the next `reason()` derives everything again from the input facts.
    /// A predicate whose facts, imports and declarations are unchanged keeps its input facts,
    /// and an unchanged import is not read again.
    /// The rows added through `importRows` to a predicate whose definition changed are
    /// dropped, with a warning.
    /// It throws as `load` does.
    pub fn reload(&mut self, program: &NemoProgram) -> Result<(), JsValue> {
        let text = program.text.clone();
        self.state_mut()?
            .ingest(&program.name, text, LoadKind::Reload, false)
            .map(drop)
    }

    /// Replaces the loaded program as `reload` does,
    /// but returns the requests that it is suspended for, as `startLoad` does.
    ///
    /// The loaded program is unchanged until the reload completes.
    /// Throws with code N0493 while another method is suspended.
    #[wasm_bindgen(js_name = "startReload")]
    pub fn start_reload(&mut self, program: &NemoProgram) -> Result<JsValue, JsValue> {
        let text = program.text.clone();
        let progress = self
            .state_mut()?
            .ingest(&program.name, text, LoadKind::Reload, true)?;
        progress_js(progress)
    }

    /// Replaces the loaded program as `reload` does,
    /// and fetches its remote resources as `loadAsync` does.
    #[wasm_bindgen(js_name = "reloadAsync")]
    pub fn reload_async(&self, program: &NemoProgram) -> Result<Promise, JsValue> {
        let (name, text) = (program.name.clone(), program.text.clone());
        self.drive(move |inner| inner.ingest(&name, text, LoadKind::Reload, true))
    }

    /// Checks a program without loading it, and returns its diagnostics as plain objects.
    ///
    /// No data is read and nothing is stored.
    /// Every problem, warnings included, is returned rather than thrown.
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

    /// Reasons to the fixpoint by semi-naive evaluation.
    ///
    /// The call is synchronous and returns when the fixpoint is reached,
    /// so it should run in a worker.
    /// If a bound import needs data from the network, it throws with code N0491.
    /// `reasonAsync` fetches that data, and `startReason` returns the requests for it.
    pub fn reason(&mut self) -> Result<(), JsValue> {
        let mut inner = self.state_mut()?;
        inner
            .engine
            .reason()
            .map_err(|diagnostic| inner.throw(diagnostic))
    }

    /// Reasons as `reason` does, but returns the requests for the data that a bound import
    /// needs from the network instead of throwing.
    ///
    /// Returns `null` at the fixpoint, or the requests that the run is suspended for,
    /// as `startLoad` does.
    /// Passing their responses to `resume` continues the run where it stopped.
    /// Throws with code N0493 while a `load`, `extend` or `reload` is suspended.
    #[wasm_bindgen(js_name = "startReason")]
    pub fn start_reason(&mut self) -> Result<JsValue, JsValue> {
        let mut inner = self.state_mut()?;
        let progress = inner
            .engine
            .start_reason()
            .map_err(|diagnostic| inner.throw(diagnostic))?;
        progress_js(progress)
    }

    /// Reasons as `reason` does, and fetches the requests of its bound imports as `loadAsync`
    /// does.
    ///
    /// The promise resolves at the fixpoint, and otherwise rejects with what `reason` throws.
    #[wasm_bindgen(js_name = "reasonAsync")]
    pub fn reason_async(&self) -> Result<Promise, JsValue> {
        self.drive(|inner| {
            inner
                .engine
                .start_reason()
                .map_err(|diagnostic| inner.throw(diagnostic))
        })
    }

    /// Traces facts, and returns the trace response as a plain object.
    ///
    /// `facts` is an array of facts as strings, such as `"reach(a, b)"`,
    /// and the terminating `.` may be left out.
    /// The response lists, for each fact, its node if the fact holds,
    /// and the nodes of the derivations.
    /// A node's `rule` is the object of its rule, and its `premises` are the nodes of its
    /// premises, so a derivation can be walked like a tree,
    /// and a fact that several derivations use is one object.
    /// Throws for a text that is not a fact, or if the facts cannot be traced,
    /// for example while the engine holds updates that `reason()` has not processed.
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

    /// Traces facts as `trace` does, and returns the response as ASCII proof trees.
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

    /// Answers a `treeForTable` query of the explanation view, given as a plain object.
    ///
    /// The response, also a plain object, contains the facts that the query selects,
    /// with their derivations expanded.
    /// The query and the response use the protocol's camelCase keys.
    /// Throws for a query that does not have the protocol's shape,
    /// or that the engine cannot answer.
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

    /// Answers a `tableEntriesForTreeNodes` query of the explanation view,
    /// given as a plain object.
    ///
    /// The response is an array of plain objects, one for each node of the tree that the
    /// query describes, with the node's facts, parents before children.
    /// The query and the response use the protocol's camelCase keys.
    /// Throws as `treeForTable` does.
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

    /// Returns the rule with the statement id `id`, by which traces and queries refer to it,
    /// or `undefined` if no rule has that id.
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
    /// For large results, read them in pages through `cursor` instead.
    /// Throws for a predicate that the program does not have,
    /// or that the engine does not store whole.
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

    /// Opens a `NemoCursor` over a predicate's rows, which `readChunk` reads.
    ///
    /// Throws as `rows` does.
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

    /// Returns the next rows of a cursor, or an empty array once all rows have been read.
    ///
    /// At most `budget` rows are returned, or a bounded default number without a budget.
    /// A row is a plain object `{entryId, values, terms}`, with the row's entry id,
    /// its values as the conversion table converts them, and the same values in term syntax.
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
    ///
    /// Throws as `rows` does.
    pub fn count(&self, predicate: &str) -> Result<usize, JsValue> {
        let inner = self.state();
        inner
            .engine
            .count(predicate)
            .map_err(|diagnostic| inner.throw(diagnostic))
    }

    /// Returns the program's outputs.
    ///
    /// They are the predicates that the program declares `@output` or exports,
    /// in declaration order.
    /// If it declares neither, they are the predicates that its rules derive, sorted by name.
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
    /// as `[name, columns, role]` triples.
    ///
    /// The role is `"derived"` for a predicate that rules derive and that has no input facts,
    /// `"both"` for one that rules derive and that also has input facts,
    /// and `"extensional"` for any other.
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
    ///
    /// Returns `undefined` for a predicate that the program never used,
    /// or that the engine does not store whole.
    #[wasm_bindgen(js_name = "columnTypes")]
    pub fn column_types(&self, predicate: &str) -> Option<Vec<String>> {
        self.state().engine.column_types(predicate).map(|types| {
            types
                .iter()
                .map(|datatype| TypeSyntax::of(datatype).to_string())
                .collect()
        })
    }

    /// Serializes the program's exports.
    ///
    /// Returns an object that maps the file name of each export to its content as a
    /// `Uint8Array`, which is compressed if the export says so.
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

    /// Serializes one predicate, exported or not, as comma-separated values with each value in
    /// term syntax.
    ///
    /// Returns the bytes, which the page can save to a file,
    /// for example as a download or into a file handle that the user picked.
    /// Throws as `rows` does.
    #[wasm_bindgen(js_name = "exportPredicate")]
    pub fn export_predicate(&mut self, predicate: &str) -> Result<Uint8Array, JsValue> {
        let mut inner = self.inner.borrow_mut();
        // A predicate that cannot be read throws here, as it does for `rows`.
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

    /// Returns the reports, such as warnings, that the engine has collected while loading and
    /// reasoning, as plain diagnostic objects.
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

    /// Returns the number of input facts: the rows of the program's facts and imports,
    /// and the rows added through `importRows`.
    #[wasm_bindgen(getter, js_name = "loadedFacts")]
    pub fn loaded_facts(&self) -> usize {
        self.state().engine.loaded_facts()
    }

    /// Returns the number of facts that reasoning stored,
    /// including the rows that bound imports fetched.
    #[wasm_bindgen(getter, js_name = "derivedFacts")]
    pub fn derived_facts(&self) -> usize {
        self.state().engine.derived_facts()
    }

    /// Returns the number of rule applications performed so far.
    #[wasm_bindgen(getter)]
    pub fn applications(&self) -> u64 {
        self.state().engine.applications()
    }

    /// Returns the number of bytes that the tables and the dictionary currently use.
    #[wasm_bindgen(getter, js_name = "memoryBytes")]
    pub fn memory_bytes(&self) -> usize {
        self.state().engine.memory_bytes()
    }
}

/// Returns a cursor row as a plain object `{entryId, values, terms}`,
/// with `entry` as its entry id, its values converted to JavaScript,
/// and its values in term syntax.
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

/// A cursor over a predicate's rows, which the engine's `readChunk` reads.
///
/// It is plain data, without a reference to the engine.
#[wasm_bindgen]
#[derive(Debug)]
pub struct NemoCursor {
    /// The engine's cursor.
    cursor: PredicateCursor,
    /// The rows read but not yet returned.
    buffered: VecDeque<Vec<DataValue>>,
}

#[wasm_bindgen]
impl NemoCursor {
    /// Returns the number of rows not yet returned.
    #[wasm_bindgen(getter)]
    pub fn remaining(&self) -> usize {
        self.cursor.remaining() + self.buffered.len()
    }

    /// Returns the position of the next row to return,
    /// which is the entry id by which a query selects that row.
    #[wasm_bindgen(getter)]
    pub fn position(&self) -> usize {
        self.cursor.position() - self.buffered.len()
    }

    /// Returns whether every row has been returned.
    #[wasm_bindgen(getter)]
    pub fn done(&self) -> bool {
        self.cursor.is_done() && self.buffered.is_empty()
    }
}
