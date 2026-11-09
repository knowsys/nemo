//! Fetching the data that a suspended engine method needs from the network.
//!
//! The requests of a suspended method are sent through the global `fetch`,
//! of a window, a worker or Node,
//! and their responses are passed to [Engine::resume](nemo::engine::Engine::resume)
//! until the method completes.
//! Failed requests are retried, and partial results detected,
//! as [Client::fetch_all](nemo::engine::remote::Client::fetch_all) requires of a client.
//!
//! No borrow of the engine is held while a request is out,
//! so the engine can still be read in the meantime.

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use futures::{StreamExt, stream};
use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use nemo::engine::remote::{Failure, Progress, Request, Retry};

use crate::engine::Inner;

/// The number of requests in flight at once.
const IN_FLIGHT: usize = 5;

/// The outcome of one attempt to send a request.
enum Sent {
    /// The body of a successful response.
    Body(Vec<u8>),
    /// A response with an error status.
    ErrorStatus {
        /// The response status.
        status: u16,
        /// The response body, as text.
        message: String,
        /// The `Retry-After` header, if the response has one.
        retry_after: Option<String>,
    },
    /// A successful response that contains only part of the result,
    /// as [Failure::of_partial_response] finds from its status or headers.
    Partial(Failure),
    /// No response came, for the reason that `fetch` gives.
    Lost(String),
}

/// Runs `call`, which starts a method of the engine,
/// and fetches the requests that the method is suspended for until it completes or fails.
///
/// The responses go to [Engine::resume](nemo::engine::Engine::resume),
/// at most [IN_FLIGHT] requests are out at once, and the result is the method's result.
/// [driving](Inner::driving) must be set before, and is cleared when this ends.
pub(crate) async fn drive(
    inner: Rc<RefCell<Inner>>,
    call: impl FnOnce(&mut Inner) -> Result<Progress, JsValue>,
) -> Result<(), JsValue> {
    let retry = Retry::default();
    let mut progress = call(&mut inner.borrow_mut());
    loop {
        let pending = match progress {
            Ok(Progress::Suspended(pending)) => pending,
            done => {
                inner.borrow_mut().driving = false;
                return done.map(drop);
            }
        };
        let responses: Vec<Result<Arc<[u8]>, Failure>> = stream::iter(&pending)
            .map(|pending| fetch_retrying(&pending.request, &retry))
            .buffered(IN_FLIGHT)
            .collect()
            .await;
        let mut inner = inner.borrow_mut();
        let responses = pending.iter().map(|pending| pending.id).zip(responses);
        progress = inner
            .engine
            .resume(responses)
            .map_err(|diagnostic| inner.throw(diagnostic));
    }
}

/// Fetches one request, sending it again as `retry` says,
/// and returns its response: the body, or the failure that prevented one.
async fn fetch_retrying(request: &Request, retry: &Retry) -> Result<Arc<[u8]>, Failure> {
    let mut attempt = 0;
    loop {
        match send(request).await {
            Sent::Body(bytes) => return Ok(bytes.into()),
            Sent::Lost(message) => return Err(Failure::transport(message)),
            Sent::Partial(failure) => return Err(failure),
            Sent::ErrorStatus {
                status,
                message,
                retry_after,
            } => match retry.delay(attempt, status, retry_after.as_deref()) {
                Some(wait) => {
                    sleep(wait)
                        .await
                        .map_err(|error| Failure::transport(describe(&error)))?;
                    attempt += 1;
                }
                None => return Err(Failure::response(status, message)),
            },
        }
    }
}

/// Sends one request through the global `fetch`,
/// and returns a JavaScript error as a lost response.
async fn send(request: &Request) -> Sent {
    match try_send(request).await {
        Ok(sent) => sent,
        Err(error) => Sent::Lost(describe(&error)),
    }
}

/// Sends one request through the global `fetch`, and returns what came back.
///
/// An error is the value that JavaScript threw or rejected with.
async fn try_send(request: &Request) -> Result<Sent, JsValue> {
    let init = Object::new();
    Reflect::set(&init, &"method".into(), &request.method().into())?;
    let headers: Array = request
        .headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .chain(request.accept.map(|accept| ("Accept", accept)))
        .map(|(name, value)| JsValue::from(Array::of2(&name.into(), &value.into())))
        .collect();
    Reflect::set(&init, &"headers".into(), &headers)?;
    if let Some(form) = &request.form {
        // `fetch` sends a `URLSearchParams` body with the content type of a form.
        // A SPARQL endpoint would read a POST with another content type as an update.
        Reflect::set(&init, &"body".into(), &search_parameters(form)?)?;
    }
    let mut url = request.url.clone();
    if !request.parameters.is_empty() {
        let query = call_method(&search_parameters(&request.parameters)?, "toString", &[])?
            .as_string()
            .unwrap_or_default();
        url.push(if url.contains('?') { '&' } else { '?' });
        url.push_str(&query);
    }

    let fetch = global_function("fetch")?;
    let response = settled(fetch.call2(&JsValue::UNDEFINED, &url.into(), &init)?).await?;
    let status = Reflect::get(&response, &"status".into())?
        .as_f64()
        .unwrap_or(0.0);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let code = status as u16;
    let headers = Reflect::get(&response, &"headers".into())?;
    if Reflect::get(&response, &"ok".into())?.is_truthy() {
        // Of a cross-origin response, a browser shows only the headers that the server
        // exposes, so there a partial result may be recognizable by its status alone.
        let mut shown: Vec<(&str, String)> = Vec::new();
        for name in Failure::PARTIAL_RESPONSE_HEADERS {
            if let Some(value) = call_method(&headers, "get", &[name.into()])?.as_string() {
                shown.push((name, value));
            }
        }
        let partial = Failure::of_partial_response(code, |name| {
            shown
                .iter()
                .find(|(shown, _)| shown.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        });
        if let Some(failure) = partial {
            return Ok(Sent::Partial(failure));
        }
        let buffer = settled(call_method(&response, "arrayBuffer", &[])?).await?;
        return Ok(Sent::Body(Uint8Array::new(&buffer).to_vec()));
    }
    let retry_after = call_method(&headers, "get", &["Retry-After".into()])?.as_string();
    let message = settled(call_method(&response, "text", &[])?)
        .await?
        .as_string()
        .unwrap_or_default();
    Ok(Sent::ErrorStatus {
        status: code,
        message,
        retry_after,
    })
}

/// Returns name-value pairs as a `URLSearchParams`, which URL-encodes them.
fn search_parameters(pairs: &[(String, String)]) -> Result<JsValue, JsValue> {
    let entries: Array = pairs
        .iter()
        .map(|(name, value)| JsValue::from(Array::of2(&name.into(), &value.into())))
        .collect();
    let constructor = global_function("URLSearchParams")?;
    Reflect::construct(&constructor, &Array::of1(&entries))
}

/// Waits for `wait`, using the global `setTimeout`.
async fn sleep(wait: Duration) -> Result<(), JsValue> {
    let set_timeout = global_function("setTimeout")?;
    #[allow(clippy::cast_precision_loss)]
    let milliseconds = wait.as_millis() as f64;
    let mut scheduled = Ok(JsValue::UNDEFINED);
    let waiting = Promise::new(&mut |resolve, _| {
        scheduled = set_timeout.call2(&JsValue::UNDEFINED, &resolve, &milliseconds.into());
    });
    scheduled?;
    JsFuture::from(waiting).await.map(drop)
}

/// Returns a future of `value`, which resolves once the value settles if it is a promise.
fn settled(value: JsValue) -> JsFuture {
    JsFuture::from(Promise::resolve(&value))
}

/// Returns a function of the JavaScript global scope, looked up by name.
fn global_function(name: &str) -> Result<Function, JsValue> {
    Reflect::get(&js_sys::global(), &name.into())?
        .dyn_into::<Function>()
        .map_err(|_| js_sys::Error::new(&format!("there is no global function `{name}`")).into())
}

/// Calls a method of a JavaScript object by name.
fn call_method(object: &JsValue, name: &str, arguments: &[JsValue]) -> Result<JsValue, JsValue> {
    let method = Reflect::get(object, &name.into())?
        .dyn_into::<Function>()
        .map_err(|_| js_sys::Error::new(&format!("`{name}` is not a method")))?;
    Reflect::apply(&method, object, &arguments.iter().collect::<Array>())
}

/// Returns the message of a value that JavaScript threw.
fn describe(error: &JsValue) -> String {
    error
        .dyn_ref::<js_sys::Error>()
        .map(|error| String::from(error.message()))
        .or_else(|| error.as_string())
        .unwrap_or_else(|| format!("{error:?}"))
}
