//! The language server inside the browser: the service, driven over messages instead of
//! standard streams.
//!
//! An editor in a web worker exchanges JSON-RPC messages with it.
//! Requests the client initiates go through [LspClientChannel] and come back as promises.
//! What the server sends on its own (diagnostics, log messages)
//! is polled from [LspServerRequests],
//! and the client's responses to those go back through [LspServerResponses].
//! There are three handles rather than one, because each is borrowed across an `await`,
//! and one `wasm-bindgen` object cannot be borrowed twice.

use std::{fmt, pin::Pin};

use futures::{Sink, SinkExt, Stream, StreamExt};
use js_sys::{Array, Promise};
use nemo_language_server::{ClientSocket, ExitedError, LspService, Request, Response, Server};
use serde::Serialize;
use tower_service::Service;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

/// Creates a language server, returning its three handles as an array
/// `[channel, requests, responses]`:
/// a [LspClientChannel], an [LspServerRequests], and an [LspServerResponses].
#[wasm_bindgen(js_name = "createNemoLanguageServer")]
pub fn create_language_server() -> Array {
    let (channel, requests, responses) = language_server_handles();
    let handles = Array::new();
    handles.push(&channel.into());
    handles.push(&requests.into());
    handles.push(&responses.into());
    handles
}

/// Returns the three handles of a fresh language server, as Rust values,
/// which [create_language_server] hands to JavaScript.
pub fn language_server_handles() -> (LspClientChannel, LspServerRequests, LspServerResponses) {
    let (service, socket) = nemo_language_server::service();
    let (requests, responses) = split(socket);
    (
        LspClientChannel { service },
        LspServerRequests { requests },
        LspServerResponses { responses },
    )
}

/// The server's own outgoing messages, pinned for polling.
type Requests = Pin<Box<dyn Stream<Item = Request>>>;
/// The sink the client's responses to the server's requests go into.
type Responses = Pin<Box<dyn Sink<Response, Error = ExitedError>>>;

/// Splits the socket into its stream and sink halves.
fn split(socket: ClientSocket) -> (Requests, Responses) {
    let (stream, sink) = socket.split();
    (Box::pin(stream), Box::pin(sink))
}

/// Returns a JSON-RPC message as a plain JavaScript object.
fn to_js(message: &impl Serialize) -> Result<JsValue, JsValue> {
    let serializer = serde_wasm_bindgen::Serializer::json_compatible();
    message
        .serialize(&serializer)
        .map_err(|error| js_sys::Error::new(&error.to_string()).into())
}

/// Requests the client sends to the server, whose responses come back as promises.
#[wasm_bindgen]
pub struct LspClientChannel {
    /// The service the requests are called on.
    service: LspService<Server>,
}

impl fmt::Debug for LspClientChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LspClientChannel")
    }
}

#[wasm_bindgen]
impl LspClientChannel {
    /// Sends one JSON-RPC request (a plain object) and resolves to the response object,
    /// or `null` for a notification, which has none.
    ///
    /// Rejects if the server has exited or the message is not a request.
    #[wasm_bindgen(js_name = "sendRequest")]
    pub fn send_request(&mut self, request: JsValue) -> Result<Promise, JsValue> {
        let request: Request = serde_wasm_bindgen::from_value(request)
            .map_err(|error| js_sys::Error::new(&error.to_string()))?;
        let pending = self.service.call(request);
        Ok(future_to_promise(async move {
            match pending.await {
                Ok(Some(response)) => to_js(&response),
                Ok(None) => Ok(JsValue::NULL),
                Err(exited) => Err(js_sys::Error::new(&exited.to_string()).into()),
            }
        }))
    }
}

/// What the server sends on its own: diagnostics, log messages, and
/// requests it makes of the client.
#[wasm_bindgen]
pub struct LspServerRequests {
    /// The server's outgoing messages.
    requests: Requests,
}

impl fmt::Debug for LspServerRequests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LspServerRequests")
    }
}

#[wasm_bindgen]
impl LspServerRequests {
    /// Resolves to the server's next message as a plain object, or
    /// `null` once the server has exited. One call at a time: await it
    /// before asking for the next.
    #[wasm_bindgen(js_name = "getNextRequest")]
    pub async fn next_request(&mut self) -> Result<JsValue, JsValue> {
        match self.requests.next().await {
            Some(request) => to_js(&request),
            None => Ok(JsValue::NULL),
        }
    }
}

/// The client's responses to the requests the server made of it.
#[wasm_bindgen]
pub struct LspServerResponses {
    /// Where the responses go.
    responses: Responses,
}

impl fmt::Debug for LspServerResponses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LspServerResponses")
    }
}

#[wasm_bindgen]
impl LspServerResponses {
    /// Sends one response (a plain object) to a server-initiated
    /// request. One at a time: await it before sending the next.
    #[wasm_bindgen(js_name = "sendResponse")]
    pub async fn send_response(&mut self, response: JsValue) -> Result<(), JsValue> {
        let response: Response = serde_wasm_bindgen::from_value(response)
            .map_err(|error| js_sys::Error::new(&error.to_string()))?;
        self.responses
            .send(response)
            .await
            .map_err(|exited| js_sys::Error::new(&exited.to_string()).into())
    }
}
