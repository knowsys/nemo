//! The language server in the browser, which exchanges messages instead of using standard
//! streams.
//!
//! An editor exchanges JSON-RPC messages with the server through three handles:
//! - The client's requests go through [LspClientChannel],
//!   and their responses come back as promises.
//! - The messages that the server sends on its own, such as diagnostics and log messages,
//!   are read from [LspServerRequests].
//! - The client's responses to the server's requests go back through [LspServerResponses].
//!
//! There are three handles rather than one, because each is borrowed across an `await`,
//! and one `wasm-bindgen` object cannot be borrowed twice at once.

use std::{fmt, pin::Pin};

use futures::{Sink, SinkExt, Stream, StreamExt};
use js_sys::{Array, Promise};
use nemo_language_server::{ClientSocket, ExitedError, LspService, Request, Response, Server};
use serde::Serialize;
use tower_service::Service;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

/// Creates a language server, and returns its three handles as an array
/// `[channel, requests, responses]`
/// of an `LspClientChannel`, an `LspServerRequests` and an `LspServerResponses`.
#[wasm_bindgen(js_name = "createNemoLanguageServer")]
pub fn create_language_server() -> Array {
    let (channel, requests, responses) = language_server_handles();
    let handles = Array::new();
    handles.push(&channel.into());
    handles.push(&requests.into());
    handles.push(&responses.into());
    handles
}

/// Returns the three handles of a new language server, as Rust values.
///
/// [create_language_server] returns the same handles to JavaScript.
pub fn language_server_handles() -> (LspClientChannel, LspServerRequests, LspServerResponses) {
    let (service, socket) = nemo_language_server::service();
    let (requests, responses) = split(socket);
    (
        LspClientChannel { service },
        LspServerRequests { requests },
        LspServerResponses { responses },
    )
}

/// The stream of the messages that the server sends on its own, pinned for polling.
type Requests = Pin<Box<dyn Stream<Item = Request>>>;
/// The sink for the client's responses to the server's requests.
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

/// The channel for the client's requests to the server, whose responses come back as
/// promises.
#[wasm_bindgen]
pub struct LspClientChannel {
    /// The language service that handles the requests.
    service: LspService<Server>,
}

impl fmt::Debug for LspClientChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LspClientChannel")
    }
}

#[wasm_bindgen]
impl LspClientChannel {
    /// Sends one JSON-RPC request, given as a plain object,
    /// and returns a promise of the response object,
    /// or of `null` for a notification, which has no response.
    ///
    /// Throws if the message is not a request, and the promise rejects if the server has
    /// exited.
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

/// The messages that the server sends on its own,
/// such as diagnostics, log messages and its requests to the client.
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
    /// Returns a promise of the server's next message as a plain object,
    /// or of `null` once the server has exited.
    ///
    /// Await each call before making the next, since only one call can run at a time.
    #[wasm_bindgen(js_name = "getNextRequest")]
    pub async fn next_request(&mut self) -> Result<JsValue, JsValue> {
        match self.requests.next().await {
            Some(request) => to_js(&request),
            None => Ok(JsValue::NULL),
        }
    }
}

/// The channel for the client's responses to the server's requests.
#[wasm_bindgen]
pub struct LspServerResponses {
    /// The sink for the responses.
    responses: Responses,
}

impl fmt::Debug for LspServerResponses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LspServerResponses")
    }
}

#[wasm_bindgen]
impl LspServerResponses {
    /// Sends one response, given as a plain object, to a request of the server.
    ///
    /// Await each call before making the next, since only one call can run at a time.
    /// The promise rejects if the message is not a response, or if the server has exited.
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
