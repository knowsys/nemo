use std::pin::Pin;

use futures::{FutureExt, SinkExt, StreamExt};
use futures::{Sink, Stream};
use gloo_utils::format::JsValueSerdeExt;
use js_sys::{Array, Promise};
use nemo_language_server::{
    create_language_server, Backend, ExitedError, LspService, Request, Response, Service,
};
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::future_to_promise;

/// Creates a Nemo language server
/// The server is split up into mutliple parts to allow concurrent sending/waiting for server/client-bound requests/responses.
/// To enable this with `wasm_bindgen`, multiple structs are required to ensure exclusive access, see https://stackoverflow.com/questions/75712197/rust-wasm-bindgen-recursive-use-of-an-object-detected-which-would-lead-to-unsaf#77013978 .
#[wasm_bindgen(js_name = "createNemoLanguageServer")]
pub fn create_nemo_language_server() -> JsValue {
    let (service, socket) = create_language_server();

    let (request_stream, responses_sink) = socket.split();

    let (a, b, c) = (
        NemoLspChannelClientInitiated(service),
        NemoLspRequestsServerInitiated(Box::pin(request_stream)),
        NemoLspResponsesServerInitiated(Box::pin(responses_sink)),
    );

    let (a, b, c): (JsValue, JsValue, JsValue) = (a.into(), b.into(), c.into());

    let array = Array::new();

    array.push(&a);
    array.push(&b);
    array.push(&c);

    array.into()
}

/// Handles requests initiated by the server
#[wasm_bindgen]
pub struct NemoLspRequestsServerInitiated(Pin<Box<dyn Stream<Item = Request>>>);

/// Handles responses corresponding to requests initiated by the server
#[wasm_bindgen]
pub struct NemoLspResponsesServerInitiated(Pin<Box<dyn Sink<Response, Error = ExitedError>>>);

#[wasm_bindgen]
impl NemoLspRequestsServerInitiated {
    #[wasm_bindgen(js_name = "getNextRequest")]
    pub async fn next_request(&mut self) -> JsValue {
        let request = self.0.next().await;

        JsValue::from_serde(&request).unwrap()
    }
}

#[wasm_bindgen]
impl NemoLspResponsesServerInitiated {
    /// Only one response may be sent at a time, wait for the promise to resolve before sending the next response
    #[wasm_bindgen(js_name = "sendResponse")]
    pub async fn send_response(&mut self, response_json_object: JsValue) {
        let response = response_json_object.into_serde().unwrap();

        self.0.send(response).await.unwrap();
    }
}

/// Handles requests initiated by the client and the corresponding responses
#[wasm_bindgen]
pub struct NemoLspChannelClientInitiated(LspService<Backend>);

#[wasm_bindgen]
impl NemoLspChannelClientInitiated {
    #[wasm_bindgen(js_name = "sendRequest")]
    pub fn send_request(&mut self, request_json_object: JsValue) -> Promise {
        let request = request_json_object.into_serde().unwrap();

        future_to_promise(
            self.0
                .call(request)
                .map(|response| Result::Ok(JsValue::from_serde(&response.unwrap()).unwrap())),
        )
    }
}
