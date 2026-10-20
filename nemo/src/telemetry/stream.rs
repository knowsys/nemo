//! The engine's events on the `tracing` stream.
//!
//! The [TracingSink] emits each event as one `tracing` event under the [TARGET] target.
//! Behind the `subscriber` feature, the `SinkLayer` decodes them back into events for any sink.

mod codec;
#[cfg(feature = "subscriber")]
mod sink_layer;
mod tracing_sink;

#[cfg(feature = "subscriber")]
pub use self::sink_layer::SinkLayer;
pub use self::{codec::TARGET, tracing_sink::TracingSink};
