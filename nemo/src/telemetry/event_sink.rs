//! The trait of the sinks that consume telemetry events.

/// A sink that consumes telemetry events of type `E`.
///
/// A sink records events through a shared reference,
/// so that parallel workers can emit events without coordination.
/// A sink with state manages its own interior mutability.
/// The trait is dyn-compatible, so that emitters can hold a sink as a trait object.
///
/// # Example
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use nemo::telemetry::EventSink;
///
/// /// Counts the events it is given.
/// struct Counting(AtomicUsize);
///
/// impl<E> EventSink<E> for Counting {
///     fn record(&self, _event: &E) {
///         self.0.fetch_add(1, Ordering::Relaxed);
///     }
/// }
///
/// let counting = Counting(AtomicUsize::new(0));
/// let sink: &dyn EventSink<&str> = &counting;
/// sink.record(&"started");
/// assert!(sink.enabled());
/// assert_eq!(counting.0.load(Ordering::Relaxed), 1);
/// ```
pub trait EventSink<E>: Send + Sync {
    /// Consumes one event.
    fn record(&self, event: &E);

    /// Returns whether emitted events are consumed at all.
    ///
    /// Emitters may skip building expensive events if this is false.
    fn enabled(&self) -> bool {
        true
    }
}
