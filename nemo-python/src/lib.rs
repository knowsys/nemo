//! Python bindings for the Nemo rule engine.
//!
//! The module is `nmo`, thin over the `nemo` crate's embedding API.
//! [Program] parses and inspects.
//! [Engine] loads, reasons (GIL released, Ctrl-C honoured),
//! and reads results back through one defined conversion table.
//! Failures raise a registered exception hierarchy carrying structured diagnostics.

#[cfg(feature = "arrow")]
mod arrow;
mod engine;
mod errors;
mod program;
mod values;

use pyo3::prelude::*;

pub use engine::{Cursor, Engine};
pub use errors::{Diagnostic, Label};
pub use program::Program;
pub use values::{Float32, Iri, Labeled, LanguageString, Literal, Null};

/// Returns the `nmo` module: every class and exception registered by name.
#[pymodule(name = "nmo")]
fn nemo_python(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Program>()?;
    module.add_class::<Engine>()?;
    module.add_class::<Cursor>()?;
    module.add_class::<Diagnostic>()?;
    module.add_class::<Label>()?;
    module.add_class::<Iri>()?;
    module.add_class::<LanguageString>()?;
    module.add_class::<Float32>()?;
    module.add_class::<Literal>()?;
    module.add_class::<Null>()?;
    module.add_class::<Labeled>()?;
    #[cfg(feature = "arrow")]
    module.add_class::<arrow::ArrowStream>()?;
    module.add("NemoError", py.get_type::<errors::NemoError>())?;
    module.add(
        "DiagnosticsError",
        py.get_type::<errors::DiagnosticsError>(),
    )?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
