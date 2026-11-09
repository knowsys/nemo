//! Python bindings for the Nemo rule engine.
//!
//! The Python module is `nmo`, a thin layer over the API of the `nemo` crate.
//! [Program] parses and inspects a program.
//! [Engine] loads programs, reasons, and reads the results,
//! converting values between the engine and Python as the `values` module describes.
//! Reasoning releases the GIL and can be interrupted with Ctrl-C.
//! A failure raises an exception of the module's own hierarchy, with structured diagnostics.

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

/// Python bindings for the Nemo rule engine.
///
/// `Program` parses and inspects a program, and `Engine` loads programs, reasons,
/// and reads the results.
/// A failure of the engine raises `NemoError` or its subclass `DiagnosticsError`.
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
