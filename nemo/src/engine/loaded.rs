//! The program that an engine holds.

use std::sync::Arc;

use crate::{
    engine::{compile::compiled::CompiledProgram, written::WrittenProgram},
    pipeline::RevisionId,
};

/// The program that an engine holds: as it was written, and as it was compiled.
#[derive(Debug)]
pub(in crate::engine) struct LoadedProgram {
    /// The written program, which results and diagnostics refer to.
    pub(in crate::engine) written: WrittenProgram,
    /// The pipeline revision that the compiled program was translated from,
    /// which is what the pipeline stages made of the written program.
    pub(in crate::engine) revision: RevisionId,
    /// The compiled program.
    ///
    /// A run shares it, and nothing changes it while a run holds it.
    pub(in crate::engine) compiled: Arc<CompiledProgram>,
}
