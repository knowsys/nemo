//! The settings of an engine: what its user sets it up with.

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use crate::{
    engine::{
        cancel::CancelToken, compile::optimize::OptimizationPasses, link::ModuleSources,
        remote::ClientSlot, staging::EngineStages,
    },
    model::Term,
    telemetry::SinkSlot,
};

/// The settings of an engine.
///
/// A loading method or a run reads them, and neither changes them.
#[derive(Debug, Default)]
pub(in crate::engine) struct Settings {
    /// The optimization passes that compilation runs.
    ///
    /// [retain_imports](crate::engine::Engine::retain_imports) removes the import passes.
    pub(in crate::engine) passes: OptimizationPasses,
    /// The pipeline stages that run over every loaded program
    /// (see [stages_mut](crate::engine::Engine::stages_mut)).
    pub(in crate::engine) stages: EngineStages,
    /// The in-memory resources registered through
    /// [add_resource](crate::engine::Engine::add_resource), by name.
    ///
    /// An import of a resource with one of these names reads the bytes from here
    /// instead of the filesystem.
    pub(in crate::engine) resources: HashMap<String, Arc<[u8]>>,
    /// The values set through [set_parameter](crate::engine::Engine::set_parameter),
    /// by the name of their parameter without its `$`.
    ///
    /// The loading methods and [check](crate::engine::Engine::check) write them into the
    /// `@parameter` declarations of the program they are given.
    pub(in crate::engine) parameters: BTreeMap<String, Term>,
    /// The module roots and the module texts registered by name,
    /// which [link](crate::engine::Engine::link) reads.
    pub(in crate::engine) modules: ModuleSources,
    /// The directory that relative import resources are resolved against, if one is set.
    pub(in crate::engine) import_directory: Option<PathBuf>,
    /// The cancellation flag, which [reason](crate::engine::Engine::reason) checks between
    /// rule applications.
    ///
    /// [cancel_token](crate::engine::Engine::cancel_token) returns handles to it.
    pub(in crate::engine) cancel: CancelToken,
    /// The sink that receives telemetry events,
    /// if one was [installed](crate::engine::Engine::set_telemetry).
    pub(in crate::engine) telemetry: SinkSlot,
    /// The client through which the plain methods send their requests
    /// (see [set_client](crate::engine::Engine::set_client)).
    pub(in crate::engine) client: ClientSlot,
}
