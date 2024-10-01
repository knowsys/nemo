//! Normalized version of Nemo's logical rule model
//! that is more suitable for the generation of execution plans.

pub(crate) mod analysis;
pub(crate) mod components;
pub(crate) mod translation;

// TODO: This is required for tracing, try to use logical model instead
pub use components::atom::ground_atom::GroundAtom;
pub use components::atom::ChaseAtom;
