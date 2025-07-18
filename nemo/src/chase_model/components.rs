//! This module contains the components of the chase model.
//!
//! In general, such components are restricted versions
//! of the respective components of Nemo's logical rule model.

use crate::rule_model::origin::Origin;

pub(crate) mod aggregate;
pub(crate) mod atom;
pub(crate) mod export;
pub(crate) mod filter;
pub(crate) mod import;
pub(crate) mod operation;
pub(crate) mod program;
pub(crate) mod rule;
pub(crate) mod rule_tracing;
pub(crate) mod term;

/// Trait implemented by components of the chase model
pub trait ChaseComponent {
    /// Return the [Origin] of this component.
    fn origin(&self) -> &Origin;

    /// Set the [Origin] of this component.
    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized;
}
