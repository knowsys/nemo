//! This module defines the representation of nemo programs

#[macro_use]
pub mod util;

pub(crate) mod origin;
pub(crate) mod syntax;

pub mod components;
pub mod error;
pub mod program;
pub mod translation;
