//! This module defines low-level data structures and operations, i.e.,
//! it corresponds to the physical layer of a data processing system.
//! It uses simplified structures and primitive datatypes that may
//! not correspond to the view at a higher (logical) level.

pub mod builder_proxy;
pub mod columnar;
pub mod datatypes;
pub mod dictionary;
pub mod management;
pub mod tabular;
pub mod util;
