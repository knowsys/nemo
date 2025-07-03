//! This module defines [IOType].
#![allow(missing_docs)]

use enum_assoc::Assoc;
use strum_macros::EnumIter;

use crate::syntax::datatypes;

#[derive(Assoc, EnumIter, Debug, Copy, Clone, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
pub enum IOType {
    /// 64bit integer number
    #[assoc(name = datatypes::INT)]
    Integer,
    /// 32bit floating point number
    #[assoc(name = datatypes::FLOAT)]
    Float,
    /// 64bit floating point number
    #[assoc(name = datatypes::DOUBLE)]
    Double,
    /// String
    #[assoc(name = datatypes::STRING)]
    String,
    /// Any data value
    #[assoc(name = datatypes::ANY)]
    Any,
}
