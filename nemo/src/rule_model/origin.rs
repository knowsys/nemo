//! This module defines

pub(crate) type OriginParseReference = usize;

#[derive(Debug)]
pub enum ComponentOrigin {
    Created,
    Parsed(OriginParseReference),
    Something(Box<ComponentOrigin>),
}
