//! This module contains utility functions, like importing data from various sources

use std::io::Read;

use ::csv::{Reader, ReaderBuilder};

pub mod csv;
pub mod parser;

pub(crate) fn reader<R>(rdr: R) -> Reader<R>
where
    R: Read,
{
    ReaderBuilder::new()
        .delimiter(b',')
        .escape(Some(b'\\'))
        .has_headers(false)
        .double_quote(true)
        .from_reader(rdr)
}
