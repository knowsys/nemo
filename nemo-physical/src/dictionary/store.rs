//! The storage mechanisms behind the domain stores.
//!
//! The domain stores keep their entries with a few shared pieces:
//! - an append-only byte [arena](arena::Arena), whose byte strings never move;
//! - [record pools](RecordPool), which intern records of a fixed header and variable bytes,
//!   and keep the bytes in an arena;
//! - [string pools](StringPool) and [pair pools](PairPool), built on record pools;
//! - the [blob encoding](content) of structured values.

mod arena;
pub(super) mod content;
mod pair;
mod pool;
mod record;

pub(super) use self::{arena::Arena, pair::PairPool, pool::StringPool, record::RecordPool};
