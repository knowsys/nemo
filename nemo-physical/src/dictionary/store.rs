//! The storage mechanisms behind the domain stores.
//!
//! Every store keeps its entries through the same few pieces:
//! an append-only byte [arena](arena::Arena) with stable addresses,
//! [record pools](RecordPool) that intern a fixed header beside variable bytes in it,
//! [string pools](StringPool) and [pair pools](PairPool) built on those,
//! and the [blob encoding](content) of structured values.

mod arena;
pub(super) mod content;
mod pair;
mod pool;
mod record;

pub(super) use self::{arena::Arena, pair::PairPool, pool::StringPool, record::RecordPool};
