//! Interning pools of pairs with a frequent and a rare part.

use std::hash::{Hash, Hasher};

use crate::dictionary::store::{pool::StringPool, record::RecordPool};

/// The index of a frequent part while it fits 32 bits: the header of a record of the 32-bit regime.
///
/// It hashes as the `u64` it widens to,
/// so the hashes a pool kept stay valid when its headers widen.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Index32(u32);

impl Hash for Index32 {
    fn hash<S: Hasher>(&self, state: &mut S) {
        state.write_u64(u64::from(self.0));
    }
}

/// The records of a [PairPool]: a frequent part's index beside the rare bytes.
enum Records {
    /// Headers hold frequent indexes below 2³².
    U32(RecordPool<Index32>),
    /// Headers hold any frequent index.
    U64(RecordPool<u64>),
}

/// An interning pool of byte-string pairs whose first part repeats across many entries:
/// the namespace of an IRI, the language of a string, the datatype of a literal,
/// the type blob of a structured value.
///
/// The frequent part is interned once in a [StringPool],
/// and an entry is a record of its index beside the rare bytes,
/// so the repeated part costs its index per entry instead of its bytes.
/// A read borrows both parts.
pub(crate) struct PairPool {
    /// The distinct frequent parts.
    frequents: StringPool,
    /// The records of a frequent part's index and the rare bytes.
    records: Records,
}

impl PairPool {
    /// Creates an empty pool.
    pub(crate) fn new() -> Self {
        Self {
            frequents: StringPool::new(),
            records: Records::U32(RecordPool::new()),
        }
    }

    /// Returns the number of distinct pairs.
    pub(crate) fn len(&self) -> u64 {
        match &self.records {
            Records::U32(records) => records.len(),
            Records::U64(records) => records.len(),
        }
    }

    /// Returns the number of distinct frequent parts.
    #[cfg(test)]
    pub(crate) fn frequents_len(&self) -> u64 {
        self.frequents.len()
    }

    /// Interns a pair and returns its index,
    /// or `None` if a pool cannot hold it.
    pub(crate) fn intern(&mut self, frequent: &[u8], rare: &[u8]) -> Option<u64> {
        let frequent = self.frequents.intern(frequent)?;

        match &mut self.records {
            Records::U32(records) => match u32::try_from(frequent) {
                Ok(index) => records.intern(Index32(index), rare),
                Err(_) => self.widen().intern(frequent, rare),
            },
            Records::U64(records) => records.intern(frequent, rare),
        }
    }

    /// Rebuilds the records in the 64-bit regime and returns them.
    fn widen(&mut self) -> &mut RecordPool<u64> {
        if let Records::U32(records) = &mut self.records {
            let old = std::mem::replace(records, RecordPool::new());
            self.records = Records::U64(old.map_headers(|Index32(index)| u64::from(index)));
        }

        match &mut self.records {
            Records::U64(records) => records,
            Records::U32(_) => unreachable!("just assigned the 64-bit regime"),
        }
    }

    /// Returns the index of a pair, or `None` if it was never interned.
    pub(crate) fn find(&self, frequent: &[u8], rare: &[u8]) -> Option<u64> {
        let frequent = self.frequents.find(frequent)?;

        match &self.records {
            Records::U32(records) => records.find(Index32(u32::try_from(frequent).ok()?), rare),
            Records::U64(records) => records.find(frequent, rare),
        }
    }

    /// Returns the frequent and the rare part of an entry,
    /// or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<(&[u8], &[u8])> {
        let (frequent, rare) = match &self.records {
            Records::U32(records) => {
                let (Index32(frequent), rare) = records.get(index)?;
                (u64::from(frequent), rare)
            }
            Records::U64(records) => records.get(index)?,
        };

        let frequent = self
            .frequents
            .get(frequent)
            .expect("an entry references an interned frequent part");

        Some((frequent, rare))
    }

    /// Returns the allocated bytes of the pool, the lookup table and the entry lists included.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.frequents.memory_bytes()
            + match &self.records {
                Records::U32(records) => records.memory_bytes(),
                Records::U64(records) => records.memory_bytes(),
            }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::BuildHasher;

    use hashbrown::DefaultHashBuilder;

    use super::*;

    #[test]
    fn pairs_share_frequent_parts() {
        let mut pool = PairPool::new();
        let first = pool.intern(b"http://example.org/", b"a").expect("fits");
        let second = pool.intern(b"http://example.org/", b"b").expect("fits");
        let third = pool.intern(b"http://other.org/", b"a").expect("fits");

        assert_eq!(pool.len(), 3);
        assert_eq!(pool.frequents_len(), 2);
        assert_eq!(pool.intern(b"http://example.org/", b"a"), Some(first));

        assert_eq!(
            pool.get(first),
            Some((b"http://example.org/".as_slice(), b"a".as_slice()))
        );
        assert_eq!(
            pool.get(second),
            Some((b"http://example.org/".as_slice(), b"b".as_slice()))
        );
        assert_eq!(
            pool.get(third),
            Some((b"http://other.org/".as_slice(), b"a".as_slice()))
        );
    }

    #[test]
    fn equal_rare_bytes_under_different_frequents_are_distinct() {
        let mut pool = PairPool::new();
        let under_a = pool.intern(b"a", b"x").expect("fits");
        let under_b = pool.intern(b"b", b"x").expect("fits");

        assert_ne!(under_a, under_b);
        assert_eq!(pool.find(b"a", b"x"), Some(under_a));
        assert_eq!(pool.find(b"c", b"x"), None);
    }

    #[test]
    fn indexes_hash_as_the_u64_they_widen_to() {
        let build = DefaultHashBuilder::default();
        for index in [0u32, 1, 7, u32::MAX] {
            assert_eq!(
                build.hash_one((Index32(index), b"rare".as_slice())),
                build.hash_one((u64::from(index), b"rare".as_slice()))
            );
        }
    }

    /// The regime switch cannot be reached by interning 2³² frequent parts in a test,
    /// so the rebuild is driven directly at the point a pool would perform it.
    #[test]
    fn widened_records_keep_every_pair() {
        let mut pool = PairPool::new();
        let indexes: Vec<(Vec<u8>, Vec<u8>, u64)> = (0..1_000u32)
            .map(|n| {
                let frequent = format!("f{}", n % 5).into_bytes();
                let rare = n.to_string().into_bytes();
                let index = pool.intern(&frequent, &rare).expect("fits");
                (frequent, rare, index)
            })
            .collect();
        assert!(matches!(pool.records, Records::U32(_)));

        pool.widen();
        assert!(matches!(pool.records, Records::U64(_)));
        let next = pool.intern(b"f0", b"next").expect("fits");

        assert_eq!(next, 1_000);
        assert_eq!(pool.len(), 1_001);
        for (frequent, rare, index) in &indexes {
            assert_eq!(pool.find(frequent, rare), Some(*index));
            assert_eq!(pool.intern(frequent, rare), Some(*index));
            assert_eq!(
                pool.get(*index),
                Some((frequent.as_slice(), rare.as_slice()))
            );
        }
        assert_eq!(pool.get(next), Some((b"f0".as_slice(), b"next".as_slice())));
    }
}
