//! Interning pools of records with a fixed header and variable bytes.

use std::hash::{BuildHasher, Hash};

use hashbrown::{DefaultHashBuilder, HashTable};

use crate::dictionary::store::arena::{Arena, Extent};

/// One entry of a [RecordPool]: the header and the location of the bytes.
#[derive(Clone, Copy)]
struct Record<H> {
    /// The fixed part.
    header: H,
    /// The location of the variable bytes.
    bytes: Extent,
}

/// The lookup table of a [RecordPool]: maps a record's hash to its index.
///
/// This table holds either indexes below 2³², as `u32`, or indexes of size `usize`.
enum Lookup {
    /// Slots hold indexes below 2³².
    U32(HashTable<u32>),
    /// Slots hold any index.
    U64(HashTable<usize>),
}

impl Lookup {
    /// Returns the index of the entry with the given hash that satisfies `eq`.
    fn find(&self, hash: u64, mut eq: impl FnMut(usize) -> bool) -> Option<usize> {
        match self {
            Lookup::U32(table) => table
                .find(hash, |&index| eq(index as usize))
                .map(|&index| index as usize),
            Lookup::U64(table) => table.find(hash, |&index| eq(index)).copied(),
        }
    }

    /// Inserts an index known to be absent, given the hash of every entry.
    ///
    /// The index must be the number of entries inserted before it.
    fn insert_unique(&mut self, hash: u64, index: usize, hashes: &[u64]) {
        match self {
            Lookup::U32(table) => match u32::try_from(index) {
                Ok(index) => {
                    table.insert_unique(hash, index, |&entry| hashes[entry as usize]);
                }
                Err(_) => {
                    self.widen(hashes)
                        .insert_unique(hash, index, |&entry| hashes[entry]);
                }
            },
            Lookup::U64(table) => {
                table.insert_unique(hash, index, |&entry| hashes[entry]);
            }
        }
    }

    /// Rebuilds the table in the 64-bit regime from the hash of every entry,
    /// with room for one more; the entries are never re-read.
    fn widen(&mut self, hashes: &[u64]) -> &mut HashTable<usize> {
        let mut rebuilt = HashTable::with_capacity(hashes.len() + 1);
        for (index, &hash) in hashes.iter().enumerate() {
            rebuilt.insert_unique(hash, index, |&entry| hashes[entry]);
        }

        *self = Lookup::U64(rebuilt);
        match self {
            Lookup::U64(table) => table,
            Lookup::U32(_) => unreachable!("just assigned the 64-bit regime"),
        }
    }

    /// Returns the allocated bytes of the table.
    fn allocation_size(&self) -> usize {
        match self {
            Lookup::U32(table) => table.allocation_size(),
            Lookup::U64(table) => table.allocation_size(),
        }
    }
}

/// An interning pool of records that split into a small fixed header
/// and a variable byte string,
/// for example the index of a pair's frequent part beside its rare bytes,
/// or the sign and exponent of a wide number beside its magnitude.
///
/// Each distinct record is stored once
/// and identified by a dense index assigned in first-insertion order.
///
/// A read borrows the bytes from the arena, whose addresses are stable.
pub(crate) struct RecordPool<H> {
    /// The variable bytes of all entries.
    arena: Arena,
    /// Maps a record to its index.
    lookup: Lookup,
    /// The entries, in index order.
    records: Vec<Record<H>>,
    /// The hash of each entry, in index order.
    hashes: Vec<u64>,
    /// The hash builder shared by all lookups of this pool.
    build: DefaultHashBuilder,
}

impl<H: Copy + Eq + Hash> RecordPool<H> {
    /// Creates an empty pool.
    pub(crate) fn new() -> Self {
        Self {
            arena: Arena::new(),
            lookup: Lookup::U32(HashTable::new()),
            records: Vec::new(),
            hashes: Vec::new(),
            build: DefaultHashBuilder::default(),
        }
    }

    /// Returns the number of distinct records.
    pub(crate) fn len(&self) -> u64 {
        self.records.len() as u64
    }

    /// Interns a record and returns its index,
    /// or `None` if its bytes exceed the arena's limit.
    pub(crate) fn intern(&mut self, header: H, bytes: &[u8]) -> Option<u64> {
        let hash = self.build.hash_one((header, bytes));

        if let Some(index) = self.find_hashed(hash, header, bytes) {
            return Some(index as u64);
        }

        let index = self.records.len();
        let bytes = self.arena.store(bytes)?;

        self.records.push(Record { header, bytes });
        self.hashes.push(hash);
        self.lookup.insert_unique(hash, index, &self.hashes);

        Some(index as u64)
    }

    /// Returns the index of a record, or `None` if it was never interned.
    pub(crate) fn find(&self, header: H, bytes: &[u8]) -> Option<u64> {
        self.find_hashed(self.build.hash_one((header, bytes)), header, bytes)
            .map(|index| index as u64)
    }

    /// Returns the index of a record whose hash is known.
    fn find_hashed(&self, hash: u64, header: H, bytes: &[u8]) -> Option<usize> {
        self.lookup.find(hash, |index| {
            let record = self.records[index];
            record.header == header && self.arena.get(record.bytes) == bytes
        })
    }

    /// Returns the header and the bytes of an entry, or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<(H, &[u8])> {
        let record = *self.records.get(usize::try_from(index).ok()?)?;
        Some((record.header, self.arena.get(record.bytes)))
    }

    /// Rebuilds the pool with every header mapped through `map`;
    /// the bytes, the hashes and the table carry over as they are.
    ///
    /// A mapped header must hash as the original did,
    /// since the kept hashes serve the lookups of the new pool.
    pub(crate) fn map_headers<M: Copy + Eq + Hash>(self, map: impl Fn(H) -> M) -> RecordPool<M> {
        RecordPool {
            arena: self.arena,
            lookup: self.lookup,
            records: self
                .records
                .into_iter()
                .map(|record| Record {
                    header: map(record.header),
                    bytes: record.bytes,
                })
                .collect(),
            hashes: self.hashes,
            build: self.build,
        }
    }

    /// Returns the allocated bytes of the pool, the lookup table and the entry lists included.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.arena.memory_bytes()
            + self.lookup.allocation_size()
            + self.records.capacity() * size_of::<Record<H>>()
            + self.hashes.capacity() * size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_intern_by_header_and_bytes() {
        let mut pool = RecordPool::new();
        let first = pool.intern((true, 3i64), b"abc").expect("fits");
        let second = pool.intern((true, 3i64), b"abd").expect("fits");
        let third = pool.intern((false, 3i64), b"abc").expect("fits");

        assert_eq!(pool.len(), 3);
        assert_eq!(pool.intern((true, 3i64), b"abc"), Some(first));
        assert_ne!(first, second);
        assert_ne!(first, third);

        assert_eq!(pool.find((true, 3i64), b"abd"), Some(second));
        assert_eq!(pool.find((true, 4i64), b"abd"), None);
        assert_eq!(pool.get(third), Some(((false, 3i64), b"abc".as_slice())));
        assert_eq!(pool.get(3), None);
    }

    #[test]
    fn many_entries_survive_table_growth() {
        let mut pool = RecordPool::new();
        let indexes: Vec<u64> = (0..10_000u32)
            .map(|n| pool.intern(n % 7, n.to_string().as_bytes()).expect("fits"))
            .collect();

        for (n, index) in indexes.iter().enumerate() {
            let n = u32::try_from(n).expect("small");
            assert_eq!(pool.intern(n % 7, n.to_string().as_bytes()), Some(*index));
        }
        assert_eq!(pool.len(), 10_000);
    }

    /// The regime switch cannot be reached by inserting 2³² entries in a test,
    /// so the rebuild is driven directly at the point a pool would perform it.
    #[test]
    fn widened_tables_find_every_entry() {
        let mut pool = RecordPool::new();
        let indexes: Vec<u64> = (0..1_000u32)
            .map(|n| pool.intern(n % 3, n.to_string().as_bytes()).expect("fits"))
            .collect();
        assert!(matches!(pool.lookup, Lookup::U32(_)));

        pool.lookup.widen(&pool.hashes);
        assert!(matches!(pool.lookup, Lookup::U64(_)));
        let next = pool.intern(0, b"next").expect("fits");

        assert_eq!(next, 1_000);
        for (n, index) in indexes.iter().enumerate() {
            let n = u32::try_from(n).expect("small");
            assert_eq!(pool.find(n % 3, n.to_string().as_bytes()), Some(*index));
            assert_eq!(pool.intern(n % 3, n.to_string().as_bytes()), Some(*index));
        }
        assert_eq!(pool.find(0, b"next"), Some(next));
        assert_eq!(pool.len(), 1_001);
    }
}
