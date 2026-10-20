//! Append-only byte arenas with stable addresses.

/// The size of an arena chunk in bytes.
///
/// A byte string that does not fit the tail of a chunk leaves the tail unused.
/// A string longer than a chunk gets a chunk of its own size.
const CHUNK_SIZE: usize = 64 * 1024;

/// The location of a byte string within an [Arena].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Extent {
    /// The index of the chunk holding the bytes.
    chunk: u32,
    /// The byte offset within the chunk.
    offset: u32,
    /// The length in bytes.
    length: u32,
}

/// An append-only byte arena.
///
/// The bytes live in chunks that are never reallocated,
/// so a stored byte string keeps its address for the arena's lifetime,
/// which lets the dictionary hand out borrowed reads.
/// A byte string that does not fit the tail of the last chunk opens a new one,
/// sized to the string if it exceeds a whole chunk.
pub(crate) struct Arena {
    /// The chunks; only the last one receives new bytes.
    chunks: Vec<Box<[u8]>>,
    /// The number of bytes used in the last chunk.
    used: usize,
}

impl Arena {
    /// The longest byte string the arena stores:
    /// the length of a [Extent] has 32 bits.
    pub(crate) const LENGTH_LIMIT: usize = u32::MAX as usize;

    /// Creates an empty arena.
    pub(crate) fn new() -> Self {
        Self {
            chunks: Vec::new(),
            used: 0,
        }
    }

    /// Stores a byte string and returns its location,
    /// or `None` for a string longer than [LENGTH_LIMIT](Self::LENGTH_LIMIT).
    pub(crate) fn store(&mut self, bytes: &[u8]) -> Option<Extent> {
        let length = u32::try_from(bytes.len()).ok()?;
        if length == 0 {
            // The empty byte string needs no chunk; `get` resolves it directly.
            return Some(Extent {
                chunk: 0,
                offset: 0,
                length: 0,
            });
        }

        let remaining = self
            .chunks
            .last()
            .map_or(0, |chunk| chunk.len() - self.used);
        if bytes.len() > remaining {
            self.chunks
                .push(vec![0; CHUNK_SIZE.max(bytes.len())].into_boxed_slice());
            self.used = 0;
        }

        let chunk =
            u32::try_from(self.chunks.len() - 1).expect("an arena has fewer than 2³² chunks");
        let offset =
            u32::try_from(self.used).expect("an offset is below the chunk's length, which fits");

        let target = self.chunks.last_mut().expect("a chunk was just ensured");
        target[self.used..self.used + bytes.len()].copy_from_slice(bytes);
        self.used += bytes.len();

        Some(Extent {
            chunk,
            offset,
            length,
        })
    }

    /// Returns the bytes at a location the arena returned from [store](Self::store).
    pub(crate) fn get(&self, extent: Extent) -> &[u8] {
        if extent.length == 0 {
            return &[];
        }
        let start = extent.offset as usize;
        &self.chunks[extent.chunk as usize][start..start + extent.length as usize]
    }

    /// Returns the allocated bytes, including the unused chunk tails.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.len()).sum::<usize>()
            + self.chunks.capacity() * size_of::<Box<[u8]>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_bytes_are_retrievable() {
        let mut arena = Arena::new();
        let hello = arena.store(b"hello").expect("fits");
        let empty = arena.store(b"").expect("fits");
        let world = arena.store(b"world").expect("fits");

        assert_eq!(arena.get(hello), b"hello");
        assert_eq!(arena.get(empty), b"");
        assert_eq!(arena.get(world), b"world");
    }

    #[test]
    fn chunk_transitions_keep_earlier_spans_valid() {
        let mut arena = Arena::new();
        let first = arena.store(b"first").expect("fits");

        // Several chunk transitions, including oversize chunks.
        let big = vec![7u8; CHUNK_SIZE + 100];
        let extents: Vec<Extent> = (0..5).map(|_| arena.store(&big).expect("fits")).collect();

        assert_eq!(arena.get(first), b"first");
        for extent in extents {
            assert_eq!(arena.get(extent), &big[..]);
        }
        assert!(arena.memory_bytes() >= 5 * big.len());
    }
}
