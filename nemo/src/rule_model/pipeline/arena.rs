//! This module defines a single threaded `Arena` allocator supporting interior mutability

use std::{
    hash::Hash,
    marker::PhantomData,
    ops::Index,
    sync::atomic::{self, AtomicUsize},
};

use orx_imp_vec::{ImpVec, PinnedVec};

#[derive(Debug, Clone)]
/// A single threaded arena allocator.
///
/// Items are referred to by [`Id<T>`].
/// Allocation can be performed through a shared reference,
/// because it uses a [`PinnedVec`] as backing structure.
pub struct Arena<T> {
    elements: ImpVec<T>,
    id: u32,
}

impl<T> Default for Arena<T> {
    fn default() -> Self {
        Self {
            elements: Default::default(),
            id: Self::new_arena_id(),
        }
    }
}

impl<T> Index<Id<T>> for Arena<T> {
    type Output = T;

    fn index(&self, index: Id<T>) -> &Self::Output {
        if index.arena != self.id {
            panic!("wrong arena")
        }

        &self.elements[index.offset]
    }
}

impl<T> Arena<T> {
    /// Returns the [`Id`] that will be returned by the next call to [`Self::alloc`].
    pub fn next_id(&self) -> Id<T> {
        Id {
            offset: self.elements.len(),
            arena: self.id,
            _phantom: PhantomData,
        }
    }

    /// Allocates an element and returns an [`Id`] pointing to it.
    pub fn alloc(&self, element: T) -> Id<T> {
        let result = self.next_id();
        self.elements.imp_push(element);
        result
    }

    /// Construct a new arena identifier.
    ///
    /// This is used to disambiguate `Id`s across different arenas. To make
    /// identifiers with the same index from different arenas compare false for
    /// equality, return a unique `u32` on every invocation. This is the
    /// default, provided implementation's behavior.
    ///
    /// To make identifiers with the same index from different arenas compare
    /// true for equality, return the same `u32` on every invocation.
    fn new_arena_id() -> u32 {
        static ARENA_COUNTER: AtomicUsize = AtomicUsize::new(0);
        ARENA_COUNTER.fetch_add(1, atomic::Ordering::SeqCst) as u32
    }
}

impl<T> IntoIterator for Arena<T> {
    type Item = T;
    type IntoIter = <ImpVec<T> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter()
    }
}

#[derive(Debug)]
/// Refers to an element of an [`Arena`].
pub struct Id<T> {
    /// The offset of the element.
    offset: usize,
    /// The id of the arena this element belongs to.
    arena: u32,

    _phantom: PhantomData<fn() -> T>,
}

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset && self.arena == other.arena
    }
}

impl<T> Eq for Id<T> {}

impl<T> Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.offset.hash(state);
        self.arena.hash(state);
    }
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Id<T> {}

impl<T> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.arena.partial_cmp(&other.arena) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }

        self.offset.partial_cmp(&other.offset)
    }
}
