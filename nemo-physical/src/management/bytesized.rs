//! This module defines the trait [ByteSized],
//! which should be implemented by types that can
//! calculate their own size.

/// Objects that are able calculate their current approximate size in bytes.
///
/// We use `u64` rather than `usize` here to avoid overflows in case of overestimations.
pub trait ByteSized {
    /// Return the number of bytes this object consumes
    fn size_bytes(&self) -> u64;
}

/// Estimates the memory required for managing the content of a Hashbrown hashmap using only
/// the direct size of keys and values, without taking into accont any data they might point to.
///
/// The computation is approximate since the hashmap does not provide access to its current bucket
/// structure and control byte overhead, so we merely consider the reported capacity.
pub(crate) fn size_inner_hashmap_flat<K, V>(object: &hashbrown::HashMap<K, V>) -> u64 {
    object.capacity() as u64 * size_of::<(K, V)>() as u64
}

/// Computes the memory required for managing the content of a vector using only
/// the direct size of content objects, without taking into accont any data they might point to.
pub(crate) fn size_inner_vec_flat<T>(object: &Vec<T>) -> u64 {
    object.capacity() as u64 * size_of::<T>() as u64
}
