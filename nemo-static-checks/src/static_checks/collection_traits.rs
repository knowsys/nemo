//! Functionality that provides generalised methods for collections.
use std::collections::HashSet;
use std::hash::Hash;
use std::iter::IntoIterator;

/// This Trait provides methods for collections to insert every element of another collection.
pub trait InsertAll<C, T> {
    /// Inserts all elements of another collection into the current collection.
    fn insert_all(&mut self, other: &C);
    /// Inserts all elements of another collection into the current collection and returns it.
    fn insert_all_ret(self, other: &C) -> Self;
    /// Inserts all elements of a consumed collection into the current collection.
    fn insert_all_take(&mut self, other: C);
    /// Inserts all elements of a consumed collection into the current collection and returns it.
    fn insert_all_take_ret(self, other: C) -> Self;
}

// NOTE: HASH NEEDED FOR T?
impl<T> InsertAll<Vec<T>, T> for Vec<T>
where
    T: Clone + Eq + PartialEq,
{
    fn insert_all(&mut self, other: &Vec<T>) {
        other.iter().for_each(|item| {
            self.push(item.clone());
        })
    }

    fn insert_all_ret(mut self, other: &Vec<T>) -> Vec<T> {
        self.insert_all(other);
        self
    }

    fn insert_all_take(&mut self, other: Vec<T>) {
        other.into_iter().for_each(|item| {
            self.push(item);
        })
    }

    fn insert_all_take_ret(mut self, other: Vec<T>) -> Vec<T> {
        self.insert_all_take(other);
        self
    }
}

impl<T> InsertAll<HashSet<T>, T> for HashSet<T>
where
    T: Clone + Eq + Hash + PartialEq,
{
    fn insert_all(&mut self, other: &HashSet<T>) {
        other.iter().for_each(|item| {
            self.insert(item.clone());
        })
    }

    fn insert_all_ret(mut self, other: &HashSet<T>) -> HashSet<T> {
        self.insert_all(other);
        self
    }

    fn insert_all_take(&mut self, other: HashSet<T>) {
        other.into_iter().for_each(|item| {
            self.insert(item);
        })
    }

    fn insert_all_take_ret(mut self, other: HashSet<T>) -> HashSet<T> {
        self.insert_all_take(other);
        self
    }
}

impl<T> InsertAll<Vec<T>, T> for HashSet<T>
where
    T: Clone + Eq + Hash + PartialEq,
{
    fn insert_all(&mut self, other: &Vec<T>) {
        other.iter().for_each(|item| {
            self.insert(item.clone());
        })
    }

    fn insert_all_ret(mut self, other: &Vec<T>) -> HashSet<T> {
        self.insert_all(other);
        self
    }

    fn insert_all_take(&mut self, other: Vec<T>) {
        other.into_iter().for_each(|item| {
            self.insert(item);
        })
    }

    fn insert_all_take_ret(mut self, other: Vec<T>) -> HashSet<T> {
        self.insert_all_take(other);
        self
    }
}

/// This Trait provides methods for collections to remove every element of another collection.
#[allow(dead_code)]
pub trait RemoveAll<C, T> {
    /// Removes all elements of another collection from the current collection.
    fn remove_all(&mut self, other: &C);
    /// Removes all elements of another collection from the current collection and returns it.
    fn remove_all_ret(self, other: &C) -> Self;
    /// Removes all elements of a consumed collection from the current collection.
    fn remove_all_take(&mut self, other: C);
    /// Removes all elements of a consumed collection from the current collection and returns it.
    fn remove_all_take_ret(self, other: C) -> Self;
}

impl<T> RemoveAll<HashSet<T>, T> for HashSet<T>
where
    T: Clone + Eq + Hash + PartialEq,
{
    fn remove_all(&mut self, other: &HashSet<T>) {
        other.iter().for_each(|item| {
            self.remove(item);
        })
    }

    fn remove_all_ret(mut self, other: &HashSet<T>) -> HashSet<T> {
        self.remove_all(other);
        self
    }

    fn remove_all_take(&mut self, other: HashSet<T>) {
        other.into_iter().for_each(|item| {
            self.remove(&item);
        })
    }

    fn remove_all_take_ret(mut self, other: HashSet<T>) -> HashSet<T> {
        self.remove_all_take(other);
        self
    }
}

/// This Trait provides a method to look for a superset relation between two instances for some type.
pub trait Superset {
    /// Checks if there is a superset relation between two instances for some type.
    fn is_superset(&self, other: &Self) -> bool;
}

/// This Trait provides a method to look for a disjoint relation between two instances for some type.
pub trait Disjoint {
    /// Checks if there is a disjoint relation between two instances for some type
    fn is_disjoint(&self, other: &Self) -> bool;
}
