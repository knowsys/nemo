use std::collections::HashSet;
use std::hash::Hash;
use std::iter::IntoIterator;

pub trait InsertAll<C, T> {
    fn insert_all(&mut self, other: &C);
    fn insert_all_ret(self, other: &C) -> Self;
    fn insert_all_take(&mut self, other: C);
    fn insert_all_take_ret(self, other: C) -> Self;
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

pub trait RemoveAll<C, T> {
    fn remove_all(&mut self, other: &C);
    fn remove_all_ret(self, other: &C) -> Self;
    fn remove_all_take(&mut self, other: C);
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

pub trait Superset {
    fn is_superset(&self, other: &Self) -> bool;
}

pub trait Disjoint {
    fn is_disjoint(&self, other: &Self) -> bool;
}
