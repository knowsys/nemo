use std::collections::HashSet;
use std::hash::Hash;
use std::iter::IntoIterator;

pub trait InsertAll<C, T>
where
    C: Clone + IntoIterator<Item = T>,
    T: Clone + Hash + PartialEq + Eq,
{
    fn insert_all(&mut self, other: &C);
    fn insert_all_take(&mut self, other: C);
}

impl<C, T> InsertAll<C, T> for HashSet<T>
where
    C: Clone + IntoIterator<Item = T>,
    T: Clone + Hash + PartialEq + Eq,
{
    fn insert_all(&mut self, other: &C) {
        other.clone().into_iter().for_each(|item| {
            self.insert(item.clone());
        })
    }

    fn insert_all_take(&mut self, other: C) {
        other.into_iter().for_each(|item| {
            self.insert(item);
        })
    }
}

pub trait InsertAllRet<C, T>: InsertAll<C, T>
where
    C: Clone + IntoIterator<Item = T>,
    T: Clone + Hash + PartialEq + Eq,
{
    fn insert_all_ret(self, other: &C) -> HashSet<T>;
    fn insert_all_take_ret(self, other: C) -> HashSet<T>;
}

impl<C, T> InsertAllRet<C, T> for HashSet<T>
where
    C: Clone + IntoIterator<Item = T>,
    T: Clone + Eq + Hash + PartialEq,
{
    fn insert_all_ret(mut self, other: &C) -> HashSet<T> {
        self.insert_all(other);
        self
    }

    fn insert_all_take_ret(mut self, other: C) -> HashSet<T> {
        self.insert_all_take(other);
        self
    }
}

trait UnionRet<T>
where
    T: Clone + Eq + Hash + PartialEq,
{
    fn union_ret(&self, other: &HashSet<T>) -> HashSet<T>;
}

// TODO: DOES .CLONE() ON A COPY TYPE JUST COPY?
impl<T> UnionRet<T> for HashSet<T>
where
    T: Clone + Eq + Hash + PartialEq,
{
    fn union_ret(&self, other: &HashSet<T>) -> HashSet<T> {
        self.union(other).cloned().collect()
    }
}
