use std::collections::HashSet;
use std::hash::Hash;

pub trait InsertAll<T>
where
    T: Clone + Hash + PartialEq + Eq,
{
    fn insert_all(&mut self, other: &HashSet<T>);
}

impl<T> InsertAll<T> for HashSet<T>
where
    T: Clone + Hash + PartialEq + Eq,
{
    fn insert_all(&mut self, other: &HashSet<T>) {
        other.iter().for_each(|item| {
            self.insert(item.clone());
        })
    }
}

pub trait InsertAllRet<T>: InsertAll<T>
where
    T: Clone + Hash + PartialEq + Eq,
{
    fn insert_all_ret(self, other: &HashSet<T>) -> HashSet<T>;
}

impl<T> InsertAllRet<T> for HashSet<T>
where
    T: Clone + Eq + Hash + PartialEq,
{
    fn insert_all_ret(mut self, other: &HashSet<T>) -> HashSet<T> {
        self.insert_all(other);
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
