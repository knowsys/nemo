use crate::rule_model::components::tag::Tag;
use crate::static_checks::positions::{Indices, Positions};

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

impl<'a> InsertAll<Positions<'a>, (&'a Tag, Indices)> for Positions<'a> {
    fn insert_all(&mut self, other: &Positions<'a>) {
        other.iter().for_each(|(pred, other_indices)| {
            if !self.contains_key(pred) {
                self.insert(pred, Indices::new());
            }
            let unioned_indices: &mut Indices = self.get_mut(pred).unwrap();
            unioned_indices.insert_all(other_indices);
        })
    }

    fn insert_all_ret(mut self, other: &Positions<'a>) -> Positions<'a> {
        self.insert_all(other);
        self
    }

    fn insert_all_take(&mut self, other: Positions<'a>) {
        other.into_iter().for_each(|(pred, other_indices)| {
            if !self.contains_key(pred) {
                self.insert(pred, Indices::new());
            }
            let unioned_indices: &mut Indices = self.get_mut(pred).unwrap();
            unioned_indices.insert_all_take(other_indices);
        })
    }

    fn insert_all_take_ret(mut self, other: Positions<'a>) -> Positions<'a> {
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

impl<'a> RemoveAll<Positions<'a>, (&'a Tag, Indices)> for Positions<'a> {
    fn remove_all(&mut self, other: &Positions<'a>) {
        other.iter().for_each(|(pred, other_indices)| {
            if let Some(differenced_indices) = self.get_mut(pred) {
                differenced_indices.remove_all(other_indices);
            }
        })
    }

    fn remove_all_ret(mut self, other: &Positions<'a>) -> Positions<'a> {
        self.remove_all(other);
        self
    }

    fn remove_all_take(&mut self, other: Positions<'a>) {
        other.into_iter().for_each(|(pred, other_indices)| {
            if let Some(differenced_indices) = self.get_mut(pred) {
                differenced_indices.remove_all_take(other_indices);
            }
        })
    }

    fn remove_all_take_ret(mut self, other: Positions<'a>) -> Positions<'a> {
        self.remove_all_take(other);
        self
    }
}

// trait Union<C, T>: InsertAll<C, T> {
//     fn union(&mut self, other: &C);
//     fn union_ret(self, other: &C) -> HashSet<T>;
//     fn union_take(&mut self, other: C);
//     fn union_take_ret(self, other: C) -> HashSet<T>;
// }

// impl<C, T> Union<C, T> for HashSet<T>
// where
//     C: Clone + IntoIterator<Item = T>,
//     T: Clone + Hash + PartialEq + Eq,
// {
//     fn union(&mut self, other: &C) {
//         self.insert_all(other)
//     }
//
//     fn union_ret(self, other: &C) -> HashSet<T> {
//         self.insert_all_ret(other)
//     }
//
//     fn union_take(&mut self, other: C) {
//         self.insert_all_take(other)
//     }
//
//     fn union_take_ret(self, other: C) -> HashSet<T> {
//         self.insert_all_take_ret(other)
//     }
// }
