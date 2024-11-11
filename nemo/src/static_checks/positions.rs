// TODO: POSSIBLY ALL OVER THAT MODULE: ONLY USE POSITIVE BODY ATOMS
// TODO: RECONSIDER USING SINGLE TUPLE STRUCTS
// TODO: CONSIDER USING REFERENCES IN SINGLE TUPLE STRUCTS
// TODO: CONSIDER IMPLEMENTING TRAIT INTO OR DEREF TO GET TO THE INNER OF THE SINGLE TUPLE STRUCT
use crate::rule_model::components::tag::Tag;
use std::collections::{
    hash_map::{Entry, Keys},
    HashMap, HashSet,
};

#[derive(Debug, Clone)]
pub struct Positions(HashMap<Tag, HashSet<usize>>);

impl Default for Positions {
    fn default() -> Self {
        Positions::new()
    }
}

impl From<HashMap<Tag, HashSet<usize>>> for Positions {
    fn from(positions: HashMap<Tag, HashSet<usize>>) -> Self {
        Positions(positions)
    }
}

impl Positions {
    fn contains_key(&self, predicate: &Tag) -> bool {
        self.0.contains_key(predicate)
    }

    // NOTE: MAYBE USE difference() OF HashSet
    pub fn difference(self, positions: &Positions) -> Positions {
        positions
            .iter()
            .fold(self, |mut differenced_pos, (pred, pos_indices)| {
                differenced_pos
                    .entry(pred.clone())
                    .and_modify(|differenced_indices| {
                        pos_indices.iter().for_each(|i| {
                            differenced_indices.remove(i);
                        });
                    });
                differenced_pos
            })
    }

    // NOTE: SAME DIFFERENCE FUNCTION BUT CHANGES SELF TO THE DIFFERENCED POSITIONS
    // pub fn difference(&mut self, positions: &Positions) {
    //     positions.iter().for_each(|(pred, pos_indices)| {
    //         self.entry(pred.clone()).and_modify(|self_indices| {
    //             pos_indices.iter().for_each(|i| {
    //                 self_indices.remove(i);
    //             })
    //         });
    //         if self.get_predicate_and_unwrap(pred).is_empty() {
    //             self.remove(pred);
    //         }
    //     });
    // }

    pub fn entry(&mut self, pred: Tag) -> Entry<Tag, HashSet<usize>> {
        self.0.entry(pred)
    }

    fn get(&self, predicate: &Tag) -> Option<&HashSet<usize>> {
        self.0.get(predicate)
    }

    fn get_mut(&mut self, predicate: &Tag) -> Option<&mut HashSet<usize>> {
        self.0.get_mut(predicate)
    }

    pub fn get_predicate_and_unwrap(&self, predicate: &Tag) -> &HashSet<usize> {
        self.get(predicate).unwrap()
    }

    pub fn get_predicate_and_unwrap_mut(&mut self, predicate: &Tag) -> &mut HashSet<usize> {
        self.get_mut(predicate).unwrap()
    }

    pub fn insert(
        &mut self,
        predicate: Tag,
        positions_in_predicate: HashSet<usize>,
    ) -> Option<HashSet<usize>> {
        self.0.insert(predicate, positions_in_predicate)
    }

    pub fn is_disjoint(&self, positions: &Positions) -> bool {
        self.keys()
            .all(|pred| !positions.contains_key(pred) || self.pred_is_disjoint(pred, positions))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_superset(&self, positions: &Positions) -> bool {
        positions
            .keys()
            .all(|pred| self.contains_key(pred) && self.pred_is_superset(pred, positions))
    }

    pub fn iter(&self) -> std::collections::hash_map::Iter<Tag, HashSet<usize>> {
        self.0.iter()
    }

    #[warn(dead_code)]
    fn iter_mut(&mut self) -> std::collections::hash_map::IterMut<Tag, HashSet<usize>> {
        self.0.iter_mut()
    }

    #[warn(dead_code)]
    fn iter_of_pred(&self, pred: &Tag) -> std::collections::hash_set::Iter<usize> {
        self.get_predicate_and_unwrap(pred).iter()
    }

    fn keys(&self) -> Keys<Tag, HashSet<usize>> {
        self.0.keys()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn new() -> Self {
        Positions(HashMap::<Tag, HashSet<usize>>::new())
    }

    #[warn(dead_code)]
    fn pred_contains_index(&self, pred: &Tag, index: &usize) -> bool {
        self.get_predicate_and_unwrap(pred).contains(index)
    }

    fn pred_is_disjoint(&self, pred: &Tag, positions: &Positions) -> bool {
        self.get_predicate_and_unwrap(pred)
            .is_disjoint(positions.get_predicate_and_unwrap(pred))
    }

    pub fn pred_is_empty(&self, pred: &Tag) -> bool {
        self.get_predicate_and_unwrap(pred).is_empty()
    }

    fn pred_is_superset(&self, pred: &Tag, positions: &Positions) -> bool {
        self.get_predicate_and_unwrap(pred)
            .is_superset(positions.get_predicate_and_unwrap(pred))
    }

    #[warn(dead_code)]
    fn pred_remove_index(&mut self, pred: &Tag, index: &usize) -> bool {
        self.get_predicate_and_unwrap_mut(pred).remove(index)
    }

    pub fn remove(&mut self, pred: &Tag) -> Option<HashSet<usize>> {
        self.0.remove(pred)
    }

    // NOTE: MAYBE USE union() OF HashSet
    pub fn union(self, positions: &Positions) -> Positions {
        positions
            .iter()
            .fold(self, |mut unioned_pos, (pred, pos_indices)| {
                unioned_pos
                    .entry(pred.clone())
                    .and_modify(|unioned_indices| {
                        pos_indices.iter().for_each(|i| {
                            unioned_indices.insert(*i);
                        });
                    })
                    .or_insert(pos_indices.clone());
                unioned_pos
            })
    }

    // NOTE: SAME UNION FUNCTION BUT CHANGES SELF TO THE UNIONED POSITIONS
    // fn union(&mut self, positions: &Positions) {
    //     positions.iter().for_each(|(pred, pos_indices)| {
    //         self.entry(pred.clone())
    //             .and_modify(|self_indices| {
    //                 pos_indices.iter().for_each(|i| {
    //                     self_indices.insert(*i);
    //                 });
    //             })
    //             .or_insert(pos_indices.clone());
    //     });
    // }
}
