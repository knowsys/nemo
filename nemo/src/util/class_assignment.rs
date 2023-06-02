//! Module containing structures for representing functions over equivalence classes

use super::equivalence_class::{ClassId, EquivalenceRelation};

use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
};

/// Represents a partial function from an equivalence class to some target type.
#[derive(Debug, Default, Clone)]
pub struct ClassAssignment<ElementType, ValueType>
where
    ElementType: Hash + Eq + Clone,
    ValueType: Eq + Clone,
{
    /// Equivalence relation over the `ElementType`.
    relation: EquivalenceRelation<ElementType>,
    /// Assigns values of the `ValueType` to equivalence classes.
    /// If a class is not contained in this map
    /// then it is intepreted as "unassigned".
    assignment: HashMap<ClassId, ValueType>,
}

impl<ElementType, ValueType> ClassAssignment<ElementType, ValueType>
where
    ElementType: Hash + Eq + Clone,
    ValueType: Eq + Clone,
{
    /// Return the underlying equivalence relation.
    pub fn relation(&self) -> &EquivalenceRelation<ElementType> {
        &self.relation
    }

    /// Return the value assigned to the equivalence class of the given element.
    pub fn value(&self, element: &ElementType) -> Option<&ValueType> {
        let class_id = self.relation.class(element)?;

        self.assignment.get(&class_id)
    }

    /// Assign a value to a given equivalence class.
    ///
    /// Returns `true` if there is no conflict caused by the new assignment,
    /// i.e. whether the equivalence class of the element was previously not
    /// assigned to a value or to the same value as given to this function.
    /// Returns `false` otherwise.
    pub fn assign_value(&mut self, element: &ElementType, value: ValueType) -> bool {
        let (_, class_id) = self.relation.add_element(element.clone());

        match self.assignment.entry(class_id) {
            Entry::Occupied(entry) => *entry.get() == value,
            Entry::Vacant(entry) => {
                entry.insert(value);
                true
            }
        }
    }

    /// Merge equivalence class of `element_a` into the equivalence class of `element_b`.
    /// Returns `true` if there is no conflict between the assignments of those two classes
    /// and `false` otherwise.
    pub fn merge_classes(&mut self, element_a: &ElementType, element_b: &ElementType) -> bool {
        let (_, class_a) = self.relation.add_element(element_a.clone());
        let (_, class_b) = self.relation.add_element(element_b.clone());

        if class_a == class_b {
            return true;
        }

        if let Some(value_b) = self.assignment.get(&class_b) {
            if let Some(value_a) = self.assignment.get(&class_a) {
                if value_a != value_b {
                    return false;
                }
            }

            self.assignment.insert(class_a, value_b.clone());
        }

        self.relation.merge_classes(class_a, class_b);
        true
    }
}
