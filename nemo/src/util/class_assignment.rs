//! Module containing structures for representing functions over equivalence classes

use super::equivalence_class::{ClassId, EquivalenceRelation};

use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
};

/// Represents a partial function from an equivalence class to some target type.
#[derive(Debug, Clone)]
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

impl<ElementType, ValueType> Default for ClassAssignment<ElementType, ValueType>
where
    ElementType: Hash + Eq + Clone,
    ValueType: Eq + Clone,
{
    fn default() -> Self {
        Self {
            relation: Default::default(),
            assignment: Default::default(),
        }
    }
}

///
#[derive(Debug)]
pub enum ClassValue<'a, ElementType, ValueType> {
    /// Element is assigned to the following value.
    Assigned(&'a ValueType),
    /// Element is not assigned to any value
    /// and is member of the equivalence class with this representative.
    Unassigned(&'a ElementType),
}

impl<'a, ElementType, ValueType> ClassValue<'a, ElementType, ValueType> {
    pub fn is_assigned(&self) -> bool {
        matches!(self, ClassValue::Assigned(_))
    }
}

impl<ElementType, ValueType> ClassAssignment<ElementType, ValueType>
where
    ElementType: Hash + Eq + Clone,
    ValueType: Eq + Clone,
{
    /// Return the value assigned to the equivalence class of the given element.
    pub fn value<'a>(&'a self, element: &'a ElementType) -> ClassValue<'a, ElementType, ValueType> {
        if let Some(class_id) = self.relation.class(element) {
            if let Some(assigned_value) = self.assignment.get(&class_id) {
                return ClassValue::Assigned(assigned_value);
            } else {
                return ClassValue::Unassigned(self.relation.representative(class_id));
            }
        } else {
            return ClassValue::Unassigned(element);
        }
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
