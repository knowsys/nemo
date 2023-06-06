//! Contains structures that define an equivalence relation over some set of values

use std::collections::{HashMap, HashSet};

/// Numeric handle for elements in a [`EquivalenceRelation`].
pub type ElementId = usize;
/// Represents an equivalence class in a [`EquivalenceRelation`].
pub type ClassId = usize;

/// Represents an equivalence relation over a set of elements.
#[derive(Debug, Clone)]
pub struct EquivalenceRelation<TypeElements>
where
    TypeElements: Eq + Clone,
{
    elements: Vec<TypeElements>,

    element_to_class: HashMap<ElementId, ClassId>,
    class_to_elements: HashMap<ClassId, HashSet<ElementId>>,
    class_to_representative: HashMap<ClassId, ElementId>,

    current_class: ClassId,
}

impl<TypeElements> Default for EquivalenceRelation<TypeElements>
where
    TypeElements: Eq + Clone,
{
    fn default() -> Self {
        Self {
            elements: Default::default(),
            element_to_class: Default::default(),
            class_to_elements: Default::default(),
            class_to_representative: Default::default(),
            current_class: Default::default(),
        }
    }
}

impl<TypeElements> EquivalenceRelation<TypeElements>
where
    TypeElements: Eq + Clone,
{
    /// Return a new unused [`ClassId`].
    fn new_class(&mut self) -> ClassId {
        let result = self.current_class;
        self.current_class += 1;

        result
    }

    /// Get the [`ElementId`] which is associated with the given `elemenet`,
    /// if the element has already been registered.
    /// Returns `None` otherwise.
    fn find_element(&self, element: &TypeElements) -> Option<ElementId> {
        self.elements.iter().position(|e| e == element)
    }

    /// Put an element into a given class.
    fn assign_class(&mut self, element_id: ElementId, class_id: ClassId) {
        let present = self.element_to_class.insert(element_id, class_id);
        debug_assert!(present.is_none());

        let members = self.class_to_elements.entry(class_id).or_default();
        members.insert(element_id);
    }

    /// Assign the given element its unique [`ElementId`].
    /// This function does not assign the new element to a class.
    fn register_element(&mut self, element: TypeElements) -> ElementId {
        let element_id = self.elements.len();
        self.elements.push(element);

        element_id
    }

    /// Adds an element into the relation.
    /// Before adding, this function checks whether the element is already present.
    /// If the element is new then it will be assigned to a new class.
    /// Returns the [`ElementId`] and the [`ClassId`] of the added element.
    pub fn add_element(&mut self, element: TypeElements) -> (ElementId, ClassId) {
        if let Some(element_id) = self.find_element(&element) {
            let class_id = *self
                .element_to_class
                .get(&element_id)
                .expect("Every element is assigned to a class");

            return (element_id, class_id);
        }

        let element_id = self.register_element(element);

        let new_class = self.new_class();
        self.assign_class(element_id, new_class);
        self.class_to_representative.insert(new_class, element_id);

        (element_id, new_class)
    }

    /// Add a (potentially new) element to an existing class.
    /// If the element already exists,
    /// this will overwrite the previous class assignment of the element.
    pub fn add_element_to_class(&mut self, element: TypeElements, class_id: ClassId) -> ElementId {
        let element_id = match self.find_element(&element) {
            Some(element_id) => {
                let current_class = self.element_to_class.get(&element_id).expect("Since the element is contained in self.elements it must also be included in the maps");
                self.class_to_elements
                    .get_mut(current_class)
                    .expect("An assigned class must exist in both maps")
                    .remove(&element_id);

                element_id
            }
            None => self.register_element(element),
        };

        self.assign_class(element_id, class_id);
        element_id
    }

    /// Return the class associated with a given element.
    pub fn class(&self, element: &TypeElements) -> Option<ClassId> {
        let element_id = self.find_element(element)?;

        Some(
            *self
                .element_to_class
                .get(&element_id)
                .expect("Every registered element must be assigned to a class"),
        )
    }

    /// Return a representative of a given equivalence class
    ///
    /// # Panics
    /// Panics if the provided class does not exist.
    pub fn representative(&self, class: ClassId) -> &TypeElements {
        &self.elements[*self
            .class_to_representative
            .get(&class)
            .expect("Function assumes that class exists.")]
    }

    /// Merges `class_b` into `class_a`.
    ///
    /// # Panics
    /// Panics if one of the provided classes does not exist.
    pub fn merge_classes(&mut self, class_a: ClassId, class_b: ClassId) {
        let class_b_elements = self
            .class_to_elements
            .get(&class_b)
            .expect("Function assumes that the class exists")
            .clone();

        for &element_id_b in &class_b_elements {
            self.element_to_class.insert(element_id_b, class_a);
        }

        self.class_to_elements
            .get_mut(&class_a)
            .expect("Function assumes that the class exists")
            .extend(&class_b_elements);
    }

    /// Merges the class from `element_b` into the class from `element_a`
    /// (if they are not the same).
    ///
    /// # Panics
    /// Panics if the elements do not exist in the relation.
    pub fn merge_elements(&mut self, element_a: &TypeElements, element_b: &TypeElements) {
        let element_a_id = self
            .find_element(&element_a)
            .expect("Function assumes that element has already been added.");
        let element_b_id = self
            .find_element(&element_b)
            .expect("Function assumes that element has already been added.");

        let class_a = *self
            .element_to_class
            .get(&element_a_id)
            .expect("Every element is assigned to a class");
        let class_b = *self
            .element_to_class
            .get(&element_b_id)
            .expect("Every element is assigned to a class");

        self.merge_classes(class_a, class_b);
    }

    /// Return an iterator over all memebers of a given class.
    pub fn class_members(&self, class_id: ClassId) -> impl Iterator<Item = &TypeElements> {
        self.class_to_elements
            .get(&class_id)
            .expect("Class is assumed to exist")
            .iter()
            .map(|&id| &self.elements[id])
    }

    /// Return an iterator over all members of the same equivalence class as the provided element.
    ///
    /// # Panics
    /// Panics if the element does not exist.
    pub fn class_mates(&self, element: TypeElements) -> impl Iterator<Item = &TypeElements> {
        let element_id = self
            .find_element(&element)
            .expect("Element is assumed to already be prsent");

        let class_id = self
            .element_to_class
            .get(&element_id)
            .expect("Every element is assigned a class");

        self.class_to_elements
            .get(class_id)
            .expect("Assigned classes exist in all maps")
            .iter()
            .map(|&id| &self.elements[id])
    }

    /// Return an iterator over all members of the same equivalence class as the provided element
    /// except the provided element itself.
    ///
    /// # Panics
    /// Panics if the element does not exist.
    pub fn class_mates_others(&self, element: TypeElements) -> impl Iterator<Item = &TypeElements> {
        let element_id = self
            .find_element(&element)
            .expect("Element is assumed to already be prsent");

        let class_id = self
            .element_to_class
            .get(&element_id)
            .expect("Every element is assigned a class");

        self.class_to_elements
            .get(class_id)
            .expect("Assigned classes exist in all maps")
            .iter()
            .filter(move |&&id| id != element_id)
            .map(|&id| &self.elements[id])
    }
}
