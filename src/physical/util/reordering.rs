use std::{
    collections::HashSet,
    fmt::{self, Debug},
    ops::Index,
};

/// Type which represents a reordering of some list of things.
/// For example, `{ reorder: [2, 0], len_source: 4 }` would be interpreted as
/// "Take the third then the first element (and dont change the second and fourth one)".
#[derive(PartialEq, Eq, Clone)]
pub struct Reordering {
    reorder: Vec<usize>,
    len_source: usize,
}

impl Reordering {
    /// Create new [`Reordering`].
    pub fn new(reorder: Vec<usize>, len_source: usize) -> Self {
        debug_assert!(reorder.iter().all(|&r| r < len_source));
        debug_assert!(!reorder.is_empty());

        Self {
            reorder,
            len_source,
        }
    }

    /// Return the default [`Reordering`]
    pub fn default(len_source: usize) -> Self {
        Self::new((0..len_source).collect(), len_source)
    }

    /// Derive a [`Reordering`] that would transform a vector of elements into another.
    /// I.e. `this.apply_to(source) = target`
    /// For example `from_transformation([x, y, z, w], [z, w, y, x]) = [2, 3, 1, 0]`.
    pub fn from_transformation<T: PartialEq>(source: &[T], target: &[T]) -> Self {
        Self::new(
            target
                .iter()
                .map(|t| {
                    source
                        .iter()
                        .position(|s| *s == *t)
                        .expect("We expect that target only uses elements from source.")
                })
                .collect(),
            source.len(),
        )
    }

    /// Apply the permutation represented by this column order to reorder a vector.
    pub fn apply_to<T: Clone>(&self, vec: &[T]) -> Vec<T> {
        debug_assert!(vec.len() >= self.len_source());

        self.iter().map(|&i| vec[i].clone()).collect()
    }

    /// Apply the
    pub fn apply_element(&self, element: usize) -> usize {
        self.reorder[element]
    }

    pub fn apply_element_reverse(&self, value: usize) -> usize {
        *self.reorder.iter().find(|&i| value == *i).unwrap()
    }

    /// Returns the amount of elements before the reordering.
    pub fn len_source(&self) -> usize {
        self.len_source
    }

    /// Returns the amount of elements after the reordering.
    pub fn len_target(&self) -> usize {
        self.reorder.len()
    }

    /// Return an iterator over the contents of this reordering.
    pub fn iter(&self) -> impl Iterator<Item = &usize> {
        self.reorder.iter()
    }

    /// Returns whether or not this [`Reordering`] is a permutation.
    /// I.e. whether source and target length a equal and every element is only used once.
    pub fn is_permutation(&self) -> bool {
        if self.len_source() != self.len_target() {
            return false;
        }

        let mut unique_entries = HashSet::<usize>::new();
        for &element in self.iter() {
            if !unique_entries.insert(element) {
                return false;
            }
        }

        true
    }

    /// Returns whether or not this [`Reordering`] is the identity function.
    /// I.e. whether it maps every element to itself.
    pub fn is_identity(&self) -> bool {
        if self.len_source() != self.len_target() {
            return false;
        }

        self.iter()
            .enumerate()
            .all(|(index, element)| index == *element)
    }

    /// Turn into a vector representation of the reordering.
    pub fn to_vector(self) -> Vec<usize> {
        self.reorder
    }

    /// Chain together two [`Reordering`]s.
    /// Applying the result of this is equivalent to applying `other` after applying `this`.
    /// For example, `[1, 0, 2].chain([2, 0, 1]) = [2, 1, 0]`.
    pub fn chain(&self, other: &Self) -> Self {
        debug_assert!(self.len_target() >= other.len_source());

        let new_reorder = other.iter().map(|&o| self[o]).collect::<Vec<usize>>();
        let new_len_source = self.len_source();

        Self::new(new_reorder, new_len_source)
    }

    /// Calculates the [`Reordering`] that would reverse the effects of the current one.
    /// I.e. this is the result of the equation `this.chain(x) == Reordering::default()`.
    /// For example `[2, 0, 1].inverse() = [1, 2, 0]`.
    /// Note: This operation is only sensible for [`Reordering`]s that are permutations.
    pub fn inverse(&self) -> Self {
        debug_assert!(self.is_permutation());

        let identity: Vec<usize> = (0..self.reorder.len()).collect();

        Self::from_transformation(&self.reorder, &identity)
    }
}

impl Index<usize> for Reordering {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        &self.reorder[index]
    }
}

impl Debug for Reordering {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reorder.fmt(f)
    }
}

#[cfg(test)]
mod test {
    use super::Reordering;

    #[test]
    fn test_from_transformation() {
        let vec_source: Vec<i32> = vec![-1, -5, 10, 3, 4];
        let vec_target: Vec<i32> = vec![-5, 3, 3, -1];

        let reordering = Reordering::from_transformation(&vec_source, &vec_target);

        assert_eq!(reordering, Reordering::new(vec![1, 3, 3, 0], 5));
    }

    #[test]
    fn test_apply_to() {
        let vec_source: Vec<i32> = vec![-1, -5, 10, 7, 8];
        let vec_target: Vec<i32> = vec![8, 7, -1];

        let reordering = Reordering::new(vec![4, 3, 0], 5);

        assert_eq!(reordering.apply_to(&vec_source), vec_target);
    }

    #[test]
    fn test_chain() {
        let first = Reordering::new(vec![1, 0, 2], 4);
        let second = Reordering::new(vec![2, 0, 1], 3);

        assert_eq!(first.chain(&second), Reordering::new(vec![2, 1, 0], 4));
    }

    #[test]
    fn test_inverse() {
        let reordering = Reordering::new(vec![2, 0, 1], 3);
        assert!(reordering.is_permutation());

        assert_eq!(reordering.inverse(), Reordering::new(vec![1, 2, 0], 3));
    }
}
