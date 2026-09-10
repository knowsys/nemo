use super::bitset::BitSet;
use super::tuples::Tuples;

pub struct Relation {
    pub arity: usize,
    pub tuple_count: usize,

    /// [variable][value]
    pub supports: Vec<Vec<BitSet>>,
}

impl Relation {
    pub fn from_flat_tuples(tuples: &Tuples, vertex_count: usize) -> Self {
        debug_assert!(!tuples.data.is_empty());

        let arity = tuples[0].len();

        let tuple_count = tuples.len();

        let mut supports = vec![vec![BitSet::new(tuple_count); vertex_count]; arity];

        for (tid, tuple) in tuples.iter().enumerate() {
            for (var, &value) in tuple.iter().enumerate() {
                supports[var][value].insert(tid);
            }
        }

        Self {
            arity,
            tuple_count,
            supports,
        }
    }
}
