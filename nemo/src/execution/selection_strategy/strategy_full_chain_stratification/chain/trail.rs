use super::bitset::BitSet;

#[derive(Clone, Copy)]
pub enum TrailTarget {
    Domain(usize),
    Constraint(usize),
}

pub enum TrailEntry {
    Word {
        target: TrailTarget,
        word: usize,
        old: usize,
    },

    Domain {
        var: usize,
        old: BitSet,
    },
}

pub struct Trail {
    pub entries: Vec<TrailEntry>,
    pub levels: Vec<usize>,
}

impl Trail {
    pub fn push_level(&mut self) {
        self.levels.push(self.entries.len());
    }
}
