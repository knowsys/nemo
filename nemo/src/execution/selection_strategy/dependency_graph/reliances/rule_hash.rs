//! Module for hashing pairs of rules to quickly access previously computed results.

use std::{collections::hash_map::DefaultHasher, hash::Hash, hash::Hasher};

use super::{
    common::RelianceType,
    rules::{Program, Rule},
};

#[derive(Debug, Hash)]
struct RulePair {
    source: Rule,
    target: Rule,
}

#[derive(Debug)]
struct RulePairHash {}

impl Hash for RulePairHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {}
}

#[derive(Debug)]
struct ProgramHasher {}

impl ProgramHasher {
    pub fn initialize(program: &Program) -> Self {
        todo!()
    }

    pub fn hash_pair(program: &Program, index_source: usize, index_target: usize) -> RulePairHash {
        let test_pair = RulePair {
            source: program.rules()[index_source].clone(),
            target: program.rules()[index_target].clone(),
        };

        let mut hasher = DefaultHasher::new();
        test_pair.hash(&mut hasher);
        let hash_value = hasher.finish();

        todo!()
    }
}

#[derive(Debug)]
pub(super) struct RelianceCache<const SIZE: usize> {}

impl<const SIZE: usize> RelianceCache<SIZE> {
    /// Create a new [`RelianceCache`].
    pub fn new() -> Self {
        todo!()
    }

    pub fn get_result(index_source: usize, index_target: usize) -> Option<RelianceType> {
        todo!()
    }

    pub fn save_result(index_source: usize, index_target: usize, result: RelianceType) {
        todo!()
    }
}
