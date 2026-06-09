use std::collections::HashMap;

use crate::execution::planning::normalization::rule::NormalizedRule;

use crate::execution::selection_strategy::strategy_full_chain_stratification::EdgeLabel;
use crate::execution::selection_strategy::strategy_full_chain_stratification::reliances::{
    aggr::is_aggregation_reliance, negr::is_negation_reliance, posr::is_positive_reliance,
    restr::is_restraint_reliance, self_restr::is_self_restraint_reliance,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::{
    encode::{RuleEncoding, encode_rule, encode_rule_pair},
    extend::Reliance,
    ordered_atoms::{Mem, ReorderAtoms, SortedHeadAtoms},
};

use strum::EnumCount;

#[derive(Default, Debug)]
enum Progress<T> {
    #[default]
    NotStarted,
    Partial(T),
    Complete(T),
}

impl<T> Progress<T> {
    fn get_unchecked(&self) -> &T {
        match self {
            Progress::NotStarted => panic!("progress should have started"),
            Progress::Partial(t) | Progress::Complete(t) => t,
        }
    }
    fn finish(&mut self) {
        if let Progress::Partial(t) = std::mem::take(self) {
            *self = Progress::Complete(t);
        }
    }
}

/// Storage type for memoized reliance relations between two rules.
/// Will be indexed by EdgeLabel variants via their usize repr.
/// Meaning of values:
///   Progress::NotStarted --> not computed yet
///   Progress::Complete(vec![]) --> no reliance of this type
///   Progress::Partial(vec![mu]) --> found reliance of this type with AtomMapping mu (and there may be more)
///   Progress::Complete(vec![mu1,mu2]) --> found reliance of this type with multiple possible AtomMappings (relevant for chain computations)
type Reliances = [Progress<Vec<Reliance>>; EdgeLabel::COUNT];

#[derive(Debug)]
pub struct RuleMemoization<'a> {
    pub rules: &'a Vec<&'a NormalizedRule>,

    // these fields memoize auxiliary per-rule data
    // uses `Vec<Option<X>>` (initialized to `vec![None; rules.len()]`) instead of `HashMap<usize,X>`
    pub reordered_atoms: ReorderAtoms<'a>,
    pub sorted_head_atoms: Mem<SortedHeadAtoms<'a>>,
    encoded_rules: Vec<Option<RuleEncoding>>,
}

impl<'a> RuleMemoization<'a> {
    pub fn new(rules: &'a Vec<&'a NormalizedRule>) -> Self {
        Self {
            rules,

            reordered_atoms: ReorderAtoms {
                reordered_body_atoms: Mem::new(rules.len()),
                reordered_negative_body_atoms: Mem::new(rules.len()),
                reordered_head_atoms: Mem::new(rules.len()),
            },
            sorted_head_atoms: Mem::new(rules.len()),
            encoded_rules: vec![None; rules.len()],
        }
    }
}

#[derive(Debug)]
pub struct RelianceMemoization<'a> {
    data: RuleMemoization<'a>,

    // the following 3 fields are instead of 2 fields of types `HashMap<(usize, usize), Vec<usize>>` and `HashMap<Vec<usize>, Reliances>`, which spares the need to store the keys twice
    reliances: Vec<Reliances>,
    reliances_key_map: HashMap<Vec<usize>, usize>, // maps canonized rule-pair keys into `reliances` Vec
    reliances_edge_map: HashMap<(usize, usize), usize>, // maps pairs of rule indices into `reliances` Vec
}

impl<'a> RelianceMemoization<'a> {
    pub fn new(rules: &'a Vec<&'a NormalizedRule>) -> Self {
        Self {
            data: RuleMemoization::new(rules),

            reliances: Vec::new(),
            reliances_key_map: HashMap::new(),
            reliances_edge_map: HashMap::new(),
        }
    }

    /// Get a mutable reference to the progress of the reliance computation of the given type between the two rules.
    fn get_progress<'b>(
        &'b mut self,
        rule1_index: usize,
        rule2_index: usize,
        label: EdgeLabel,
    ) -> (&'b mut Progress<Vec<Reliance>>, &'b mut RuleMemoization<'a>)
    where
        'a: 'b,
    {
        let rule1 = *&self.data.rules[rule1_index];
        let rule2 = *&self.data.rules[rule2_index];
        let reliances_index = *self
            .reliances_edge_map
            .entry((rule1_index, rule2_index))
            .or_insert_with(|| {
                let _ =
                    self.data.encoded_rules[rule1_index].get_or_insert_with(|| encode_rule(rule1));
                let _ =
                    self.data.encoded_rules[rule2_index].get_or_insert_with(|| encode_rule(rule2));
                let enc1 = self.data.encoded_rules[rule1_index].as_ref().unwrap();
                let enc2 = self.data.encoded_rules[rule2_index].as_ref().unwrap();
                let enc = encode_rule_pair(enc1, enc2);
                *self.reliances_key_map.entry(enc).or_insert_with(|| {
                    let new_index = self.reliances.len();
                    self.reliances
                        .push([const { Progress::NotStarted }; EdgeLabel::COUNT]);
                    new_index
                })
            });
        (
            &mut self.reliances[reliances_index][label as usize],
            &mut self.data,
        )
    }

    /// Given the previous reliance result, determine the next atom mapping constituting a reliance of the given type between the two rules if it exists.
    fn next_reliance(
        mem: &mut RuleMemoization,
        rule1_index: usize,
        rule2_index: usize,
        label: EdgeLabel,
        previous_opt: Option<&Reliance>,
    ) -> Option<Reliance> {
        match label {
            EdgeLabel::Positive => {
                is_positive_reliance(mem, rule1_index, rule2_index, previous_opt)
            }
            EdgeLabel::Restraint => {
                if rule1_index == rule2_index {
                    match is_self_restraint_reliance(mem, rule1_index, previous_opt) {
                        None => is_restraint_reliance(mem, rule1_index, rule2_index, None),
                        x => x,
                    }
                } else {
                    is_restraint_reliance(mem, rule1_index, rule2_index, previous_opt)
                }
            }
            EdgeLabel::Negation => {
                is_negation_reliance(mem, rule1_index, rule2_index, previous_opt)
            }
            EdgeLabel::Aggregation => {
                is_aggregation_reliance(mem, rule1_index, rule2_index, previous_opt)
            }
        }
    }

    /// Get one atom mappings constituting a reliance of the given type between the two rules if it exists.
    pub fn get_one<'b>(
        &'b mut self,
        rule1_index: usize,
        rule2_index: usize,
        label: EdgeLabel,
    ) -> Option<&'b Reliance>
    where
        'a: 'b,
    {
        let (progress, mem) = self.get_progress(rule1_index, rule2_index, label);
        let atom_mappings_vec = match progress {
            Progress::NotStarted => {
                if let Some(mu) = Self::next_reliance(mem, rule1_index, rule2_index, label, None) {
                    *progress = Progress::Partial(vec![mu]);
                } else {
                    *progress = Progress::Complete(vec![]);
                }
                progress.get_unchecked()
            }
            Progress::Partial(t) | Progress::Complete(t) => t,
        };
        atom_mappings_vec.last()
    }

    /// Get all atom mappings constituting a reliance of the given type between the two rules.
    pub fn get_all<'b>(
        &'b mut self,
        rule1_index: usize,
        rule2_index: usize,
        label: EdgeLabel,
    ) -> &'b Vec<Reliance>
    where
        'a: 'b,
    {
        let (progress, mem) = self.get_progress(rule1_index, rule2_index, label);
        match progress {
            Progress::NotStarted => {
                let mut t = Vec::new();
                while let Some(rel) =
                    Self::next_reliance(mem, rule1_index, rule2_index, label, t.last())
                {
                    t.push(rel);
                }
                *progress = Progress::Complete(t);
                progress.get_unchecked()
            }
            Progress::Partial(t) => {
                debug_assert!(
                    t.last().is_some(),
                    "if this is an empty vec, progress should be complete"
                );
                while let Some(rel) =
                    Self::next_reliance(mem, rule1_index, rule2_index, label, t.last())
                {
                    t.push(rel);
                }
                progress.finish();
                progress.get_unchecked()
            }
            Progress::Complete(t) => t,
        }
    }
}
