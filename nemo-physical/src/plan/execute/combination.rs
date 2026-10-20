//! Combinations: running a member's join once per combination of its inputs' blocks.
//!
//! An input may consist of several blocks, each a trie.
//! A combination takes one block of each input, and the join runs over those tries.

use crate::{
    execution::{JoinArena, JoinTemplate, SubtractedShape, TrieJoin},
    plan::execute::results::Accounting,
    stopwatch::Stopwatch,
    trie::Trie,
};

/// The blocks a member's join reads.
#[derive(Debug)]
pub(super) struct JoinBlocks<'a> {
    /// Per joined input, its blocks.
    pub(super) inputs: Vec<Vec<&'a Trie>>,
    /// The subtracted blocks, each subtracted on its own.
    pub(super) subtracted: Vec<&'a Trie>,
    /// Per subtracted block, its shape.
    pub(super) subtracted_shapes: Vec<SubtractedShape>,
}

impl<'a> JoinBlocks<'a> {
    /// Returns the number of blocks, joined and subtracted.
    pub(super) fn count(&self) -> usize {
        self.inputs.iter().map(Vec::len).sum::<usize>() + self.subtracted.len()
    }

    /// Returns the combinations of the joined blocks, one block of each input,
    /// with the last input varying fastest.
    ///
    /// An input without blocks leaves no combination.
    pub(super) fn combinations(&self) -> impl Iterator<Item = Vec<&'a Trie>> + '_ {
        let inputs = &self.inputs;
        let mut selection = vec![0usize; inputs.len()];
        let mut done = inputs.iter().any(Vec::is_empty);

        std::iter::from_fn(move || {
            if done {
                return None;
            }
            let tries: Vec<&Trie> = inputs
                .iter()
                .zip(&selection)
                .map(|(blocks, &block)| blocks[block])
                .collect();

            // The odometer step: the last input carries over into the one before.
            done = true;
            for position in (0..selection.len()).rev() {
                selection[position] += 1;
                if selection[position] < inputs[position].len() {
                    done = false;
                    break;
                }
                selection[position] = 0;
            }

            Some(tries)
        })
    }
}

/// The sequential join of a member, reused across its combinations.
///
/// Each combination rebinds the previous combination's join to its tries if the join allows,
/// and builds a join of its own otherwise.
#[derive(Debug)]
pub(super) struct SequentialJoin<'a> {
    /// What the joins share.
    template: JoinTemplate<'a>,
    /// The arena the joins are built in.
    arena: &'a JoinArena,
    /// The join of the previous combination.
    reusable: Option<TrieJoin<'a>>,
}

impl<'a> SequentialJoin<'a> {
    /// Creates the sequential join of a member, building its joins in the given arena.
    pub(super) fn new(template: JoinTemplate<'a>, arena: &'a JoinArena) -> Self {
        Self {
            template,
            arena,
            reusable: None,
        }
    }

    /// Runs the join of one combination through `drain`,
    /// and accounts for its time, its rebinding and its evaluation errors.
    pub(super) fn run<T>(
        &mut self,
        tries: &[&'a Trie],
        accounting: &mut Accounting,
        drain: impl FnOnce(&mut TrieJoin<'a>) -> T,
    ) -> T {
        let begun = Stopwatch::start();
        let rebound = self.reusable.take().and_then(|mut join| {
            join.rebind(&self.template.shape, tries, self.arena)
                .then_some(join)
        });
        accounting.counts.rebound += u64::from(rebound.is_some());
        let mut join = rebound.unwrap_or_else(|| self.template.build(tries, self.arena));
        accounting.timings.setup += begun.nanos();

        let drained = drain(&mut join);
        accounting.timings.join += begun.nanos();
        accounting.errors.absorb(&join.evaluation_errors());
        self.reusable = Some(join);

        drained
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combinations_run_the_last_input_fastest() {
        let tries: Vec<Trie> = (0..4).map(|_| Trie::empty(1)).collect();
        let [a, b, c, d] = [&tries[0], &tries[1], &tries[2], &tries[3]];
        let positions = |inputs: Vec<Vec<&Trie>>| -> Vec<Vec<usize>> {
            let blocks = JoinBlocks {
                inputs,
                subtracted: Vec::new(),
                subtracted_shapes: Vec::new(),
            };
            blocks
                .combinations()
                .map(|combination| {
                    combination
                        .iter()
                        .map(|&trie| {
                            tries
                                .iter()
                                .position(|candidate| std::ptr::eq(candidate, trie))
                                .expect("the tries are the test's")
                        })
                        .collect()
                })
                .collect()
        };

        assert_eq!(
            positions(vec![vec![a, b], vec![c, d]]),
            vec![vec![0, 2], vec![0, 3], vec![1, 2], vec![1, 3]]
        );
        assert_eq!(positions(vec![vec![a], vec![]]), Vec::<Vec<usize>>::new());
        assert_eq!(positions(vec![]), vec![Vec::<usize>::new()]);
    }
}
