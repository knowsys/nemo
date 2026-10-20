//! The aggregating sink: the aggregation of a join's rows.
//!
//! An aggregation groups the rows by their leading layers
//! and computes any number of aggregates per group, in one pass.
//! The join's rows arrive sorted, so the rows of a group are adjacent,
//! and the changed layer the join hands over marks where a group starts.
//! As in SPARQL, a group on which an aggregate fails gives no row.
//!
//! Aggregation runs in two phases.
//! The [Aggregator] collects the groups as a join drains into it,
//! while the join may still hold the dictionary.
//! [Encoding](AggregatedRows::encode) then adds the results to the dictionary.

mod accumulator;

use crate::{
    dictionary::Dictionary,
    execution::{
        AggregateShape, AggregationShape,
        sink::{RowSet, RowSink, aggregating::accumulator::Accumulator},
    },
    storage::StorageValue,
    trie::{RowBuffer, SortedRowBuffer},
    value::{DataValue, ErrorCounts, EvaluationError},
};

/// How an aggregate reads each of its identities once per group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Deduplication {
    /// The group key and the identity are exactly the leading layers, this many:
    /// the rows arrive sorted and distinct,
    /// so a row starts a new identity exactly if it opens a group or changes within them.
    Prefix(usize),
    /// The identity lies elsewhere; a set remembers the identities read in the group.
    Set,
}

impl Deduplication {
    /// Returns how an aggregate grouped by the leading `group_layers` reads its identities.
    fn of(aggregate: &AggregateShape, group_layers: usize) -> Self {
        let mut covered = vec![false; group_layers];
        for layer in aggregate.identity_layers() {
            if layer >= covered.len() {
                covered.resize(layer + 1, false);
            }
            covered[layer] = true;
        }
        covered[..group_layers].fill(true);

        if covered.iter().all(|&layer| layer) {
            Deduplication::Prefix(covered.len())
        } else {
            Deduplication::Set
        }
    }
}

/// A collected group: its key,
/// and per aggregate its result or the error it failed with.
type Group = (Vec<StorageValue>, Vec<Result<DataValue, EvaluationError>>);

/// The sink aggregating a join's rows, as its [shape](AggregationShape) specifies.
///
/// A join drains into the aggregator, which collects each group's key and results;
/// [finish](Self::finish) hands over the groups for encoding.
#[derive(Debug)]
pub(crate) struct Aggregator<'a> {
    /// The shape of the aggregation.
    shape: AggregationShape<'a>,
    /// Per aggregate, how it reads each identity once.
    deduplication: Vec<Deduplication>,
    /// The dictionary the values decode through.
    dictionary: &'a Dictionary,
    /// The groups collected so far, in the join's order.
    groups: Vec<Group>,
    /// The key and the accumulators of the current group; `None` before the first row.
    current: Option<(Vec<StorageValue>, Vec<Accumulator>)>,
    /// Per aggregate that needs one, the identities read in the current group.
    seen: Vec<Option<RowSet>>,
    /// The scratch for the identity of a row.
    identity: Vec<StorageValue>,
}

impl<'a> Aggregator<'a> {
    /// Creates the aggregator of a shape over joins emitting `emitted` layers;
    /// the values decode through the dictionary.
    ///
    /// # Panics
    ///
    /// Panics if the shape is malformed or does not fit the joins.
    pub(crate) fn new(
        shape: AggregationShape<'a>,
        emitted: usize,
        dictionary: &'a Dictionary,
    ) -> Self {
        if let Err(violation) = shape.validate() {
            panic!("{violation}");
        }
        if let Err(mismatch) = shape.validate_fit(emitted) {
            panic!("{mismatch}");
        }

        let deduplication: Vec<Deduplication> = shape
            .aggregates
            .iter()
            .map(|aggregate| Deduplication::of(aggregate, shape.group_layers))
            .collect();
        let seen = shape
            .aggregates
            .iter()
            .zip(&deduplication)
            .map(|(aggregate, deduplication)| {
                (*deduplication == Deduplication::Set)
                    .then(|| RowSet::new(aggregate.identity_layers().count()))
            })
            .collect();

        Self {
            shape,
            deduplication,
            dictionary,
            groups: Vec::new(),
            current: None,
            seen,
            identity: Vec::new(),
        }
    }

    /// Ends the aggregation, and returns the groups collected.
    ///
    /// A join without rows gives no groups:
    /// a row for the empty group, as SPARQL gives for an ungrouped count,
    /// is up to the caller.
    pub(crate) fn finish(mut self) -> AggregatedRows {
        self.close_group();

        AggregatedRows {
            groups: self.groups,
            arity: self.shape.group_layers + self.shape.aggregates.len(),
        }
    }

    /// Moves the current group, if there is one, to the groups collected.
    fn close_group(&mut self) {
        if let Some((key, accumulators)) = self.current.take() {
            let results = accumulators.into_iter().map(Accumulator::finish).collect();
            self.groups.push((key, results));
        }
    }
}

impl RowSink for Aggregator<'_> {
    #[inline]
    fn accept(&mut self, row: &[StorageValue], changed: usize) {
        if changed < self.shape.group_layers {
            self.close_group();

            for set in self.seen.iter_mut().flatten() {
                set.begin_group();
            }
        }

        let dictionary = self.dictionary;
        let Self {
            shape,
            deduplication,
            current,
            seen,
            identity,
            ..
        } = self;

        // The first row of a group is a fresh identity even where no layer tells identities apart.
        let opens_group = current.is_none();
        let (_, accumulators) = current.get_or_insert_with(|| {
            let fresh = shape
                .aggregates
                .iter()
                .map(|aggregate| Accumulator::new(aggregate.kind));
            (row[..shape.group_layers].to_vec(), fresh.collect())
        });

        for (((accumulator, aggregate), deduplication), set) in accumulators
            .iter_mut()
            .zip(shape.aggregates)
            .zip(deduplication.iter())
            .zip(seen.iter_mut())
        {
            let fresh = match deduplication {
                Deduplication::Prefix(layers) => opens_group || changed < *layers,
                Deduplication::Set => {
                    let set = set.as_mut().expect("set aggregates keep a set");

                    identity.clear();
                    identity.extend(aggregate.identity_layers().map(|layer| row[layer]));

                    set.insert(identity)
                }
            };

            if !fresh {
                continue;
            }

            accumulator.update(
                aggregate.input.map(|layer| row[layer]),
                aggregate.pair.map(|layer| row[layer]),
                dictionary,
            );
        }
    }
}

/// The groups an aggregator collected from a join, not yet encoded.
#[derive(Debug)]
pub(crate) struct AggregatedRows {
    /// The groups, in the join's order.
    groups: Vec<Group>,
    /// The number of values per result row:
    /// the group key and one value per aggregate.
    arity: usize,
}

impl AggregatedRows {
    /// Encodes the groups into result rows, adding the results to the dictionary,
    /// and returns the rows with the errors counted per kind.
    ///
    /// A group with a failed aggregate gives no row, and its error is counted.
    /// So does a group with a result the dictionary cannot represent,
    /// counted as [unsupported](EvaluationError::Unsupported).
    /// The groups arrive distinct and in key order, so the rows are sorted.
    pub(crate) fn encode(self, dictionary: &mut Dictionary) -> (SortedRowBuffer, ErrorCounts) {
        let mut buffer = RowBuffer::new(self.arity);
        let mut errors = ErrorCounts::default();
        let mut row = Vec::with_capacity(self.arity);

        'groups: for (key, results) in self.groups {
            row.clear();
            row.extend_from_slice(&key);

            for result in results {
                let encoded = result.and_then(|value| {
                    dictionary
                        .encode(value.as_ref())
                        .ok_or(EvaluationError::Unsupported)
                });

                match encoded {
                    Ok(value) => row.push(value),
                    Err(error) => {
                        errors.record(error);
                        continue 'groups;
                    }
                }
            }

            buffer.push_row(&row);
        }

        (buffer.assume_sorted(), errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::execution::AggregateKind;

    #[test]
    fn identities_leading_the_rows_need_no_set() {
        let aggregate = |input, per: &[usize]| AggregateShape {
            kind: AggregateKind::Count,
            input: Some(input),
            pair: None,
            per: per.to_vec(),
        };

        assert_eq!(
            Deduplication::of(&aggregate(1, &[]), 1),
            Deduplication::Prefix(2)
        );
        assert_eq!(
            Deduplication::of(&aggregate(1, &[2]), 1),
            Deduplication::Prefix(3)
        );
        assert_eq!(
            Deduplication::of(&aggregate(2, &[1]), 1),
            Deduplication::Prefix(3)
        );
        // An identity within the group key reads each group once.
        assert_eq!(
            Deduplication::of(&aggregate(0, &[0]), 1),
            Deduplication::Prefix(1)
        );
        // A gap after the group key leaves identities apart.
        assert_eq!(Deduplication::of(&aggregate(2, &[]), 1), Deduplication::Set);
        assert_eq!(Deduplication::of(&aggregate(1, &[]), 0), Deduplication::Set);
    }
}
