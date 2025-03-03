//! Implementation of the aggregate full trie scan

use std::{
    cell::{RefCell, UnsafeCell},
    fmt::Debug,
};

use crate::{
    aggregates::{
        operation::AggregateOperation,
        processors::processor::{AggregateGroupProcessor, AggregateProcessor},
    },
    columnar::columnscan::ColumnScanT,
    management::database::Dict,
    storagevalues::{
        storagetype::{StorageType, StorageTypeBitSet},
        storagevalue::StorageValueT,
    },
    tabular::triescan::{PartialTrieScan, TrieScan, TrieScanEnum},
};

use super::{prune::TrieScanPrune, OperationColumnMarker, OperationGenerator, OperationTable};

/// Holds information on how to perform an aggregation, e.g. column markers and the type of aggregation.
#[derive(Debug, Clone)]
pub struct AggregateAssignment {
    /// Operation to perform
    pub aggregate_operation: AggregateOperation,
    /// Distinct column markers (not including aggregated column)
    pub distinct_columns: Vec<OperationColumnMarker>,
    /// Group-by column markers
    pub group_by_columns: Vec<OperationColumnMarker>,
    /// Aggregated input column marker
    pub aggregated_column: OperationColumnMarker,
}

#[derive(Debug, Clone)]
pub(crate) struct GeneratorAggregate {
    instructions: AggregationInstructions,
}

impl GeneratorAggregate {
    /// Creates an aggregate generator and computes the required input order
    pub(crate) fn new(input: &OperationTable, assignment: &AggregateAssignment) -> Self {
        let aggregated_column_index = input
            .position(&assignment.aggregated_column)
            .expect("aggregate variable has to be in input operation table");
        let mut last_distinct_column_index = input
            .iter()
            .enumerate()
            .rev()
            .find(|(_index, marker)| assignment.distinct_columns.contains(marker))
            .map(|(index, _marker)| index)
            .unwrap_or(aggregated_column_index);

        if aggregated_column_index > last_distinct_column_index {
            last_distinct_column_index = aggregated_column_index;
        }

        let instructions = AggregationInstructions {
            aggregate_operation: assignment.aggregate_operation,
            group_by_column_count: assignment.group_by_columns.len(),
            aggregated_column_index,
            last_distinct_column_index,
        };

        Self { instructions }
    }
}

impl OperationGenerator for GeneratorAggregate {
    fn generate<'a>(
        &'_ self,
        mut input: Vec<Option<TrieScanEnum<'a>>>,
        _dictionary: &'a RefCell<Dict>,
    ) -> Option<TrieScanEnum<'a>> {
        debug_assert!(input.len() == 1);

        let input_scan = input.remove(0)?;

        let prune = TrieScanPrune::new(input_scan);

        Some(TrieScanEnum::AggregateWrapper(
            TrieScanAggregate::new(prune, self.instructions).into(),
        ))
    }
}

/// Describes which columns of the input trie scan will be group-by, distinct and aggregate columns and other information about the aggregation.
#[derive(Debug, Clone, Copy)]
struct AggregationInstructions {
    /// Type of the aggregate operation, which determines the aggregate processor that will be used
    aggregate_operation: AggregateOperation,
    /// Number of group-by columns
    ///
    /// These are exactly the first columns of the input scan.
    group_by_column_count: usize,
    /// Index of the aggregated column in the input scan
    ///
    /// The aggregated column has to come after the group-by columns.
    aggregated_column_index: usize,
    /// Index of the last column (thus highest column index) that should still be forwarded to the underlying aggregate implementation
    ///
    /// All remaining columns of the input scan will be ignored.
    /// All columns between `highest_distinct_column_index` and the group-by columns are seen as distinct, except for the `aggregated_column_index`.
    last_distinct_column_index: usize,
}

impl AggregationInstructions {
    /// Returns true iff the column indices are not conflicting with each other.
    pub(crate) fn is_valid(&self) -> bool {
        self.aggregated_column_index >= self.group_by_column_count
            && self.last_distinct_column_index >= self.aggregated_column_index
    }

    /// Returns whether a given index of `column_types` is an group-by layer
    fn is_output_column_group_by(&self, layer: usize) -> bool {
        layer < self.group_by_column_count
    }

    /// Returns whether a given index of `column_types` is the aggregated layer
    fn is_output_column_aggregated(&self, layer: usize) -> bool {
        !self.is_output_column_group_by(layer)
    }

    // Translates an output layer to the corresponding layer of the input trie scan
    fn output_layer_to_input_layer(&self, layer: usize) -> usize {
        if self.is_output_column_group_by(layer) {
            layer
        } else {
            self.aggregated_column_index
        }
    }
}

/// [TrieScan] which performs an aggregate operation.
///
/// It works by iterating though all the input rows.
/// As long as the group-by columns do not change, all values are passed to an [AggregateProcessor] which performs the actual aggregation.
/// As soon as the group-by values input change, a new output row and a new [AggregateGroupProcessor] gets created.
/// The [crate::aggregates::operation] module is also the place to add new aggregate operations to `nemo-phyiscal`.
///
/// Input columns (specific order is required):
/// * Zero or more group-by columns, followed by
/// * a single aggregated column possibly mixed with additional distinct columns, followed by
/// * zero or more peripheral columns, that do not impact the result of the aggregate at all and are not used during aggregation.
///
/// Output columns:
/// * Zero or more group-by columns, followed by
/// * one aggregate output column
#[derive(Debug)]
pub(crate) struct TrieScanAggregate<T: TrieScan> {
    input_scan: T,
    instructions: AggregationInstructions,

    current_aggregated_output_value: AggregatedOutputValue,
    peeked_row_information: Option<PeekedRowInformation>,
}

/// Information on how a row for the next group-by values has been looked at already,
/// even tough the [TrieScanAggregate] is still at the previous group.
///
/// Peeking into the next row can occur during the aggregation process.
/// When trying to find out if the next row still belongs to the current group-by values, it could be that the [TrieScanAggregate] already looks into a row of the next group.
#[derive(Debug)]
struct PeekedRowInformation {
    /// Return value of the `advance_on_layer` call
    uppermost_modified_column_index: Option<usize>,
    /// Group-by values before the advancement
    /// This is required to service calls to `current` of the [TrieScanAggregate]
    original_group_by_values: Vec<StorageValueT>,
}

impl<T: TrieScan> TrieScanAggregate<T> {
    /// Creates a new [TrieScanAggregate] for processing an input full [TrieScan]. The group-by layers will get copied, an aggregate column will be computed based on the input aggregate/distinct columns, and any other columns will get dismissed.
    fn new(input_scan: T, instructions: AggregationInstructions) -> Self {
        if !instructions.is_valid() {
            panic!("cannot create TrieScanAggregate with invalid aggregation instructions")
        }

        Self {
            input_scan,
            instructions,
            current_aggregated_output_value: AggregatedOutputValue::None,
            peeked_row_information: None,
        }
    }
}

#[derive(Debug)]
enum AggregatedOutputValue {
    /// The end for the current group has been reached, panic if `current()` is called
    None,
    /// Compute the aggregate value when `current()` is called
    NotYetComputed,
    /// The aggregate value has already been computed for the current group
    Some(StorageValueT),
}

impl<T: TrieScan> TrieScan for TrieScanAggregate<T> {
    fn num_columns(&self) -> usize {
        self.instructions.group_by_column_count + 1
    }

    fn advance_on_layer(&mut self, layer: usize) -> Option<usize> {
        let mut advancement_result = None;

        if let Some(peeked_row_information) = &self.peeked_row_information {
            if let Some(uppermost_modified_column_index) =
                peeked_row_information.uppermost_modified_column_index
            {
                if uppermost_modified_column_index <= layer {
                    advancement_result = Some(uppermost_modified_column_index);
                }
            }
        }
        self.peeked_row_information = None;

        if advancement_result.is_none() {
            let uppermost_modified_input_column_index = self
                .input_scan
                .advance_on_layer(self.instructions.output_layer_to_input_layer(layer));
            advancement_result = uppermost_modified_input_column_index.map(|input_layer| {
                // Translate input layer to output layer
                if input_layer < self.instructions.group_by_column_count {
                    input_layer
                } else {
                    self.instructions.group_by_column_count
                }
            })
        }

        if let Some(new_modified_column_index) = advancement_result {
            self.current_aggregated_output_value = AggregatedOutputValue::NotYetComputed;
            Some(new_modified_column_index)
        } else {
            self.current_aggregated_output_value = AggregatedOutputValue::None;
            None
        }
    }

    fn current_value(&mut self, layer: usize) -> StorageValueT {
        if self.instructions.is_output_column_aggregated(layer) {
            match self.current_aggregated_output_value {
                AggregatedOutputValue::None => {
                    panic!("advance on layer needs to return Some first for TrieScanAggregate")
                }
                AggregatedOutputValue::Some(v) => v,
                AggregatedOutputValue::NotYetComputed => {
                    // We're trying to compute the current aggregate value.
                    // We know the underlying trie scan currently points to the first row of the current group (this is ensured by the last `advance_on_layer` call).
                    // Furthermore we know that there is at least one row to aggregate in the current group, otherwise the [AggregatedOutputValue] would be [AggregatedOutputValue::None]
                    // We now need to loop through all the rows of the current group to determine the aggregate result.

                    let original_group_by_values: Vec<_> = {
                        // Cache the group by values. This is required in case we already peek into the next group, where the group-by values in the underlying trie scan would be overwritten
                        (0..self.instructions.group_by_column_count)
                            .map(|layer_index| self.input_scan.current_value(layer_index))
                            .collect()
                    };

                    // TODO: Update use of dynamic dispatch
                    let processor = self.instructions.aggregate_operation.create_processor();
                    let mut group_processor: Box<dyn AggregateGroupProcessor> = processor.group();

                    loop {
                        let new_value = self
                            .input_scan
                            .current_value(self.instructions.aggregated_column_index);

                        // Perform aggregation
                        group_processor.write_aggregate_input_value(new_value);

                        // Advance the underlying trie scan to find out if the is another row with the same group-by values.
                        if let Some(uppermost_modified_column_index) = self
                            .input_scan
                            .advance_on_layer(self.instructions.last_distinct_column_index)
                        {
                            // Check if a group-by column was modified
                            if uppermost_modified_column_index
                                < self.instructions.group_by_column_count
                            {
                                // We have left the current aggregation group
                                self.peeked_row_information = Some(PeekedRowInformation {
                                    uppermost_modified_column_index: Some(
                                        uppermost_modified_column_index,
                                    ),
                                    original_group_by_values,
                                });
                                // Thus, there are no more values to aggregate in the current group.
                                break;
                            } else {
                                // We have new values for at least one of the distinct columns, but all group by columns are still the same.
                                // Thus, continue aggregating.
                                continue;
                            }
                        } else {
                            self.peeked_row_information = Some(PeekedRowInformation {
                                uppermost_modified_column_index: None,
                                original_group_by_values,
                            });
                            // There are no more values to aggregate, because the input trie scan is fully consumed
                            break;
                        }
                    }

                    if let Some(result) = group_processor.finish() {
                        self.current_aggregated_output_value = AggregatedOutputValue::Some(result);
                        result
                    } else {
                        panic!("failed to compute aggregate result");
                    }
                }
            }
        } else {
            match self.current_aggregated_output_value {
                AggregatedOutputValue::None => {
                    panic!("advance on layer needs to return Some first for TrieScanAggregate")
                }
                AggregatedOutputValue::Some(_) => match &self.peeked_row_information {
                    Some(peeked_row_information) => {
                        peeked_row_information.original_group_by_values[layer]
                    }
                    None => self.input_scan.current_value(layer),
                },
                AggregatedOutputValue::NotYetComputed => self.input_scan.current_value(layer),
            }
        }
    }
}

/// Special partial trie scan that simply owns a full trie scan,
/// so that it can be used by the execution plan code that currently can only deal with partial trie scans.
/// This is then unwrapped by the materialize code.
///
/// TODO: Refractor execution plan code to allow for full trie scan and then remove this wrapper
#[derive(Debug)]
pub(crate) struct TrieScanAggregateWrapper<'a> {
    /// Wrapped full trie scan
    pub(crate) trie_scan: TrieScanAggregate<TrieScanPrune<'a>>,
    /// Column types of the wrapped trie scan
    pub(crate) arity: usize,
}

impl<'a> PartialTrieScan<'a> for TrieScanAggregateWrapper<'a> {
    fn up(&mut self) {
        panic!("TrieScanAggregateWrapper::up cannot be called");
    }

    fn down(&mut self, _storage_type: StorageType) {
        panic!("TrieScanAggregateWrapper::down cannot be called");
    }

    fn current_scan(&self) -> Option<&'a UnsafeCell<ColumnScanT<'a>>> {
        panic!("TrieScanAggregateWrapper::current_scan cannot be called");
    }

    fn current_layer(&self) -> Option<usize> {
        panic!("TrieScanAggregateWrapper::current_layer cannot be called");
    }

    fn arity(&self) -> usize {
        self.arity
    }

    fn scan<'b>(&'b self, _layer: usize) -> &'b UnsafeCell<ColumnScanT<'a>> {
        panic!("TrieScanAggregateWrapper::scan cannot be called");
    }

    fn possible_types(&self, _layer: usize) -> StorageTypeBitSet {
        panic!("TrieScanAggregateWrapper::possible_types cannot be called");
    }
}

impl<'a> From<TrieScanAggregate<TrieScanPrune<'a>>> for TrieScanAggregateWrapper<'a> {
    fn from(value: TrieScanAggregate<TrieScanPrune<'a>>) -> Self {
        let arity = value.num_columns();

        Self {
            trie_scan: value,
            arity,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{AggregationInstructions, TrieScanAggregate};
    use crate::aggregates::operation::AggregateOperation;
    use crate::storagevalues::storagetype::StorageType;
    use crate::tabular::operations::prune::TrieScanPrune;
    use crate::tabular::trie::Trie;
    use crate::tabular::triescan::TrieScanEnum;
    use crate::util::test_util::test::trie_int64;

    fn trie_scan_prune_from_trie(input_trie: &Trie) -> TrieScanPrune<'_> {
        TrieScanPrune::new(TrieScanEnum::Generic(input_trie.partial_iterator()))
    }

    fn aggregate_and_materialize(
        input_trie: &Trie,
        aggregation_instructions: AggregationInstructions,
        _aggregated_column_storage_type: StorageType,
    ) -> Trie {
        let trie_scan_prune = trie_scan_prune_from_trie(input_trie);

        let trie_scan_aggregate = TrieScanAggregate::new(trie_scan_prune, aggregation_instructions);

        Trie::from_full_trie_scan(trie_scan_aggregate, 0)
    }

    fn do_tries_equal(trie1: Trie, trie2: Trie) -> bool {
        let mut iter2 = trie2.row_iterator();

        for row1 in trie1.row_iterator() {
            let row2 = iter2.next();
            if let Some(row2) = row2 {
                if row1 != row2 {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check that `iter2` reached it's end, too
        iter2.next().is_none()
    }

    #[test]
    fn test_aggregate_i64() {
        let input_trie = trie_int64(vec![
            &[i64::MIN, i64::MIN, i64::MIN],
            &[1, 3, 8],
            &[3, 2, 9],
            &[3, 5, 7],
            &[3, 11, 4],
            &[5, 0, i64::MIN],
            &[5, 0, i64::MAX],
            &[i64::MAX, i64::MAX, i64::MAX],
        ]);

        // Max, No group-by columns
        assert!(do_tries_equal(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Max,
                    group_by_column_count: 0,
                    aggregated_column_index: 2,
                    last_distinct_column_index: 2,
                },
                StorageType::Int64
            ),
            trie_int64(vec![&[i64::MAX],])
        ));
        // Max, one group-by column
        assert!(do_tries_equal(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Max,
                    group_by_column_count: 1,
                    aggregated_column_index: 2,
                    last_distinct_column_index: 2,
                },
                StorageType::Int64
            ),
            trie_int64(vec![
                &[i64::MIN, i64::MIN],
                &[1, 8],
                &[3, 9],
                &[5, i64::MAX],
                &[i64::MAX, i64::MAX],
            ])
        ));
        // Max, two group-by columns
        assert!(do_tries_equal(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Max,
                    group_by_column_count: 2,
                    aggregated_column_index: 2,
                    last_distinct_column_index: 2,
                },
                StorageType::Int64
            ),
            trie_int64(vec![
                &[i64::MIN, i64::MIN, i64::MIN],
                &[1, 3, 8],
                &[3, 2, 9],
                &[3, 5, 7],
                &[3, 11, 4],
                &[5, 0, i64::MAX],
                &[i64::MAX, i64::MAX, i64::MAX],
            ])
        ));
        // Count
        assert!(do_tries_equal(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Count,
                    group_by_column_count: 0,
                    aggregated_column_index: 0,
                    last_distinct_column_index: 0,
                },
                StorageType::Int64
            ),
            trie_int64(vec![&[5],])
        ));
        // Sum
        assert!(do_tries_equal(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Sum,
                    group_by_column_count: 1,
                    aggregated_column_index: 1,
                    last_distinct_column_index: 1,
                },
                StorageType::Int64
            ),
            trie_int64(vec![
                &[i64::MIN, i64::MIN],
                &[1, 3],
                &[3, 18],
                &[5, 0],
                &[i64::MAX, i64::MAX],
            ])
        ));
    }
}
