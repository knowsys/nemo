use std::{cell::UnsafeCell, fmt::Debug};

use crate::{
    aggregates::{
        operation::AggregateOperation,
        processors::processor::{AggregateGroupProcessor, AggregateProcessor},
    },
    columnar::columnscan::ColumnScanT,
    datatypes::{Double, Float, StorageTypeName, StorageValueT},
    tabular::traits::{partial_trie_scan::PartialTrieScan, trie_scan::TrieScan},
};

use super::TrieScanPrune;

#[derive(Debug)]
enum AggregatedOutputValue {
    /// The end for the current group has been reached, panic if `current()` is called
    None,
    /// Compute the aggregate value when `current()` is called
    NotYetComputed,
    /// The aggregate value has already been computed for the current group
    Some(StorageValueT),
}

/// Describes which columns of the input trie scan will be group-by, distinct and aggregate columns and other information about the aggregation.
#[derive(Debug, Clone, Copy)]
pub struct AggregationInstructions {
    /// Type of the aggregate operation, which determines the aggregate processor that will be used
    pub aggregate_operation: AggregateOperation,
    /// Number of group-by columns
    ///
    /// These are exactly the first columns of the input scan.
    pub group_by_column_count: usize,
    /// Index of the aggregated column in the input scan
    ///
    /// The aggregated column has to come after the group-by columns.
    pub aggregated_column_index: usize,
    /// Index of the last column (thus highest column index) that should still be forwarded to the underlying aggregate implementation
    ///
    /// All remaining columns of the input scan will be ignored.
    /// All columns between `highest_distinct_column_index` and the group-by columns are seen as distinct, except for the `aggregated_column_index`.
    pub last_distinct_column_index: usize,
}

impl AggregationInstructions {
    /// Returns true iff the column indices are not conflicting with each other.
    pub fn is_valid(&self) -> bool {
        self.aggregated_column_index >= self.group_by_column_count
            && self.last_distinct_column_index >= self.aggregated_column_index
    }

    /// Returns the index of the aggregate output column in the output scan
    pub fn aggregate_output_column_index(&self) -> usize {
        self.group_by_column_count
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

/// [`TrieScan`] which performs an aggregate operation.
///
/// Input columns:
/// * Zero or more group-by columns, followed by
/// * a single aggregated column possibly mixed with additional distinct columns, followed by
/// * zero or more peripheral columns, that do not impact the result of the aggregate at all and are not used during aggregation.
///
/// Output columns:
/// * Zero or more group-by columns, followed by
/// * one aggregate output column
#[derive(Debug)]
pub struct TrieScanAggregate<T: TrieScan> {
    aggregated_input_column_storage_type: StorageTypeName,
    input_scan: T,
    column_types: Vec<StorageTypeName>,
    instructions: AggregationInstructions,
    current_aggregated_output_value: AggregatedOutputValue,
    peeked_row_information: Option<PeekedRowInformation>,
}

/// Information on how a row for the next group-by values has been looked at already,
/// even tough the [`TrieScanAggregate`] is still at the previous group.
///
/// Peeking into the next row can occur during the aggregation process.
/// When trying to find out if the next row still belongs to the current group-by values, it could be that the [`TrieScanAggregate`] already looks into a row of the next group.
#[derive(Debug)]
struct PeekedRowInformation {
    /// Return value of the `advance_on_layer` call
    uppermost_modified_column_index: Option<usize>,
    /// Group-by values before the advancement
    /// This is required to service calls to `current` of the [`TrieScanAggregate`]
    original_group_by_values: Vec<StorageValueT>,
}

impl<T: TrieScan> TrieScanAggregate<T> {
    /// Creates a new [`TrieScanAggregate`] for processing an input full [`TrieScan`]. The group-by layers will get copied, an aggregate column will be computed based on the input aggregate/distinct columns, and any other columns will get dismissed.
    pub fn new(
        input_scan: T,
        instructions: AggregationInstructions,
        aggregated_input_column_storage_type: StorageTypeName,
    ) -> Self {
        if !instructions.is_valid() {
            panic!("cannot create TrieScanAggregate with invalid aggregation instructions")
        }

        let mut column_types = Vec::with_capacity(instructions.group_by_column_count + 1);
        column_types
            .extend_from_slice(&input_scan.column_types()[0..instructions.group_by_column_count]);
        column_types.push(
            instructions
                .aggregate_operation
                .static_output_type()
                .map(|data_type| data_type.to_storage_type_name())
                .unwrap_or(aggregated_input_column_storage_type),
        );

        Self {
            aggregated_input_column_storage_type,
            input_scan,
            column_types,
            instructions,
            current_aggregated_output_value: AggregatedOutputValue::None,
            peeked_row_information: None,
        }
    }
}

impl<T: TrieScan> TrieScan for TrieScanAggregate<T> {
    fn column_types(&self) -> &[StorageTypeName] {
        &self.column_types[..]
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

    fn current(&mut self, layer: usize) -> StorageValueT {
        if self.instructions.is_output_column_aggregated(layer) {
            match self.current_aggregated_output_value {
                AggregatedOutputValue::None => {
                    panic!("advance on layer needs to return Some first for TrieScanAggregate")
                }
                AggregatedOutputValue::Some(v) => v,
                AggregatedOutputValue::NotYetComputed => {
                    // We're trying to compute the current aggregate value.
                    // We know the underlying trie scan currently points to the first row of the current group (this is ensured by the last `advance_on_layer` call).
                    // Furthermore we know that there is at least one row to aggregate in the current group, otherwise the [`AggregatedOutputValue`] would be [`AggregatedOutputValue::None`]
                    // We now need to aggregate through all the rows of the current group to determine the aggregate result.

                    let original_group_by_values: Vec<_> = {
                        // Compute group by values in case we already peek into the next group, where the group-by values in the underlying trie scan would be change
                        (0..self.instructions.group_by_column_count)
                            .map(|layer_index| self.input_scan.current(layer_index))
                            .collect()
                    };

                    macro_rules! aggregate_for_storage_type {
                        ($variant:ident, $type:ty) => {{

                            // TODO: Update use of dynamic dispatch
                            let processor = self.instructions.aggregate_operation.create_processor();
                            let mut group_processor: Box<dyn AggregateGroupProcessor<$type>> = processor.group();

                            loop {
                                let new_value = match self
                                    .input_scan
                                    .current(self.instructions.aggregated_column_index)
                                {
                                    StorageValueT::$variant(v) => v,
                                    _ => {
                                        panic!("invalid storage value during aggregation")
                                    }
                                };

                                // Perform aggregation
                                group_processor.write_aggregate_input_value(new_value);

                                // Advance the underlying trie scan to find out if the is another row with the same group-by values.
                                if let Some(uppermost_modified_column_index) = self
                                    .input_scan
                                    .advance_on_layer(self.instructions.last_distinct_column_index)
                                {
                                    // Check if a group-by column was modified
                                    if uppermost_modified_column_index < self.instructions.group_by_column_count {
                                        // We have left the current aggregation group
                                        self.peeked_row_information = Some(PeekedRowInformation {
                                            uppermost_modified_column_index: Some(uppermost_modified_column_index),
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
                                self.current_aggregated_output_value =
                                    AggregatedOutputValue::Some(result);
                                return result;
                            } else {
                                panic!("failed to compute aggregate result");
                            }
                        }};
                    }

                    match self.aggregated_input_column_storage_type {
                        StorageTypeName::Id32 => aggregate_for_storage_type!(Id32, u32),
                        StorageTypeName::Id64 => aggregate_for_storage_type!(Id64, u64),
                        StorageTypeName::Int64 => aggregate_for_storage_type!(Int64, i64),
                        StorageTypeName::Float => {
                            aggregate_for_storage_type!(Float, Float)
                        }
                        StorageTypeName::Double => {
                            aggregate_for_storage_type!(Double, Double)
                        }
                    };
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
                    None => self.input_scan.current(layer),
                },
                AggregatedOutputValue::NotYetComputed => self.input_scan.current(layer),
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
pub struct TrieScanAggregateWrapper<'a> {
    /// Wrapped full trie scan
    pub trie_scan: TrieScanAggregate<TrieScanPrune<'a>>,
    /// Column types of the wrapped trie scan
    pub types: Vec<StorageTypeName>,
}

impl<'a> PartialTrieScan<'a> for TrieScanAggregateWrapper<'a> {
    fn up(&mut self) {
        panic!("TrieScanAggregateWrapper::up cannot be called");
    }

    fn down(&mut self) {
        panic!("TrieScanAggregateWrapper::down cannot be called");
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        panic!("TrieScanAggregateWrapper::current_scan cannot be called");
    }

    fn current_layer(&self) -> Option<usize> {
        panic!("TrieScanAggregateWrapper::current_layer cannot be called");
    }

    fn get_scan(&self, _index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        panic!("TrieScanAggregateWrapper::get_scan cannot be called");
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.types
    }
}

impl<'a> From<TrieScanAggregate<TrieScanPrune<'a>>> for TrieScanAggregateWrapper<'a> {
    fn from(value: TrieScanAggregate<TrieScanPrune<'a>>) -> Self {
        let types = value.column_types().to_vec();

        Self {
            trie_scan: value,
            types,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{AggregationInstructions, TrieScanAggregate};
    use crate::aggregates::operation::AggregateOperation;
    use crate::datatypes::storage_value::VecT;
    use crate::datatypes::StorageTypeName;

    use crate::tabular::operations::{materialize, TrieScanPrune};
    use crate::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::tabular::traits::partial_trie_scan::TrieScanEnum;
    use crate::tabular::traits::table::Table;

    fn trie_scan_prune_from_trie(input_trie: &Trie) -> TrieScanPrune<'_> {
        TrieScanPrune::new(TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(
            input_trie,
        )))
    }

    fn aggregate_and_materialize(
        input_trie: &Trie,
        aggregation_instructions: AggregationInstructions,
        aggregated_column_storage_type: StorageTypeName,
    ) -> Option<Trie> {
        let trie_scan_prune = trie_scan_prune_from_trie(input_trie);

        let mut trie_scan_aggregate = TrieScanAggregate::new(
            trie_scan_prune,
            aggregation_instructions,
            aggregated_column_storage_type,
        );

        materialize(&mut trie_scan_aggregate)
    }

    #[ignore]
    #[test]
    fn test_aggregate_i64() {
        let input_trie = Trie::from_cols(vec![
            VecT::Int64(vec![i64::MIN, 1, 3, 3, 3, 5, 5, i64::MAX]),
            VecT::Int64(vec![i64::MIN, 3, 2, 5, 11, 0, 0, i64::MAX]),
            VecT::Int64(vec![i64::MIN, 8, 9, 7, 4, i64::MIN, i64::MAX, i64::MAX]),
        ]);

        // Max, No group-by columns
        assert_eq!(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Max,
                    group_by_column_count: 0,
                    aggregated_column_index: 2,
                    last_distinct_column_index: 2,
                },
                StorageTypeName::Int64
            ),
            Some(Trie::from_cols(vec![VecT::Int64(vec![i64::MAX]),]))
        );
        // Max, one group-by column
        assert_eq!(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Max,
                    group_by_column_count: 1,
                    aggregated_column_index: 2,
                    last_distinct_column_index: 2,
                },
                StorageTypeName::Int64
            ),
            Some(Trie::from_cols(vec![
                VecT::Int64(vec![i64::MIN, 1, 3, 5, i64::MAX]),
                VecT::Int64(vec![i64::MIN, 8, 9, i64::MAX, i64::MAX]),
            ]))
        );
        // Max, two group-by columns
        assert_eq!(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Max,
                    group_by_column_count: 2,
                    aggregated_column_index: 2,
                    last_distinct_column_index: 2,
                },
                StorageTypeName::Int64
            ),
            Some(Trie::from_cols(vec![
                VecT::Int64(vec![i64::MIN, 1, 3, 3, 3, 5, i64::MAX]),
                VecT::Int64(vec![i64::MIN, 3, 2, 5, 11, 0, i64::MAX]),
                VecT::Int64(vec![i64::MIN, 8, 9, 7, 4, i64::MAX, i64::MAX]),
            ]))
        );
        // Count
        assert_eq!(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Count,
                    group_by_column_count: 0,
                    aggregated_column_index: 0,
                    last_distinct_column_index: 0,
                },
                StorageTypeName::Int64
            ),
            Some(Trie::from_cols(vec![VecT::Int64(vec![5])]))
        );
        // Sum
        assert_eq!(
            aggregate_and_materialize(
                &input_trie,
                AggregationInstructions {
                    aggregate_operation: AggregateOperation::Sum,
                    group_by_column_count: 1,
                    aggregated_column_index: 1,
                    last_distinct_column_index: 1,
                },
                StorageTypeName::Int64
            ),
            Some(Trie::from_cols(vec![
                VecT::Int64(vec![i64::MIN, 1, 3, 5, i64::MAX]),
                VecT::Int64(vec![i64::MIN, 3, 18, 0, i64::MAX]),
            ]))
        );
    }
}
