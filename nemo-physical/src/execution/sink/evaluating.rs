//! The evaluating sink: a row stage over a join's rows,
//! which evaluates computed columns and post-filters on each row.
//!
//! The joins read the dictionary while they run, so no value can be added to it yet.
//! The row stage therefore runs in two phases.
//! The [RowEvaluator] evaluates the programs as a join drains into it,
//! and stores their results separately from the rows.
//! Once every join is done, [encoding](PendingRows::encode) adds the results to the dictionary
//! and completes the rows.

use crate::{
    dictionary::Dictionary,
    execution::{RowInput, RowStageShape, sink::RowSink},
    function::{
        EvaluationContext, IntegerProgram, IntegerValue, SpecializedProgram, StackProgram,
        StackValue,
    },
    storage::{PayloadEncoding, StorageValue, Tag},
    trie::RowBuffer,
    value::{DataValue, ErrorCounts, EvaluationError, ValueRef},
};

/// The result of a computed column on one row.
#[derive(Debug, Clone)]
enum Computed {
    /// A value that has a storage value:
    /// an inline value, or an entry that the dictionary has already.
    Stored(StorageValue),
    /// A value without a known storage value, encoded once the joins are done.
    Value(DataValue),
}

impl Computed {
    /// Returns the result for a value.
    ///
    /// The result is the value's storage value if the value is inline,
    /// or if it is a tuple or map read from the dictionary, which knows its entry.
    /// Any other value is kept as it is, to be encoded later.
    fn of(value: ValueRef<'_>, dictionary: &Dictionary) -> Self {
        match StorageValue::from_value(value).or_else(|| dictionary.entry_of(value)) {
            Some(stored) => Computed::Stored(stored),
            None => Computed::Value(value.to_owned()),
        }
    }
}

/// The result of a computed column on one row, or the error its evaluation raised.
type ComputedResult = Result<Computed, EvaluationError>;

/// The sink that evaluates a row stage on a join's rows, as its [shape](RowStageShape) specifies.
///
/// A join drains into the evaluator, which keeps the rows that every post-filter passes,
/// with the results of their computed columns.
/// [finish](Self::finish) returns them for encoding.
///
/// Each computed column keeps its result until a row changes the deepest layer it reads,
/// or a shallower one, so a program over shallow layers runs once per distinct prefix.
/// A program runs its integer form if it has one and its inputs are integers on the row,
/// and the generic interpreter otherwise.
#[derive(Debug)]
pub(crate) struct RowEvaluator<'a> {
    /// The shape of the row stage.
    shape: &'a RowStageShape,
    /// The dictionary the layer values are decoded through.
    dictionary: &'a Dictionary,
    /// The context the programs evaluate in.
    context: EvaluationContext,
    /// The emitted values of the kept rows, in join order.
    prefixes: RowBuffer,
    /// Per kept row and computed column, the index of the column's result in `unique`.
    chosen: Vec<usize>,
    /// Per computed column, the results of its evaluations so far,
    /// the last of them the current one.
    unique: Vec<Vec<ComputedResult>>,
    /// The errors of the post-filters, each of which dropped its row, counted by kind.
    dropped: ErrorCounts,
    /// The evaluation stack of the integer forms.
    integer_stack: Vec<IntegerValue>,
    /// The buffer for the inputs of an integer form.
    integers: Vec<i64>,
    /// The evaluation stack of the generic interpreter.
    stack: Vec<StackValue<'a>>,
    /// The buffer for the decoded inputs of the generic interpreter.
    values: Vec<ValueRef<'a>>,
}

impl<'a> RowEvaluator<'a> {
    /// Creates the evaluator of a shape over joins that emit `emitted` layers.
    ///
    /// The values are decoded through `dictionary`, and the programs evaluate in `context`.
    /// Only debug builds check that the shape fits the joins.
    pub(crate) fn new(
        shape: &'a RowStageShape,
        emitted: usize,
        dictionary: &'a Dictionary,
        context: EvaluationContext,
    ) -> Self {
        debug_assert!(
            shape.validate_fit(emitted).is_ok(),
            "a row stage fits the joins it reads"
        );

        Self {
            shape,
            dictionary,
            context,
            prefixes: RowBuffer::new(emitted),
            chosen: Vec::new(),
            unique: (0..shape.computed.len()).map(|_| Vec::new()).collect(),
            dropped: ErrorCounts::default(),
            integer_stack: Vec::new(),
            integers: Vec::new(),
            stack: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Ends the evaluation, and returns the rows kept.
    pub(crate) fn finish(self) -> PendingRows {
        PendingRows {
            prefixes: self.prefixes,
            chosen: self.chosen,
            unique: self.unique,
            dropped: self.dropped,
        }
    }

    /// Returns whether a row passes the row stage, without collecting the row.
    ///
    /// A row passes if each of its computed columns has a result,
    /// and every post-filter holds on it.
    /// Whether the dictionary can store the results is not known yet,
    /// and [unstored](Self::unstored) returns the results that this depends on.
    /// The errors of a row that does not pass are not counted.
    ///
    /// `changed` is the shallowest layer the row changed since the row checked before it.
    /// An evaluator either checks rows in this way or collects the rows it
    /// [accepts](RowSink::accept), since a check keeps only the results of its own row.
    pub(crate) fn passes(&mut self, row: &[StorageValue], changed: usize) -> bool {
        debug_assert!(
            self.prefixes.is_empty(),
            "an evaluator that collects rows keeps the results of them all"
        );

        // No row refers to the results of the rows checked before.
        for results in &mut self.unique {
            let earlier = results.len().saturating_sub(1);
            results.drain(..earlier);
        }

        self.evaluate_columns(row, changed);

        let computed = self
            .unique
            .iter()
            .all(|results| results.last().is_some_and(Result::is_ok));

        computed && matches!(self.check_filters(row), Ok(true))
    }

    /// Ends the evaluation, and returns the results of the computed columns on the last row
    /// evaluated that have no storage value yet.
    ///
    /// The row is a row of the row stage only if the dictionary can store them all.
    pub(crate) fn unstored(self) -> Vec<DataValue> {
        self.unique
            .into_iter()
            .filter_map(|mut results| match results.pop() {
                Some(Ok(Computed::Value(value))) => Some(value),
                _ => None,
            })
            .collect()
    }

    /// Evaluates the computed columns whose results are stale on a row.
    ///
    /// `changed` is the shallowest layer the row changed.
    /// A result is stale if the column is volatile, if the column has no result yet,
    /// or if the row changed the deepest layer the column reads, or a shallower one.
    fn evaluate_columns(&mut self, row: &[StorageValue], changed: usize) {
        let shape = self.shape;
        for (column, computed) in shape.computed.iter().enumerate() {
            let stale = computed.volatile
                || self.unique[column].is_empty()
                || computed.deepest.is_some_and(|deepest| changed <= deepest);
            if !stale {
                continue;
            }

            let integer = self.run_integers(
                &computed.program,
                &computed.inputs,
                row,
                IntegerProgram::evaluate,
            );
            let result = match integer {
                Some(IntegerValue::Integer(value)) => {
                    Ok(Computed::Stored(StorageValue::integer(value)))
                }
                Some(IntegerValue::Boolean(value)) => {
                    Ok(Computed::Stored(StorageValue::boolean(value)))
                }
                None => {
                    let dictionary = self.dictionary;
                    self.run_generic(
                        computed.program.generic(),
                        &computed.inputs,
                        row,
                        |program, context, stack, values| {
                            program
                                .evaluate(context, stack, values)
                                .as_result()
                                .map(|value| Computed::of(value, dictionary))
                        },
                    )
                }
            };
            self.unique[column].push(result);
        }
    }

    /// Returns whether a row passes every post-filter.
    ///
    /// Fails with the error of a post-filter that fails to evaluate,
    /// which may come from a computed column it reads.
    fn check_filters(&mut self, row: &[StorageValue]) -> Result<bool, EvaluationError> {
        let shape = self.shape;
        for filter in &shape.post_filters {
            let integer =
                self.run_integers(&filter.program, &filter.inputs, row, IntegerProgram::filter);
            let passes = match integer {
                Some(passes) => passes,
                None => self.run_generic(
                    filter.program.generic(),
                    &filter.inputs,
                    row,
                    |program, context, stack, values| program.filter(context, stack, values),
                )?,
            };
            if !passes {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Returns the current result of a computed column.
    fn current(&self, column: usize) -> &ComputedResult {
        self.unique[column]
            .last()
            .expect("computed columns evaluate in order")
    }

    /// Decodes a layer's value.
    fn decode(&self, value: StorageValue) -> ValueRef<'a> {
        let dictionary: &'a Dictionary = self.dictionary;
        dictionary
            .decode(value)
            .expect("storage values decode against their dictionary")
    }

    /// Runs a program's integer form on a row through `run`,
    /// one of its entry points such as [IntegerProgram::evaluate].
    ///
    /// Returns `None` if the program has no integer form,
    /// if an input is not an integer on this row, or if the integer form falls back.
    /// The generic interpreter decides then.
    fn run_integers<T>(
        &mut self,
        program: &SpecializedProgram,
        inputs: &[RowInput],
        row: &[StorageValue],
        run: impl FnOnce(&IntegerProgram, &mut Vec<IntegerValue>, &[i64]) -> Option<T>,
    ) -> Option<T> {
        let integer = program.integer()?;

        self.integers.clear();
        for &input in inputs {
            let value = match input {
                RowInput::Layer(layer) => row[layer],
                RowInput::Computed(column) => match self.current(column) {
                    Ok(Computed::Stored(value)) => *value,
                    // A value without a storage value is no integer, since integers are inline,
                    // and the generic interpreter passes an error on.
                    Ok(Computed::Value(_)) | Err(_) => return None,
                },
            };
            if value.tag != Tag::Int {
                return None;
            }
            self.integers.push(i64::from_payload(value.payload));
        }

        run(integer, &mut self.integer_stack, &self.integers)
    }

    /// Runs the generic interpreter on a row through `run`,
    /// one of its entry points such as [StackProgram::evaluate].
    ///
    /// The layer inputs are decoded through the dictionary.
    /// The interpreter runs in the reusable buffers if every input is a layer
    /// or a result with a storage value.
    /// Any other result lives in `unique`, which the reusable buffers cannot borrow,
    /// so the run then uses buffers of its own, through [run_borrowing](Self::run_borrowing).
    /// An input that failed passes its error on.
    fn run_generic<T>(
        &mut self,
        program: &'a StackProgram,
        inputs: &[RowInput],
        row: &[StorageValue],
        run: impl for<'v> FnOnce(
            &'v StackProgram,
            EvaluationContext,
            &mut Vec<StackValue<'v>>,
            &[ValueRef<'v>],
        ) -> Result<T, EvaluationError>,
    ) -> Result<T, EvaluationError> {
        self.values.clear();
        for &input in inputs {
            let stored = match input {
                RowInput::Layer(layer) => row[layer],
                RowInput::Computed(column) => match self.current(column) {
                    Ok(Computed::Stored(value)) => *value,
                    Ok(Computed::Value(_)) => return self.run_borrowing(program, inputs, row, run),
                    Err(error) => return Err(*error),
                },
            };
            let value = self.decode(stored);
            self.values.push(value);
        }

        run(program, self.context, &mut self.stack, &self.values)
    }

    /// Runs the generic interpreter on a row as [run_generic](Self::run_generic) does,
    /// but in buffers of its own, which can borrow the results without a storage value.
    fn run_borrowing<T>(
        &self,
        program: &StackProgram,
        inputs: &[RowInput],
        row: &[StorageValue],
        run: impl for<'v> FnOnce(
            &'v StackProgram,
            EvaluationContext,
            &mut Vec<StackValue<'v>>,
            &[ValueRef<'v>],
        ) -> Result<T, EvaluationError>,
    ) -> Result<T, EvaluationError> {
        let mut values = Vec::with_capacity(inputs.len());
        for &input in inputs {
            let stored = match input {
                RowInput::Layer(layer) => row[layer],
                RowInput::Computed(column) => match self.current(column) {
                    Ok(Computed::Stored(value)) => *value,
                    Ok(Computed::Value(value)) => {
                        values.push(value.as_ref());
                        continue;
                    }
                    Err(error) => return Err(*error),
                },
            };
            values.push(self.decode(stored));
        }

        let mut stack = Vec::with_capacity(program.stack_bound());
        run(program, self.context, &mut stack, &values)
    }
}

impl RowSink for RowEvaluator<'_> {
    #[inline]
    fn accept(&mut self, row: &[StorageValue], changed: usize) {
        self.evaluate_columns(row, changed);
        match self.check_filters(row) {
            Ok(true) => {
                self.chosen
                    .extend(self.unique.iter().map(|results| results.len() - 1));
                self.prefixes.push_row(row);
            }
            Ok(false) => {}
            Err(error) => self.dropped.record(error),
        }
    }
}

/// The rows a row evaluator kept, awaiting their computed values.
#[derive(Debug)]
pub(crate) struct PendingRows {
    /// The emitted values of the rows, in join order.
    prefixes: RowBuffer,
    /// Per row and computed column, the index of the column's result in `unique`.
    chosen: Vec<usize>,
    /// Per computed column, the results of its evaluations, in order.
    unique: Vec<Vec<ComputedResult>>,
    /// The errors of the post-filters, each of which dropped its row, counted by kind.
    dropped: ErrorCounts,
}

impl PendingRows {
    /// Encodes the results of the rows, and adds the completed rows to `buffer`.
    ///
    /// A completed row has the emitted layers,
    /// then the computed columns that `delivered` lists by index.
    /// Returns the errors counted by kind, those of the post-filters included.
    ///
    /// Each result is encoded once, however many rows share it,
    /// and a result with a storage value needs no encoding.
    /// A row is dropped if one of its computed columns failed,
    /// or has a result the dictionary cannot store,
    /// even if the row does not deliver that column.
    /// Such an error is counted once per evaluation, not per row.
    pub(crate) fn encode(
        self,
        delivered: &[usize],
        dictionary: &mut Dictionary,
        buffer: &mut RowBuffer,
    ) -> ErrorCounts {
        let mut errors = self.dropped;

        let columns = self.unique.len();
        let encoded: Vec<Vec<Option<StorageValue>>> = self
            .unique
            .into_iter()
            .map(|results| {
                results
                    .into_iter()
                    .map(|result| match result {
                        Ok(Computed::Stored(value)) => Some(value),
                        Ok(Computed::Value(value)) => {
                            let encoded = dictionary.encode(value.as_ref());
                            if encoded.is_none() {
                                errors.record(EvaluationError::Unsupported);
                            }
                            encoded
                        }
                        Err(error) => {
                            errors.record(error);
                            None
                        }
                    })
                    .collect()
            })
            .collect();

        // The collected rows are distinct and sorted, and extending them keeps them so.
        let mut row = Vec::with_capacity(self.prefixes.arity() + delivered.len());
        'rows: for index in 0..self.prefixes.len() {
            let chosen = &self.chosen[index * columns..(index + 1) * columns];
            for (column, &result) in chosen.iter().enumerate() {
                if encoded[column][result].is_none() {
                    continue 'rows;
                }
            }

            row.clear();
            row.extend_from_slice(self.prefixes.row(index));
            for &column in delivered {
                row.push(encoded[column][chosen[column]].expect("the row's results were checked"));
            }
            buffer.push_row(&row);
        }

        errors
    }
}
