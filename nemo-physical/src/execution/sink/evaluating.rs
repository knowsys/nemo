//! The evaluating sink: a row stage over a join's rows,
//! which evaluates computed columns and post-filters on each row.
//!
//! The joins read the dictionary while they run, so no value can be added to it yet.
//! The row stage therefore runs in two phases.
//! The [RowEvaluator] evaluates the programs as a join drains into it,
//! and keeps their results apart from the rows.
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
    /// A value with its storage value: inline, or an entry the dictionary holds already.
    Stored(StorageValue),
    /// A value the dictionary stores, added once the join is done.
    Value(DataValue),
}

impl Computed {
    /// Returns the result holding a value:
    /// its storage value if the value is inline or a stored structure read from the dictionary,
    /// and the value otherwise.
    fn of(value: ValueRef<'_>, dictionary: &Dictionary) -> Self {
        match StorageValue::from_value(value).or_else(|| dictionary.entry_of(value)) {
            Some(stored) => Computed::Stored(stored),
            None => Computed::Value(value.to_owned()),
        }
    }
}

/// The result of a computed column on one row, or the error its evaluation raised.
type ComputedResult = Result<Computed, EvaluationError>;

/// The sink evaluating a row stage on a join's rows, as its [shape](RowStageShape) specifies.
///
/// A join drains into the evaluator, which keeps the rows every post-filter passes
/// together with the results of their computed columns;
/// [finish](Self::finish) hands them over for encoding.
///
/// Each computed column keeps its result until a row changes the deepest layer it reads,
/// or one above it, so a program over shallow layers runs once per distinct prefix.
/// A program runs its integer form if it has one and its inputs are integers on the row,
/// and the generic interpreter otherwise.
#[derive(Debug)]
pub(crate) struct RowEvaluator<'a> {
    /// The shape of the row stage.
    shape: &'a RowStageShape,
    /// The dictionary the layer values decode through.
    dictionary: &'a Dictionary,
    /// The context the programs evaluate in.
    context: EvaluationContext,
    /// The emitted values of the rows kept, in join order.
    prefixes: RowBuffer,
    /// Per row kept and computed column, the index of its result.
    chosen: Vec<usize>,
    /// Per computed column, its distinct results so far, the last of them the current one.
    unique: Vec<Vec<ComputedResult>>,
    /// The rows a post-filter dropped by erroring, counted by kind.
    dropped: ErrorCounts,
    /// The evaluation stack of the integer forms.
    integer_stack: Vec<IntegerValue>,
    /// The inputs of the integer form run last.
    integers: Vec<i64>,
    /// The evaluation stack of the generic interpreter.
    stack: Vec<StackValue<'a>>,
    /// The inputs of the generic interpreter run last, decoded.
    values: Vec<ValueRef<'a>>,
}

impl<'a> RowEvaluator<'a> {
    /// Creates the evaluator of a shape over joins emitting `emitted` layers;
    /// the values decode through the dictionary, and the programs evaluate in the context.
    ///
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

    /// Evaluates the computed columns whose results are stale on a row,
    /// given the shallowest layer the row changed.
    ///
    /// A result is stale if the column is volatile, if it has none yet,
    /// or if the row changed the deepest layer the column reads, or one above it.
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

    /// Checks the post-filters on a row, and returns whether every one passes it.
    ///
    /// A post-filter that errors drops the row with its error.
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

    /// Returns a layer's value decoded.
    fn decode(&self, value: StorageValue) -> ValueRef<'a> {
        let dictionary: &'a Dictionary = self.dictionary;
        dictionary
            .decode(value)
            .expect("storage values decode against their dictionary")
    }

    /// Runs a program's integer form on a row, through one of its entry points,
    /// such as [IntegerProgram::evaluate].
    ///
    /// Returns `None` if the program has no integer form,
    /// if an input is not an integer on this row, or if the integer form gives up.
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
                    // A value the dictionary stores is no integer,
                    // and an error propagates through the generic interpreter.
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

    /// Runs the generic interpreter over a row through one of its entry points,
    /// such as [StackProgram::evaluate], its layer inputs decoded through the dictionary.
    ///
    /// The interpreter runs in the reusable buffers if every input is a layer or an inline result.
    /// An owned result among the inputs lives in the results collected so far,
    /// which the buffers cannot borrow; the run then borrows them in buffers of its own.
    /// An erroring input propagates its error.
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

    /// Runs the generic interpreter over a row in buffers of its own,
    /// borrowing the owned results among its inputs; see [run_generic](Self::run_generic).
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
    /// Per row and computed column, the index of its result.
    chosen: Vec<usize>,
    /// Per computed column, its distinct results, in the order of their first use.
    unique: Vec<Vec<ComputedResult>>,
    /// The rows a post-filter dropped by erroring, counted by kind.
    dropped: ErrorCounts,
}

impl PendingRows {
    /// Encodes the results of the rows and adds the completed rows to a buffer:
    /// the emitted layers, then the delivered computed columns, given by index.
    /// Returns the errors counted per kind, those of the post-filters included.
    ///
    /// Each distinct result is encoded once, and one with its storage value needs no encoding.
    /// A row whose computation errored, or whose result the dictionary cannot store, is dropped,
    /// and the error is counted per evaluation;
    /// that holds for the columns the row does not deliver as well.
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
