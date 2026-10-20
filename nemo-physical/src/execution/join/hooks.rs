//! The hooks of a join: its subtracted inputs, filters and bound layers.
//!
//! At every layer, the driver asks the hooks whether a value found there stands.
//! A join without hooks runs with [NoHooks].
//! [FullHooks] holds the state of the others:
//! - the cursors of the subtracted inputs,
//! - the filters, each an identity over storage values or a program with its evaluation buffers,
//! - the binders, which hold or compute the single value of a bound layer.

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::{
    dictionary::Dictionary,
    execution::{
        BoundValue, JoinShape,
        join::cursor::{SeekCursor, TaggedCursor},
    },
    function::{
        EvaluationContext, IdentityCondition, IdentityOperand, IntegerProgram, IntegerValue,
        StackProgram, StackValue,
    },
    storage::{PayloadEncoding, StorageValue, Tag},
    trie::{Intervals, Trie},
    value::{ErrorCounts, ValueRef},
};

/// A column of a subtracted input, following the descent.
#[derive(Debug)]
struct SubtractedColumn<'a> {
    /// The cursor over the column.
    cursor: TaggedCursor<'a>,
    /// The intervals of the input's next column, if that is mapped.
    child_intervals: Option<&'a Intervals>,
    /// Whether every previous column matched the current row,
    /// which is when this column constrains it.
    armed: bool,
}

/// A subtracted input, with its state following the descent:
/// a match of its last mapped column drops the value found at that layer.
#[derive(Debug)]
struct SubtractionState<'a> {
    /// The mapped columns, in trie order.
    columns: BumpVec<'a, SubtractedColumn<'a>>,
}

/// The program of a filter or a binder, with its evaluation state.
#[derive(Debug)]
struct ProgramState<'a> {
    /// The program.
    program: &'a StackProgram,
    /// The layers the program reads, in the order of its inputs.
    inputs: &'a [usize],
    /// The integer form of the program,
    /// selected if every layer it reads holds integers only.
    integer: Option<&'a IntegerProgram>,
    /// The evaluation stack of the generic interpreter, reused.
    stack: Vec<StackValue<'a>>,
    /// The inputs of the generic interpreter, reused.
    values: BumpVec<'a, ValueRef<'a>>,
    /// The evaluation stack of the integer form, reused.
    integer_stack: Vec<IntegerValue>,
    /// The inputs of the integer form, reused.
    integer_values: BumpVec<'a, i64>,
}

impl<'a> ProgramState<'a> {
    /// Creates the state of a program over the given layers,
    /// with its integer form if every layer allows it.
    fn new(
        program: &'a StackProgram,
        inputs: &'a [usize],
        integer: Option<&'a IntegerProgram>,
        bump: &'a Bump,
    ) -> Self {
        Self {
            program,
            inputs,
            integer,
            // The generic stack is allocated up front only where it is the only path:
            // allocating both stacks for every join costs,
            // and the integer form rarely falls back.
            stack: if integer.is_some() {
                Vec::new()
            } else {
                Vec::with_capacity(program.stack_bound())
            },
            values: BumpVec::with_capacity_in(inputs.len(), bump),
            integer_stack: Vec::new(),
            integer_values: BumpVec::with_capacity_in(inputs.len(), bump),
        }
    }

    /// Runs the integer form over the row through one of its entry points,
    /// such as [IntegerProgram::evaluate];
    /// `None` if the form is not selected, or falls back.
    fn run_integers<T>(
        &mut self,
        row: &[StorageValue],
        run: impl FnOnce(&IntegerProgram, &mut Vec<IntegerValue>, &[i64]) -> Option<T>,
    ) -> Option<T> {
        let program = self.integer?;

        self.integer_values.clear();
        self.integer_values.extend(
            self.inputs
                .iter()
                .map(|&layer| i64::from_payload(row[layer].payload)),
        );

        run(program, &mut self.integer_stack, &self.integer_values)
    }

    /// Runs the generic interpreter over the row through one of its entry points,
    /// such as [StackProgram::evaluate], its inputs decoded through the dictionary.
    fn run_values<T>(
        &mut self,
        row: &[StorageValue],
        dictionary: &'a Dictionary,
        context: EvaluationContext,
        run: impl FnOnce(
            &'a StackProgram,
            EvaluationContext,
            &mut Vec<StackValue<'a>>,
            &[ValueRef<'a>],
        ) -> T,
    ) -> T {
        self.values.clear();
        for &layer in self.inputs {
            let value = dictionary
                .decode(row[layer])
                .expect("trie values decode against their dictionary");
            self.values.push(value);
        }

        run(self.program, context, &mut self.stack, &self.values)
    }
}

/// What an [IdentityFilter] compares its layer's value with.
#[derive(Debug, Clone, Copy)]
enum IdentityTarget {
    /// The value of another layer.
    Layer(usize),
    /// The storage value of a constant,
    /// or `None` for a constant no storage value represents, which no layer holds.
    Constant(Option<StorageValue>),
}

/// The identity form of a filter, over the layers it reads; see [IdentityCondition].
#[derive(Debug, Clone, Copy)]
struct IdentityFilter {
    /// The layer compared.
    layer: usize,
    /// What the layer's value is compared with.
    target: IdentityTarget,
    /// Whether the filter passes if the values differ, rather than if they are identical.
    negated: bool,
}

impl IdentityFilter {
    /// Creates the identity form of a condition over the layers its inputs read,
    /// looking up the storage value of a constant in the dictionary.
    fn new(condition: &IdentityCondition, inputs: &[usize], dictionary: &Dictionary) -> Self {
        let target = match &condition.operand {
            IdentityOperand::Input(input) => IdentityTarget::Layer(inputs[*input]),
            IdentityOperand::Constant(value) => {
                IdentityTarget::Constant(dictionary.probe(value.as_ref()))
            }
        };

        Self {
            layer: inputs[condition.input],
            target,
            negated: condition.negated,
        }
    }

    /// Returns whether the row passes the filter.
    fn holds(self, row: &[StorageValue]) -> bool {
        let value = row[self.layer];
        let identical = match self.target {
            IdentityTarget::Layer(other) => value == row[other],
            IdentityTarget::Constant(constant) => constant == Some(value),
        };
        identical != self.negated
    }
}

/// A filter, with its evaluation state.
#[derive(Debug)]
enum FilterState<'a> {
    /// A filter decided on storage values.
    Identity(IdentityFilter),
    /// A filter evaluated by its program.
    Program(ProgramState<'a>),
}

/// The binder of a bound layer: what gives the layer its single value.
#[derive(Debug)]
struct BinderState<'a> {
    /// The layer's value.
    value: BinderValue<'a>,
    /// Whether the layer's value is still unconsumed in the current descent.
    pending: bool,
}

/// The value of a bound layer, as its binder holds it.
#[derive(Debug)]
enum BinderValue<'a> {
    /// The storage value of a constant,
    /// or `None` for a constant no storage value represents, which no layer holds.
    Constant(Option<StorageValue>),
    /// The program computing the value, with its evaluation state.
    Computed(ProgramState<'a>),
}

/// What a layer's binder dictates for the descent.
#[derive(Debug, Clone, Copy)]
pub(super) enum Bind {
    /// The layer is not bound, and enumerates its intersection.
    Free,
    /// The layer's value, to probe for.
    Value(StorageValue),
    /// Nothing matches:
    /// the value was consumed, the evaluation failed,
    /// or no stored value represents the constant or the result.
    Dead,
}

/// The hooks of a join, over the type `V` of the values its layers find.
///
/// The driver is generic over the hooks,
/// so a join without hooks runs with [NoHooks] and its checks vanish.
pub(super) trait Hooks<'a, V> {
    /// Returns whether the hooks at a layer admit a value found there;
    /// the row holds the value at that layer.
    fn admit(&mut self, layer: usize, value: V, row: &[V]) -> bool;

    /// Prepares the hooks of a layer for a descent from above.
    fn enter_layer(&mut self, layer: usize);

    /// Returns what the binder of a layer dictates, consuming its value.
    fn bind(&mut self, layer: usize, row: &[V]) -> Bind;

    /// Restarts the hooks for a fresh run of the same shape over other tries;
    /// the integer forms follow the tags of the new columns.
    fn rebind(&mut self, shape: &JoinShape<'a>, tries: &[&'a Trie]);
}

/// The hooks of a join without subtracted inputs, filters or bound layers: none.
#[derive(Debug)]
pub(super) struct NoHooks;

impl<'a, V> Hooks<'a, V> for NoHooks {
    #[inline]
    fn admit(&mut self, _layer: usize, _value: V, _row: &[V]) -> bool {
        true
    }

    #[inline]
    fn enter_layer(&mut self, _layer: usize) {}

    #[inline]
    fn bind(&mut self, _layer: usize, _row: &[V]) -> Bind {
        Bind::Free
    }

    #[inline]
    fn rebind(&mut self, _shape: &JoinShape<'a>, _tries: &[&'a Trie]) {}
}

/// The hooks of a join with subtracted inputs, filters or bound layers.
#[derive(Debug)]
pub(super) struct FullHooks<'a> {
    /// The subtracted inputs.
    subtractions: BumpVec<'a, SubtractionState<'a>>,
    /// Per layer, the subtracted columns compared there,
    /// as pairs of the input's and the column's index.
    subtracted_at_layer: BumpVec<'a, BumpVec<'a, (usize, usize)>>,
    /// The filters.
    filters: BumpVec<'a, FilterState<'a>>,
    /// Per layer, the filters checked there: at the deepest layer each reads.
    filters_at_layer: BumpVec<'a, BumpVec<'a, usize>>,
    /// The binders of the bound layers.
    binders: BumpVec<'a, BinderState<'a>>,
    /// Per layer, its binder, if the layer is bound.
    binder_at_layer: BumpVec<'a, Option<usize>>,
    /// The dictionary the programs decode their inputs through;
    /// present if there are filters or binders.
    dictionary: Option<&'a Dictionary>,
    /// The context the programs evaluate in.
    context: EvaluationContext,
    /// The evaluation errors counted per kind;
    /// the rows they occurred on were dropped.
    errors: ErrorCounts,
}

impl<'a> Hooks<'a, StorageValue> for FullHooks<'a> {
    fn admit(&mut self, layer: usize, value: StorageValue, row: &[StorageValue]) -> bool {
        if self.blocked(layer, value) {
            return false;
        }
        self.passes_filters(layer, row)
    }

    fn enter_layer(&mut self, layer: usize) {
        for entry in 0..self.subtracted_at_layer[layer].len() {
            let (input, column) = self.subtracted_at_layer[layer][entry];
            self.subtractions[input].columns[column].cursor.rewind();
        }

        if let Some(binder) = self.binder_at_layer[layer] {
            self.binders[binder].pending = true;
        }
    }

    fn bind(&mut self, layer: usize, row: &[StorageValue]) -> Bind {
        let Some(index) = self.binder_at_layer[layer] else {
            return Bind::Free;
        };

        let binder = &mut self.binders[index];

        if !std::mem::replace(&mut binder.pending, false) {
            return Bind::Dead;
        }

        let state = match &mut binder.value {
            BinderValue::Constant(value) => return value.map_or(Bind::Dead, Bind::Value),
            BinderValue::Computed(state) => state,
        };

        if let Some(IntegerValue::Integer(result)) =
            state.run_integers(row, IntegerProgram::evaluate)
        {
            return Bind::Value(StorageValue::integer(result));
        }
        // The integer form is not selected, fell back, or its result is not an integer:
        // the generic interpreter decides.

        let dictionary = self
            .dictionary
            .expect("joins with binders carry a dictionary");
        match state
            .run_values(row, dictionary, self.context, StackProgram::evaluate)
            .as_result()
        {
            Ok(value) => dictionary.probe(value).map_or(Bind::Dead, Bind::Value),
            Err(error) => {
                self.errors.record(error);
                Bind::Dead
            }
        }
    }

    fn rebind(&mut self, shape: &JoinShape<'a>, tries: &[&'a Trie]) {
        for subtraction in &mut self.subtractions {
            for (index, column) in subtraction.columns.iter_mut().enumerate() {
                column.armed = index == 0;
                column.cursor.rewind();
            }
        }
        for binder in &mut self.binders {
            binder.pending = false;
        }
        self.errors = ErrorCounts::default();

        for (state, filter) in self.filters.iter_mut().zip(shape.filters) {
            if let FilterState::Program(state) = state {
                state.integer =
                    integer_form(shape, tries, &filter.inputs, filter.program.integer());
            }
        }
        for (binder, bound) in self.binders.iter_mut().zip(shape.bound_layers) {
            if let (BinderValue::Computed(state), BoundValue::Computed { inputs, program }) =
                (&mut binder.value, &bound.value)
            {
                state.integer = integer_form(shape, tries, inputs, program.integer());
            }
        }
    }
}

impl<'a> FullHooks<'a> {
    /// Returns the hooks of a shape over the tries it joins, indexed as the shape indexes them,
    /// or `None` for a shape without hooks.
    ///
    /// The programs decode their inputs through the dictionary and evaluate in the context.
    pub(super) fn build(
        shape: &JoinShape<'a>,
        tries: &[&'a Trie],
        subtracted: &[&'a Trie],
        dictionary: Option<&'a Dictionary>,
        context: EvaluationContext,
        bump: &'a Bump,
    ) -> Option<Self> {
        if shape.subtracted.is_empty() && shape.filters.is_empty() && shape.bound_layers.is_empty()
        {
            return None;
        }

        let (subtractions, subtracted_at_layer) = subtractions(shape, subtracted, bump);
        let (filters, filters_at_layer) = filter_hooks(shape, tries, dictionary, bump);
        let (binders, binder_at_layer) = binder_hooks(shape, tries, dictionary, bump);

        Some(Self {
            subtractions,
            subtracted_at_layer,
            filters,
            filters_at_layer,
            binders,
            binder_at_layer,
            dictionary,
            context,
            errors: ErrorCounts::default(),
        })
    }

    /// Returns the evaluation errors counted so far, per kind.
    pub(super) fn errors(&self) -> ErrorCounts {
        self.errors
    }

    /// Returns whether a filter runs its integer form.
    #[cfg(test)]
    pub(super) fn filter_uses_integer_form(&self, index: usize) -> bool {
        matches!(&self.filters[index], FilterState::Program(state) if state.integer.is_some())
    }

    /// Returns whether a filter is decided on storage values.
    #[cfg(test)]
    pub(super) fn filter_uses_identity_form(&self, index: usize) -> bool {
        matches!(self.filters[index], FilterState::Identity(_))
    }

    /// Compares a value with the subtracted columns at a layer,
    /// arms or disarms the columns after them,
    /// and returns whether some subtracted input matches the row on every mapped column.
    fn blocked(&mut self, layer: usize, value: StorageValue) -> bool {
        let mut blocked = false;
        for entry in 0..self.subtracted_at_layer[layer].len() {
            let (input, column) = self.subtracted_at_layer[layer][entry];
            let subtraction = &mut self.subtractions[input];

            let hit = subtraction.columns[column].armed
                && subtraction.columns[column].cursor.seek(value) == Some(value);

            if column + 1 < subtraction.columns.len() {
                // A column constrains the row only where the columns before it matched;
                // a miss disarms the rest, since each column passes its state on.
                subtraction.columns[column + 1].armed = hit;
                if hit {
                    let position = subtraction.columns[column].cursor.position();
                    let intervals = subtraction.columns[column]
                        .child_intervals
                        .expect("a subtracted column before the last has child intervals");
                    subtraction.columns[column + 1]
                        .cursor
                        .set_range(intervals.children(position));
                }
            } else if hit {
                blocked = true;
            }
        }
        blocked
    }

    /// Returns whether the row passes the filters checked at a layer;
    /// a failed evaluation is counted and rejects the row.
    fn passes_filters(&mut self, layer: usize, row: &[StorageValue]) -> bool {
        for entry in 0..self.filters_at_layer[layer].len() {
            let filter = self.filters_at_layer[layer][entry];
            if !self.evaluate_filter(filter, row) {
                return false;
            }
        }

        true
    }

    /// Returns whether the row passes a filter:
    /// by its identity form, else by its integer form if selected, else by its program.
    fn evaluate_filter(&mut self, index: usize, row: &[StorageValue]) -> bool {
        let state = match &mut self.filters[index] {
            FilterState::Identity(identity) => return identity.holds(row),
            FilterState::Program(state) => state,
        };

        if let Some(pass) = state.run_integers(row, IntegerProgram::filter) {
            return pass;
        }
        // The integer form is not selected, or fell back where an intermediate outgrew `i64`:
        // the generic interpreter decides.

        let dictionary = self
            .dictionary
            .expect("joins with filters carry a dictionary");
        match state.run_values(row, dictionary, self.context, StackProgram::filter) {
            Ok(pass) => pass,
            Err(error) => {
                self.errors.record(error);
                false
            }
        }
    }
}

/// Returns the cursors of the subtracted inputs,
/// and per layer the pairs of input and column index compared there.
fn subtractions<'a>(
    shape: &JoinShape<'a>,
    subtracted: &[&'a Trie],
    bump: &'a Bump,
) -> (
    BumpVec<'a, SubtractionState<'a>>,
    BumpVec<'a, BumpVec<'a, (usize, usize)>>,
) {
    let mut subtractions = BumpVec::with_capacity_in(shape.subtracted.len(), bump);
    let mut subtracted_at_layer =
        BumpVec::from_iter_in((0..shape.layers.len()).map(|_| BumpVec::new_in(bump)), bump);

    for (index, (input, &trie)) in shape.subtracted.iter().zip(subtracted).enumerate() {
        let columns = BumpVec::from_iter_in(
            input.layers.iter().enumerate().map(|(column, &layer)| {
                subtracted_at_layer[layer].push((index, column));
                SubtractedColumn {
                    cursor: TaggedCursor::new(trie.column(column)),
                    child_intervals: (column + 1 < input.layers.len())
                        .then(|| trie.intervals(column + 1)),
                    armed: column == 0,
                }
            }),
            bump,
        );
        subtractions.push(SubtractionState { columns });
    }

    (subtractions, subtracted_at_layer)
}

/// Returns the states of the filters,
/// and per layer the filters checked there: at the deepest layer each reads.
///
/// The constants of the identity forms are looked up in the dictionary here, once.
fn filter_hooks<'a>(
    shape: &JoinShape<'a>,
    tries: &[&'a Trie],
    dictionary: Option<&'a Dictionary>,
    bump: &'a Bump,
) -> (
    BumpVec<'a, FilterState<'a>>,
    BumpVec<'a, BumpVec<'a, usize>>,
) {
    let mut filters = BumpVec::with_capacity_in(shape.filters.len(), bump);
    let mut filters_at_layer =
        BumpVec::from_iter_in((0..shape.layers.len()).map(|_| BumpVec::new_in(bump)), bump);

    for (index, filter) in shape.filters.iter().enumerate() {
        let attach = *filter
            .inputs
            .iter()
            .max()
            .expect("shapes validate filters to read a layer");
        filters_at_layer[attach].push(index);

        let state = match filter.program.identity() {
            Some(condition) => {
                let dictionary = dictionary.expect("joins with filters carry a dictionary");
                FilterState::Identity(IdentityFilter::new(condition, &filter.inputs, dictionary))
            }
            None => {
                let integer = integer_form(shape, tries, &filter.inputs, filter.program.integer());
                FilterState::Program(ProgramState::new(
                    filter.program.generic(),
                    &filter.inputs,
                    integer,
                    bump,
                ))
            }
        };
        filters.push(state);
    }

    (filters, filters_at_layer)
}

/// Returns the binders of the bound layers, and per layer its binder.
///
/// The constants are looked up in the dictionary here, once.
fn binder_hooks<'a>(
    shape: &JoinShape<'a>,
    tries: &[&'a Trie],
    dictionary: Option<&'a Dictionary>,
    bump: &'a Bump,
) -> (BumpVec<'a, BinderState<'a>>, BumpVec<'a, Option<usize>>) {
    let mut binders = BumpVec::with_capacity_in(shape.bound_layers.len(), bump);
    let mut binder_at_layer =
        BumpVec::from_iter_in(std::iter::repeat_n(None, shape.layers.len()), bump);

    for (index, bound) in shape.bound_layers.iter().enumerate() {
        binder_at_layer[bound.layer] = Some(index);

        let value = match &bound.value {
            BoundValue::Constant(constant) => {
                let dictionary = dictionary.expect("joins with binders carry a dictionary");
                BinderValue::Constant(dictionary.probe(constant.as_ref()))
            }
            BoundValue::Computed { inputs, program } => {
                let integer = integer_form(shape, tries, inputs, program.integer());
                BinderValue::Computed(ProgramState::new(program.generic(), inputs, integer, bump))
            }
        };
        binders.push(BinderState {
            value,
            pending: false,
        });
    }

    (binders, binder_at_layer)
}

/// Returns a program's integer form if the tries allow it:
/// if every column intersected at a layer the program reads holds integers only.
fn integer_form<'a>(
    shape: &JoinShape<'a>,
    tries: &[&'a Trie],
    inputs: &[usize],
    integer: Option<&'a IntegerProgram>,
) -> Option<&'a IntegerProgram> {
    let integers_only = |layer: usize| {
        shape.layers[layer].iter().all(|input_column| {
            tries[input_column.input]
                .column(input_column.column)
                .uniform_tag()
                == Some(Tag::Int)
        })
    };

    let integer = integer?;
    inputs
        .iter()
        .all(|&layer| integers_only(layer))
        .then_some(integer)
}
