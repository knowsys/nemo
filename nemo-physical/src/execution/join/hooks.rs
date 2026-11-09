//! The hooks of a join: its subtracted inputs, filters and bound layers.
//!
//! At every layer, the driver asks the hooks whether the layer is bound to a value,
//! and whether to keep a value found there.
//! A join without hooks runs with [NoHooks], and any other join with [FullHooks].

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

/// A column of a subtracted input, whose cursor follows the join's descent.
#[derive(Debug)]
struct SubtractedColumn<'a> {
    /// The cursor over the column.
    cursor: TaggedCursor<'a>,
    /// The intervals of the input's next column, if the subtracted input maps it.
    child_intervals: Option<&'a Intervals>,
    /// Whether every earlier column of the input matched the current row,
    /// so that this column constrains the row.
    armed: bool,
}

/// The state of a subtracted input during the join's descent.
///
/// A match of its last column drops the value found at that column's layer.
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
    /// selected if every layer it reads has integers only.
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
    /// Creates the state of a program over the given layers.
    ///
    /// `integer` is the program's integer form, if it is selected.
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
            // The generic stack is allocated up front only if there is no integer form.
            // Allocating both stacks for every join is costly,
            // and the integer form rarely falls back to the generic interpreter.
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

    /// Runs the integer form on the row through `run`,
    /// one of its entry points such as [IntegerProgram::evaluate].
    ///
    /// Returns `None` if the integer form is not selected, or if it falls back.
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

    /// Runs the program on the row through `run`,
    /// one of the generic interpreter's entry points such as [StackProgram::evaluate].
    ///
    /// The inputs are decoded through the dictionary.
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
    /// or `None` for a constant without a storage value, which no layer has.
    Constant(Option<StorageValue>),
}

/// The identity form of a filter, with layers in place of the condition's inputs.
///
/// It compares the storage value of a layer with that of another layer or a constant,
/// as [IdentityCondition] describes.
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
    /// Creates the identity form of a condition whose input `i` reads layer `inputs[i]`.
    ///
    /// The storage value of a constant is looked up in the dictionary.
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
    /// A filter decided by its identity form, on storage values.
    Identity(IdentityFilter),
    /// A filter evaluated by its program.
    Program(ProgramState<'a>),
}

/// The binder of a bound layer: what gives the layer its single value.
#[derive(Debug)]
struct BinderState<'a> {
    /// The layer's value.
    value: BinderValue<'a>,
    /// Whether the current descent into the layer has not probed for its value yet.
    pending: bool,
}

/// The value of a bound layer, as its binder stores it.
#[derive(Debug)]
enum BinderValue<'a> {
    /// The storage value of a constant,
    /// or `None` for a constant without a storage value, which no layer has.
    Constant(Option<StorageValue>),
    /// The program computing the value, with its evaluation state.
    Computed(ProgramState<'a>),
}

/// What the driver does at a layer, as the layer's binder decides.
#[derive(Debug, Clone, Copy)]
pub(super) enum Bind {
    /// The layer is not bound, and enumerates its intersection.
    Free,
    /// The layer's value, which the driver probes for.
    Value(StorageValue),
    /// Nothing matches at the layer, because the descent already probed for the value,
    /// its evaluation failed, or the value has no storage value.
    Dead,
}

/// The hooks of a join, over the type `V` of the values its layers find.
///
/// The driver is generic over the hooks,
/// so a join without hooks runs with [NoHooks], whose checks compile to nothing.
pub(super) trait Hooks<V> {
    /// Returns whether the hooks at a layer accept a value found there.
    ///
    /// `row` has the value at that layer, and the values of the shallower layers.
    fn admit(&mut self, layer: usize, value: V, row: &[V]) -> bool;

    /// Prepares the hooks of a layer for a descent from above.
    fn enter_layer(&mut self, layer: usize);

    /// Returns what the driver does at a layer, as the layer's binder decides.
    ///
    /// A bound layer gives its value once per descent, and [Bind::Dead] after that.
    fn bind(&mut self, layer: usize, row: &[V]) -> Bind;
}

/// The hooks of a join without subtracted inputs, filters or bound layers,
/// which accept every value.
#[derive(Debug)]
pub(super) struct NoHooks;

impl<V> Hooks<V> for NoHooks {
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
}

/// The hooks of a join with subtracted inputs, filters or bound layers.
///
/// They keep the cursors of the subtracted inputs,
/// the filters with their evaluation state,
/// and the binders, which store or compute the single value of a bound layer.
#[derive(Debug)]
pub(super) struct FullHooks<'a> {
    /// The subtracted inputs.
    subtractions: BumpVec<'a, SubtractionState<'a>>,
    /// Per layer, the subtracted columns compared there,
    /// as pairs of the input's and the column's index.
    subtracted_at_layer: BumpVec<'a, BumpVec<'a, (usize, usize)>>,
    /// The filters.
    filters: BumpVec<'a, FilterState<'a>>,
    /// Per layer, the indices of the filters checked there,
    /// which are those whose deepest input is that layer.
    filters_at_layer: BumpVec<'a, BumpVec<'a, usize>>,
    /// The binders of the bound layers.
    binders: BumpVec<'a, BinderState<'a>>,
    /// Per layer, the index of its binder, if the layer is bound.
    binder_at_layer: BumpVec<'a, Option<usize>>,
    /// The dictionary the programs decode their inputs through,
    /// present if there are filters or binders.
    dictionary: Option<&'a Dictionary>,
    /// The context the programs evaluate in.
    context: EvaluationContext,
    /// The evaluation errors counted so far, by kind.
    ///
    /// The rows they occurred on were dropped.
    errors: ErrorCounts,
}

impl Hooks<StorageValue> for FullHooks<'_> {
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
        // The integer form is not selected, fell back, or gave no integer,
        // so the generic interpreter decides.

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
}

impl<'a> FullHooks<'a> {
    /// Returns the hooks of a shape, or `None` for a shape without hooks.
    ///
    /// `inputs` has the blocks of each input, indexed as the shape indexes the inputs,
    /// and `subtracted` has one trie per subtracted input.
    /// The programs decode their inputs through `dictionary` and evaluate in `context`.
    pub(super) fn build(
        shape: &JoinShape<'a>,
        inputs: &[&[&'a Trie]],
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
        let (filters, filters_at_layer) = filter_hooks(shape, inputs, dictionary, bump);
        let (binders, binder_at_layer) = binder_hooks(shape, inputs, dictionary, bump);

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

    /// Returns the evaluation errors counted so far, by kind.
    pub(super) fn errors(&self) -> ErrorCounts {
        self.errors
    }

    /// Returns whether the filter of the given index runs its integer form.
    #[cfg(test)]
    pub(super) fn filter_uses_integer_form(&self, index: usize) -> bool {
        matches!(&self.filters[index], FilterState::Program(state) if state.integer.is_some())
    }

    /// Returns whether the filter of the given index is decided by its identity form,
    /// on storage values.
    #[cfg(test)]
    pub(super) fn filter_uses_identity_form(&self, index: usize) -> bool {
        matches!(self.filters[index], FilterState::Identity(_))
    }

    /// Returns whether a value found at a layer completes a match of some subtracted input.
    ///
    /// The value is compared with each armed subtracted column at the layer.
    /// A hit arms the input's next column, restricted to the hit's children,
    /// and a miss disarms it.
    /// A hit on an input's last column is a match of the whole input.
    fn blocked(&mut self, layer: usize, value: StorageValue) -> bool {
        let mut blocked = false;
        for entry in 0..self.subtracted_at_layer[layer].len() {
            let (input, column) = self.subtracted_at_layer[layer][entry];
            let subtraction = &mut self.subtractions[input];

            let hit = subtraction.columns[column].armed
                && subtraction.columns[column].cursor.seek(value) == Some(value);

            if column + 1 < subtraction.columns.len() {
                // A column constrains the row only if the columns before it matched.
                // A miss disarms the next column, and through it every later one.
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

    /// Returns whether the row passes the filters checked at a layer.
    ///
    /// A failed evaluation is counted, and rejects the row.
    fn passes_filters(&mut self, layer: usize, row: &[StorageValue]) -> bool {
        for entry in 0..self.filters_at_layer[layer].len() {
            let filter = self.filters_at_layer[layer][entry];
            if !self.evaluate_filter(filter, row) {
                return false;
            }
        }

        true
    }

    /// Returns whether the row passes a filter.
    ///
    /// The identity form decides if the filter has one.
    /// Otherwise the integer form decides if it is selected and does not fall back.
    /// In every other case, the generic interpreter decides.
    fn evaluate_filter(&mut self, index: usize, row: &[StorageValue]) -> bool {
        let state = match &mut self.filters[index] {
            FilterState::Identity(identity) => return identity.holds(row),
            FilterState::Program(state) => state,
        };

        if let Some(pass) = state.run_integers(row, IntegerProgram::filter) {
            return pass;
        }
        // The integer form is not selected, or fell back because an intermediate value
        // outgrew `i64`, so the generic interpreter decides.

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

/// Returns the states of the subtracted inputs,
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
/// and per layer the filters checked there, each at the deepest layer it reads.
///
/// The constants of the identity forms are looked up in the dictionary here, once.
fn filter_hooks<'a>(
    shape: &JoinShape<'a>,
    inputs: &[&[&'a Trie]],
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
                let integer = integer_form(shape, inputs, &filter.inputs, filter.program.integer());
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

/// Returns the binders of the bound layers, and per layer the index of its binder.
///
/// The constants are looked up in the dictionary here, once.
fn binder_hooks<'a>(
    shape: &JoinShape<'a>,
    inputs: &[&[&'a Trie]],
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
            BoundValue::Computed {
                inputs: layers,
                program,
            } => {
                let integer = integer_form(shape, inputs, layers, program.integer());
                BinderValue::Computed(ProgramState::new(program.generic(), layers, integer, bump))
            }
        };
        binders.push(BinderState {
            value,
            pending: false,
        });
    }

    (binders, binder_at_layer)
}

/// Returns a program's integer form if the inputs allow it, or `None`.
///
/// `layers` are the layers the program reads, and `integer` is its integer form, if it has one.
/// The inputs allow the integer form if, at every layer the program reads,
/// every intersected column has integers only, in every block of its input.
fn integer_form<'a>(
    shape: &JoinShape<'a>,
    inputs: &[&[&'a Trie]],
    layers: &[usize],
    integer: Option<&'a IntegerProgram>,
) -> Option<&'a IntegerProgram> {
    let integers_only = |layer: usize| {
        shape.layers[layer].iter().all(|input_column| {
            inputs[input_column.input]
                .iter()
                .all(|trie| trie.column(input_column.column).uniform_tag() == Some(Tag::Int))
        })
    };

    let integer = integer?;
    layers
        .iter()
        .all(|&layer| integers_only(layer))
        .then_some(integer)
}
