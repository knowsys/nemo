//! The shapes of joins and aggregations: what they compute, with layers given by index.
//!
//! A join descends through its layers in order, choosing a value at each.
//! A *match* chooses a value for every layer,
//! such that each input has a row with these values in the columns it maps.
//! The values of a match at the emitted layers form a result row.
//! For the deeper layers, the join only checks that some match exists,
//! so it emits each result row once.
//!
//! The hooks of a join restrict its matches further:
//! [subtracted inputs](SubtractedShape), [filters](FilterShape)
//! and [bound layers](BoundLayerShape).
//!
//! The rows a join emits may pass through a [row stage](RowStageShape)
//! or through a [projection](ProjectionShape).
//! A row stage extends each row by [computed columns](ComputedShape),
//! and its [post-filters](PostFilterShape) may drop the row.
//! An [aggregation](AggregationShape) groups the rows of a join by their leading layers,
//! and computes its [aggregates](AggregateShape) per group.

use crate::{
    dictionary::Dictionary,
    function::{SpecializedProgram, StackProgram},
    trie::Trie,
    value::DataValue,
};

/// A column of one of a join's inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InputColumn {
    /// The index of the input among the inputs the join is given.
    pub(crate) input: usize,
    /// The index of the column in the input's tries.
    pub(crate) column: usize,
}

/// The shape of a subtracted input of a join.
///
/// The join drops a match if some row of the subtracted trie
/// agrees with it on the compared columns.
/// The compared columns are the first columns of the subtracted trie,
/// each compared with one layer.
/// Any further columns are not compared,
/// so the join subtracts the projection of the trie onto the compared columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SubtractedShape<'a> {
    /// The layer of each compared column.
    ///
    /// Column `i` of the subtracted trie is compared with layer `layers[i]`.
    /// The layers strictly increase, since the join follows the trie down as it descends.
    pub(crate) layers: &'a [usize],
}

/// A filter of a join: a condition over the values of some layers.
///
/// The join checks the filter at the deepest layer the filter reads.
/// If the condition does not hold there, or fails to evaluate,
/// the join moves on to that layer's next value, skipping everything below.
/// Each failed evaluation counts as an error.
#[derive(Debug, Clone)]
pub(crate) struct FilterShape {
    /// The layers the condition reads.
    ///
    /// Input `i` of the program is the value of layer `inputs[i]`.
    pub(crate) inputs: Vec<usize>,
    /// The condition, with its specialization.
    pub(crate) program: SpecializedProgram,
}

impl FilterShape {
    /// Creates the filter of a condition over the given layers,
    /// and compiles the condition's specialization.
    pub(crate) fn new(inputs: Vec<usize>, program: StackProgram) -> Self {
        Self {
            inputs,
            program: SpecializedProgram::new(program),
        }
    }
}

/// A bound layer of a join: a layer with a single value,
/// which is a constant or computed by a program from shallower layers.
///
/// Instead of enumerating the layer,
/// the join looks the value up in the layer's input columns.
/// Nothing matches at the layer if the value has no storage value,
/// such as a string the dictionary does not have, or if its evaluation fails.
/// Each failed evaluation counts as an error.
#[derive(Debug, Clone)]
pub(crate) struct BoundLayerShape {
    /// The bound layer.
    pub(crate) layer: usize,
    /// The value of the layer.
    pub(crate) value: BoundValue,
}

/// The value of a [bound layer](BoundLayerShape).
#[derive(Debug, Clone)]
pub(crate) enum BoundValue {
    /// A constant.
    Constant(DataValue),
    /// The result of a program.
    Computed {
        /// The layers the program reads, each shallower than the bound layer.
        ///
        /// Input `i` of the program is the value of layer `inputs[i]`.
        inputs: Vec<usize>,
        /// The program, with its specialization.
        program: SpecializedProgram,
    },
}

impl BoundLayerShape {
    /// Creates a layer bound to a constant.
    pub(crate) fn constant(layer: usize, value: DataValue) -> Self {
        Self {
            layer,
            value: BoundValue::Constant(value),
        }
    }

    /// Creates a layer bound to the value a program computes from the given layers,
    /// and compiles the program's specialization.
    pub(crate) fn computed(layer: usize, inputs: Vec<usize>, program: StackProgram) -> Self {
        Self {
            layer,
            value: BoundValue::Computed {
                inputs,
                program: SpecializedProgram::new(program),
            },
        }
    }

    /// Returns whether the layer is bound to a constant.
    pub(crate) fn is_constant(&self) -> bool {
        matches!(self.value, BoundValue::Constant(_))
    }
}

/// The shape of a join, from which a join is built.
///
/// The shape borrows its parts.
/// [TrieJoin::new](crate::execution::TrieJoin::new) validates it, and
/// [TrieJoin::with_trusted_shape](crate::execution::TrieJoin::with_trusted_shape) does not,
/// for a shape that is valid by construction.
/// A valid shape passes [validate](Self::validate),
/// and [validate_fit](Self::validate_fit) with what the join is given.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JoinShape<'a> {
    /// Per layer, the input columns intersected there.
    pub(crate) layers: &'a [Vec<InputColumn>],
    /// The number of leading layers whose values form the result rows.
    pub(crate) emitted: usize,
    /// The subtracted inputs, one per subtracted trie the join is given, in the same order.
    pub(crate) subtracted: &'a [SubtractedShape<'a>],
    /// The filters.
    pub(crate) filters: &'a [FilterShape],
    /// The bound layers.
    pub(crate) bound_layers: &'a [BoundLayerShape],
}

impl JoinShape<'_> {
    /// Checks the invariants of the shape.
    ///
    /// Returns the first violated invariant as a message.
    /// A malformed shape is a bug in the code that built it, not an error in the data.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        self.validate_layers()?;
        self.validate_subtracted()?;
        self.validate_filters()?;
        self.validate_bound_layers()
    }

    /// Checks the layers:
    /// - there is at least one, and each intersects at least one column,
    /// - the join emits no more layers than it has,
    /// - each input maps the first columns of its trie, in order, to strictly increasing layers.
    fn validate_layers(&self) -> Result<(), &'static str> {
        if self.layers.is_empty() {
            return Err("a join shape needs at least one layer");
        }
        if self.layers.iter().any(Vec::is_empty) {
            return Err("every join layer must intersect at least one column");
        }
        if self.emitted > self.layers.len() {
            return Err("a join cannot emit more layers than it has");
        }

        let input_count = self
            .layers
            .iter()
            .flatten()
            .map(|column| column.input + 1)
            .max()
            .unwrap_or(0);
        let mut next_column = vec![0usize; input_count];

        for layer in self.layers {
            let mut seen_this_layer = Vec::new();
            for column in layer {
                if seen_this_layer.contains(&column.input) {
                    return Err("an input cannot contribute two columns to one layer");
                }
                seen_this_layer.push(column.input);

                if column.column != next_column[column.input] {
                    return Err("an input's columns must be mapped as a prefix, in layer order");
                }
                next_column[column.input] += 1;
            }
        }

        Ok(())
    }

    /// Checks that each subtracted input maps at least one column,
    /// and maps its columns to strictly increasing layers that the join has.
    fn validate_subtracted(&self) -> Result<(), &'static str> {
        for input in self.subtracted {
            let Some(&last) = input.layers.last() else {
                return Err("a subtracted input must map at least one column");
            };

            if !input.layers.is_sorted_by(|left, right| left < right) {
                return Err("a subtracted input's layers must be strictly increasing");
            }

            if last >= self.layers.len() {
                return Err("a subtracted input maps a layer the join does not have");
            }
        }

        Ok(())
    }

    /// Checks that each filter reads at least one layer, only layers the join has,
    /// and as many layers as its program has inputs.
    fn validate_filters(&self) -> Result<(), &'static str> {
        for filter in self.filters {
            if filter.inputs.is_empty() {
                return Err("a join filter must read at least one layer");
            }

            if filter
                .inputs
                .iter()
                .any(|&layer| layer >= self.layers.len())
            {
                return Err("a join filter reads a layer the join does not have");
            }

            if filter.program.generic().inputs() != filter.inputs.len() {
                return Err("a join filter's program arity must match its input layers");
            }
        }

        Ok(())
    }

    /// Checks the bound layers:
    /// - each binds a layer the join has, and no layer is bound twice,
    /// - each program reads at least one layer, since a value that reads none is a constant,
    ///   and only layers shallower than the one it binds,
    /// - each program reads as many layers as it has inputs.
    fn validate_bound_layers(&self) -> Result<(), &'static str> {
        for bound in self.bound_layers {
            if bound.layer >= self.layers.len() {
                return Err("a bound layer must be one the join has");
            }

            if self
                .bound_layers
                .iter()
                .filter(|other| other.layer == bound.layer)
                .count()
                > 1
            {
                return Err("a layer can be bound at most once");
            }

            let BoundValue::Computed { inputs, program } = &bound.value else {
                continue;
            };

            if inputs.is_empty() {
                return Err("a computed bound layer must read at least one layer");
            }

            if inputs.iter().any(|&input| input >= bound.layer) {
                return Err("a bound layer's inputs must all precede it");
            }

            if program.generic().inputs() != inputs.len() {
                return Err("a bound layer's program arity must match its input layers");
            }
        }
        Ok(())
    }

    /// Checks that the shape fits what a join over it is given.
    ///
    /// A join is given the blocks of its inputs, one trie per subtracted input,
    /// and a dictionary, which it needs if it has filters or bound layers.
    /// Returns the first mismatch as a message.
    /// Like a malformed shape, a mismatch is a bug in the code that built the join.
    pub(crate) fn validate_fit(
        &self,
        inputs: &[&[&Trie]],
        subtracted: &[&Trie],
        dictionary: Option<&Dictionary>,
    ) -> Result<(), &'static str> {
        self.validate_input_fit(inputs)?;

        if subtracted.len() != self.subtracted.len() {
            return Err("the join must be given one trie per subtracted input of the shape");
        }

        if self
            .subtracted
            .iter()
            .zip(subtracted)
            .any(|(input, trie)| input.layers.len() > trie.arity())
        {
            return Err("the shape maps more columns than the subtracted trie has");
        }

        if (!self.filters.is_empty() || !self.bound_layers.is_empty()) && dictionary.is_none() {
            return Err(
                "a join with filters or bound layers needs the dictionary its values decode through",
            );
        }

        Ok(())
    }

    /// Checks that each input column names a given input,
    /// and a column that every block of that input has.
    fn validate_input_fit(&self, inputs: &[&[&Trie]]) -> Result<(), &'static str> {
        for input_column in self.layers.iter().flatten() {
            let Some(blocks) = inputs.get(input_column.input) else {
                return Err("the shape references an input the join was not given");
            };

            if blocks
                .iter()
                .any(|trie| input_column.column >= trie.arity())
            {
                return Err("the shape maps more columns than the input's tries have");
            }
        }
        Ok(())
    }
}

/// An input of a computed column or a post-filter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RowInput {
    /// An emitted join layer.
    Layer(usize),
    /// The result of an earlier computed column.
    Computed(usize),
}

/// A computed column of a row stage: a program evaluated on each row.
#[derive(Debug)]
pub(crate) struct ComputedShape {
    /// The inputs of the program, in order.
    pub(crate) inputs: Vec<RowInput>,
    /// The program computing the value, with its specialization.
    pub(crate) program: SpecializedProgram,
    /// The deepest layer the column reads, also through the computed columns it reads.
    ///
    /// The result goes stale when a row changes that layer or a shallower one.
    /// It is `None` for a column that reads no layer, such as a constant,
    /// whose result never goes stale.
    pub(crate) deepest: Option<usize>,
    /// Whether the column is evaluated on every row,
    /// because its program, or that of a computed column it reads, is nondeterministic.
    pub(crate) volatile: bool,
}

/// A post-filter of a row stage: a condition checked on each row after the computed columns.
///
/// A row on which the condition does not hold, or fails to evaluate, is dropped.
/// Each failed evaluation counts as an error.
#[derive(Debug)]
pub(crate) struct PostFilterShape {
    /// The inputs of the program, in order.
    pub(crate) inputs: Vec<RowInput>,
    /// The condition, with its specialization.
    pub(crate) program: SpecializedProgram,
}

/// The shape of a row stage: the programs evaluated on each row a join emits.
///
/// A [RowEvaluator](crate::execution::RowEvaluator) is built from it.
/// Computed columns extend the row, and post-filters may drop it.
/// A result row consists of the emitted layers, followed by the delivered computed columns.
#[derive(Debug)]
pub(crate) struct RowStageShape {
    /// The computed columns, each reading only layers and earlier computed columns.
    pub(crate) computed: Vec<ComputedShape>,
    /// The filters checked after the computed columns.
    pub(crate) post_filters: Vec<PostFilterShape>,
    /// The indices of the computed columns that follow the emitted layers in a result row,
    /// in order.
    pub(crate) delivered: Vec<usize>,
}

impl RowStageShape {
    /// Checks that the shape fits joins that emit `emitted` layers:
    /// - its programs read only emitted layers and earlier computed columns,
    /// - each program has as many inputs as it reads,
    /// - it delivers only computed columns it has.
    ///
    /// Returns the first mismatch as a message.
    /// Like a malformed shape, a mismatch is a bug in the code that built the row stage.
    pub(crate) fn validate_fit(&self, emitted: usize) -> Result<(), &'static str> {
        let fits = |inputs: &[RowInput], earlier: usize| {
            inputs.iter().all(|input| match *input {
                RowInput::Layer(layer) => layer < emitted,
                RowInput::Computed(column) => column < earlier,
            })
        };

        for (index, column) in self.computed.iter().enumerate() {
            if !fits(&column.inputs, index) {
                return Err("a computed column must read emitted layers and earlier columns");
            }
            if column.program.generic().inputs() != column.inputs.len() {
                return Err("a computed column's program arity must match its inputs");
            }
        }

        for filter in &self.post_filters {
            if !fits(&filter.inputs, self.computed.len()) {
                return Err("a post-filter must read emitted layers and computed columns");
            }
            if filter.program.generic().inputs() != filter.inputs.len() {
                return Err("a post-filter's program arity must match its inputs");
            }
        }

        if self
            .delivered
            .iter()
            .any(|&column| column >= self.computed.len())
        {
            return Err("a row stage delivers a computed column it does not have");
        }

        Ok(())
    }
}

/// The shape of a projection of a join's rows.
///
/// A [ProjectingSink](crate::execution::ProjectingSink) is built from it.
#[derive(Debug)]
pub(crate) struct ProjectionShape {
    /// Per projected column, the emitted layer it reads.
    pub(crate) columns: Vec<usize>,
    /// The length of the *group prefix*:
    /// the leading projected columns that read the leading layers, in order.
    ///
    /// On the group prefix, the projected rows keep the order the join emits them in,
    /// and a change in it starts a new group of rows to deduplicate.
    pub(crate) group_prefix: usize,
}

impl ProjectionShape {
    /// Creates the projection to the given emitted layers.
    pub(crate) fn new(columns: Vec<usize>) -> Self {
        let group_prefix = columns
            .iter()
            .enumerate()
            .take_while(|&(position, &layer)| layer == position)
            .count();

        Self {
            columns,
            group_prefix,
        }
    }
}

/// An aggregation function, as [AggregateFunction](crate::plan::AggregateFunction) describes it,
/// without the columns it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateKind {
    /// The number of rows.
    Count,
    /// The sum of the values.
    Sum,
    /// The smallest value.
    Min,
    /// The largest value.
    Max,
    /// The average of the values.
    Average,
    /// The values as a tuple.
    Tuple,
    /// The key–value pairs as a map.
    Map,
}

/// One aggregate of an aggregation: its function and the layers it reads.
///
/// A row's *identity* for the aggregate is its values at the input, pair and `per` layers.
/// Within a group, the aggregate reads each identity once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AggregateShape {
    /// The aggregation function.
    pub(crate) kind: AggregateKind,
    /// The layer whose values are aggregated, which has the keys for a map,
    /// or `None` for a count, which reads no value.
    pub(crate) input: Option<usize>,
    /// The layer of a map's values, or `None` for every other function.
    pub(crate) pair: Option<usize>,
    /// The further layers of a row's identity, beside the input and the pair.
    pub(crate) per: Vec<usize>,
}

impl AggregateShape {
    /// Returns the layers of a row's identity: the input, the pair, then the `per` layers.
    pub(crate) fn identity_layers(&self) -> impl Iterator<Item = usize> + '_ {
        self.input
            .into_iter()
            .chain(self.pair)
            .chain(self.per.iter().copied())
    }
}

/// The shape of an aggregation over a join's rows.
///
/// An [Aggregator](crate::execution::Aggregator) is built from it,
/// and the shape borrows its aggregates.
/// The leading layers of a row are its group key.
/// A result row consists of the group key, followed by one value per aggregate.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AggregationShape<'a> {
    /// The number of leading layers forming the group key.
    pub(crate) group_layers: usize,
    /// The aggregates computed per group.
    pub(crate) aggregates: &'a [AggregateShape],
}

impl AggregationShape<'_> {
    /// Checks the invariants of the shape:
    /// - there is at least one aggregate,
    /// - every aggregate but a count reads an input layer,
    /// - exactly the map aggregates read a layer of values.
    ///
    /// Returns the first violated invariant as a message.
    /// A malformed shape is a bug in the code that built it, not an error in the data.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.aggregates.is_empty() {
            return Err("an aggregation must compute at least one aggregate");
        }

        if self
            .aggregates
            .iter()
            .any(|aggregate| aggregate.input.is_none() && aggregate.kind != AggregateKind::Count)
        {
            return Err("every aggregate but a count reads an input layer");
        }

        if self
            .aggregates
            .iter()
            .any(|aggregate| aggregate.pair.is_some() != (aggregate.kind == AggregateKind::Map))
        {
            return Err("exactly the map aggregate carries a value layer");
        }

        Ok(())
    }

    /// Checks that the shape fits joins that emit `emitted` layers.
    ///
    /// The shape must group by no more layers than the joins emit,
    /// and its aggregates must read only emitted layers.
    /// Returns the first mismatch as a message.
    /// Like a malformed shape, a mismatch is a bug in the code that built the aggregation.
    pub(crate) fn validate_fit(&self, emitted: usize) -> Result<(), &'static str> {
        if self.group_layers > emitted {
            return Err("an aggregation cannot group by more layers than the join emits");
        }

        if self
            .aggregates
            .iter()
            .any(|aggregate| aggregate.identity_layers().any(|layer| layer >= emitted))
        {
            return Err("an aggregate reads a layer the join does not emit");
        }

        Ok(())
    }
}

/// The parts of a join's shape, owned and validated, for tests to build joins from.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct OwnedJoinShape {
    /// Per layer, the input columns intersected there.
    layers: Vec<Vec<InputColumn>>,
    /// The number of leading layers whose values form the result rows.
    emitted: usize,
    /// The subtracted inputs, one per subtracted trie the join is given, in the same order.
    subtracted: Vec<SubtractedShape<'static>>,
    /// The filters.
    filters: Vec<FilterShape>,
    /// The bound layers.
    bound_layers: Vec<BoundLayerShape>,
}

#[cfg(test)]
impl OwnedJoinShape {
    /// Creates a validated shape without bound layers.
    ///
    /// Returns the first violated invariant as an error message if the shape is malformed.
    pub(crate) fn new(
        layers: Vec<Vec<InputColumn>>,
        emitted: usize,
        subtracted: Vec<SubtractedShape<'static>>,
        filters: Vec<FilterShape>,
    ) -> Result<Self, &'static str> {
        let shape = Self {
            layers,
            emitted,
            subtracted,
            filters,
            bound_layers: Vec::new(),
        };
        shape.view().validate()?;

        Ok(shape)
    }

    /// Returns the shape with the given bound layers, validated.
    ///
    /// Returns the first violated invariant as an error message if the shape is malformed.
    pub(crate) fn with_bound_layers(
        mut self,
        bound_layers: Vec<BoundLayerShape>,
    ) -> Result<Self, &'static str> {
        self.bound_layers = bound_layers;
        self.view().validate()?;

        Ok(self)
    }

    /// Returns the shape, borrowing its parts.
    pub(crate) fn view(&self) -> JoinShape<'_> {
        JoinShape {
            layers: &self.layers,
            emitted: self.emitted,
            subtracted: &self.subtracted,
            filters: &self.filters,
            bound_layers: &self.bound_layers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{function::OpCode, testing::at};

    /// A well-formed shape with a subtracted input passes validation.
    #[test]
    fn valid_shapes_are_accepted() {
        // R(a, b) ⋈ S(b, c), emitting all three layers.
        let shape = OwnedJoinShape::new(
            vec![vec![at(0, 0)], vec![at(0, 1), at(1, 0)], vec![at(1, 1)]],
            3,
            vec![SubtractedShape { layers: &[0, 2] }],
            vec![],
        );
        assert!(shape.is_ok());
    }

    /// Each kind of malformed join shape fails validation.
    #[test]
    fn malformed_shapes_are_rejected() {
        // No layers.
        assert!(OwnedJoinShape::new(vec![], 0, vec![], vec![]).is_err());
        // An empty layer.
        assert!(OwnedJoinShape::new(vec![vec![at(0, 0)], vec![]], 1, vec![], vec![]).is_err());
        // Emitting more layers than exist.
        assert!(OwnedJoinShape::new(vec![vec![at(0, 0)]], 2, vec![], vec![]).is_err());
        // A column skipped: input 0 maps column 1 before column 0.
        assert!(OwnedJoinShape::new(vec![vec![at(0, 1)]], 1, vec![], vec![]).is_err());
        // Two columns of one input on the same layer.
        assert!(OwnedJoinShape::new(vec![vec![at(0, 0), at(0, 1)]], 1, vec![], vec![]).is_err());
        // A subtracted input with unordered layers.
        assert!(
            OwnedJoinShape::new(
                vec![vec![at(0, 0)], vec![at(0, 1)]],
                2,
                vec![SubtractedShape { layers: &[1, 0] }],
                vec![],
            )
            .is_err()
        );
        // A subtracted input beyond the join's depth.
        assert!(
            OwnedJoinShape::new(
                vec![vec![at(0, 0)]],
                1,
                vec![SubtractedShape { layers: &[1] }],
                vec![],
            )
            .is_err()
        );
    }

    /// A row stage fits only joins that emit every layer it reads,
    /// and only if its programs and delivered columns match its computed columns.
    #[test]
    fn row_stages_must_fit_the_emitted_layers() {
        let sum = || {
            SpecializedProgram::new(StackProgram::new(
                vec![OpCode::Input(0), OpCode::Input(1), OpCode::Add],
                vec![],
                2,
            ))
        };
        let stage = |inputs: Vec<RowInput>, delivered: Vec<usize>| RowStageShape {
            computed: vec![ComputedShape {
                inputs,
                program: sum(),
                deepest: Some(1),
                volatile: false,
            }],
            post_filters: vec![],
            delivered,
        };

        let valid = stage(vec![RowInput::Layer(0), RowInput::Layer(1)], vec![0]);
        assert!(valid.validate_fit(2).is_ok());
        // Layer 1 is not emitted.
        assert!(valid.validate_fit(1).is_err());
        // A column reading itself.
        let cyclic = stage(vec![RowInput::Layer(0), RowInput::Computed(0)], vec![0]);
        assert!(cyclic.validate_fit(2).is_err());
        // A program of two inputs that is given one.
        assert!(
            stage(vec![RowInput::Layer(0)], vec![0])
                .validate_fit(2)
                .is_err()
        );
        // A delivered column that does not exist.
        let beyond = stage(vec![RowInput::Layer(0), RowInput::Layer(1)], vec![1]);
        assert!(beyond.validate_fit(2).is_err());
    }

    /// The group prefix of a projection is its leading run of columns
    /// that read layers 0, 1, 2 and so on, in order.
    #[test]
    fn projections_group_by_their_leading_identity_run() {
        assert_eq!(ProjectionShape::new(vec![0, 1, 3]).group_prefix, 2);
        assert_eq!(ProjectionShape::new(vec![1, 0]).group_prefix, 0);
        assert_eq!(ProjectionShape::new(vec![0, 2, 1]).group_prefix, 1);
        assert_eq!(ProjectionShape::new(vec![]).group_prefix, 0);
    }
}
