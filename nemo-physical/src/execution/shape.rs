//! The shapes of joins and aggregations: what they compute, in layer positions.
//!
//! A join descends through its layers in order, choosing a value at each.
//! A match chooses a value for every layer,
//! such that each input holds a row with these values in the columns it maps.
//! The values of a match at the leading, emitted layers form a result row.
//! For the deeper layers, the join only checks that some match exists,
//! so it emits each result row once.
//!
//! The hooks of a join restrict its matches further:
//! [subtracted inputs](SubtractedShape), [filters](FilterShape) and [bound layers](BoundLayerShape).
//!
//! The rows a join emits may pass through a [row stage](RowStageShape),
//! whose [computed columns](ComputedShape) extend them
//! and whose [post-filters](PostFilterShape) may drop them,
//! or through a [projection](ProjectionShape).
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
    /// The index of the input's trie among the tries the join is given.
    pub(crate) input: usize,
    /// The column within the input's trie.
    pub(crate) column: usize,
}

/// A subtracted input of a join: a trie whose rows the join removes from its matches.
///
/// The join finds its matches in its inputs,
/// and drops a match if some row of the subtracted trie
/// agrees with it on the compared columns.
/// These are the first columns of the subtracted trie, each compared with one layer.
/// Any further columns are not compared,
/// so the join subtracts the trie's projection onto the compared columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubtractedShape {
    /// The layer of each compared column:
    /// column `i` of the subtracted trie is compared with layer `layers[i]`.
    /// The layers strictly increase, since the join follows the trie along its descent.
    pub(crate) layers: Vec<usize>,
}

/// A filter of a join: a condition over the values of some layers.
///
/// The join checks the filter at the deepest layer it reads.
/// If the condition does not hold there, or fails to evaluate,
/// the join moves on to that layer's next value, skipping everything below;
/// it counts each failed evaluation as an error.
#[derive(Debug, Clone)]
pub(crate) struct FilterShape {
    /// The layers the program reads:
    /// the program's input `i` is the value of layer `inputs[i]`.
    pub(crate) inputs: Vec<usize>,
    /// The condition, with its specialization.
    pub(crate) program: SpecializedProgram,
}

impl FilterShape {
    /// Creates the filter of a program over the given layers,
    /// compiling its specialization.
    pub(crate) fn new(inputs: Vec<usize>, program: StackProgram) -> Self {
        Self {
            inputs,
            program: SpecializedProgram::new(program),
        }
    }
}

/// A bound layer of a join: a layer whose single value is a constant
/// or computed by a program from shallower layers.
///
/// Instead of enumerating the layer,
/// the join looks up the value in the layer's input columns.
/// If the value has no storage value, such as a string the dictionary does not hold,
/// or the evaluation fails, nothing matches at the layer;
/// the join counts each failed evaluation as an error.
#[derive(Debug, Clone)]
pub(crate) struct BoundLayerShape {
    /// The bound layer.
    pub(crate) layer: usize,
    /// What the layer's value is.
    pub(crate) value: BoundValue,
}

/// The value of a [bound layer](BoundLayerShape).
#[derive(Debug, Clone)]
pub(crate) enum BoundValue {
    /// A constant.
    Constant(DataValue),
    /// The result of a program.
    Computed {
        /// The layers the program reads, each shallower than the bound layer:
        /// the program's input `i` is the value of layer `inputs[i]`.
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

    /// Creates a bound layer whose value the program computes from the given layers,
    /// compiling its specialization.
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

/// The shape of a join, from which a join is built; it borrows its parts.
///
/// [TrieJoin::new](crate::execution::TrieJoin::new) validates the shape.
/// A caller whose shapes are valid by construction builds through
/// [TrieJoin::with_trusted_shape](crate::execution::TrieJoin::with_trusted_shape) instead,
/// which neither clones nor validates anything.
/// Such a shape must pass [validate](Self::validate)
/// and [fit](Self::validate_fit) what the join is given.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JoinShape<'a> {
    /// Per layer, the input columns intersected there.
    pub(crate) layers: &'a [Vec<InputColumn>],
    /// The number of leading layers whose values form the result rows.
    pub(crate) emitted: usize,
    /// The subtracted inputs, one per subtracted trie the join is given, in the same order.
    pub(crate) subtracted: &'a [SubtractedShape],
    /// The filters.
    pub(crate) filters: &'a [FilterShape],
    /// The bound layers.
    pub(crate) bound_layers: &'a [BoundLayerShape],
}

impl JoinShape<'_> {
    /// Checks the invariants of the shape.
    ///
    /// Returns the first invariant violated, as a message:
    /// a malformed shape is a bug of the code that built it, not an error in the data.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        self.validate_layers()?;
        self.validate_subtracted()?;
        self.validate_filters()?;
        self.validate_bound_layers()
    }

    /// Checks the layers:
    /// - there is at least one, and each intersects at least one column,
    /// - no more are emitted than there are,
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

    /// Checks the subtracted inputs:
    /// each maps at least one column, to strictly increasing layers the join has.
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

    /// Checks the filters: each reads
    /// - at least one layer, and only layers the join has,
    /// - as many layers as its program has inputs.
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
    /// - each program reads at least one layer, and only layers shallower than the one it binds;
    ///   a value that reads no layer is a constant,
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

    /// Checks that the shape fits what a join over it is given:
    /// the tries of its inputs, one trie per subtracted input,
    /// and the dictionary, if filters or bound layers decode values through it.
    ///
    /// Returns the first mismatch, as a message:
    /// like a malformed shape, it is a bug of the code that built the join.
    pub(crate) fn validate_fit(
        &self,
        tries: &[&Trie],
        subtracted: &[&Trie],
        dictionary: Option<&Dictionary>,
    ) -> Result<(), &'static str> {
        self.validate_input_fit(tries)?;

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

    /// Checks that the input columns fit the tries of the inputs:
    /// each names an input that is given, and a column its trie has.
    pub(crate) fn validate_input_fit(&self, tries: &[&Trie]) -> Result<(), &'static str> {
        for input_column in self.layers.iter().flatten() {
            let Some(trie) = tries.get(input_column.input) else {
                return Err("the shape references an input the join was not given");
            };

            if input_column.column >= trie.arity() {
                return Err("the shape maps more columns than the input's trie has");
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
    /// The inputs of the program, in program input order.
    pub(crate) inputs: Vec<RowInput>,
    /// The program computing the value, with its specialization.
    pub(crate) program: SpecializedProgram,
    /// The deepest layer the column reads, also through computed inputs.
    ///
    /// The result is stale once a row changes that layer or one above it.
    /// It is `None` for a constant, which never goes stale.
    pub(crate) deepest: Option<usize>,
    /// Whether the column evaluates on every row,
    /// since its program, or one it reads, is nondeterministic.
    pub(crate) volatile: bool,
}

/// A post-filter of a row stage: a condition checked on each row after the computed columns.
///
/// A row on which the condition does not hold, or fails to evaluate, is dropped;
/// each failed evaluation counts as an error.
#[derive(Debug)]
pub(crate) struct PostFilterShape {
    /// The inputs of the program, in program input order.
    pub(crate) inputs: Vec<RowInput>,
    /// The condition, with its specialization.
    pub(crate) program: SpecializedProgram,
}

/// The shape of a row stage over a join's rows, from which a row evaluator is built:
/// the programs evaluated on each emitted row.
///
/// Computed columns extend the row, and post-filters may drop it.
/// A result row holds the emitted layers, followed by the delivered computed columns.
#[derive(Debug)]
pub(crate) struct RowStageShape {
    /// The computed columns, each reading only layers and earlier columns.
    pub(crate) computed: Vec<ComputedShape>,
    /// The filters checked after the computed columns.
    pub(crate) post_filters: Vec<PostFilterShape>,
    /// The computed columns a row delivers after the emitted layers, by index, in order.
    pub(crate) delivered: Vec<usize>,
}

impl RowStageShape {
    /// Checks that the shape fits the joins whose rows it reads, which emit `emitted` layers:
    /// - its programs read only emitted layers and earlier computed columns,
    /// - each program has as many inputs as it reads,
    /// - it delivers only computed columns it has.
    ///
    /// Returns the first mismatch, as a message:
    /// like a malformed shape, it is a bug of the code that built the row stage.
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

/// The shape of a projection of a join's rows, from which a projecting sink is built.
#[derive(Debug)]
pub(crate) struct ProjectionShape {
    /// Per projected column, the emitted layer it reads.
    pub(crate) columns: Vec<usize>,
    /// The length of the projection's leading identity run:
    /// the leading projected columns that read the leading layers, in order.
    ///
    /// Within it, the projected rows keep the order of the drain,
    /// and a change there starts a fresh group of rows to deduplicate.
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

/// One aggregate of an aggregation:
/// its function, the layers it reads, and the layers that tell its rows apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AggregateShape {
    /// The aggregation function.
    pub(crate) kind: AggregateKind,
    /// The layer whose values are aggregated; the keys of a map.
    /// `None` for a count, which reads no value.
    pub(crate) input: Option<usize>,
    /// The layer of a map's values; `None` for every other function.
    pub(crate) pair: Option<usize>,
    /// The layers that, with the input and the pair,
    /// identify a row for this aggregate;
    /// the aggregate reads each identity of a group once.
    pub(crate) per: Vec<usize>,
}

impl AggregateShape {
    /// Returns the layers identifying a row for this aggregate:
    /// the input, the pair, then the `per` layers.
    pub(crate) fn identity_layers(&self) -> impl Iterator<Item = usize> + '_ {
        self.input
            .into_iter()
            .chain(self.pair)
            .chain(self.per.iter().copied())
    }
}

/// The shape of an aggregation over a join's rows, from which an aggregator is built;
/// it borrows its aggregates.
///
/// The leading layers of a row are its group key.
/// A result row holds the group key, followed by one value per aggregate.
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
    /// - exactly the map aggregate reads a layer of values.
    ///
    /// Returns the first invariant violated, as a message:
    /// a malformed shape is a bug of the code that built it, not an error in the data.
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

    /// Checks that the shape fits the joins it aggregates, which emit `emitted` layers:
    /// it groups by no more layers than they emit, and its aggregates read only emitted layers.
    ///
    /// Returns the first mismatch, as a message:
    /// like a malformed shape, it is a bug of the code that built the aggregation.
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
    subtracted: Vec<SubtractedShape>,
    /// The filters.
    filters: Vec<FilterShape>,
    /// The bound layers.
    bound_layers: Vec<BoundLayerShape>,
}

#[cfg(test)]
impl OwnedJoinShape {
    /// Creates the parts of a join's shape, validated.
    ///
    /// Returns the first invariant a malformed shape violates, as a message.
    pub(crate) fn new(
        layers: Vec<Vec<InputColumn>>,
        emitted: usize,
        subtracted: Vec<SubtractedShape>,
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

    #[test]
    fn valid_shapes_are_accepted() {
        // R(a, b) ⋈ S(b, c), emitting all three layers.
        let shape = OwnedJoinShape::new(
            vec![vec![at(0, 0)], vec![at(0, 1), at(1, 0)], vec![at(1, 1)]],
            3,
            vec![SubtractedShape { layers: vec![0, 2] }],
            vec![],
        );
        assert!(shape.is_ok());
    }

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
                vec![SubtractedShape { layers: vec![1, 0] }],
                vec![],
            )
            .is_err()
        );
        // A subtracted input beyond the join's depth.
        assert!(
            OwnedJoinShape::new(
                vec![vec![at(0, 0)]],
                1,
                vec![SubtractedShape { layers: vec![1] }],
                vec![],
            )
            .is_err()
        );
    }

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
        // A program of two inputs given one.
        assert!(
            stage(vec![RowInput::Layer(0)], vec![0])
                .validate_fit(2)
                .is_err()
        );
        // A delivered column that does not exist.
        let beyond = stage(vec![RowInput::Layer(0), RowInput::Layer(1)], vec![1]);
        assert!(beyond.validate_fit(2).is_err());
    }

    #[test]
    fn projections_group_by_their_leading_identity_run() {
        assert_eq!(ProjectionShape::new(vec![0, 1, 3]).group_prefix, 2);
        assert_eq!(ProjectionShape::new(vec![1, 0]).group_prefix, 0);
        assert_eq!(ProjectionShape::new(vec![0, 2, 1]).group_prefix, 1);
        assert_eq!(ProjectionShape::new(vec![]).group_prefix, 0);
    }
}
