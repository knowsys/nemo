use nemo_physical::{
    columnar::columnbuilder::{
        adaptive::{
            ColumnBuilderAdaptive, ColumnImplDecisionThreshold, TargetMinLengthForRleElements,
        },
        ColumnBuilder,
    },
    datatypes::{Double, Float, StorageTypeName, StorageValueT},
};

use super::interval::{ColumnWithIntervals, ColumnWithIntervalsT};

/// Enum for ColumnBuilderAdaptive with different underlying datatypes
#[derive(Debug)]
pub enum ColumnBuilderAdaptiveT {
    /// Case u64
    Id32(ColumnBuilderAdaptive<u32>),
    /// Case u64
    Id64(ColumnBuilderAdaptive<u64>),
    /// Case i64
    Int64(ColumnBuilderAdaptive<i64>),
    /// Case Float
    Float(ColumnBuilderAdaptive<Float>),
    /// Case Double
    Double(ColumnBuilderAdaptive<Double>),
}

impl ColumnBuilderAdaptiveT {
    /// Creates a new empty ColumnBuilderAdaptiveT for the given StorageTypeName
    pub fn new(
        dtn: StorageTypeName,
        decision_threshold: ColumnImplDecisionThreshold,
        target_min_length_for_rle_elements: TargetMinLengthForRleElements,
    ) -> Self {
        match dtn {
            StorageTypeName::Id32 => Self::Id32(ColumnBuilderAdaptive::new(
                decision_threshold,
                target_min_length_for_rle_elements,
            )),
            StorageTypeName::Id64 => Self::Id64(ColumnBuilderAdaptive::new(
                decision_threshold,
                target_min_length_for_rle_elements,
            )),
            StorageTypeName::Int64 => Self::Int64(ColumnBuilderAdaptive::new(
                decision_threshold,
                target_min_length_for_rle_elements,
            )),
            StorageTypeName::Float => Self::Float(ColumnBuilderAdaptive::new(
                decision_threshold,
                target_min_length_for_rle_elements,
            )),
            StorageTypeName::Double => Self::Double(ColumnBuilderAdaptive::new(
                decision_threshold,
                target_min_length_for_rle_elements,
            )),
        }
    }

    /// Adds value of arbitrary type to the column builder
    pub fn add(&mut self, value: StorageValueT) {
        match self {
            Self::Id32(cb) => {
                if let StorageValueT::Id32(v) = value {
                    cb.add(v);
                } else {
                    panic!(
                        "value of type {} does not match AdaptiveColumn type Id32",
                        value.get_type()
                    );
                }
            }
            Self::Id64(cb) => {
                if let StorageValueT::Id64(v) = value {
                    cb.add(v);
                } else {
                    panic!(
                        "value of type {} does not match AdaptiveColumn type Id64",
                        value.get_type()
                    );
                }
            }
            Self::Int64(cb) => {
                if let StorageValueT::Int64(v) = value {
                    cb.add(v);
                } else {
                    panic!(
                        "value of type {} does not match AdaptiveColumn type Int64",
                        value.get_type()
                    );
                }
            }
            Self::Float(cb) => {
                if let StorageValueT::Float(v) = value {
                    cb.add(v);
                } else {
                    panic!(
                        "value of type {} does not match AdaptiveColumn type Float",
                        value.get_type()
                    );
                }
            }
            Self::Double(cb) => {
                if let StorageValueT::Double(v) = value {
                    cb.add(v);
                } else {
                    panic!(
                        "value of type {} does not match AdaptiveColumn type Double",
                        value.get_type()
                    );
                }
            }
        }
    }

    /// Return the number of elements that were already added.
    pub fn count(&self) -> usize {
        match self {
            Self::Id32(cb) => cb.count(),
            Self::Id64(cb) => cb.count(),
            Self::Int64(cb) => cb.count(),
            Self::Float(cb) => cb.count(),
            Self::Double(cb) => cb.count(),
        }
    }

    /// Turn the column builder into the finalized Column
    pub fn finalize(self, interval_column: ColumnBuilderAdaptive<usize>) -> ColumnWithIntervalsT {
        match self {
            ColumnBuilderAdaptiveT::Id32(c) => ColumnWithIntervalsT::Id32(
                ColumnWithIntervals::new(c.finalize(), interval_column.finalize()),
            ),
            ColumnBuilderAdaptiveT::Id64(c) => ColumnWithIntervalsT::Id64(
                ColumnWithIntervals::new(c.finalize(), interval_column.finalize()),
            ),
            ColumnBuilderAdaptiveT::Int64(c) => ColumnWithIntervalsT::Int64(
                ColumnWithIntervals::new(c.finalize(), interval_column.finalize()),
            ),
            ColumnBuilderAdaptiveT::Float(c) => ColumnWithIntervalsT::Float(
                ColumnWithIntervals::new(c.finalize(), interval_column.finalize()),
            ),
            ColumnBuilderAdaptiveT::Double(c) => ColumnWithIntervalsT::Double(
                ColumnWithIntervals::new(c.finalize(), interval_column.finalize()),
            ),
        }
    }
}

impl FromIterator<StorageValueT> for ColumnBuilderAdaptiveT {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = StorageValueT>,
    {
        let mut peekable = iter.into_iter().peekable();
        let mut builder = Self::new(
            peekable
                .peek()
                .map(|dv| dv.get_type())
                .unwrap_or(StorageTypeName::Id64),
            Default::default(),
            Default::default(),
        );
        for item in peekable {
            builder.add(item);
        }
        builder
    }
}
