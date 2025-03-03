//! This module defines the trait [ColumnType].

use std::fmt::Debug;

use rle::RunLengthEncodable;

pub(crate) mod floor_to_usize;
pub(crate) mod rle;

/// Trait implemented by all types that appear in a
/// [crate::columnar::column::Column].

pub(crate) trait ColumnType: Debug + Copy + Ord + Default + RunLengthEncodable {}
impl<T> ColumnType for T where T: Debug + Copy + Ord + Default + RunLengthEncodable {}
